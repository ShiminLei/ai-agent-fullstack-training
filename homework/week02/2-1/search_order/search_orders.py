"""search_orders 最小 Function Calling 协议闭环。"""

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SearchOrdersInput(StrictModel):
    """模型可以填写的查询条件，不包含身份、角色或审批。"""

    status: Literal["pending", "paid", "completed", "cancelled"] | None = None
    created_from: date | None = None
    limit: int = Field(default=10, ge=1, le=50)

    @model_validator(mode="after")
    def require_filter(self) -> "SearchOrdersInput":
        if self.status is None and self.created_from is None:
            raise ValueError("status 与 created_from 至少提供一个")
        return self


class OrderSummary(StrictModel):
    order_id: str
    status: Literal["pending", "paid", "completed", "cancelled"]
    created_at: str
    amount_cents: int = Field(ge=0)


class SearchOrdersOutput(StrictModel):
    orders: list[OrderSummary]
    total: int = Field(ge=0)


class ToolCall(StrictModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    arguments_json: str


class ToolResult(StrictModel):
    role: Literal["tool"] = "tool"
    tool_call_id: str = Field(min_length=1)
    content: str


SearchHandler = Callable[[SearchOrdersInput, "DemoOrderService"], Awaitable[SearchOrdersOutput]]


@dataclass(frozen=True)
class ToolDefinition:
    # 模型可见的协议信息
    name: str
    description: str
    input_model: type[BaseModel]

    # 仅应用内部使用的信息
    output_model: type[BaseModel]
    permission: str
    internal_dependencies: tuple[str, ...]
    handler: SearchHandler

    def to_model_tool(self) -> dict:
        """只投影模型选择和构造参数所必需的信息。"""

        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_model.model_json_schema(),
            },
        }


class DemoOrderService:
    """练习用模拟数据；调用次数用于证明参数先校验、后执行。"""

    def __init__(self) -> None:
        self.search_call_count = 0
        self.records = [
            {
                "order_id": "ord_1001",
                "status": "pending",
                "created_at": "2026-09-10T09:30:00+08:00",
                "amount_cents": 2990,
            },
            {
                "order_id": "ord_1002",
                "status": "paid",
                "created_at": "2026-09-11T14:10:00+08:00",
                "amount_cents": 1990,
            },
            {
                "order_id": "ord_1003",
                "status": "completed",
                "created_at": "2026-09-01T08:00:00+08:00",
                "amount_cents": 3590,
            },
        ]

    async def search(self, args: SearchOrdersInput) -> list[dict]:
        self.search_call_count += 1
        rows = [
            row
            for row in self.records
            if (args.status is None or row["status"] == args.status)
            and (
                args.created_from is None
                or date.fromisoformat(row["created_at"][:10]) >= args.created_from
            )
        ]
        return rows[: args.limit]


async def search_orders(
    args: SearchOrdersInput,
    service: DemoOrderService,
) -> SearchOrdersOutput:
    rows = await service.search(args)
    return SearchOrdersOutput(
        orders=[OrderSummary.model_validate(row) for row in rows],
        total=len(rows),
    )


SEARCH_ORDERS = ToolDefinition(
    name="search_orders",
    description=(
        "按订单状态或创建日期查询模拟订单，最多返回 50 条。"
        "该工具只读取订单，不会取消、退款或修改订单。"
    ),
    input_model=SearchOrdersInput,
    output_model=SearchOrdersOutput,
    permission="order:read",
    internal_dependencies=("demo_order_service",),
    handler=search_orders,
)


async def execute_minimal(
    call: ToolCall,
    service: DemoOrderService,
) -> ToolResult:
    """最小执行：名称匹配 -> 参数校验 -> Handler -> 输出校验 -> Result。"""

    if call.name != SEARCH_ORDERS.name:
        raise ValueError(f"unknown tool: {call.name}")

    args = SearchOrdersInput.model_validate_json(call.arguments_json)
    raw_output = await SEARCH_ORDERS.handler(args, service)
    output = SearchOrdersOutput.model_validate(raw_output)

    return ToolResult(
        tool_call_id=call.id,
        content=output.model_dump_json(),
    )


def build_demo_calls() -> tuple[ToolCall, ToolCall]:
    """构造两个同名、不同 ID 的 Tool Call。"""

    return (
        ToolCall(
            id="call-pending",
            name="search_orders",
            arguments_json=json.dumps({"status": "pending", "limit": 10}),
        ),
        ToolCall(
            id="call-paid",
            name="search_orders",
            arguments_json=json.dumps({"status": "paid", "limit": 10}),
        ),
    )
