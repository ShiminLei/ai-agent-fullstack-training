"""第 2 步：分开模型可见协议与 Runtime 可信上下文。"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


Permission = Literal["order:read", "order:write"]
RiskLevel = Literal["low", "medium", "high"]
ToolHandler = Callable[[BaseModel, "ExecutionContext"], Awaitable[BaseModel]]


@dataclass(frozen=True)
class ExecutionContext:
    """由登录态/Harness 注入；模型不能填写或修改。"""

    user_id: str
    tenant_id: str
    permissions: frozenset[str]
    approved_call_ids: frozenset[str] = field(default_factory=frozenset)
    trace_id: str = ""
    order_service: object | None = None


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    error_model: type[BaseModel]
    permission: Permission
    risk: RiskLevel
    timeout_seconds: float
    max_retries: int
    idempotent: bool
    handler: ToolHandler

    def to_model_tool(self) -> dict:
        """只投影模型选择工具需要的信息，不暴露治理信息和 Handler。"""

        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_model.model_json_schema(),
            },
        }


class ToolCall(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=100)
    arguments_json: str


class ToolResultMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    role: Literal["tool"] = "tool"
    tool_call_id: str
    name: str
    content: str
    is_error: bool = False

    def to_model_message(self) -> dict[str, str]:
        return {
            "role": self.role,
            "tool_call_id": self.tool_call_id,
            "content": self.content,
        }
