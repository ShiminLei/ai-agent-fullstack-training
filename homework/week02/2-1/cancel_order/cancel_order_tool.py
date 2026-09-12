"""第 4 步：实现真实业务 Handler，并装配 cancel_order 工具。"""

import asyncio

from contracts import ExecutionContext, ToolDefinition
from schemas import CancelOrderInput, CancelOrderOutput, ToolError


async def cancel_order(
    args: CancelOrderInput,
    ctx: ExecutionContext,
) -> CancelOrderOutput:
    """只做业务操作；是否允许执行由 Runtime 在调用前决定。"""

    if ctx.order_service is None:
        raise RuntimeError("order_service 未配置")

    result = await ctx.order_service.cancel(
        # user_id 只取自可信上下文，不能取自模型参数。
        user_id=ctx.user_id,
        order_id=args.order_id,
        reason=args.reason,
    )
    return CancelOrderOutput.model_validate(result)


CANCEL_ORDER = ToolDefinition(
    name="cancel_order",
    description=(
        "取消当前用户的一笔待处理订单，需要用户明确确认。"
        "不能查询、退款或取消其他用户的订单。"
    ),
    input_model=CancelOrderInput,
    output_model=CancelOrderOutput,
    error_model=ToolError,
    permission="order:write",
    risk="high",
    timeout_seconds=0.05,
    # 取消是写操作，超时并不表示上游没有成功，因此禁止自动重试。
    max_retries=0,
    idempotent=False,
    handler=cancel_order,
)


class DemoOrderService:
    """可观察调用次数的内存服务，便于证明拒绝时没有副作用。"""

    def __init__(self, *, delay_seconds: float = 0) -> None:
        self.delay_seconds = delay_seconds
        self.cancel_call_count = 0
        self.orders = {
            "ord_1001": {"user_id": "user_demo", "status": "pending"},
            "ord_2001": {"user_id": "user_other", "status": "pending"},
        }

    async def cancel(
        self,
        *,
        user_id: str,
        order_id: str,
        reason: str,
    ) -> dict:
        self.cancel_call_count += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)

        order = self.orders.get(order_id)
        if order is None or order["user_id"] != user_id:
            raise ValueError("订单不存在或不属于当前用户")
        if order["status"] != "pending":
            raise ValueError("订单当前状态不允许取消")

        order["status"] = "cancelled"
        return {
            "order_id": order_id,
            "status": "cancelled",
            "operation_id": f"cancel-{self.cancel_call_count:04d}",
        }
