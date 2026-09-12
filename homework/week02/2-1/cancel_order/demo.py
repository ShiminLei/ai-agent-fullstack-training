"""第 7 步：顺序观察公开 Schema、拒绝、成功、超时与审计记录。"""

import asyncio
import json

from audit import InMemoryAuditWriter
from cancel_order_tool import CANCEL_ORDER, DemoOrderService
from contracts import ExecutionContext, ToolCall
from runtime import ToolRuntime


def context(
    service,
    *,
    permissions=frozenset({"order:write"}),
    approved=(),
    trace_id="trace-demo",
):
    return ExecutionContext(
        user_id="user_demo",
        tenant_id="tenant_demo",
        permissions=permissions,
        approved_call_ids=frozenset(approved),
        trace_id=trace_id,
        order_service=service,
    )


def call(call_id, *, arguments=None):
    if arguments is None:
        arguments = {
            "order_id": "ord_1001",
            "reason": "用户改变主意",
        }
    return ToolCall(
        id=call_id,
        name="cancel_order",
        arguments_json=json.dumps(arguments, ensure_ascii=False),
    )


async def main():
    audit = InMemoryAuditWriter()
    runtime = ToolRuntime(audit.write)
    runtime.register(CANCEL_ORDER)

    print("\n1. 模型可见的工具（没有身份、审批、Handler）")
    print(json.dumps(
        runtime.model_tools(context(DemoOrderService())),
        ensure_ascii=False,
        indent=2,
    ))

    print("\n2. 三种拒绝：每一种的 Handler 调用次数都必须为 0")
    rejection_cases = [
        (
            "非法参数",
            call("call-invalid", arguments={
                "order_id": "ord_1001",
                "reason": "用户改变主意",
                "approved": True,
            }),
            {"trace_id": "trace-invalid"},
        ),
        (
            "没有权限",
            call("call-denied"),
            {
                "permissions": frozenset(),
                "trace_id": "trace-denied",
            },
        ),
        (
            "没有审批",
            call("call-unapproved"),
            {"trace_id": "trace-unapproved"},
        ),
    ]
    for label, rejected_call, context_options in rejection_cases:
        rejected_service = DemoOrderService()
        rejected = await runtime.execute(
            rejected_call,
            context(rejected_service, **context_options),
        )
        error = json.loads(rejected.content)["error"]
        print(
            f"- {label}: {error['code']}; "
            f"Handler 调用次数={rejected_service.cancel_call_count}"
        )

    print("\n3. 已确认：Runtime 执行，并返回原 tool_call_id")
    success_service = DemoOrderService()
    success_call = call("call-success")
    success = await runtime.execute(
        success_call,
        context(
            success_service,
            approved={success_call.id},
            trace_id="trace-success",
        ),
    )
    print(success.model_dump_json(indent=2))

    print("\n4. 超时：非幂等取消操作只尝试 1 次")
    slow_service = DemoOrderService(delay_seconds=0.1)
    timeout_call = call("call-timeout")
    timeout = await runtime.execute(
        timeout_call,
        context(
            slow_service,
            approved={timeout_call.id},
            trace_id="trace-timeout",
        ),
    )
    print(timeout.content)
    print("Handler 调用次数:", slow_service.cancel_call_count)

    print("\n5. 所有结果都可以通过两个 ID 定位")
    print(json.dumps(audit.events, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
