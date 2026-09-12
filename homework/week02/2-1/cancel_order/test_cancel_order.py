"""第 6 步：用验收测试证明安全边界真实生效。"""

import asyncio
import json

from audit import InMemoryAuditWriter
from cancel_order_tool import CANCEL_ORDER, DemoOrderService
from contracts import ExecutionContext, ToolCall
from runtime import ToolRuntime


def run(coro):
    return asyncio.run(coro)


def make_runtime():
    audit = InMemoryAuditWriter()
    runtime = ToolRuntime(audit.write)
    runtime.register(CANCEL_ORDER)
    return runtime, audit


def make_context(
    service,
    *,
    permissions=frozenset({"order:write"}),
    approved_call_ids=frozenset(),
    trace_id="trace-test",
):
    return ExecutionContext(
        user_id="user_demo",
        tenant_id="tenant_demo",
        permissions=permissions,
        approved_call_ids=approved_call_ids,
        trace_id=trace_id,
        order_service=service,
    )


def make_call(call_id="call-1", arguments=None):
    if arguments is None:
        arguments = {
            "order_id": "ord_1001",
            "reason": "不再需要",
        }
    return ToolCall(
        id=call_id,
        name="cancel_order",
        arguments_json=json.dumps(arguments, ensure_ascii=False),
    )


def error_code(result):
    return json.loads(result.content)["error"]["code"]


def test_model_only_sees_public_input_schema():
    runtime, _ = make_runtime()
    ctx = make_context(DemoOrderService())

    public_tool = runtime.model_tools(ctx)[0]
    function = public_tool["function"]

    assert set(function["parameters"]["properties"]) == {"order_id", "reason"}
    assert function["parameters"]["additionalProperties"] is False
    assert "handler" not in function
    assert "permission" not in function
    assert "risk" not in function


def test_illegal_extra_argument_never_reaches_handler():
    runtime, audit = make_runtime()
    service = DemoOrderService()
    call = make_call(arguments={
        "order_id": "ord_1001",
        "reason": "不再需要",
        "user_id": "user_other",
    })

    result = run(runtime.execute(call, make_context(service)))

    assert error_code(result) == "INVALID_ARGUMENT"
    assert service.cancel_call_count == 0
    assert audit.events[0]["handler_invoked"] is False


def test_missing_permission_never_reaches_handler():
    runtime, audit = make_runtime()
    service = DemoOrderService()

    result = run(runtime.execute(
        make_call(),
        make_context(service, permissions=frozenset()),
    ))

    assert error_code(result) == "PERMISSION_DENIED"
    assert service.cancel_call_count == 0
    assert audit.events[0]["stage"] == "authorization"


def test_missing_approval_never_reaches_handler():
    runtime, audit = make_runtime()
    service = DemoOrderService()

    result = run(runtime.execute(make_call(), make_context(service)))

    assert error_code(result) == "APPROVAL_REQUIRED"
    assert service.cancel_call_count == 0
    assert audit.events[0]["stage"] == "approval"


def test_approved_call_succeeds_and_keeps_both_ids():
    runtime, audit = make_runtime()
    service = DemoOrderService()
    call = make_call(call_id="call-success")
    ctx = make_context(
        service,
        approved_call_ids=frozenset({call.id}),
        trace_id="trace-success",
    )

    result = run(runtime.execute(call, ctx))
    output = json.loads(result.content)

    assert result.is_error is False
    assert result.tool_call_id == call.id
    assert output == {
        "order_id": "ord_1001",
        "status": "cancelled",
        "operation_id": "cancel-0001",
    }
    assert service.cancel_call_count == 1
    assert audit.find(
        trace_id="trace-success",
        tool_call_id="call-success",
    )[0]["outcome"] == "success"


def test_timeout_is_not_retried_and_is_traceable():
    runtime, audit = make_runtime()
    service = DemoOrderService(delay_seconds=0.1)
    call = make_call(call_id="call-timeout")
    ctx = make_context(
        service,
        approved_call_ids=frozenset({call.id}),
        trace_id="trace-timeout",
    )

    result = run(runtime.execute(call, ctx))
    error = json.loads(result.content)["error"]

    assert error["code"] == "TIMEOUT"
    assert error["retryable"] is False
    assert service.cancel_call_count == 1
    event = audit.find(
        trace_id="trace-timeout",
        tool_call_id="call-timeout",
    )[0]
    assert event["outcome"] == "failed"
    assert event["attempt"] == 1


def test_every_result_preserves_original_tool_call_id():
    cases = [
        ("call-invalid", {"order_id": "ord_1001", "extra": True}),
        ("call-valid", {"order_id": "ord_1001", "reason": "不再需要"}),
    ]

    for call_id, arguments in cases:
        runtime, _ = make_runtime()
        result = run(runtime.execute(
            make_call(call_id=call_id, arguments=arguments),
            make_context(DemoOrderService()),
        ))
        assert result.tool_call_id == call_id


def main():
    """不依赖第三方测试框架，逐个执行本文件中的测试。"""

    tests = [
        value
        for name, value in globals().items()
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"\n{len(tests)} tests passed")


if __name__ == "__main__":
    main()
