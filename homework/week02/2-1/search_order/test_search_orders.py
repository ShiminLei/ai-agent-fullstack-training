"""4.2 最小调用与 4.3 协议审查的验收测试。"""

import asyncio
import json

from pydantic import ValidationError

from search_orders import (
    SEARCH_ORDERS,
    DemoOrderService,
    SearchOrdersInput,
    ToolCall,
    ToolResult,
    build_demo_calls,
    execute_minimal,
)


def run(coro):
    return asyncio.run(coro)


def expect_validation_error(action) -> None:
    try:
        action()
    except ValidationError:
        return
    raise AssertionError("expected ValidationError")


def test_name_description_and_input_schema_are_defined():
    assert SEARCH_ORDERS.name == "search_orders"
    assert "只读取订单" in SEARCH_ORDERS.description
    schema = SEARCH_ORDERS.input_model.model_json_schema()
    assert set(schema["properties"]) == {"status", "created_from", "limit"}
    assert schema["additionalProperties"] is False


def test_model_tool_does_not_leak_internal_fields():
    public_tool = SEARCH_ORDERS.to_model_tool()
    public_text = json.dumps(public_tool)

    assert set(public_tool["function"]) == {"name", "description", "parameters"}
    assert "handler" not in public_text
    assert "permission" not in public_text
    assert "internal_dependencies" not in public_text


def test_two_calls_have_different_ids():
    first, second = build_demo_calls()
    assert first.name == second.name == "search_orders"
    assert first.id != second.id


def test_results_keep_original_call_ids():
    service = DemoOrderService()
    calls = build_demo_calls()
    results = [run(execute_minimal(call, service)) for call in calls]

    assert results[0].tool_call_id == calls[0].id
    assert results[1].tool_call_id == calls[1].id
    assert json.loads(results[0].content)["orders"][0]["status"] == "pending"
    assert json.loads(results[1].content)["orders"][0]["status"] == "paid"


def test_identity_role_and_approval_are_forbidden_model_arguments():
    for forbidden_field, value in [
        ("user_id", "user_other"),
        ("role", "admin"),
        ("approved", True),
    ]:
        expect_validation_error(lambda field=forbidden_field, item=value: (
            SearchOrdersInput.model_validate({
                "status": "pending",
                field: item,
            })
        ))


def test_arguments_are_validated_before_handler():
    service = DemoOrderService()
    call = ToolCall(
        id="call-invalid",
        name="search_orders",
        arguments_json=json.dumps({
            "status": "pending",
            "user_id": "user_other",
        }),
    )

    expect_validation_error(lambda: run(execute_minimal(call, service)))
    assert service.search_call_count == 0


def test_tool_result_requires_original_call_id_field():
    expect_validation_error(lambda: ToolResult.model_validate({
        "role": "tool",
        "content": "{}",
    }))


def main() -> None:
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
