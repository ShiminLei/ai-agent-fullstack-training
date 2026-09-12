"""按作业要求依次展示四个步骤。"""

import asyncio
import json

from search_orders import (
    SEARCH_ORDERS,
    DemoOrderService,
    build_demo_calls,
    execute_minimal,
)


async def main() -> None:
    print("1. 名称、描述与 Input Schema")
    print(SEARCH_ORDERS.name)
    print(SEARCH_ORDERS.description)
    print(json.dumps(
        SEARCH_ORDERS.input_model.model_json_schema(),
        ensure_ascii=False,
        indent=2,
    ))

    print("\n2. 模型可见的 Tool JSON")
    public_tool = SEARCH_ORDERS.to_model_tool()
    print(json.dumps(public_tool, ensure_ascii=False, indent=2))
    public_text = json.dumps(public_tool)
    assert "handler" not in public_text
    assert "permission" not in public_text
    assert "internal_dependencies" not in public_text

    print("\n3. 两个不同 tool_call_id 的 Tool Call")
    calls = build_demo_calls()
    for call in calls:
        print(call.model_dump_json(indent=2))
    assert calls[0].id != calls[1].id

    print("\n4. 分别生成 Tool Result，并核对原始 ID")
    service = DemoOrderService()
    for call in calls:
        result = await execute_minimal(call, service)
        assert result.tool_call_id == call.id
        print(result.model_dump_json(indent=2))
        print(f"ID 一致: {result.tool_call_id == call.id}")


if __name__ == "__main__":
    asyncio.run(main())
