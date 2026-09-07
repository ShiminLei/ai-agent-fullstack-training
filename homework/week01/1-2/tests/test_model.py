from typing import Any

import pytest

from app.model import ModelStreamEvent, StreamingModel
from tests.fakes import FakeStreamingModel


async def collect_events(
    model: StreamingModel,
    messages: list[dict[str, Any]],
) -> list[ModelStreamEvent]:
    return [event async for event in model.stream(messages)]


@pytest.mark.asyncio
async def test_fake_model_emits_events_in_order() -> None:
    model = FakeStreamingModel(["你", "好"])

    events = await collect_events(
        model,
        [{"role": "user", "content": "打个招呼"}],
    )

    assert [event.type for event in events] == [
        "text.delta",
        "text.delta",
        "model.finished",
        "model.usage",
    ]
    assert [event.data["delta"] for event in events[:2]] == ["你", "好"]


@pytest.mark.asyncio
async def test_fake_model_reports_finish_reason_and_usage() -> None:
    model = FakeStreamingModel(["完成"])

    events = await collect_events(
        model,
        [{"role": "user", "content": "执行任务"}],
    )

    assert events[-2].data == {"finish_reason": "stop"}
    assert events[-1].data == {
        "input_tokens": 10,
        "output_tokens": 1,
        "total_tokens": 11,
    }


@pytest.mark.asyncio
async def test_fake_model_records_received_messages() -> None:
    messages = [{"role": "user", "content": "你好"}]
    model = FakeStreamingModel([])

    await collect_events(model, messages)

    assert model.received_messages == [messages]
