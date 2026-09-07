from collections.abc import AsyncIterator
from typing import Any

import pytest

from app.model import ModelStreamEvent
from app.runner import execute_run
from app.store import InMemoryRunStore
from tests.fakes import FakeStreamingModel


class FailingModel:
    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        raise RuntimeError("provider unavailable")
        yield


class MustNotBeCalledModel:
    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        raise AssertionError("cancelled run must not call the model")
        yield


class CancelAfterFirstDeltaModel:
    def __init__(self, store: InMemoryRunStore, run_id: str) -> None:
        self.store = store
        self.run_id = run_id

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        yield ModelStreamEvent(type="text.delta", data={"delta": "你"})
        await self.store.request_cancel(self.run_id)
        yield ModelStreamEvent(type="text.delta", data={"delta": "好"})


@pytest.mark.asyncio
async def test_execute_run_emits_events_in_order() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    model = FakeStreamingModel(["你", "好"])

    await execute_run(
        store,
        model,
        "run_001",
        [{"role": "user", "content": "打个招呼"}],
    )

    state = await store.require("run_001")
    assert [event.type for event in state.events] == [
        "run.started",
        "text.delta",
        "text.delta",
        "run.completed",
    ]
    assert [event.seq for event in state.events] == [0, 1, 2, 3]
    assert state.status == "completed"


@pytest.mark.asyncio
async def test_execute_run_collects_final_result() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")

    await execute_run(
        store,
        FakeStreamingModel(["任务", "完成"]),
        "run_001",
        [{"role": "user", "content": "执行任务"}],
    )

    state = await store.require("run_001")
    assert state.events[-1].data == {
        "text": "任务完成",
        "finish_reason": "stop",
        "usage": {
            "input_tokens": 10,
            "output_tokens": 2,
            "total_tokens": 12,
        },
    }


@pytest.mark.asyncio
async def test_execute_run_converts_model_error_to_failed_event() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")

    await execute_run(
        store,
        FailingModel(),
        "run_001",
        [{"role": "user", "content": "执行任务"}],
    )

    state = await store.require("run_001")
    assert [event.type for event in state.events] == [
        "run.started",
        "run.failed",
    ]
    assert state.events[-1].data == {
        "error_type": "RuntimeError",
        "message": "provider unavailable",
        "partial_text": "",
    }
    assert state.status == "failed"


@pytest.mark.asyncio
async def test_execute_run_honours_cancel_before_model_call() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    await store.request_cancel("run_001")

    await execute_run(
        store,
        MustNotBeCalledModel(),
        "run_001",
        [{"role": "user", "content": "执行任务"}],
    )

    state = await store.require("run_001")
    assert [event.type for event in state.events] == [
        "run.started",
        "run.cancelled",
    ]
    assert state.events[-1].data == {
        "reason": "user_requested",
        "partial_text": "",
    }
    assert state.status == "cancelled"


@pytest.mark.asyncio
async def test_execute_run_stops_after_cancel_during_stream() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    model = CancelAfterFirstDeltaModel(store, "run_001")

    await execute_run(
        store,
        model,
        "run_001",
        [{"role": "user", "content": "打个招呼"}],
    )

    state = await store.require("run_001")
    assert [event.type for event in state.events] == [
        "run.started",
        "text.delta",
        "run.cancelled",
    ]
    assert state.events[-1].data == {
        "reason": "user_requested",
        "partial_text": "你",
    }
