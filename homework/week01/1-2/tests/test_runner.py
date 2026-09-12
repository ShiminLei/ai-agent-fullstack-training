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


class FinishReasonModel:
    def __init__(self, finish_reason: str) -> None:
        self.finish_reason = finish_reason

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        yield ModelStreamEvent(type="text.delta", data={"delta": "部分"})
        yield ModelStreamEvent(
            type="model.finished",
            data={"finish_reason": self.finish_reason},
        )


class RetryOnceModel:
    def __init__(self) -> None:
        self.calls = 0

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("temporary network failure")
        yield ModelStreamEvent(type="text.delta", data={"delta": "成功"})
        yield ModelStreamEvent(
            type="model.finished",
            data={"finish_reason": "stop"},
        )


class FailAfterDeltaModel:
    """模拟供应商已输出正文后才断线；此时自动重跑会导致正文重复。"""

    def __init__(self) -> None:
        self.calls = 0

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        self.calls += 1
        yield ModelStreamEvent(type="text.delta", data={"delta": "已经输出"})
        raise ConnectionError("connection lost after first delta")


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
        "code": "MODEL_PROTOCOL_FAILED",
        "stage": "model.streaming",
        "retryable": False,
        "hint": "可以创建新的 Run 重试",
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
    assert [event.type for event in state.events] == ["run.cancelled"]
    assert state.events[-1].data["reason"] == "user_requested"
    assert state.events[-1].data["partial_text"] == ""
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
    assert state.events[-1].data["reason"] == "user_requested"
    assert state.events[-1].data["partial_text"] == "你"
    assert state.events[-1].data["last_seq"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("finish_reason", "expected_code"),
    [("length", "OUTPUT_TRUNCATED"), ("content_filter", "CONTENT_FILTERED")],
)
async def test_non_success_finish_reason_becomes_failed_event(
    finish_reason: str,
    expected_code: str,
) -> None:
    store = InMemoryRunStore()
    await store.create("run_001")

    await execute_run(
        store,
        FinishReasonModel(finish_reason),
        "run_001",
        [{"role": "user", "content": "执行任务"}],
    )

    state = await store.require("run_001")
    assert state.status == "failed"
    assert state.events[-1].type == "run.failed"
    assert state.events[-1].data["code"] == expected_code


@pytest.mark.asyncio
async def test_retryable_failure_retries_only_before_business_event() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    model = RetryOnceModel()

    await execute_run(
        store,
        model,
        "run_001",
        [{"role": "user", "content": "执行任务"}],
        retry_base_delay=0,
    )

    state = await store.require("run_001")
    assert model.calls == 2
    assert [event.type for event in state.events] == [
        "run.started",
        "run.retrying",
        "text.delta",
        "run.completed",
    ]
    assert state.trace["retry_count"] == 1
    assert state.trace["ttft_ms"] is not None
    assert state.checkpoint is not None


@pytest.mark.asyncio
async def test_failure_after_delta_is_not_blindly_retried() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    model = FailAfterDeltaModel()

    await execute_run(
        store,
        model,
        "run_001",
        [{"role": "user", "content": "执行任务"}],
        max_attempts=3,
        retry_base_delay=0,
    )

    state = await store.require("run_001")
    assert model.calls == 1
    assert [event.type for event in state.events] == [
        "run.started",
        "text.delta",
        "run.failed",
    ]
    assert state.events[-1].data["retryable"] is False
    assert "禁止自动拼接重跑" in state.events[-1].data["hint"]
    assert state.trace["total_duration_ms"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("finish_reason", "expected_code"),
    [("tool_calls", "TOOL_CALLS_UNSUPPORTED"), ("unknown", "MODEL_FINISH_REASON_INVALID")],
)
async def test_unsupported_finish_reason_never_becomes_completed(
    finish_reason: str,
    expected_code: str,
) -> None:
    store = InMemoryRunStore()
    await store.create("run_001")

    await execute_run(
        store,
        FinishReasonModel(finish_reason),
        "run_001",
        [{"role": "user", "content": "执行任务"}],
    )

    state = await store.require("run_001")
    assert state.status == "failed"
    assert state.events[-1].data["code"] == expected_code
