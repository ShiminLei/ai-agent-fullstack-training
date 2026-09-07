import asyncio
from typing import Any

from app.model import StreamingModel
from app.store import InMemoryRunStore, RunState, TERMINAL_STATUSES


async def _append_cancelled(
    store: InMemoryRunStore,
    state: RunState,
    text_parts: list[str],
) -> None:
    """将用户取消转换成唯一的 run.cancelled 终态事件。"""

    await store.append(
        state.run_id,
        "run.cancelled",
        {
            "reason": "user_requested",
            "partial_text": "".join(text_parts),
        },
    )


async def execute_run(
    store: InMemoryRunStore,
    model: StreamingModel,
    run_id: str,
    messages: list[dict[str, Any]],
) -> None:
    """消费模型事件流，并将它转换为可保存、可重放的 RunEvent。"""

    state = await store.require(run_id)
    text_parts: list[str] = []
    finish_reason: str | None = None
    usage: dict[str, Any] = {}

    try:
        await store.append(run_id, "run.started", {"status": "running"})

        # 如果用户在任务真正开始前就取消，不再调用模型。
        if state.cancel_event.is_set():
            await _append_cancelled(store, state, text_parts)
            return

        async for model_event in model.stream(messages):
            # 取消是协作式的：Agent Loop 在每个模型事件之间主动检查。
            if state.cancel_event.is_set():
                await _append_cancelled(store, state, text_parts)
                return

            if model_event.type == "text.delta":
                delta = model_event.data["delta"]
                text_parts.append(delta)
                await store.append(run_id, "text.delta", {"delta": delta})
            elif model_event.type == "model.finished":
                finish_reason = model_event.data["finish_reason"]
            elif model_event.type == "model.usage":
                usage = model_event.data

        # 模型流可能刚结束时才收到取消请求，因此结束前再检查一次。
        if state.cancel_event.is_set():
            await _append_cancelled(store, state, text_parts)
            return

        await store.append(
            run_id,
            "run.completed",
            {
                "text": "".join(text_parts),
                "finish_reason": finish_reason,
                "usage": usage,
            },
        )
    except asyncio.CancelledError:
        # HTTP 取消接口会取消后台 Task；在退出前仍要留下明确的终态事件。
        if state.status not in TERMINAL_STATUSES:
            await _append_cancelled(store, state, text_parts)
        raise
    except Exception as exc:
        # 若其他代码已经写入终态，就不能再追加第二个终态。
        if state.status not in TERMINAL_STATUSES:
            await store.append(
                run_id,
                "run.failed",
                {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "partial_text": "".join(text_parts),
                },
            )
