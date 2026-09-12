import asyncio
import hashlib
import json
import random
import time
from datetime import datetime, timezone
from typing import Any

from openai import APIConnectionError, APITimeoutError, InternalServerError, RateLimitError

from app.model import StreamingModel
from app.store import InMemoryRunStore, RunState, TERMINAL_STATUSES


RETRYABLE_EXCEPTIONS = (
    APIConnectionError,
    APITimeoutError,
    RateLimitError,
    InternalServerError,
    ConnectionError,
    TimeoutError,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _context_digest(messages: list[dict[str, Any]]) -> str:
    """Checkpoint 只保存输入摘要，不复制完整 Prompt。"""

    payload = json.dumps(messages, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _total_duration_ms(state: RunState, now: datetime) -> float | None:
    """根据 Trace 中的开始时间计算终态耗时；开始时间缺失时安全返回 None。"""

    started_at = state.trace.get("started_at")
    if not started_at:
        return None
    started = datetime.fromisoformat(str(started_at))
    return round((now - started).total_seconds() * 1000, 3)


async def _save_checkpoint(
    store: InMemoryRunStore,
    state: RunState,
    *,
    loop_step: str,
    completed: list[str],
    text_parts: list[str],
    usage: dict[str, Any],
) -> None:
    await store.save_checkpoint(
        state.run_id,
        loop_step=loop_step,
        completed=completed,
        next_cursor=None,
        context_digest=str(state.trace.get("context_digest", "")),
        tool_state=[],
        partial_text="".join(text_parts),
        usage=usage,
    )


async def _append_cancelled(
    store: InMemoryRunStore,
    state: RunState,
    text_parts: list[str],
    usage: dict[str, Any],
) -> None:
    """模型或后台 Task 真正停止后，才写入 run.cancelled。"""

    now = _utc_now()
    cancel_requested_at = state.trace.get("cancel_requested_at")
    cancel_latency_ms: float | None = None
    if cancel_requested_at:
        requested = datetime.fromisoformat(str(cancel_requested_at))
        cancel_latency_ms = round((now - requested).total_seconds() * 1000, 3)

    await store.append(
        state.run_id,
        "run.cancelled",
        {
            "reason": "user_requested",
            "partial_text": "".join(text_parts),
            "last_seq": state.next_seq - 1,
            "usage": usage,
            "cancelled_at": now.isoformat(),
        },
    )
    await store.update_trace(
        state.run_id,
        cancel_latency_ms=cancel_latency_ms,
        total_duration_ms=_total_duration_ms(state, now),
        token_usage=usage,
        completed_at=now.isoformat(),
        final_status="cancelled",
    )
    await _save_checkpoint(
        store,
        state,
        loop_step="cancelled",
        completed=[],
        text_parts=text_parts,
        usage=usage,
    )


async def _append_failed(
    store: InMemoryRunStore,
    state: RunState,
    text_parts: list[str],
    usage: dict[str, Any],
    *,
    code: str,
    stage: str,
    retryable: bool,
    hint: str,
) -> None:
    """对客户端只发布稳定失败协议，不暴露原始异常内容。"""

    now = _utc_now()
    await store.append(
        state.run_id,
        "run.failed",
        {
            "code": code,
            "stage": stage,
            "retryable": retryable,
            "hint": hint,
            "partial_text": "".join(text_parts),
        },
    )
    await store.update_trace(
        state.run_id,
        total_duration_ms=_total_duration_ms(state, now),
        token_usage=usage,
        completed_at=now.isoformat(),
        final_status="failed",
        error_code=code,
    )
    await _save_checkpoint(
        store,
        state,
        loop_step="failed",
        completed=[],
        text_parts=text_parts,
        usage=usage,
    )


async def execute_run(
    store: InMemoryRunStore,
    model: StreamingModel,
    run_id: str,
    messages: list[dict[str, Any]],
    *,
    max_attempts: int = 2,
    retry_base_delay: float = 0.05,
) -> None:
    """驱动模型、产生业务事件，并记录终态、Trace 和 Checkpoint。"""

    state = await store.require(run_id)
    text_parts: list[str] = []
    finish_reason: str | None = None
    usage: dict[str, Any] = {}
    emitted_business_event = False
    started_clock = time.perf_counter()

    await store.update_trace(
        run_id,
        started_at=_utc_now().isoformat(),
        model_version=getattr(model, "model", type(model).__name__),
        context_digest=_context_digest(messages),
    )

    try:
        # 创建后、任务获得执行机会前也可能已经收到取消请求。
        if state.cancel_event.is_set():
            await _append_cancelled(store, state, text_parts, usage)
            return

        await store.append(run_id, "run.started", {"status": "running"})
        await _save_checkpoint(
            store,
            state,
            loop_step="model.streaming",
            completed=[],
            text_parts=text_parts,
            usage=usage,
        )

        attempt = 1
        while True:
            try:
                async for model_event in model.stream(messages):
                    if state.cancel_event.is_set():
                        await _append_cancelled(store, state, text_parts, usage)
                        return

                    if model_event.type == "text.delta":
                        delta = model_event.data["delta"]
                        # 空 delta 没有业务内容，不应占用 Run seq。
                        if not delta:
                            continue
                        if not emitted_business_event:
                            await store.update_trace(
                                run_id,
                                first_delta_at=_utc_now().isoformat(),
                                ttft_ms=round(
                                    (time.perf_counter() - started_clock) * 1000,
                                    3,
                                ),
                            )
                        emitted_business_event = True
                        text_parts.append(delta)
                        await store.append(run_id, "text.delta", {"delta": delta})
                    elif model_event.type == "model.finished":
                        finish_reason = model_event.data["finish_reason"]
                    elif model_event.type == "model.usage":
                        usage = dict(model_event.data)
                break
            except RETRYABLE_EXCEPTIONS as exc:
                can_retry = not emitted_business_event and attempt < max_attempts
                if not can_retry:
                    raise exc

                attempt += 1
                await store.increment_trace_counter(run_id, "retry_count")
                delay = retry_base_delay * (2 ** (attempt - 2))
                delay += random.uniform(0, retry_base_delay)
                await store.append(
                    run_id,
                    "run.retrying",
                    {
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "delay_ms": round(delay * 1000, 3),
                        "code": "MODEL_CONNECTION_FAILED",
                    },
                )
                await asyncio.sleep(delay)

        if state.cancel_event.is_set():
            await _append_cancelled(store, state, text_parts, usage)
            return

        if finish_reason == "length":
            await _append_failed(
                store,
                state,
                text_parts,
                usage,
                code="OUTPUT_TRUNCATED",
                stage="model.finished",
                retryable=False,
                hint="增加输出上限或缩小任务范围后创建新的 Run",
            )
            return
        if finish_reason == "content_filter":
            await _append_failed(
                store,
                state,
                text_parts,
                usage,
                code="CONTENT_FILTERED",
                stage="model.finished",
                retryable=False,
                hint="调整请求内容后创建新的 Run",
            )
            return

        # 正常文本回答只接受 stop。缺失或未知原因不能伪装成成功；
        # tool_calls 也必须交给真正的 Tool Runtime，当前作业不静默吞掉它。
        if finish_reason != "stop":
            await _append_failed(
                store,
                state,
                text_parts,
                usage,
                code=(
                    "TOOL_CALLS_UNSUPPORTED"
                    if finish_reason == "tool_calls"
                    else "MODEL_FINISH_REASON_INVALID"
                ),
                stage="model.finished",
                retryable=False,
                hint="检查模型结束原因或接入 Tool Runtime 后创建新的 Run",
            )
            return

        completed_at = _utc_now()
        await store.append(
            run_id,
            "run.completed",
            {
                "text": "".join(text_parts),
                "finish_reason": finish_reason,
                "usage": usage,
            },
        )
        await store.update_trace(
            run_id,
            total_duration_ms=round(
                (time.perf_counter() - started_clock) * 1000,
                3,
            ),
            token_usage=usage,
            completed_at=completed_at.isoformat(),
            final_status="completed",
        )
        await _save_checkpoint(
            store,
            state,
            loop_step="completed",
            completed=["model.streaming"],
            text_parts=text_parts,
            usage=usage,
        )
    except asyncio.CancelledError:
        # CancelledError 是控制信号，保存取消事实后必须继续向上传播。
        if state.status not in TERMINAL_STATUSES:
            await _append_cancelled(store, state, text_parts, usage)
        raise
    except Exception as exc:
        if state.cancel_event.is_set() and state.status not in TERMINAL_STATUSES:
            await _append_cancelled(store, state, text_parts, usage)
            return
        if state.status not in TERMINAL_STATUSES:
            retryable = isinstance(exc, RETRYABLE_EXCEPTIONS)
            await _append_failed(
                store,
                state,
                text_parts,
                usage,
                code=(
                    "MODEL_STREAM_FAILED"
                    if retryable
                    else "MODEL_PROTOCOL_FAILED"
                ),
                stage="model.streaming",
                retryable=retryable and not emitted_business_event,
                hint=(
                    "可以创建新的 Run 重试"
                    if not emitted_business_event
                    else "已有业务事件，禁止自动拼接重跑；请重放现有事件"
                ),
            )
            await store.update_trace(run_id, internal_error_type=type(exc).__name__)
