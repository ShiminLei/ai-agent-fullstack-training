import json
from collections.abc import AsyncIterator

from app.events import RunEvent
from app.store import InMemoryRunStore, TERMINAL_STATUSES


def encode_sse(event: RunEvent) -> bytes:
    payload = event.model_dump(mode="json")

    lines = [
        f"id: {event.seq}",
        f"event: {event.type}",
        "data: " + json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "",
        "",
    ]

    return "\n".join(lines).encode("utf-8")


def encode_heartbeat() -> bytes:
    return b": heartbeat\n\n"


async def stream_run_events(
    store: InMemoryRunStore,
    run_id: str,
    after_seq: int = -1,
) -> AsyncIterator[bytes]:
    """先重放历史事件，再等待新事件，遇到 Run 终态后关闭流。"""

    last_seq = after_seq

    while True:
        events = await store.list_after(run_id, last_seq)

        if not events:
            state = await store.require(run_id)
            if state.status in TERMINAL_STATUSES:
                return
            events = await store.wait_for_events(run_id, last_seq)

        for event in events:
            yield encode_sse(event)
            last_seq = event.seq

            if event.is_terminal():
                return
