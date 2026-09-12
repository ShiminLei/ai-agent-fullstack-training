import asyncio
import codecs
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

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


@dataclass(frozen=True, slots=True)
class DecodedSSE:
    """客户端从 SSE 字节流中恢复出的一条完整协议帧。"""

    event: str
    data: str
    event_id: str | None = None
    retry: int | None = None


class SSEDecoder:
    """增量解析任意拆分或粘连的 UTF-8 SSE 字节块。"""

    def __init__(self) -> None:
        # 增量 UTF-8 decoder 能保留被切成两半的中文字符字节。
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._buffer = ""
        self._frame_lines: list[str] = []

    def feed_bytes(self, data: bytes) -> list[DecodedSSE]:
        self._buffer += self._decoder.decode(data, final=False)
        events: list[DecodedSSE] = []

        # 按行消费才能同时正确处理 LF、CRLF 以及分割在两次 read 的 CRLF。
        while True:
            newline_positions = [
                position
                for marker in ("\n", "\r")
                if (position := self._buffer.find(marker)) >= 0
            ]
            if not newline_positions:
                break
            position = min(newline_positions)
            marker = self._buffer[position]
            # 如果 byte chunk 恰好以 CR 结束，先等待下一块确认是否为 CRLF。
            if marker == "\r" and position == len(self._buffer) - 1:
                break
            line = self._buffer[:position]
            consumed = 2 if marker == "\r" and self._buffer[position + 1] == "\n" else 1
            self._buffer = self._buffer[position + consumed:]

            if line:
                self._frame_lines.append(line)
                continue

            decoded = self._decode_frame("\n".join(self._frame_lines))
            self._frame_lines.clear()
            if decoded is not None:
                events.append(decoded)
        return events

    @staticmethod
    def _decode_frame(frame: str) -> DecodedSSE | None:
        event_type = "message"
        event_id: str | None = None
        retry: int | None = None
        data_lines: list[str] = []

        for line in frame.splitlines():
            # 心跳属于 SSE 注释，不产生业务事件，也不占用 seq。
            if not line or line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if separator and value.startswith(" "):
                value = value[1:]
            if field == "event":
                event_type = value
            elif field == "id":
                event_id = value
            elif field == "data":
                data_lines.append(value)
            elif field == "retry" and value.isdigit():
                retry = int(value)

        if not data_lines:
            return None
        return DecodedSSE(
            event=event_type,
            data="\n".join(data_lines),
            event_id=event_id,
            retry=retry,
        )


async def stream_run_events(
    store: InMemoryRunStore,
    run_id: str,
    after_seq: int = -1,
    heartbeat_interval: float = 15.0,
) -> AsyncIterator[bytes]:
    """先重放历史事件，再等待新事件，遇到 Run 终态后关闭流。"""

    last_seq = after_seq

    while True:
        events = await store.list_after(run_id, last_seq)

        if not events:
            state = await store.require(run_id)
            if state.status in TERMINAL_STATUSES:
                return
            try:
                events = await asyncio.wait_for(
                    store.wait_for_events(run_id, last_seq),
                    timeout=heartbeat_interval,
                )
            except TimeoutError:
                # 心跳只维持 HTTP 连接，不属于 RunEvent。
                yield encode_heartbeat()
                continue

        for event in events:
            yield encode_sse(event)
            last_seq = event.seq

            if event.is_terminal():
                return
