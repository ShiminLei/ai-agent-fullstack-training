import asyncio
import json

import pytest

from app.events import RunEvent
from app.sse import SSEDecoder, encode_heartbeat, encode_sse, stream_run_events
from app.store import InMemoryRunStore


def extract_payload(wire: bytes) -> dict:
    text = wire.decode("utf-8")
    data_line = next(
        line for line in text.splitlines()
        if line.startswith("data: ")
    )
    return json.loads(data_line.removeprefix("data: "))


def test_encode_sse_structure() -> None:
    event = RunEvent(
        run_id="run_001",
        seq=17,
        type="text.delta",
        data={"delta": "你好"},
    )

    wire = encode_sse(event)

    assert wire.startswith(b"id: 17\n")
    assert b"event: text.delta\n" in wire
    assert wire.endswith(b"\n\n")


def test_chinese_text_round_trip() -> None:
    event = RunEvent(
        run_id="run_001",
        seq=1,
        type="text.delta",
        data={"delta": "正在分析"},
    )

    payload = extract_payload(encode_sse(event))

    assert payload["run_id"] == "run_001"
    assert payload["seq"] == 1
    assert payload["data"]["delta"] == "正在分析"


def test_newline_does_not_break_sse_frame() -> None:
    event = RunEvent(
        run_id="run_001",
        seq=2,
        type="text.delta",
        data={"delta": "第一行\n第二行"},
    )

    wire = encode_sse(event)
    payload = extract_payload(wire)

    assert payload["data"]["delta"] == "第一行\n第二行"
    assert wire.count(b"data: ") == 1


def test_large_payload_round_trip() -> None:
    large_text = "流" * 70_000
    event = RunEvent(
        run_id="run_001",
        seq=3,
        type="text.delta",
        data={"delta": large_text},
    )

    payload = extract_payload(encode_sse(event))

    assert payload["data"]["delta"] == large_text


def test_encode_heartbeat() -> None:
    assert encode_heartbeat() == b": heartbeat\n\n"


def test_quotes_backslashes_and_empty_delta_round_trip() -> None:
    event = RunEvent(
        run_id="run_001",
        seq=4,
        type="text.delta",
        data={"delta": '路径是 "C:\\\\temp"，下一段为空：'},
    )
    empty = RunEvent(
        run_id="run_001",
        seq=5,
        type="text.delta",
        data={"delta": ""},
    )

    assert extract_payload(encode_sse(event))["data"]["delta"] == event.data["delta"]
    assert extract_payload(encode_sse(empty))["data"]["delta"] == ""


def test_decoder_recovers_event_from_every_byte_split() -> None:
    wire = encode_sse(
        RunEvent(
            run_id="run_001",
            seq=17,
            type="text.delta",
            data={"delta": "正在分析"},
        )
    )
    decoder = SSEDecoder()
    decoded = []

    for byte in wire:
        decoded.extend(decoder.feed_bytes(bytes([byte])))

    assert len(decoded) == 1
    assert decoded[0].event == "text.delta"
    assert decoded[0].event_id == "17"
    assert json.loads(decoded[0].data)["seq"] == 17


def test_decoder_handles_multiple_sticky_frames_and_heartbeat() -> None:
    first = encode_sse(
        RunEvent(run_id="run_001", seq=0, type="run.started")
    )
    second = encode_sse(
        RunEvent(run_id="run_001", seq=1, type="run.completed")
    )
    decoder = SSEDecoder()

    decoded = decoder.feed_bytes(encode_heartbeat() + first + second)

    assert [event.event for event in decoded] == ["run.started", "run.completed"]
    assert [event.event_id for event in decoded] == ["0", "1"]


def test_decoder_supports_crlf_split_between_reads() -> None:
    decoder = SSEDecoder()
    first = decoder.feed_bytes(b"id: 3\r")
    second = decoder.feed_bytes(b"\nevent: text.delta\r\ndata: {}\r\n\r\n")

    assert first == []
    assert len(second) == 1
    assert second[0].event_id == "3"


def extract_event_type(wire: bytes) -> str:
    text = wire.decode("utf-8")
    event_line = next(
        line for line in text.splitlines()
        if line.startswith("event: ")
    )
    return event_line.removeprefix("event: ")


@pytest.mark.asyncio
async def test_stream_replays_only_events_after_last_sequence() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    await store.append("run_001", "run.started")
    await store.append("run_001", "text.delta", {"delta": "你好"})
    await store.append("run_001", "run.completed")

    frames = [
        frame
        async for frame in stream_run_events(
            store,
            "run_001",
            after_seq=0,
        )
    ]

    assert [extract_event_type(frame) for frame in frames] == [
        "text.delta",
        "run.completed",
    ]
    assert [extract_payload(frame)["seq"] for frame in frames] == [1, 2]


@pytest.mark.asyncio
async def test_stream_waits_for_new_event_and_stops_at_terminal() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    stream = stream_run_events(store, "run_001")

    first_frame_task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    assert not first_frame_task.done()

    await store.append("run_001", "run.started")
    first_frame = await asyncio.wait_for(first_frame_task, timeout=1)
    assert extract_event_type(first_frame) == "run.started"

    await store.append("run_001", "run.completed")
    second_frame = await asyncio.wait_for(anext(stream), timeout=1)
    assert extract_event_type(second_frame) == "run.completed"

    with pytest.raises(StopAsyncIteration):
        await anext(stream)


@pytest.mark.asyncio
async def test_stream_sends_heartbeat_without_consuming_sequence() -> None:
    store = InMemoryRunStore()
    state = await store.create("run_001")
    stream = stream_run_events(store, "run_001", heartbeat_interval=0.001)

    frame = await asyncio.wait_for(anext(stream), timeout=1)

    assert frame == b": heartbeat\n\n"
    assert state.next_seq == 0
    await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_type",
    ["run.completed", "run.failed", "run.cancelled"],
)
async def test_stream_stops_for_every_terminal_event(terminal_type: str) -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    await store.append("run_001", "run.started")
    await store.append("run_001", terminal_type)

    frames = [
        frame
        async for frame in stream_run_events(store, "run_001")
    ]

    assert extract_event_type(frames[-1]) == terminal_type
    assert len(frames) == 2
