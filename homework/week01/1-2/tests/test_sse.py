import json

from app.events import RunEvent
from app.sse import encode_heartbeat, encode_sse


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
