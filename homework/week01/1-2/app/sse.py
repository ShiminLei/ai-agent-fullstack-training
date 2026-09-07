import json

from app.events import RunEvent


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