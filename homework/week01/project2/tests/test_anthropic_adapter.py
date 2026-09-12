from __future__ import annotations

import json

import httpx
import pytest

from app.adapters.anthropic import AnthropicMessagesAdapter
from app.schemas import CompletionRequest


def request(stream: bool = False) -> CompletionRequest:
    return CompletionRequest(
        model="public-flash",
        messages=[
            {"role": "system", "content": "Be concise"},
            {"role": "developer", "content": "Answer in Chinese"},
            {"role": "user", "content": "Hello"},
        ],
        stream=stream,
        max_tokens=200,
        temperature=0.2,
    )


@pytest.mark.asyncio
async def test_complete_translates_request_and_normalizes_response():
    captured: dict = {}

    def handler(http_request: httpx.Request) -> httpx.Response:
        captured["request"] = http_request
        captured["body"] = json.loads(http_request.content)
        return httpx.Response(
            200,
            json={
                "id": "msg_123",
                "content": [{"type": "text", "text": "你好！"}],
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 8,
                    "output_tokens": 3,
                    "cache_read_input_tokens": 2,
                    "cache_creation_input_tokens": 1,
                },
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = AnthropicMessagesAdapter("https://anthropic.test", "secret", client)

    result = await adapter.complete(request(), "real-flash-model")
    await client.aclose()

    assert captured["request"].url.path == "/v1/messages"
    assert captured["request"].headers["x-api-key"] == "secret"
    assert captured["request"].headers["anthropic-version"] == "2023-06-01"
    assert captured["body"] == {
        "model": "real-flash-model",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 200,
        "stream": False,
        "system": "Be concise\n\nAnswer in Chinese",
        "temperature": 0.2,
    }
    assert result.content == "你好！"
    assert result.finish_reason == "end_turn"
    assert result.usage.input_tokens == 8
    assert result.usage.cached_input_tokens == 2
    assert result.usage.cache_creation_input_tokens == 1


@pytest.mark.asyncio
async def test_stream_translates_anthropic_events_to_unified_events():
    def handler(_: httpx.Request) -> httpx.Response:
        body = (
            'event: message_start\ndata: {"type":"message_start","message":'
            '{"usage":{"input_tokens":5,"cache_read_input_tokens":1}}}\n\n'
            'event: content_block_delta\ndata: {"type":"content_block_delta",'
            '"delta":{"type":"text_delta","text":"你"}}\n\n'
            'event: content_block_delta\ndata: {"type":"content_block_delta",'
            '"delta":{"type":"text_delta","text":"好"}}\n\n'
            'event: message_delta\ndata: {"type":"message_delta",'
            '"delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}\n\n'
            'event: message_stop\ndata: {"type":"message_stop"}\n\n'
        )
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = AnthropicMessagesAdapter("https://anthropic.test", "secret", client)

    events = [event async for event in adapter.stream(request(stream=True), "real-flash-model")]
    await client.aclose()

    assert "".join(event.delta for event in events) == "你好"
    assert events[-1].finish_reason == "end_turn"
    assert events[-1].usage is not None
    assert events[-1].usage.input_tokens == 5
    assert events[-1].usage.output_tokens == 2


@pytest.mark.asyncio
async def test_json_schema_is_added_to_anthropic_system_instruction():
    client = httpx.AsyncClient()
    adapter = AnthropicMessagesAdapter("https://anthropic.test", "secret", client)
    structured_request = request().model_copy(
        update={
            "response_format": {
                "type": "json_schema",
                "json_schema": {"schema": {"type": "object"}},
            }
        }
    )

    body = adapter._body(structured_request, "real-flash-model", stream=False)
    await client.aclose()

    assert "Return only valid JSON" in body["system"]
    assert '{"type": "object"}' in body["system"]
