from __future__ import annotations

import json

import httpx
import pytest

from app.adapters.responses import ResponsesAdapter
from app.errors import UpstreamError
from app.schemas import CompletionRequest


def request(stream: bool = False) -> CompletionRequest:
    return CompletionRequest(
        model="public-pro",
        messages=[
            {"role": "system", "content": "Be concise"},
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
                "id": "resp_123",
                "status": "completed",
                "output_text": "Hi!",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 3,
                    "input_tokens_details": {"cached_tokens": 4},
                },
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = ResponsesAdapter("https://responses.test", "secret", client)

    result = await adapter.complete(request(), "real-pro-model")
    await client.aclose()

    assert captured["request"].url.path == "/v1/responses"
    assert captured["request"].headers["authorization"] == "Bearer secret"
    assert captured["body"] == {
        "model": "real-pro-model",
        "input": [
            {"role": "system", "content": "Be concise"},
            {"role": "user", "content": "Hello"},
        ],
        "max_output_tokens": 200,
        "stream": False,
        "temperature": 0.2,
    }
    assert result.content == "Hi!"
    assert result.usage.input_tokens == 10
    assert result.usage.output_tokens == 3
    assert result.usage.cached_input_tokens == 4


@pytest.mark.asyncio
async def test_stream_translates_responses_events_to_unified_events():
    def handler(_: httpx.Request) -> httpx.Response:
        body = (
            'data: {"type":"response.output_text.delta","delta":"Hel"}\n\n'
            'data: {"type":"response.output_text.delta","delta":"lo"}\n\n'
            'data: {"type":"response.completed","response":{"status":"completed",'
            '"usage":{"input_tokens":6,"output_tokens":2}}}\n\n'
            "data: [DONE]\n\n"
        )
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = ResponsesAdapter("https://responses.test", "secret", client)

    events = [event async for event in adapter.stream(request(stream=True), "real-pro-model")]
    await client.aclose()

    assert "".join(event.delta for event in events) == "Hello"
    assert events[-1].finish_reason == "completed"
    assert events[-1].usage is not None
    assert events[-1].usage.output_tokens == 2


@pytest.mark.asyncio
async def test_retryable_http_status_is_marked_for_gateway():
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503, json={"error": "busy"}))
    )
    adapter = ResponsesAdapter("https://responses.test", "secret", client)

    with pytest.raises(UpstreamError) as captured:
        await adapter.complete(request(), "real-pro-model")
    await client.aclose()

    assert captured.value.status_code == 502
    assert captured.value.retryable is True
