from __future__ import annotations

import json

import httpx
import pytest

from tests.conftest import gateway_client, make_settings


def responses_result(content: str = "hello") -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "id": "resp_1",
            "status": "completed",
            "output_text": content,
            "usage": {
                "input_tokens": 10,
                "output_tokens": 4,
                "input_tokens_details": {"cached_tokens": 3},
            },
        },
    )


def anthropic_result(content: str = "hello") -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "id": "msg_1",
            "content": [{"type": "text", "text": content}],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 8,
                "output_tokens": 3,
                "cache_read_input_tokens": 2,
                "cache_creation_input_tokens": 1,
            },
        },
    )


@pytest.mark.asyncio
async def test_model_routes_to_two_different_protocols(tmp_path, headers):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "responses.test":
            return responses_result("from responses")
        return anthropic_result("from anthropic")

    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, handler) as client:
        pro = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": "pro", "messages": [{"role": "user", "content": "hi"}]},
        )
        flash = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": "flash", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert pro.json()["choices"][0]["message"]["content"] == "from responses"
    assert flash.json()["choices"][0]["message"]["content"] == "from anthropic"
    assert requests[0].url.path == "/v1/responses"
    assert json.loads(requests[0].content)["input"][0]["role"] == "user"
    assert requests[1].url.path == "/v1/messages"
    assert requests[1].headers["x-api-key"] == "anthropic-secret"
    assert json.loads(requests[1].content)["model"] == "flash-upstream"


@pytest.mark.asyncio
async def test_stream_is_normalized_to_sse_and_records_ttft(tmp_path, headers):
    def handler(_: httpx.Request) -> httpx.Response:
        body = (
            'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":5}}}\n\n'
            "event: content_block_delta\n"
            'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}\n\n'
            "event: message_delta\n"
            'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},'
            '"usage":{"output_tokens":2}}\n\n'
            'event: message_stop\ndata: {"type":"message_stop"}\n\n'
        )
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, handler) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": "flash", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
        )
        usage = (await client.get("/admin/usage", headers=headers)).json()["data"][0]

    assert '"content": "Hi"' in response.text
    assert "data: [DONE]" in response.text
    assert usage["input_tokens"] == 5
    assert usage["output_tokens"] == 2
    assert usage["first_token_ms"] is not None


@pytest.mark.asyncio
async def test_structured_output_is_forwarded_and_validated(tmp_path, headers):
    upstream_body = {}

    def handler(request: httpx.Request) -> httpx.Response:
        upstream_body.update(json.loads(request.content))
        return responses_result('{"name":"Ada"}')

    schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    }
    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, handler) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={
                "model": "pro",
                "messages": [{"role": "user", "content": "extract"}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {"name": "person", "strict": True, "schema": schema},
                },
            },
        )

    assert response.status_code == 200
    assert upstream_body["text"]["format"]["schema"] == schema


@pytest.mark.asyncio
async def test_invalid_structured_output_returns_422(tmp_path, headers):
    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, lambda _: responses_result('{"name":123}')) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={
                "model": "pro",
                "messages": [{"role": "user", "content": "extract"}],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "schema": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["name"],
                        }
                    },
                },
            },
        )

    assert response.status_code == 422
    assert response.json()["error"]["type"] == "structured_output_error"


@pytest.mark.asyncio
async def test_prompt_versions_and_variable_injection(tmp_path, headers):
    bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return anthropic_result()

    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, handler) as client:
        first = await client.post(
            "/v1/prompts", headers=headers, json={"id": "teacher", "content": "Old {{ topic }}"}
        )
        second = await client.post(
            "/v1/prompts", headers=headers, json={"id": "teacher", "content": "Teach {{ topic }}"}
        )
        await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={
                "model": "flash",
                "messages": [{"role": "user", "content": "start"}],
                "prompt": {"id": "teacher", "version": 1, "variables": {"topic": "adapters"}},
            },
        )

    assert first.json()["version"] == 1
    assert second.json()["version"] == 2
    assert bodies[-1]["system"] == "Old adapters"


@pytest.mark.asyncio
async def test_retry_uses_exponential_policy_and_stops_at_three(tmp_path, headers):
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "busy"}) if calls < 3 else responses_result()

    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, handler) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": "pro", "messages": [{"role": "user", "content": "hi"}]},
        )
        usage = (await client.get("/admin/usage", headers=headers)).json()["data"][0]

    assert response.status_code == 200
    assert calls == 3
    assert usage["attempts"] == 3


@pytest.mark.asyncio
async def test_rate_limit_is_independent_for_each_model(tmp_path, headers):
    models = {
        "pro": {"provider": "responses", "upstream_model": "pro", "requests_per_minute": 1, "burst": 1},
        "flash": {
            "provider": "anthropic",
            "upstream_model": "flash",
            "requests_per_minute": 1,
            "burst": 1,
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return responses_result() if request.url.host == "responses.test" else anthropic_result()

    settings = make_settings(tmp_path / "gateway.db", models=models)
    async with gateway_client(settings, handler) as client:
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        first_pro = await client.post(
            "/v1/chat/completions", headers=headers, json={"model": "pro", **payload}
        )
        limited_pro = await client.post(
            "/v1/chat/completions", headers=headers, json={"model": "pro", **payload}
        )
        first_flash = await client.post(
            "/v1/chat/completions", headers=headers, json={"model": "flash", **payload}
        )

    assert first_pro.status_code == 200
    assert limited_pro.status_code == 429
    assert first_flash.status_code == 200


@pytest.mark.asyncio
async def test_authentication_and_usage_categories(tmp_path, headers):
    settings = make_settings(tmp_path / "gateway.db")
    async with gateway_client(settings, lambda _: responses_result()) as client:
        unauthorized = await client.get("/v1/models")
        authorized = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": "pro", "messages": [{"role": "user", "content": "hi"}]},
        )
        usage = (await client.get("/admin/usage", headers=headers)).json()["data"][0]

    assert unauthorized.status_code == 401
    assert authorized.status_code == 200
    assert usage["input_tokens"] == 10
    assert usage["output_tokens"] == 4
    assert usage["cached_input_tokens"] == 3
    assert usage["latency_ms"] >= 0
