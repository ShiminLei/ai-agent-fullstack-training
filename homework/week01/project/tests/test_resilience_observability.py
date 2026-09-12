from __future__ import annotations

import httpx
import pytest

from app.gateway import Gateway
from app.main import create_app
from app.observability import UsageRepository
from app.prompts import PromptRepository
from app.router import ModelRouter
from app.schemas import CompletionRequest
from tests.test_api import HEADERS, api_client, settings


def responses_result() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "id": "resp_1",
            "status": "completed",
            "output_text": "ok",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 4,
                "input_tokens_details": {"cached_tokens": 3},
            },
        },
    )


@pytest.mark.asyncio
async def test_gateway_retries_and_records_usage(tmp_path):
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            return httpx.Response(503, json={"error": "busy"})
        return responses_result()

    async with api_client(handler, str(tmp_path / "gateway.db")) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=HEADERS,
            json={"model": "pro", "messages": [{"role": "user", "content": "hello"}]},
        )
        usage = (await client.get("/admin/usage", headers=HEADERS)).json()["data"][0]

    assert response.status_code == 200
    assert calls == 3
    assert usage["attempts"] == 3
    assert usage["provider"] == "responses"
    assert usage["upstream_model"] == "real-pro"
    assert usage["input_tokens"] == 10
    assert usage["output_tokens"] == 4
    assert usage["cached_input_tokens"] == 3
    assert usage["latency_ms"] >= 0


@pytest.mark.asyncio
async def test_stream_records_first_token_latency_and_usage(tmp_path):
    def handler(_: httpx.Request) -> httpx.Response:
        body = (
            'event: message_start\ndata: {"type":"message_start","message":'
            '{"usage":{"input_tokens":5}}}\n\n'
            'event: content_block_delta\ndata: {"type":"content_block_delta",'
            '"delta":{"type":"text_delta","text":"Hi"}}\n\n'
            'event: message_delta\ndata: {"type":"message_delta",'
            '"delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}\n\n'
        )
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    async with api_client(handler, str(tmp_path / "gateway.db")) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=HEADERS,
            json={
                "model": "flash",
                "stream": True,
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        usage = (await client.get("/admin/usage", headers=HEADERS)).json()["data"][0]

    assert response.status_code == 200
    assert usage["stream"] == 1
    assert usage["input_tokens"] == 5
    assert usage["output_tokens"] == 2
    assert usage["first_token_ms"] is not None


@pytest.mark.asyncio
async def test_rate_limit_buckets_are_independent_by_model(tmp_path):
    database_path = str(tmp_path / "gateway.db")
    base_settings = settings(database_path)
    limited_settings = base_settings.model_copy(
        update={
            "models": {
                name: model.model_copy(
                    update={"requests_per_minute": 1, "burst": 1}
                )
                for name, model in base_settings.models.items()
            }
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "responses.test":
            return responses_result()
        return httpx.Response(
            200,
            json={
                "id": "msg_1",
                "content": [{"type": "text", "text": "ok"}],
                "stop_reason": "end_turn",
                "usage": {},
            },
        )

    upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(limited_settings, http_client=upstream)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway.test"
        ) as client:
            message = {"messages": [{"role": "user", "content": "hello"}]}
            first_pro = await client.post(
                "/v1/chat/completions", headers=HEADERS, json={"model": "pro", **message}
            )
            second_pro = await client.post(
                "/v1/chat/completions", headers=HEADERS, json={"model": "pro", **message}
            )
            first_flash = await client.post(
                "/v1/chat/completions", headers=HEADERS, json={"model": "flash", **message}
            )
    await upstream.aclose()

    assert first_pro.status_code == 200
    assert second_pro.status_code == 429
    assert second_pro.json()["error"]["code"] == "model_rate_limit_exceeded"
    assert first_flash.status_code == 200


@pytest.mark.asyncio
async def test_stream_stops_when_the_client_disconnects(tmp_path):
    body = (
        'data: {"type":"response.output_text.delta","delta":"unused"}\n\n'
        'data: {"type":"response.completed","response":{"status":"completed",'
        '"usage":{"input_tokens":2,"output_tokens":1}}}\n\n'
    )
    upstream = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                text=body,
                headers={"content-type": "text/event-stream"},
            )
        )
    )
    database_path = str(tmp_path / "gateway.db")
    app_settings = settings(database_path)
    prompts = PromptRepository(database_path)
    usage = UsageRepository(database_path)
    await prompts.initialize()
    await usage.initialize()
    gateway = Gateway(
        app_settings,
        ModelRouter(app_settings, upstream),
        prompts,
        usage,
    )

    async def disconnected() -> bool:
        return True

    request = CompletionRequest(
        model="pro",
        stream=True,
        messages=[{"role": "user", "content": "hello"}],
    )
    events = await gateway.stream(request, "req_disconnected", "test", disconnected)
    received = [event async for event in events]
    recorded = (await usage.recent())[0]
    await upstream.aclose()

    assert received == []
    assert recorded["status"] == "cancelled"
    assert recorded["status_code"] == 499
