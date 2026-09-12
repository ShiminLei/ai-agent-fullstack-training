from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx
import pytest

from app.config import Settings
from app.main import create_app


def settings(database_path: str) -> Settings:
    return Settings.model_validate(
        {
            "api_keys": ["gateway-secret"],
            "database_url": database_path,
            "retry": {
                "max_attempts": 3,
                "base_delay_seconds": 0,
                "max_delay_seconds": 0,
            },
            "providers": {
                "responses": {
                    "protocol": "responses",
                    "base_url": "https://responses.test",
                    "api_key": "upstream-a",
                },
                "anthropic": {
                    "protocol": "anthropic",
                    "base_url": "https://anthropic.test",
                    "api_key": "upstream-b",
                },
            },
            "models": {
                "pro": {"provider": "responses", "upstream_model": "real-pro"},
                "flash": {"provider": "anthropic", "upstream_model": "real-flash"},
            },
        }
    )


@asynccontextmanager
async def api_client(
    handler: Callable[[httpx.Request], httpx.Response],
    database_path: str,
) -> AsyncIterator[httpx.AsyncClient]:
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(settings(database_path), http_client=upstream)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway.test"
        ) as client:
            yield client
    await upstream.aclose()


HEADERS = {"Authorization": "Bearer gateway-secret"}


@pytest.mark.asyncio
async def test_non_streaming_http_api_returns_one_unified_shape(tmp_path):
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "resp_1",
                "status": "completed",
                "output_text": "Hello from Responses",
                "usage": {"input_tokens": 4, "output_tokens": 3},
            },
        )

    async with api_client(handler, str(tmp_path / "gateway.db")) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=HEADERS,
            json={"model": "pro", "messages": [{"role": "user", "content": "Hello"}]},
        )

    assert response.status_code == 200
    assert response.headers["x-request-id"].startswith("req_")
    assert response.json()["choices"][0]["message"]["content"] == "Hello from Responses"
    assert response.json()["choices"][0]["finish_reason"] == "stop"
    assert response.json()["usage"]["total_tokens"] == 7


@pytest.mark.asyncio
async def test_streaming_http_api_returns_normalized_sse(tmp_path):
    def handler(_: httpx.Request) -> httpx.Response:
        body = (
            'event: message_start\ndata: {"type":"message_start","message":'
            '{"usage":{"input_tokens":3}}}\n\n'
            'event: content_block_delta\ndata: {"type":"content_block_delta",'
            '"delta":{"type":"text_delta","text":"Hi"}}\n\n'
            'event: message_delta\ndata: {"type":"message_delta",'
            '"delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n'
        )
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    async with api_client(handler, str(tmp_path / "gateway.db")) as client:
        response = await client.post(
            "/v1/chat/completions",
            headers=HEADERS,
            json={
                "model": "flash",
                "stream": True,
                "messages": [{"role": "user", "content": "Hello"}],
            },
        )

    assert response.status_code == 200
    assert '"content": "Hi"' in response.text
    assert '"finish_reason": "stop"' in response.text
    assert response.text.endswith("data: [DONE]\n\n")


@pytest.mark.asyncio
async def test_authentication_model_list_and_unknown_model_error(tmp_path):
    async with api_client(
        lambda _: httpx.Response(500), str(tmp_path / "gateway.db")
    ) as client:
        unauthorized = await client.get("/v1/models")
        models = await client.get("/v1/models", headers=HEADERS)
        missing = await client.post(
            "/v1/chat/completions",
            headers=HEADERS,
            json={"model": "missing", "messages": [{"role": "user", "content": "Hello"}]},
        )

    assert unauthorized.status_code == 401
    assert [item["id"] for item in models.json()["data"]] == ["pro", "flash"]
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "model_not_found"
