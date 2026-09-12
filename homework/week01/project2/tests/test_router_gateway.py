from __future__ import annotations

import json

import httpx
import pytest
from pydantic import ValidationError

from app.adapters import AnthropicMessagesAdapter, ResponsesAdapter
from app.config import Settings
from app.errors import GatewayError
from app.gateway import Gateway
from app.router import ModelRouter
from app.schemas import CompletionRequest


def settings() -> Settings:
    return Settings.model_validate(
        {
            "providers": {
                "provider_a": {
                    "protocol": "responses",
                    "base_url": "https://responses.test",
                    "api_key": "responses-secret",
                },
                "provider_b": {
                    "protocol": "anthropic",
                    "base_url": "https://anthropic.test",
                    "api_key": "anthropic-secret",
                },
            },
            "models": {
                "pro": {"provider": "provider_a", "upstream_model": "real-pro"},
                "flash": {"provider": "provider_b", "upstream_model": "real-flash"},
            },
        }
    )


def completion_request(model: str) -> CompletionRequest:
    return CompletionRequest(
        model=model,
        messages=[{"role": "user", "content": "Hello"}],
    )


def test_router_maps_public_models_to_adapter_and_real_model():
    client = httpx.AsyncClient()
    router = ModelRouter(settings(), client)

    pro = router.resolve("pro")
    flash = router.resolve("flash")

    assert isinstance(pro.adapter, ResponsesAdapter)
    assert pro.provider == "provider_a"
    assert pro.upstream_model == "real-pro"
    assert isinstance(flash.adapter, AnthropicMessagesAdapter)
    assert flash.upstream_model == "real-flash"


def test_router_rejects_an_unknown_public_model():
    client = httpx.AsyncClient()
    router = ModelRouter(settings(), client)

    with pytest.raises(GatewayError) as captured:
        router.resolve("missing")

    assert captured.value.status_code == 404
    assert captured.value.code == "model_not_found"


def test_settings_reject_a_model_with_an_unknown_provider():
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {
                "providers": {},
                "models": {
                    "pro": {"provider": "missing", "upstream_model": "real-pro"}
                },
            }
        )


@pytest.mark.asyncio
async def test_gateway_uses_model_to_call_the_correct_protocol():
    calls: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append((request.url.path, body))
        if request.url.host == "responses.test":
            return httpx.Response(
                200,
                json={"id": "resp_1", "status": "completed", "output_text": "pro answer"},
            )
        return httpx.Response(
            200,
            json={
                "id": "msg_1",
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "flash answer"}],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = Gateway(ModelRouter(settings(), client))

    pro_result = await gateway.complete(completion_request("pro"))
    flash_result = await gateway.complete(completion_request("flash"))
    await client.aclose()

    assert pro_result.content == "pro answer"
    assert flash_result.content == "flash answer"
    assert calls[0][0] == "/v1/responses"
    assert calls[0][1]["model"] == "real-pro"
    assert calls[1][0] == "/v1/messages"
    assert calls[1][1]["model"] == "real-flash"
