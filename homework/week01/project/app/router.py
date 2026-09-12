from __future__ import annotations

import httpx

from app.adapters import AnthropicMessagesAdapter, ResponsesAdapter
from app.adapters.base import BaseAdapter
from app.config import Settings
from app.errors import GatewayError


class ModelRouter:
    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self.settings = settings
        adapter_types = {"responses": ResponsesAdapter, "anthropic": AnthropicMessagesAdapter}
        self.adapters: dict[str, BaseAdapter] = {
            name: adapter_types[provider.protocol](name, provider, client, settings.retry.statuses)
            for name, provider in settings.providers.items()
        }

    def resolve(self, model: str) -> tuple[BaseAdapter, str, str]:
        route = self.settings.models.get(model)
        if route is None:
            raise GatewayError(
                f"Unknown model: {model}",
                status_code=404,
                error_type="invalid_request_error",
                code="model_not_found",
            )
        return self.adapters[route.provider], route.upstream_model, route.provider
