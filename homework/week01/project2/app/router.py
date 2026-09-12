from __future__ import annotations

from dataclasses import dataclass

import httpx

from app.adapters import AnthropicMessagesAdapter, BaseAdapter, ResponsesAdapter
from app.config import Settings
from app.errors import GatewayError


@dataclass(frozen=True)
class Route:
    """一次模型查找得到的完整路由结果。"""

    adapter: BaseAdapter
    provider: str
    upstream_model: str


class ModelRouter:
    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self.settings = settings
        self.adapters: dict[str, BaseAdapter] = {}

        for provider_name, provider in settings.providers.items():
            if provider.protocol == "responses":
                adapter: BaseAdapter = ResponsesAdapter(
                    provider.base_url,
                    provider.api_key.get_secret_value(),
                    client,
                    timeout_seconds=provider.timeout_seconds,
                )
            else:
                adapter = AnthropicMessagesAdapter(
                    provider.base_url,
                    provider.api_key.get_secret_value(),
                    client,
                    anthropic_version=provider.anthropic_version,
                    timeout_seconds=provider.timeout_seconds,
                )
            self.adapters[provider_name] = adapter

    def resolve(self, public_model: str) -> Route:
        model = self.settings.models.get(public_model)
        if model is None:
            raise GatewayError(
                f"Unknown model: {public_model}",
                status_code=404,
                error_type="invalid_request_error",
                code="model_not_found",
            )
        return Route(
            adapter=self.adapters[model.provider],
            provider=model.provider,
            upstream_model=model.upstream_model,
        )

