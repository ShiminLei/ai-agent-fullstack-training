from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.config import Settings
from app.main import create_app


def make_settings(database: Path, **overrides: Any) -> Settings:
    raw: dict[str, Any] = {
        "api_keys": ["test-key"],
        "database_url": str(database),
        "retry": {"max_attempts": 3, "base_delay_seconds": 0, "max_delay_seconds": 0},
        "providers": {
            "responses": {
                "protocol": "responses",
                "base_url": "https://responses.test",
                "api_key": "responses-secret",
            },
            "anthropic": {
                "protocol": "anthropic",
                "base_url": "https://anthropic.test",
                "api_key": "anthropic-secret",
            },
        },
        "models": {
            "pro": {
                "provider": "responses",
                "upstream_model": "pro-upstream",
                "requests_per_minute": 600,
                "burst": 20,
            },
            "flash": {
                "provider": "anthropic",
                "upstream_model": "flash-upstream",
                "requests_per_minute": 600,
                "burst": 20,
            },
        },
    }
    raw.update(overrides)
    return Settings.model_validate(raw)


@asynccontextmanager
async def gateway_client(
    settings: Settings, handler: Callable[[httpx.Request], httpx.Response]
) -> AsyncIterator[httpx.AsyncClient]:
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(settings, http_client=upstream)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway.test"
        ) as client:
            yield client
    await upstream.aclose()


@pytest.fixture
def headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-key"}
