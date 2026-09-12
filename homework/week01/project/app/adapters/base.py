from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.config import ProviderConfig
from app.errors import UpstreamError
from app.schemas import AdapterResult, CompletionRequest, StreamEvent


class BaseAdapter(ABC):
    """Protocol boundary: each vendor is normalized behind this contract."""

    def __init__(
        self,
        provider_name: str,
        config: ProviderConfig,
        client: httpx.AsyncClient,
        retry_statuses: set[int],
    ) -> None:
        self.provider_name = provider_name
        self.config = config
        self.client = client
        self.retry_statuses = retry_statuses

    @abstractmethod
    async def complete(
        self, request: CompletionRequest, upstream_model: str, request_id: str
    ) -> AdapterResult:
        raise NotImplementedError

    @abstractmethod
    def stream(
        self, request: CompletionRequest, upstream_model: str, request_id: str
    ) -> AsyncIterator[StreamEvent]:
        raise NotImplementedError

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self.config.timeout_seconds)

    def _raise_for_error(self, response: httpx.Response, retry_statuses: set[int]) -> None:
        if not response.is_error:
            return
        try:
            details: Any = response.json()
        except json.JSONDecodeError:
            details = response.text[:1000]
        status = response.status_code if 400 <= response.status_code < 500 else 502
        raise UpstreamError(
            f"{self.provider_name} returned HTTP {response.status_code}",
            status_code=status,
            retryable=response.status_code in retry_statuses,
            details=details,
        )


async def iter_sse(response: httpx.Response) -> AsyncIterator[tuple[str | None, str]]:
    event_name: str | None = None
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if not line:
            if data_lines:
                yield event_name, "\n".join(data_lines)
            event_name, data_lines = None, []
        elif line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())
    if data_lines:
        yield event_name, "\n".join(data_lines)
