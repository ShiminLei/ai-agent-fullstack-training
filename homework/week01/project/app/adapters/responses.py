from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.adapters.base import BaseAdapter, iter_sse
from app.errors import UpstreamError
from app.schemas import AdapterResult, CompletionRequest, StreamEvent, TokenUsage


class ResponsesAdapter(BaseAdapter):
    def _headers(self, request_id: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key.get_secret_value()}",
            "Content-Type": "application/json",
            "X-Request-ID": request_id,
        }

    def _body(self, request: CompletionRequest, upstream_model: str, *, stream: bool) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": upstream_model,
            "input": [message.model_dump() for message in request.messages],
            "max_output_tokens": request.max_tokens,
            "stream": stream,
        }
        if request.temperature is not None:
            body["temperature"] = request.temperature
        if request.response_format:
            json_schema = request.response_format.get("json_schema", {})
            body["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": json_schema.get("name", "response"),
                    "strict": json_schema.get("strict", True),
                    "schema": json_schema.get("schema", {}),
                }
            }
        return body

    @staticmethod
    def _usage(payload: dict[str, Any]) -> TokenUsage:
        usage = payload.get("usage") or {}
        details = usage.get("input_tokens_details") or {}
        return TokenUsage(
            input_tokens=usage.get("input_tokens", 0) or 0,
            output_tokens=usage.get("output_tokens", 0) or 0,
            cached_input_tokens=details.get("cached_tokens", 0) or 0,
        )

    @staticmethod
    def _content(payload: dict[str, Any]) -> str:
        if isinstance(payload.get("output_text"), str):
            return payload["output_text"]
        parts: list[str] = []
        for output in payload.get("output", []):
            for item in output.get("content", []):
                if item.get("type") in {"output_text", "text"} and isinstance(item.get("text"), str):
                    parts.append(item["text"])
        return "".join(parts)

    async def complete(
        self, request: CompletionRequest, upstream_model: str, request_id: str
    ) -> AdapterResult:
        try:
            response = await self.client.post(
                f"{self.config.base_url}/v1/responses",
                headers=self._headers(request_id),
                json=self._body(request, upstream_model, stream=False),
                timeout=self._timeout(),
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(f"{self.provider_name}: {type(exc).__name__}", retryable=True) from exc
        self._raise_for_error(response, self.retry_statuses)
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise UpstreamError(f"{self.provider_name}: invalid JSON response") from exc
        return AdapterResult(
            content=self._content(payload),
            finish_reason=payload.get("status"),
            usage=self._usage(payload),
            upstream_id=payload.get("id"),
        )

    async def stream(
        self, request: CompletionRequest, upstream_model: str, request_id: str
    ) -> AsyncIterator[StreamEvent]:
        try:
            async with self.client.stream(
                "POST",
                f"{self.config.base_url}/v1/responses",
                headers=self._headers(request_id),
                json=self._body(request, upstream_model, stream=True),
                timeout=self._timeout(),
            ) as response:
                if response.is_error:
                    await response.aread()
                    self._raise_for_error(response, self.retry_statuses)
                async for _, data in iter_sse(response):
                    if data == "[DONE]":
                        continue
                    payload = json.loads(data)
                    event_type = payload.get("type")
                    if event_type == "response.output_text.delta":
                        yield StreamEvent(delta=payload.get("delta", ""))
                    elif event_type in {"response.completed", "response.incomplete"}:
                        completed = payload.get("response") or payload
                        yield StreamEvent(
                            finish_reason=completed.get("status", "stop"),
                            usage=self._usage(completed),
                        )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(f"{self.provider_name}: {type(exc).__name__}", retryable=True) from exc
