from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.adapters.base import BaseAdapter, iter_sse
from app.errors import UpstreamError
from app.schemas import AdapterResult, CompletionRequest, StreamEvent, TokenUsage


class AnthropicMessagesAdapter(BaseAdapter):
    def _headers(self, request_id: str) -> dict[str, str]:
        return {
            "x-api-key": self.config.api_key.get_secret_value(),
            "anthropic-version": self.config.anthropic_version,
            "Content-Type": "application/json",
            "X-Request-ID": request_id,
        }

    def _body(self, request: CompletionRequest, upstream_model: str, *, stream: bool) -> dict[str, Any]:
        system_parts = [m.content for m in request.messages if m.role in {"system", "developer"}]
        messages = [
            {"role": message.role, "content": message.content}
            for message in request.messages
            if message.role in {"user", "assistant"}
        ]
        if request.response_format:
            schema = request.response_format.get("json_schema", {}).get("schema", {})
            system_parts.append(
                "Return only valid JSON matching this JSON Schema, without Markdown fences: "
                + json.dumps(schema, ensure_ascii=False)
            )
        body: dict[str, Any] = {
            "model": upstream_model,
            "messages": messages,
            "max_tokens": request.max_tokens,
            "stream": stream,
        }
        if system_parts:
            body["system"] = "\n\n".join(system_parts)
        if request.temperature is not None:
            body["temperature"] = request.temperature
        return body

    @staticmethod
    def _usage(payload: dict[str, Any]) -> TokenUsage:
        usage = payload.get("usage") or {}
        return TokenUsage(
            input_tokens=usage.get("input_tokens", 0) or 0,
            output_tokens=usage.get("output_tokens", 0) or 0,
            cached_input_tokens=usage.get("cache_read_input_tokens", 0) or 0,
            cache_creation_input_tokens=usage.get("cache_creation_input_tokens", 0) or 0,
        )

    async def complete(
        self, request: CompletionRequest, upstream_model: str, request_id: str
    ) -> AdapterResult:
        try:
            response = await self.client.post(
                f"{self.config.base_url}/v1/messages",
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
        content = "".join(
            item.get("text", "") for item in payload.get("content", []) if item.get("type") == "text"
        )
        return AdapterResult(
            content=content,
            finish_reason=payload.get("stop_reason"),
            usage=self._usage(payload),
            upstream_id=payload.get("id"),
        )

    async def stream(
        self, request: CompletionRequest, upstream_model: str, request_id: str
    ) -> AsyncIterator[StreamEvent]:
        usage = TokenUsage()
        try:
            async with self.client.stream(
                "POST",
                f"{self.config.base_url}/v1/messages",
                headers=self._headers(request_id),
                json=self._body(request, upstream_model, stream=True),
                timeout=self._timeout(),
            ) as response:
                if response.is_error:
                    await response.aread()
                    self._raise_for_error(response, self.retry_statuses)
                async for event_name, data in iter_sse(response):
                    payload = json.loads(data)
                    event_type = payload.get("type", event_name)
                    if event_type == "message_start":
                        usage = self._usage(payload.get("message") or {})
                    elif event_type == "content_block_delta":
                        delta = payload.get("delta") or {}
                        if delta.get("type") == "text_delta":
                            yield StreamEvent(delta=delta.get("text", ""))
                    elif event_type == "message_delta":
                        delta_usage = payload.get("usage") or {}
                        usage.output_tokens = delta_usage.get("output_tokens", usage.output_tokens) or 0
                        yield StreamEvent(
                            finish_reason=(payload.get("delta") or {}).get("stop_reason", "stop"),
                            usage=usage,
                        )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(f"{self.provider_name}: {type(exc).__name__}", retryable=True) from exc
