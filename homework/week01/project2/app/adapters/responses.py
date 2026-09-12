from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.adapters.base import BaseAdapter, iter_sse, json_body
from app.errors import UpstreamError
from app.schemas import AdapterResult, CompletionRequest, StreamEvent, TokenUsage

RETRYABLE_STATUSES = {408, 409, 429, 500, 502, 503, 504}


class ResponsesAdapter(BaseAdapter):
    """把网关统一格式转换为 OpenAI Responses API 格式。"""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        client: httpx.AsyncClient,
        timeout_seconds: float = 120,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = client
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _body(
        self,
        request: CompletionRequest,
        upstream_model: str,
        *,
        stream: bool,
    ) -> dict[str, Any]:
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
                if item.get("type") == "output_text" and isinstance(item.get("text"), str):
                    parts.append(item["text"])
        return "".join(parts)

    @staticmethod
    def _finish_reason(status: str | None) -> str | None:
        return {"completed": "stop", "incomplete": "length"}.get(status, status)

    def _raise_for_status(self, response: httpx.Response) -> None:
        if not response.is_error:
            return
        details = json_body(response)
        status_code = response.status_code if response.status_code < 500 else 502
        raise UpstreamError(
            f"Responses API returned HTTP {response.status_code}",
            status_code=status_code,
            retryable=response.status_code in RETRYABLE_STATUSES,
            details=details,
        )

    async def complete(
        self,
        request: CompletionRequest,
        upstream_model: str,
    ) -> AdapterResult:
        try:
            response = await self.client.post(
                f"{self.base_url}/v1/responses",
                headers=self._headers(),
                json=self._body(request, upstream_model, stream=False),
                timeout=self.timeout_seconds,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(type(exc).__name__, retryable=True) from exc
        self._raise_for_status(response)
        payload = json_body(response)
        return AdapterResult(
            content=self._content(payload),
            finish_reason=self._finish_reason(payload.get("status")),
            usage=self._usage(payload),
            upstream_id=payload.get("id"),
        )

    async def stream(
        self,
        request: CompletionRequest,
        upstream_model: str,
    ) -> AsyncIterator[StreamEvent]:
        try:
            async with self.client.stream(
                "POST",
                f"{self.base_url}/v1/responses",
                headers=self._headers(),
                json=self._body(request, upstream_model, stream=True),
                timeout=self.timeout_seconds,
            ) as response:
                if response.is_error:
                    await response.aread()
                    self._raise_for_status(response)
                async for _, data in iter_sse(response):
                    if data == "[DONE]":
                        continue
                    payload = json.loads(data)
                    if payload.get("type") == "response.output_text.delta":
                        yield StreamEvent(delta=payload.get("delta", ""))
                    elif payload.get("type") == "response.completed":
                        completed = payload.get("response") or payload
                        yield StreamEvent(
                            finish_reason=self._finish_reason(completed.get("status", "completed")),
                            usage=self._usage(completed),
                        )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise UpstreamError(type(exc).__name__, retryable=True) from exc
