from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import AsyncIterator

from fastapi import Request

from app.config import Settings
from app.errors import GatewayError, UpstreamError, error_body
from app.observability import UsageEvent, UsageRepository
from app.prompts import PromptRepository
from app.router import ModelRouter
from app.schemas import AdapterResult, CompletionRequest, Message, TokenUsage
from app.structured import requested_schema, validate_content

logger = logging.getLogger(__name__)


class Gateway:
    def __init__(
        self,
        settings: Settings,
        router: ModelRouter,
        prompts: PromptRepository,
        usage: UsageRepository,
    ) -> None:
        self.settings = settings
        self.router = router
        self.prompts = prompts
        self.usage = usage

    async def prepare(
        self, request: CompletionRequest
    ) -> tuple[CompletionRequest, tuple[str | None, int | None]]:
        if request.prompt is None:
            return request, (None, None)
        prompt, rendered = await self.prompts.render(
            request.prompt.id, request.prompt.variables, request.prompt.version
        )
        prepared = request.model_copy(deep=True)
        prepared.messages.insert(0, Message(role="system", content=rendered))
        prepared.prompt = None
        return prepared, (prompt.id, prompt.version)

    async def complete(
        self, request: CompletionRequest, prompt_meta: tuple[str | None, int | None]
    ) -> tuple[dict, str]:
        request_id = f"req_{uuid.uuid4().hex}"
        started = time.perf_counter()
        adapter, upstream_model, provider = self.router.resolve(request.model)
        event = UsageEvent(
            request_id=request_id,
            model=request.model,
            provider=provider,
            upstream_model=upstream_model,
            prompt_id=prompt_meta[0],
            prompt_version=prompt_meta[1],
        )
        result: AdapterResult | None = None
        try:
            for attempt in range(self.settings.retry.max_attempts):
                event.attempts = attempt + 1
                try:
                    result = await adapter.complete(request, upstream_model, request_id)
                    break
                except UpstreamError as exc:
                    if not exc.retryable or attempt + 1 >= self.settings.retry.max_attempts:
                        raise
                    await self._backoff(attempt)
            if result is None:
                raise GatewayError("Upstream returned no result", status_code=502)
            self._copy_usage(event, result.usage)
            validate_content(result.content, requested_schema(request.response_format))
            return self._response(request, result, request_id), request_id
        except GatewayError as exc:
            event.status = "error"
            event.status_code = exc.status_code
            event.error_type = exc.error_type
            raise
        finally:
            event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
            await self._record_safely(event)

    def stream(
        self,
        request: CompletionRequest,
        prompt_meta: tuple[str | None, int | None],
        client_request: Request,
    ) -> tuple[AsyncIterator[bytes], str]:
        request_id = f"req_{uuid.uuid4().hex}"
        adapter, upstream_model, provider = self.router.resolve(request.model)
        event = UsageEvent(
            request_id=request_id,
            model=request.model,
            provider=provider,
            upstream_model=upstream_model,
            stream=True,
            prompt_id=prompt_meta[0],
            prompt_version=prompt_meta[1],
        )

        async def generate() -> AsyncIterator[bytes]:
            started = time.perf_counter()
            emitted = False
            try:
                for attempt in range(self.settings.retry.max_attempts):
                    event.attempts = attempt + 1
                    try:
                        async for item in adapter.stream(request, upstream_model, request_id):
                            if await client_request.is_disconnected():
                                event.status = "cancelled"
                                event.status_code = 499
                                return
                            if item.usage is not None:
                                self._copy_usage(event, item.usage)
                            if item.delta:
                                if not emitted:
                                    emitted = True
                                    event.first_token_ms = round((time.perf_counter() - started) * 1000, 3)
                                yield self._chunk(request_id, request.model, delta=item.delta)
                            if item.finish_reason:
                                yield self._chunk(
                                    request_id,
                                    request.model,
                                    finish_reason=item.finish_reason,
                                    usage=item.usage,
                                )
                        yield b"data: [DONE]\n\n"
                        return
                    except UpstreamError as exc:
                        if emitted or not exc.retryable or attempt + 1 >= self.settings.retry.max_attempts:
                            raise
                        await self._backoff(attempt)
            except asyncio.CancelledError:
                event.status = "cancelled"
                event.status_code = 499
                raise
            except GatewayError as exc:
                event.status = "error"
                event.status_code = exc.status_code
                event.error_type = exc.error_type
                yield f"data: {json.dumps(error_body(exc), ensure_ascii=False)}\n\n".encode()
                yield b"data: [DONE]\n\n"
            finally:
                event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
                await self._record_safely(event)

        return generate(), request_id

    async def _backoff(self, attempt: int) -> None:
        delay = min(
            self.settings.retry.base_delay_seconds * (2**attempt),
            self.settings.retry.max_delay_seconds,
        )
        await asyncio.sleep(delay * random.uniform(0.75, 1.25))

    async def _record_safely(self, event: UsageEvent) -> None:
        try:
            await self.usage.record(event)
        except Exception:
            logger.exception("Could not record usage for %s", event.request_id)

    @staticmethod
    def _copy_usage(event: UsageEvent, usage: TokenUsage) -> None:
        event.input_tokens = usage.input_tokens
        event.output_tokens = usage.output_tokens
        event.cached_input_tokens = usage.cached_input_tokens
        event.cache_creation_input_tokens = usage.cache_creation_input_tokens

    @staticmethod
    def _response(request: CompletionRequest, result: AdapterResult, request_id: str) -> dict:
        return {
            "id": request_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": result.content},
                    "finish_reason": result.finish_reason,
                }
            ],
            "usage": {
                "prompt_tokens": result.usage.input_tokens,
                "completion_tokens": result.usage.output_tokens,
                "total_tokens": result.usage.total_tokens,
                "prompt_tokens_details": {
                    "cached_tokens": result.usage.cached_input_tokens,
                    "cache_creation_tokens": result.usage.cache_creation_input_tokens,
                },
            },
        }

    @staticmethod
    def _chunk(
        request_id: str,
        model: str,
        *,
        delta: str = "",
        finish_reason: str | None = None,
        usage: TokenUsage | None = None,
    ) -> bytes:
        payload: dict = {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {"index": 0, "delta": {"content": delta} if delta else {}, "finish_reason": finish_reason}
            ],
        }
        if usage is not None:
            payload["usage"] = {
                "prompt_tokens": usage.input_tokens,
                "completion_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
                "prompt_tokens_details": {
                    "cached_tokens": usage.cached_input_tokens,
                    "cache_creation_tokens": usage.cache_creation_input_tokens,
                },
            }
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()
