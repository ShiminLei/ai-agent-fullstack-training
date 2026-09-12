from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable

from app.config import Settings
from app.errors import GatewayError, UpstreamError
from app.observability import UsageEvent, UsageRepository
from app.prompts import PromptRepository
from app.router import ModelRouter
from app.schemas import AdapterResult, CompletionRequest, Message, StreamEvent, TokenUsage
from app.structured import requested_schema, validate_content

logger = logging.getLogger(__name__)


class Gateway:
    """管理路由、Prompt、重试和观测，并把协议转换交给 Adapter。"""

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

    async def _prepare(
        self,
        request: CompletionRequest,
    ) -> tuple[CompletionRequest, tuple[str | None, int | None]]:
        if request.prompt is None:
            return request, (None, None)
        prompt, rendered = await self.prompts.render(
            request.prompt.id,
            request.prompt.variables,
            request.prompt.version,
        )
        prepared = request.model_copy(deep=True)
        prepared.messages.insert(0, Message(role="system", content=rendered))
        prepared.prompt = None
        return prepared, (prompt.id, prompt.version)

    async def complete(
        self,
        request: CompletionRequest,
        request_id: str,
        identity: str,
    ) -> AdapterResult:
        started = time.perf_counter()
        event = UsageEvent(request_id=request_id, api_key_hash=identity, model=request.model)
        try:
            prepared, prompt_meta = await self._prepare(request)
            event.prompt_id, event.prompt_version = prompt_meta
            route = self.router.resolve(prepared.model)
            event.provider = route.provider
            event.upstream_model = route.upstream_model

            result: AdapterResult | None = None
            for attempt in range(self.settings.retry.max_attempts):
                event.attempts = attempt + 1
                try:
                    result = await route.adapter.complete(
                        prepared,
                        route.upstream_model,
                        request_id,
                    )
                    break
                except UpstreamError as error:
                    if not error.retryable or attempt + 1 >= self.settings.retry.max_attempts:
                        raise
                    await self._backoff(attempt)

            if result is None:
                raise GatewayError("Upstream returned no result", status_code=502)
            self._copy_usage(event, result.usage)
            validate_content(result.content, requested_schema(prepared.response_format))
            return result
        except GatewayError as error:
            event.status = "error"
            event.status_code = error.status_code
            event.error_type = error.error_type
            raise
        finally:
            event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
            await self._record_safely(event)

    async def stream(
        self,
        request: CompletionRequest,
        request_id: str,
        identity: str,
        is_disconnected: Callable[[], Awaitable[bool]] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        started = time.perf_counter()
        event = UsageEvent(
            request_id=request_id,
            api_key_hash=identity,
            model=request.model,
            stream=True,
        )
        try:
            prepared, prompt_meta = await self._prepare(request)
            event.prompt_id, event.prompt_version = prompt_meta
            route = self.router.resolve(prepared.model)
            event.provider = route.provider
            event.upstream_model = route.upstream_model
        except GatewayError as error:
            event.status = "error"
            event.status_code = error.status_code
            event.error_type = error.error_type
            event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
            await self._record_safely(event)
            raise

        async def generate() -> AsyncIterator[StreamEvent]:
            emitted = False
            try:
                for attempt in range(self.settings.retry.max_attempts):
                    event.attempts = attempt + 1
                    try:
                        async for item in route.adapter.stream(
                            prepared,
                            route.upstream_model,
                            request_id,
                        ):
                            if is_disconnected is not None and await is_disconnected():
                                event.status = "cancelled"
                                event.status_code = 499
                                return
                            if item.usage is not None:
                                self._copy_usage(event, item.usage)
                            if item.delta and not emitted:
                                emitted = True
                                event.first_token_ms = round(
                                    (time.perf_counter() - started) * 1000,
                                    3,
                                )
                            yield item
                        return
                    except UpstreamError as error:
                        if (
                            emitted
                            or not error.retryable
                            or attempt + 1 >= self.settings.retry.max_attempts
                        ):
                            raise
                        await self._backoff(attempt)
            except asyncio.CancelledError:
                event.status = "cancelled"
                event.status_code = 499
                raise
            except GatewayError as error:
                event.status = "error"
                event.status_code = error.status_code
                event.error_type = error.error_type
                raise
            finally:
                event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
                await self._record_safely(event)

        return generate()

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
            logger.exception("Failed to record usage for %s", event.request_id)

    @staticmethod
    def _copy_usage(event: UsageEvent, usage: TokenUsage) -> None:
        event.input_tokens = usage.input_tokens
        event.output_tokens = usage.output_tokens
        event.cached_input_tokens = usage.cached_input_tokens
        event.cache_creation_input_tokens = usage.cache_creation_input_tokens
