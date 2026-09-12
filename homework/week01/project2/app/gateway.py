from __future__ import annotations

from collections.abc import AsyncIterator

from app.prompts import PromptRepository
from app.router import ModelRouter
from app.schemas import AdapterResult, CompletionRequest, Message, StreamEvent
from app.structured import requested_schema, validate_content


class Gateway:
    """编排一次统一调用；协议转换工作全部交给 Adapter。"""

    def __init__(self, router: ModelRouter, prompts: PromptRepository) -> None:
        self.router = router
        self.prompts = prompts

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

    async def complete(self, request: CompletionRequest) -> AdapterResult:
        prepared, _ = await self._prepare(request)
        route = self.router.resolve(prepared.model)
        result = await route.adapter.complete(prepared, route.upstream_model)
        validate_content(result.content, requested_schema(prepared.response_format))
        return result

    async def stream(self, request: CompletionRequest) -> AsyncIterator[StreamEvent]:
        prepared, _ = await self._prepare(request)
        route = self.router.resolve(prepared.model)

        async def generate() -> AsyncIterator[StreamEvent]:
            async for event in route.adapter.stream(prepared, route.upstream_model):
                yield event

        return generate()
