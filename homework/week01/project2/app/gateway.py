from __future__ import annotations

from collections.abc import AsyncIterator

from app.router import ModelRouter
from app.schemas import AdapterResult, CompletionRequest, StreamEvent


class Gateway:
    """编排一次统一调用；协议转换工作全部交给 Adapter。"""

    def __init__(self, router: ModelRouter) -> None:
        self.router = router

    async def complete(self, request: CompletionRequest) -> AdapterResult:
        route = self.router.resolve(request.model)
        return await route.adapter.complete(request, route.upstream_model)

    async def stream(self, request: CompletionRequest) -> AsyncIterator[StreamEvent]:
        route = self.router.resolve(request.model)
        async for event in route.adapter.stream(request, route.upstream_model):
            yield event

