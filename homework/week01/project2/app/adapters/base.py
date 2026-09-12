from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from app.schemas import AdapterResult, CompletionRequest, StreamEvent


class BaseAdapter(ABC):
    """所有上游协议适配器必须遵守的统一契约。"""

    @abstractmethod
    async def complete(self, request: CompletionRequest, upstream_model: str) -> AdapterResult:
        """执行一次非流式模型调用。"""

        raise NotImplementedError

    @abstractmethod
    def stream(self, request: CompletionRequest, upstream_model: str) -> AsyncIterator[StreamEvent]:
        """执行一次流式模型调用，并产生统一流事件。"""

        raise NotImplementedError

