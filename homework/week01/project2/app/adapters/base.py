from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

import httpx

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


async def iter_sse(response: httpx.Response) -> AsyncIterator[tuple[str | None, str]]:
    """把通用 SSE 字节流拆成 `(event 名称, data 内容)`。"""

    event_name: str | None = None
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if not line:
            if data_lines:
                yield event_name, "\n".join(data_lines)
            event_name = None
            data_lines = []
        elif line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())
    if data_lines:
        yield event_name, "\n".join(data_lines)


def json_body(response: httpx.Response) -> dict:
    """读取 JSON；上游返回非法 JSON 时转换为统一错误。"""

    from app.errors import UpstreamError

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise UpstreamError("Upstream returned invalid JSON") from exc
