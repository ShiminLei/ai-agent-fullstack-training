from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from pydantic import ValidationError

from app.adapters.base import BaseAdapter
from app.schemas import AdapterResult, CompletionRequest, StreamEvent, TokenUsage


class FakeAdapter(BaseAdapter):
    async def complete(self, request: CompletionRequest, upstream_model: str) -> AdapterResult:
        return AdapterResult(
            content=f"{upstream_model}: {request.messages[-1].content}",
            finish_reason="stop",
            usage=TokenUsage(input_tokens=3, output_tokens=2),
        )

    async def stream(
        self, request: CompletionRequest, upstream_model: str
    ) -> AsyncIterator[StreamEvent]:
        yield StreamEvent(delta=upstream_model)
        yield StreamEvent(delta=request.messages[-1].content)
        yield StreamEvent(finish_reason="stop", usage=TokenUsage(output_tokens=2))


@pytest.mark.asyncio
async def test_any_adapter_can_be_used_through_the_same_contract():
    request = CompletionRequest(
        model="public-model",
        messages=[{"role": "user", "content": "hello"}],
    )
    adapter: BaseAdapter = FakeAdapter()

    result = await adapter.complete(request, "real-model")
    events = [event async for event in adapter.stream(request, "real-model")]

    assert result.content == "real-model: hello"
    assert result.usage.total_tokens == 5
    assert "".join(event.delta for event in events) == "real-modelhello"
    assert events[-1].finish_reason == "stop"


def test_unified_request_validates_important_constraints_and_keeps_extensions():
    request = CompletionRequest(
        model="public-model",
        messages=[{"role": "user", "content": "hello"}],
        vendor_option="kept-for-adapter",
    )

    assert request.model_extra == {"vendor_option": "kept-for-adapter"}

    with pytest.raises(ValidationError):
        CompletionRequest(model="public-model", messages=[], max_tokens=0)
