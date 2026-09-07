from types import SimpleNamespace
from typing import Any

import pytest

from app.model import DeepSeekChatAdapter, ModelStreamEvent


class FakeCompletions:
    def __init__(self, chunks: list[Any]) -> None:
        self.chunks = chunks
        self.received_options: dict[str, Any] | None = None

    async def create(self, **options: Any):
        self.received_options = options

        async def generate_chunks():
            for chunk in self.chunks:
                yield chunk

        return generate_chunks()


class FakeOpenAIClient:
    def __init__(self, chunks: list[Any]) -> None:
        self.completions = FakeCompletions(chunks)
        self.chat = SimpleNamespace(completions=self.completions)


def make_choice_chunk(
    *,
    content: str | None = None,
    finish_reason: str | None = None,
) -> Any:
    choice = SimpleNamespace(
        delta=SimpleNamespace(content=content),
        finish_reason=finish_reason,
    )
    return SimpleNamespace(choices=[choice], usage=None)


def make_usage_chunk() -> Any:
    usage = SimpleNamespace(
        prompt_tokens=10,
        completion_tokens=2,
        total_tokens=12,
    )
    return SimpleNamespace(choices=[], usage=usage)


async def collect_events(adapter: DeepSeekChatAdapter) -> list[ModelStreamEvent]:
    messages = [{"role": "user", "content": "打个招呼"}]
    return [event async for event in adapter.stream(messages)]


@pytest.mark.asyncio
async def test_adapter_converts_deepseek_chunks_to_model_events() -> None:
    client = FakeOpenAIClient(
        [
            make_choice_chunk(content="你"),
            make_choice_chunk(content="好"),
            make_choice_chunk(finish_reason="stop"),
            make_usage_chunk(),
        ]
    )
    adapter = DeepSeekChatAdapter(client=client, model="deepseek-v4-flash")

    events = await collect_events(adapter)

    assert events == [
        ModelStreamEvent(type="text.delta", data={"delta": "你"}),
        ModelStreamEvent(type="text.delta", data={"delta": "好"}),
        ModelStreamEvent(
            type="model.finished",
            data={"finish_reason": "stop"},
        ),
        ModelStreamEvent(
            type="model.usage",
            data={
                "input_tokens": 10,
                "output_tokens": 2,
                "total_tokens": 12,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_adapter_sends_expected_options_to_deepseek() -> None:
    client = FakeOpenAIClient([])
    adapter = DeepSeekChatAdapter(client=client, model="deepseek-v4-flash")

    await collect_events(adapter)

    assert client.completions.received_options == {
        "model": "deepseek-v4-flash",
        "messages": [{"role": "user", "content": "打个招呼"}],
        "stream": True,
        "max_tokens": 1024,
        "stream_options": {"include_usage": True},
        "extra_body": {"thinking": {"type": "disabled"}},
    }
