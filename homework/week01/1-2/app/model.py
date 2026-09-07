from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from openai import AsyncOpenAI


ModelEventType = Literal[
    "text.delta",
    "model.finished",
    "model.usage",
]


@dataclass(frozen=True, slots=True)
class ModelStreamEvent:
    type: ModelEventType
    data: dict[str, Any]


class StreamingModel(Protocol):
    def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        ...


class DeepSeekChatAdapter:
    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        self.client = client
        self.model = model

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            stream=True,
            max_tokens=1024,
            stream_options={"include_usage": True},
            extra_body={"thinking": {"type": "disabled"}},
        )

        async for chunk in response:
            choice = chunk.choices[0] if chunk.choices else None

            if choice and choice.delta.content:
                yield ModelStreamEvent(
                    type="text.delta",
                    data={"delta": choice.delta.content},
                )

            if choice and choice.finish_reason:
                yield ModelStreamEvent(
                    type="model.finished",
                    data={"finish_reason": choice.finish_reason},
                )

            if chunk.usage is not None:
                yield ModelStreamEvent(
                    type="model.usage",
                    data={
                        "input_tokens": chunk.usage.prompt_tokens,
                        "output_tokens": chunk.usage.completion_tokens,
                        "total_tokens": chunk.usage.total_tokens,
                    },
                )
