from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal, Protocol


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