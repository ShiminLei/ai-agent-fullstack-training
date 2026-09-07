import asyncio
from typing import Any

from app.model import ModelStreamEvent


class FakeStreamingModel:
    def __init__(self, deltas: list[str]) -> None:
        self.deltas = deltas
        self.received_messages: list[list[dict[str, Any]]] = []

    async def stream(
        self,
        messages: list[dict[str, Any]],
    ):
        self.received_messages.append(messages)

        for delta in self.deltas:
            await asyncio.sleep(0)

            yield ModelStreamEvent(
                type="text.delta",
                data={"delta": delta},
            )

        yield ModelStreamEvent(
            type="model.finished",
            data={"finish_reason": "stop"},
        )

        yield ModelStreamEvent(
            type="model.usage",
            data={
                "input_tokens": 10,
                "output_tokens": len(self.deltas),
                "total_tokens": 10 + len(self.deltas),
            },
        )
