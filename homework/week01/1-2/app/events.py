from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


EventType = Literal[
    "run.started",
    "text.delta",
    "tool.started",
    "tool.completed",
    "run.retrying",
    "run.completed",
    "run.failed",
    "run.cancelled",
]


TERMINAL_EVENT_TYPES = frozenset({
    "run.completed",
    "run.failed",
    "run.cancelled",
})


class RunEvent(BaseModel):
    model_config = ConfigDict(extra="forbid") # 遇到未知字段立即报错

    schema_version: Literal["1"] = "1"
    run_id: str = Field(min_length=1)
    seq: int = Field(ge=0)
    type: EventType
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    data: dict[str, Any] = Field(default_factory=dict)

    def is_terminal(self) -> bool:
        return self.type in TERMINAL_EVENT_TYPES