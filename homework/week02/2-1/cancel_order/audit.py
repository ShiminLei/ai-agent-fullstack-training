"""第 3 步：用 trace_id 和 tool_call_id 记录每次执行决策。"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class InMemoryAuditWriter:
    """课堂演示用审计存储；生产环境通常写入日志或可观测平台。"""

    events: list[dict[str, Any]] = field(default_factory=list)

    async def write(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def find(self, *, trace_id: str, tool_call_id: str) -> list[dict[str, Any]]:
        return [
            event
            for event in self.events
            if event["trace_id"] == trace_id
            and event["tool_call_id"] == tool_call_id
        ]
