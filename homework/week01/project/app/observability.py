from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import aiosqlite


@dataclass
class UsageEvent:
    request_id: str
    model: str
    provider: str | None = None
    upstream_model: str | None = None
    status: str = "success"
    status_code: int = 200
    stream: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    latency_ms: float = 0
    first_token_ms: float | None = None
    attempts: int = 0
    error_type: str | None = None
    prompt_id: str | None = None
    prompt_version: int | None = None


class UsageRepository:
    def __init__(self, database_path: str) -> None:
        self.database_path = database_path

    async def initialize(self) -> None:
        async with aiosqlite.connect(self.database_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS usage_events (
                    request_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    model TEXT NOT NULL,
                    provider TEXT,
                    upstream_model TEXT,
                    status TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    stream INTEGER NOT NULL,
                    input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,
                    cached_input_tokens INTEGER NOT NULL,
                    cache_creation_input_tokens INTEGER NOT NULL,
                    latency_ms REAL NOT NULL,
                    first_token_ms REAL,
                    attempts INTEGER NOT NULL,
                    error_type TEXT,
                    prompt_id TEXT,
                    prompt_version INTEGER
                )
                """
            )
            await db.commit()

    async def record(self, event: UsageEvent) -> None:
        values = asdict(event)
        async with aiosqlite.connect(self.database_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO usage_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    values["request_id"],
                    datetime.now(timezone.utc).isoformat(),
                    values["model"],
                    values["provider"],
                    values["upstream_model"],
                    values["status"],
                    values["status_code"],
                    int(values["stream"]),
                    values["input_tokens"],
                    values["output_tokens"],
                    values["cached_input_tokens"],
                    values["cache_creation_input_tokens"],
                    values["latency_ms"],
                    values["first_token_ms"],
                    values["attempts"],
                    values["error_type"],
                    values["prompt_id"],
                    values["prompt_version"],
                ),
            )
            await db.commit()

    async def recent(self, limit: int) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.database_path) as db:
            db.row_factory = aiosqlite.Row
            rows = await (
                await db.execute(
                    "SELECT * FROM usage_events ORDER BY created_at DESC LIMIT ?", (min(limit, 1000),)
                )
            ).fetchall()
        return [dict(row) for row in rows]
