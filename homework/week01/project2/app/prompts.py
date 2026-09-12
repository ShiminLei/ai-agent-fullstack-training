from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiosqlite
from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

from app.errors import GatewayError
from app.schemas import PromptCreate, PromptRecord


class PromptRepository:
    """在 SQLite 中保存 Prompt 的每个版本，并负责安全渲染。"""

    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        self.environment = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)

    async def initialize(self) -> None:
        async with aiosqlite.connect(self.database_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS prompts (
                    id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    is_active INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (id, version)
                )
                """
            )
            await db.commit()

    async def create(self, value: PromptCreate) -> PromptRecord:
        created_at = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.database_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM prompts WHERE id = ?",
                (value.id,),
            )
            version = int((await cursor.fetchone())[0])
            if value.activate:
                await db.execute("UPDATE prompts SET is_active = 0 WHERE id = ?", (value.id,))
            await db.execute(
                "INSERT INTO prompts VALUES (?, ?, ?, ?, ?)",
                (value.id, version, value.content, int(value.activate), created_at),
            )
            await db.commit()
        return PromptRecord(
            id=value.id,
            version=version,
            content=value.content,
            is_active=value.activate,
            created_at=created_at,
        )

    async def get(self, prompt_id: str, version: int | None = None) -> PromptRecord:
        query = "SELECT id, version, content, is_active, created_at FROM prompts WHERE id = ?"
        parameters: tuple[Any, ...] = (prompt_id,)
        if version is None:
            query += " AND is_active = 1 ORDER BY version DESC LIMIT 1"
        else:
            query += " AND version = ?"
            parameters = (prompt_id, version)
        async with aiosqlite.connect(self.database_path) as db:
            db.row_factory = aiosqlite.Row
            row = await (await db.execute(query, parameters)).fetchone()
        if row is None:
            raise GatewayError(
                f"Prompt {prompt_id!r} version {version or 'active'} was not found",
                status_code=404,
                error_type="invalid_request_error",
                code="prompt_not_found",
            )
        return PromptRecord(**dict(row))

    async def render(
        self,
        prompt_id: str,
        variables: dict[str, Any],
        version: int | None = None,
    ) -> tuple[PromptRecord, str]:
        prompt = await self.get(prompt_id, version)
        try:
            content = self.environment.from_string(prompt.content).render(**variables)
        except Exception as exc:
            raise GatewayError(
                f"Prompt rendering failed: {exc}",
                status_code=422,
                error_type="invalid_request_error",
                code="prompt_render_error",
            ) from exc
        return prompt, content

