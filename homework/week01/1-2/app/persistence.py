import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

from app.events import EventType, RunEvent
from app.store import InMemoryRunStore, RunState


class SQLiteRunStore(InMemoryRunStore):
    """在内存 Store 的接口之上，将 Run、事件、Trace 和 Checkpoint 落到 SQLite。"""

    def __init__(self, database_path: str | Path) -> None:
        super().__init__()
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        # FastAPI 测试和服务线程可能不同，因此关闭 sqlite 的线程绑定检查；
        # 实际并发仍由 _database_lock 串行保护。
        self._connection = sqlite3.connect(
            self.database_path,
            check_same_thread=False,
        )
        self._database_lock = asyncio.Lock()
        self._create_schema()
        self._load_from_database()

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                next_seq INTEGER NOT NULL,
                trace_json TEXT NOT NULL,
                checkpoint_json TEXT
            );

            CREATE TABLE IF NOT EXISTS run_events (
                run_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (run_id, seq),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );
            """
        )
        self._connection.commit()

    def _load_from_database(self) -> None:
        """进程启动时重建内存索引，使已落库事件可以继续查询和重放。"""

        rows = self._connection.execute(
            "SELECT run_id, status, next_seq, trace_json, checkpoint_json FROM runs"
        ).fetchall()
        for run_id, status, next_seq, trace_json, checkpoint_json in rows:
            state = RunState(
                run_id=run_id,
                status=status,
                next_seq=next_seq,
                trace=json.loads(trace_json),
                checkpoint=(
                    json.loads(checkpoint_json) if checkpoint_json else None
                ),
            )
            event_rows = self._connection.execute(
                """
                SELECT payload_json
                FROM run_events
                WHERE run_id = ?
                ORDER BY seq
                """,
                (run_id,),
            ).fetchall()
            state.events = [
                RunEvent.model_validate_json(payload_json)
                for (payload_json,) in event_rows
            ]
            self._runs[run_id] = state

    async def _persist_state(self, state: RunState) -> None:
        async with self._database_lock:
            self._connection.execute(
                """
                INSERT INTO runs (
                    run_id, status, next_seq, trace_json, checkpoint_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status = excluded.status,
                    next_seq = excluded.next_seq,
                    trace_json = excluded.trace_json,
                    checkpoint_json = excluded.checkpoint_json
                """,
                (
                    state.run_id,
                    state.status,
                    state.next_seq,
                    json.dumps(state.trace, ensure_ascii=False),
                    (
                        json.dumps(state.checkpoint, ensure_ascii=False)
                        if state.checkpoint
                        else None
                    ),
                ),
            )
            self._connection.commit()

    async def create(self, run_id: str) -> RunState:
        state = await super().create(run_id)
        await self._persist_state(state)
        return state

    async def append(
        self,
        run_id: str,
        event_type: EventType,
        data: dict[str, Any] | None = None,
    ) -> RunEvent:
        event = await super().append(run_id, event_type, data)
        state = await self.require(run_id)
        async with self._database_lock:
            self._connection.execute(
                """
                INSERT INTO run_events (run_id, seq, payload_json)
                VALUES (?, ?, ?)
                """,
                (run_id, event.seq, event.model_dump_json()),
            )
            self._connection.execute(
                """
                UPDATE runs
                SET status = ?, next_seq = ?, trace_json = ?, checkpoint_json = ?
                WHERE run_id = ?
                """,
                (
                    state.status,
                    state.next_seq,
                    json.dumps(state.trace, ensure_ascii=False),
                    (
                        json.dumps(state.checkpoint, ensure_ascii=False)
                        if state.checkpoint
                        else None
                    ),
                    run_id,
                ),
            )
            self._connection.commit()
        return event

    async def update_trace(self, run_id: str, **fields: Any) -> dict[str, Any]:
        trace = await super().update_trace(run_id, **fields)
        await self._persist_state(await self.require(run_id))
        return trace

    async def increment_trace_counter(self, run_id: str, field_name: str) -> int:
        value = await super().increment_trace_counter(run_id, field_name)
        await self._persist_state(await self.require(run_id))
        return value

    async def save_checkpoint(self, run_id: str, **fields: Any) -> dict[str, Any]:
        checkpoint = await super().save_checkpoint(run_id, **fields)
        await self._persist_state(await self.require(run_id))
        return checkpoint

    async def request_cancel(self, run_id: str) -> None:
        await super().request_cancel(run_id)
        await self._persist_state(await self.require(run_id))

    async def close(self) -> None:
        """提交剩余事务并关闭数据库连接。"""

        async with self._database_lock:
            self._connection.commit()
            self._connection.close()
