import pytest

from app.persistence import SQLiteRunStore


@pytest.mark.asyncio
async def test_sqlite_store_restores_events_trace_and_checkpoint(tmp_path) -> None:
    database = tmp_path / "runs.db"
    first_store = SQLiteRunStore(database)
    await first_store.create("run_001")
    await first_store.append("run_001", "run.started")
    await first_store.append("run_001", "text.delta", {"delta": "你好"})
    await first_store.update_trace("run_001", ttft_ms=12.5)
    await first_store.save_checkpoint(
        "run_001",
        loop_step="model.streaming",
        completed=[],
        next_cursor=None,
        context_digest="abc123",
        tool_state=[],
        partial_text="你好",
        usage={"total_tokens": 2},
    )
    await first_store.close()

    restored_store = SQLiteRunStore(database)
    restored = await restored_store.require("run_001")
    checkpoint = await restored_store.get_checkpoint("run_001")

    assert [event.seq for event in restored.events] == [0, 1]
    assert [event.type for event in restored.events] == ["run.started", "text.delta"]
    assert restored.trace["ttft_ms"] == 12.5
    assert checkpoint is not None
    assert checkpoint["partial_text"] == "你好"
    assert checkpoint["version"] == 1
    await restored_store.close()
