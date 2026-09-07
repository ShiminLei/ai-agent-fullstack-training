import pytest

from app.store import InMemoryRunStore


@pytest.mark.asyncio
async def test_create_and_get_run() -> None:
    store = InMemoryRunStore()

    created = await store.create("run_001")
    found = await store.get("run_001")

    assert found is created
    assert created.run_id == "run_001"
    assert created.status == "created"
    assert created.next_seq == 0
    assert created.events == []


@pytest.mark.asyncio
async def test_create_rejects_duplicate_run_id() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")

    with pytest.raises(ValueError, match="run already exists"):
        await store.create("run_001")


@pytest.mark.asyncio
async def test_append_assigns_sequence_and_updates_status() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")

    started = await store.append("run_001", "run.started")
    delta = await store.append(
        "run_001",
        "text.delta",
        {"delta": "你好"},
    )
    completed = await store.append("run_001", "run.completed")
    state = await store.require("run_001")

    assert [started.seq, delta.seq, completed.seq] == [0, 1, 2]
    assert state.next_seq == 3
    assert state.status == "completed"
    assert state.events == [started, delta, completed]


@pytest.mark.asyncio
async def test_list_after_returns_only_missing_events() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    await store.append("run_001", "run.started")
    second = await store.append(
        "run_001",
        "text.delta",
        {"delta": "你"},
    )
    third = await store.append(
        "run_001",
        "text.delta",
        {"delta": "好"},
    )

    events = await store.list_after("run_001", after_seq=0)

    assert events == [second, third]


@pytest.mark.asyncio
async def test_terminal_run_rejects_more_events() -> None:
    store = InMemoryRunStore()
    await store.create("run_001")
    await store.append("run_001", "run.started")
    await store.append("run_001", "run.completed")

    with pytest.raises(RuntimeError, match="already terminal"):
        await store.append("run_001", "run.failed")


@pytest.mark.asyncio
async def test_request_cancel_sets_cancel_event() -> None:
    store = InMemoryRunStore()
    state = await store.create("run_001")

    await store.request_cancel("run_001")

    assert state.cancel_event.is_set()
