import pytest
from pydantic import ValidationError

from app.events import RunEvent


def test_create_text_delta_event() -> None:
    event = RunEvent(
        run_id="run_001",
        seq=1,
        type="text.delta",
        data={"delta": "你好"},
    )

    assert event.schema_version == "1"
    assert event.run_id == "run_001"
    assert event.seq == 1
    assert event.data["delta"] == "你好"
    assert event.created_at.tzinfo is not None
    assert event.is_terminal() is False


def test_completed_event_is_terminal() -> None:
    event = RunEvent(
        run_id="run_001",
        seq=2,
        type="run.completed",
    )

    assert event.is_terminal() is True


def test_negative_seq_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RunEvent(
            run_id="run_001",
            seq=-1,
            type="run.started",
        )


def test_unknown_event_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RunEvent(
            run_id="run_001",
            seq=0,
            type="something.unknown",
        )


def test_unknown_field_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RunEvent(
            run_id="run_001",
            seq=0,
            type="run.started",
            unexpected="value",
        )