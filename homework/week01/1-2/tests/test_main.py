import asyncio
from collections.abc import AsyncIterator
from typing import Any

from fastapi.testclient import TestClient

from app.main import create_app
from app.model import ModelStreamEvent
from tests.fakes import FakeStreamingModel


class BlockingModel:
    async def stream(
        self,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ModelStreamEvent]:
        yield ModelStreamEvent(type="text.delta", data={"delta": "部分"})
        await asyncio.Event().wait()


def test_create_stream_and_get_completed_run() -> None:
    app = create_app(FakeStreamingModel(["你", "好"]))

    with TestClient(app) as client:
        created = client.post("/v1/runs", json={"prompt": "打个招呼"})
        assert created.status_code == 201
        run_id = created.json()["run_id"]

        streamed = client.get(f"/v1/runs/{run_id}/events")
        assert streamed.status_code == 200
        assert streamed.headers["content-type"].startswith("text/event-stream")
        assert "event: run.started" in streamed.text
        assert streamed.text.count("event: text.delta") == 2
        assert "event: run.completed" in streamed.text

        status = client.get(f"/v1/runs/{run_id}")
        assert status.json() == {
            "run_id": run_id,
            "status": "completed",
            "next_seq": 4,
        }


def test_events_support_last_event_id_replay() -> None:
    app = create_app(FakeStreamingModel(["你", "好"]))

    with TestClient(app) as client:
        run_id = client.post(
            "/v1/runs",
            json={"prompt": "打个招呼"},
        ).json()["run_id"]

        streamed = client.get(
            f"/v1/runs/{run_id}/events",
            headers={"Last-Event-ID": "1"},
        )

        assert "id: 0\n" not in streamed.text
        assert "id: 1\n" not in streamed.text
        assert "id: 2\n" in streamed.text
        assert "event: run.completed" in streamed.text


def test_invalid_last_event_id_returns_400() -> None:
    app = create_app(FakeStreamingModel([]))

    with TestClient(app) as client:
        run_id = client.post("/v1/runs", json={"prompt": "测试"}).json()[
            "run_id"
        ]
        response = client.get(
            f"/v1/runs/{run_id}/events",
            headers={"Last-Event-ID": "not-a-number"},
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "invalid Last-Event-ID"}


def test_cancel_run_stops_task_and_creates_terminal_event() -> None:
    app = create_app(BlockingModel())

    with TestClient(app) as client:
        run_id = client.post("/v1/runs", json={"prompt": "慢任务"}).json()[
            "run_id"
        ]
        cancelled = client.post(f"/v1/runs/{run_id}/cancel")
        streamed = client.get(f"/v1/runs/{run_id}/events")

        assert cancelled.status_code == 200
        assert cancelled.json() == {"run_id": run_id, "status": "cancelled"}
        assert "event: run.cancelled" in streamed.text


def test_missing_run_returns_404() -> None:
    app = create_app(FakeStreamingModel([]))

    with TestClient(app) as client:
        assert client.get("/v1/runs/missing").status_code == 404
        assert client.get("/v1/runs/missing/events").status_code == 404
        assert client.post("/v1/runs/missing/cancel").status_code == 404
