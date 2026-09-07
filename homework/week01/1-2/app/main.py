import asyncio
import os
import uuid
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field

from app.model import DeepSeekChatAdapter, StreamingModel
from app.runner import execute_run
from app.sse import stream_run_events
from app.store import InMemoryRunStore, TERMINAL_STATUSES


SYSTEM_PROMPT = "你是代码审查助手，只报告有证据的问题。"


class CreateRunRequest(BaseModel):
    """创建 Run 时客户端提交的 JSON 请求体。"""

    model_config = ConfigDict(extra="forbid")
    prompt: str = Field(min_length=1)


def build_model_from_environment() -> StreamingModel:
    """从环境变量创建真实 DeepSeek Adapter；导入模块时不会读取 API Key。"""

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("请先设置环境变量 DEEPSEEK_API_KEY")

    client = AsyncOpenAI(
        api_key=api_key,
        base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        timeout=60.0,
        max_retries=0,
    )
    model_name = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
    return DeepSeekChatAdapter(client=client, model=model_name)


def create_app(model: StreamingModel | None = None) -> FastAPI:
    """创建应用；测试可注入假模型，生产环境则延迟创建真实模型。"""

    app = FastAPI(title="Streaming Agent Gateway")
    app.state.store = InMemoryRunStore()
    app.state.model = model
    app.state.tasks: dict[str, asyncio.Task[None]] = {}

    def resolve_model() -> StreamingModel:
        if app.state.model is None:
            app.state.model = build_model_from_environment()
        return app.state.model

    @app.post("/v1/runs", status_code=201)
    async def create_run(body: CreateRunRequest) -> dict[str, str]:
        try:
            run_model = resolve_model()
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        run_id = f"run_{uuid.uuid4().hex}"
        state = await app.state.store.create(run_id)
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": body.prompt},
        ]

        task = asyncio.create_task(
            execute_run(app.state.store, run_model, run_id, messages)
        )
        app.state.tasks[run_id] = task
        task.add_done_callback(lambda _: app.state.tasks.pop(run_id, None))

        return {"run_id": run_id, "status": state.status}

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        state = await app.state.store.get(run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="run not found")

        return {
            "run_id": state.run_id,
            "status": state.status,
            "next_seq": state.next_seq,
        }

    @app.get("/v1/runs/{run_id}/events")
    async def get_run_events(
        run_id: str,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        if await app.state.store.get(run_id) is None:
            raise HTTPException(status_code=404, detail="run not found")

        try:
            after_seq = int(last_event_id) if last_event_id is not None else -1
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="invalid Last-Event-ID",
            ) from exc

        return StreamingResponse(
            stream_run_events(app.state.store, run_id, after_seq),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @app.post("/v1/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> dict[str, str]:
        state = await app.state.store.get(run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="run not found")

        if state.status in TERMINAL_STATUSES:
            return {"run_id": run_id, "status": state.status}

        await app.state.store.request_cancel(run_id)
        partial_text = "".join(
            event.data["delta"]
            for event in state.events
            if event.type == "text.delta"
        )
        try:
            await app.state.store.append(
                run_id,
                "run.cancelled",
                {"reason": "user_requested", "partial_text": partial_text},
            )
        except RuntimeError:
            # Agent Loop 可能恰好先写入了 completed/failed，保留先到达的终态。
            state = await app.state.store.require(run_id)
            return {"run_id": run_id, "status": state.status}

        task = app.state.tasks.get(run_id)
        if task is not None:
            task.cancel()

        return {"run_id": run_id, "status": "cancelled"}

    return app


app = create_app()
