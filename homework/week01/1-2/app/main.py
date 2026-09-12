import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field

from app.model import DeepSeekChatAdapter, StreamingModel
from app.persistence import SQLiteRunStore
from app.runner import execute_run
from app.sse import stream_run_events
from app.store import InMemoryRunStore, TERMINAL_STATUSES


SYSTEM_PROMPT = "你是代码审查助手，只报告有证据的问题。"
PROJECT_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = PROJECT_DIR / "static"
DEFAULT_DATABASE_PATH = PROJECT_DIR / "data" / "runs.db"


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


def create_app(
    model: StreamingModel | None = None,
    store: InMemoryRunStore | None = None,
) -> FastAPI:
    """创建应用；测试可注入假模型，生产环境则延迟创建真实模型。"""

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        """关闭服务时先停止后台 Run，再释放可持久化 Store。"""

        yield
        tasks = list(application.state.tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        close = getattr(application.state.store, "close", None)
        if close is not None:
            await close()

    app = FastAPI(title="Streaming Agent Gateway", lifespan=lifespan)
    app.state.store = store or InMemoryRunStore()
    app.state.model = model
    app.state.tasks: dict[str, asyncio.Task[None]] = {}

    def resolve_model() -> StreamingModel:
        if app.state.model is None:
            app.state.model = build_model_from_environment()
        return app.state.model

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        """提供本作业的 Web 客户端。"""

        return FileResponse(STATIC_DIR / "index.html")

    @app.post("/v1/runs", status_code=202)
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
            "trace_id": state.trace["trace_id"],
        }

    @app.get("/v1/runs/{run_id}/trace")
    async def get_run_trace(run_id: str) -> dict[str, Any]:
        state = await app.state.store.get(run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="run not found")
        return dict(state.trace)

    @app.get("/v1/runs/{run_id}/checkpoint")
    async def get_run_checkpoint(run_id: str) -> dict[str, Any]:
        state = await app.state.store.get(run_id)
        if state is None:
            raise HTTPException(status_code=404, detail="run not found")
        checkpoint = await app.state.store.get_checkpoint(run_id)
        if checkpoint is None:
            raise HTTPException(status_code=404, detail="checkpoint not found")
        return checkpoint

    @app.get("/v1/runs/{run_id}/events")
    async def get_run_events(
        run_id: str,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
        after_seq: int | None = Query(default=None, ge=-1),
    ) -> StreamingResponse:
        if await app.state.store.get(run_id) is None:
            raise HTTPException(status_code=404, detail="run not found")

        try:
            header_seq = int(last_event_id) if last_event_id is not None else None
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="invalid Last-Event-ID",
            ) from exc

        # EventSource 页面刷新后不能自定义 Last-Event-ID header，因此也接受 query。
        cursor = (
            after_seq
            if after_seq is not None
            else (header_seq if header_seq is not None else -1)
        )
        if last_event_id is not None or after_seq is not None:
            await app.state.store.increment_trace_counter(run_id, "reconnect_count")

        return StreamingResponse(
            stream_run_events(app.state.store, run_id, cursor),
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

        # 先进入 cancelling，再向后台任务传播强制取消。
        await app.state.store.request_cancel(run_id)
        task = app.state.tasks.get(run_id)
        if task is not None:
            task.cancel()
            try:
                # 等待 CancelledError 清理分支写完 checkpoint 和终态。
                await task
            except asyncio.CancelledError:
                pass

        state = await app.state.store.require(run_id)
        if state.status not in TERMINAL_STATUSES:
            # Task 可能在第一次获得执行机会前就被取消，此时由接口补写终态。
            now = datetime.now(timezone.utc)
            partial_text = "".join(
                event.data["delta"]
                for event in state.events
                if event.type == "text.delta"
            )
            await app.state.store.append(
                run_id,
                "run.cancelled",
                {
                    "reason": "user_requested",
                    "partial_text": partial_text,
                    "last_seq": state.next_seq - 1,
                    "usage": {},
                    "cancelled_at": now.isoformat(),
                },
            )
            cancel_requested_at = state.trace.get("cancel_requested_at")
            cancel_latency_ms = None
            if cancel_requested_at:
                requested_at = datetime.fromisoformat(str(cancel_requested_at))
                cancel_latency_ms = round(
                    (now - requested_at).total_seconds() * 1000,
                    3,
                )
            started_at = state.trace.get("started_at")
            total_duration_ms = None
            if started_at:
                started = datetime.fromisoformat(str(started_at))
                total_duration_ms = round(
                    (now - started).total_seconds() * 1000,
                    3,
                )
            await app.state.store.update_trace(
                run_id,
                cancel_latency_ms=cancel_latency_ms,
                total_duration_ms=total_duration_ms,
                token_usage={},
                completed_at=now.isoformat(),
                final_status="cancelled",
            )
            await app.state.store.save_checkpoint(
                run_id,
                loop_step="cancelled",
                completed=[],
                next_cursor=None,
                context_digest=str(state.trace.get("context_digest", "")),
                tool_state=[],
                partial_text=partial_text,
                usage={},
            )

        return {"run_id": run_id, "status": "cancelled"}

    return app


app = create_app(
    store=SQLiteRunStore(
        os.environ.get("RUN_DATABASE_PATH", str(DEFAULT_DATABASE_PATH))
    )
)
