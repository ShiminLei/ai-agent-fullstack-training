from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse

from app import __version__
from app.config import Settings, load_settings
from app.errors import GatewayError, handle_gateway_error
from app.gateway import Gateway
from app.observability import UsageRepository
from app.prompts import PromptRepository
from app.rate_limit import ModelRateLimiter
from app.router import ModelRouter
from app.schemas import CompletionRequest, PromptCreate, PromptRecord
from app.security import authenticate

Identity = Annotated[str, Depends(authenticate)]


def create_app(settings: Settings | None = None, *, http_client: httpx.AsyncClient | None = None) -> FastAPI:
    config = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database_path = Path(config.database_url)
        database_path.parent.mkdir(parents=True, exist_ok=True)
        prompts = PromptRepository(str(database_path))
        usage = UsageRepository(str(database_path))
        await prompts.initialize()
        await usage.initialize()
        client = http_client or httpx.AsyncClient()
        app.state.settings = config
        app.state.prompts = prompts
        app.state.usage = usage
        app.state.rate_limiter = ModelRateLimiter(config)
        app.state.gateway = Gateway(config, ModelRouter(config, client), prompts, usage)
        yield
        if http_client is None:
            await client.aclose()

    app = FastAPI(title="Multi-Protocol LLM Gateway", version=__version__, lifespan=lifespan)
    app.add_exception_handler(GatewayError, handle_gateway_error)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "message": "Invalid request",
                    "type": "invalid_request_error",
                    "code": "validation_error",
                    "details": exc.errors(),
                }
            },
            status_code=422,
        )

    @app.post("/v1/chat/completions")
    async def completions(payload: CompletionRequest, request: Request, _: Identity):
        await request.app.state.rate_limiter.check(payload.model)
        prepared, prompt_meta = await request.app.state.gateway.prepare(payload)
        if payload.stream:
            iterator, request_id = request.app.state.gateway.stream(prepared, prompt_meta, request)
            return StreamingResponse(
                iterator,
                media_type="text/event-stream",
                headers={
                    "X-Request-ID": request_id,
                    "Cache-Control": "no-cache, no-transform",
                    "X-Accel-Buffering": "no",
                },
            )
        result, request_id = await request.app.state.gateway.complete(prepared, prompt_meta)
        return JSONResponse(result, headers={"X-Request-ID": request_id})

    @app.get("/v1/models")
    async def models(_: Identity):
        return {"object": "list", "data": [{"id": name, "object": "model"} for name in config.models]}

    @app.post("/v1/prompts", response_model=PromptRecord, status_code=201)
    async def create_prompt(payload: PromptCreate, request: Request, _: Identity):
        return await request.app.state.prompts.create(payload)

    @app.get("/v1/prompts/{prompt_id}", response_model=PromptRecord)
    async def get_prompt(prompt_id: str, request: Request, _: Identity, version: int | None = None):
        return await request.app.state.prompts.get(prompt_id, version)

    @app.get("/admin/usage")
    async def recent_usage(request: Request, _: Identity, limit: int = Query(default=100, ge=1, le=1000)):
        return {"data": await request.app.state.usage.recent(limit)}

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
