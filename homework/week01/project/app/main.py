from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse

from app import __version__
from app.config import Settings, load_settings
from app.errors import GatewayError
from app.gateway import Gateway
from app.observability import UsageRepository
from app.prompts import PromptRepository
from app.rate_limit import ModelRateLimiter
from app.router import ModelRouter
from app.schemas import AdapterResult, CompletionRequest, PromptCreate, PromptRecord, StreamEvent


def error_body(error: GatewayError) -> dict:
    body = {
        "error": {
            "message": error.message,
            "type": error.error_type,
            "code": error.code,
        }
    }
    if error.details is not None:
        body["error"]["details"] = error.details
    return body


async def authenticate(
    request: Request,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
) -> str:
    configured = [
        key.get_secret_value()
        for key in request.app.state.settings.api_keys
        if key.get_secret_value()
    ]
    supplied = x_api_key
    if authorization and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if configured and (
        not supplied or not any(hmac.compare_digest(supplied, key) for key in configured)
    ):
        raise GatewayError(
            "Invalid or missing API key",
            status_code=401,
            error_type="authentication_error",
            code="invalid_api_key",
        )
    return hashlib.sha256((supplied or "anonymous").encode()).hexdigest()[:16]


Identity = Annotated[str, Depends(authenticate)]


def completion_body(
    request_id: str,
    model: str,
    result: AdapterResult,
) -> dict:
    return {
        "id": request_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": result.content},
                "finish_reason": result.finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": result.usage.input_tokens,
            "completion_tokens": result.usage.output_tokens,
            "total_tokens": result.usage.total_tokens,
            "prompt_tokens_details": {
                "cached_tokens": result.usage.cached_input_tokens,
                "cache_creation_tokens": result.usage.cache_creation_input_tokens,
            },
        },
    }


def stream_chunk(request_id: str, model: str, event: StreamEvent) -> bytes:
    payload: dict = {
        "id": request_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {"content": event.delta} if event.delta else {},
                "finish_reason": event.finish_reason,
            }
        ],
    }
    if event.usage is not None:
        payload["usage"] = {
            "prompt_tokens": event.usage.input_tokens,
            "completion_tokens": event.usage.output_tokens,
            "total_tokens": event.usage.total_tokens,
        }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


def create_app(settings: Settings, *, http_client: httpx.AsyncClient | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database_path = Path(settings.database_url)
        database_path.parent.mkdir(parents=True, exist_ok=True)
        prompts = PromptRepository(str(database_path))
        usage = UsageRepository(str(database_path))
        await prompts.initialize()
        await usage.initialize()
        client = http_client or httpx.AsyncClient()
        app.state.settings = settings
        app.state.prompts = prompts
        app.state.usage = usage
        app.state.rate_limiter = ModelRateLimiter(settings)
        app.state.gateway = Gateway(settings, ModelRouter(settings, client), prompts, usage)
        yield
        if http_client is None:
            await client.aclose()

    app = FastAPI(title="Multi-Protocol LLM Gateway", version=__version__, lifespan=lifespan)

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, error: GatewayError) -> JSONResponse:
        headers = {"Retry-After": "1"} if error.status_code == 429 else None
        return JSONResponse(error_body(error), status_code=error.status_code, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "message": "Invalid request",
                    "type": "invalid_request_error",
                    "code": "validation_error",
                    "details": error.errors(),
                }
            },
            status_code=422,
        )

    @app.post("/v1/chat/completions")
    async def completions(payload: CompletionRequest, request: Request, identity: Identity):
        request_id = f"req_{uuid.uuid4().hex}"
        await request.app.state.rate_limiter.check(payload.model)
        if payload.stream:
            events = await request.app.state.gateway.stream(
                payload,
                request_id,
                identity,
                request.is_disconnected,
            )

            async def sse() -> AsyncIterator[bytes]:
                try:
                    async for event in events:
                        yield stream_chunk(request_id, payload.model, event)
                except GatewayError as error:
                    yield f"data: {json.dumps(error_body(error), ensure_ascii=False)}\n\n".encode()
                yield b"data: [DONE]\n\n"

            return StreamingResponse(
                sse(),
                media_type="text/event-stream",
                headers={
                    "X-Request-ID": request_id,
                    "Cache-Control": "no-cache, no-transform",
                    "X-Accel-Buffering": "no",
                },
            )
        result = await request.app.state.gateway.complete(payload, request_id, identity)
        return JSONResponse(
            completion_body(request_id, payload.model, result),
            headers={"X-Request-ID": request_id},
        )

    @app.get("/v1/models")
    async def models(_: Identity):
        return {
            "object": "list",
            "data": [{"id": model, "object": "model"} for model in settings.models],
        }

    @app.post("/v1/prompts", response_model=PromptRecord, status_code=201)
    async def create_prompt(payload: PromptCreate, request: Request, _: Identity):
        return await request.app.state.prompts.create(payload)

    @app.get("/v1/prompts/{prompt_id}", response_model=PromptRecord)
    async def get_prompt(
        prompt_id: str,
        request: Request,
        _: Identity,
        version: int | None = None,
    ):
        return await request.app.state.prompts.get(prompt_id, version)

    @app.get("/admin/usage")
    async def usage(request: Request, _: Identity, limit: int = Query(default=100, ge=1, le=1000)):
        return {"data": await request.app.state.usage.recent(limit)}

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app(load_settings())
