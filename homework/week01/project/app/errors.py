from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


class GatewayError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 500,
        error_type: str = "gateway_error",
        code: str = "gateway_error",
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_type = error_type
        self.code = code
        self.details = details


class UpstreamError(GatewayError):
    def __init__(self, message: str, *, status_code: int = 502, retryable: bool = False, details: Any = None):
        super().__init__(
            message,
            status_code=status_code,
            error_type="upstream_error",
            code="upstream_error",
            details=details,
        )
        self.retryable = retryable


def error_body(error: GatewayError) -> dict[str, Any]:
    body: dict[str, Any] = {
        "error": {"message": error.message, "type": error.error_type, "code": error.code}
    }
    if error.details is not None:
        body["error"]["details"] = error.details
    return body


async def handle_gateway_error(_: Request, error: GatewayError) -> JSONResponse:
    headers = {"Retry-After": "1"} if error.status_code == 429 else None
    return JSONResponse(error_body(error), status_code=error.status_code, headers=headers)
