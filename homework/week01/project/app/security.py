from __future__ import annotations

import hashlib
import hmac

from fastapi import Header, Request

from app.errors import GatewayError


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
    if configured and (not supplied or not any(hmac.compare_digest(supplied, key) for key in configured)):
        raise GatewayError(
            "Invalid or missing API key",
            status_code=401,
            error_type="authentication_error",
            code="invalid_api_key",
        )
    return hashlib.sha256((supplied or "anonymous").encode()).hexdigest()[:16]
