from __future__ import annotations

import json
import re
from typing import Any

from jsonschema import ValidationError, validate

from app.errors import GatewayError

JSON_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.DOTALL | re.IGNORECASE)


def requested_schema(response_format: dict[str, Any] | None) -> dict[str, Any] | None:
    if not response_format or response_format.get("type") != "json_schema":
        return None
    schema = (response_format.get("json_schema") or {}).get("schema")
    if not isinstance(schema, dict):
        raise GatewayError(
            "response_format.json_schema.schema is required",
            status_code=422,
            error_type="invalid_request_error",
            code="invalid_response_format",
        )
    return schema


def validate_content(content: str, schema: dict[str, Any] | None) -> None:
    if schema is None:
        return
    fenced = JSON_FENCE.match(content)
    source = fenced.group(1) if fenced else content
    try:
        value = json.loads(source)
    except json.JSONDecodeError as exc:
        raise GatewayError(
            "Model output is not valid JSON",
            status_code=422,
            error_type="structured_output_error",
            code="invalid_model_output",
            details={"line": exc.lineno, "column": exc.colno},
        ) from exc
    try:
        validate(value, schema)
    except ValidationError as exc:
        raise GatewayError(
            "Model output does not match the requested JSON Schema",
            status_code=422,
            error_type="structured_output_error",
            code="invalid_model_output",
            details={"path": list(exc.absolute_path), "message": exc.message},
        ) from exc
