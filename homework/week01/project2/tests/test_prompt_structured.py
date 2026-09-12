from __future__ import annotations

import json

import httpx
import pytest

from tests.test_api import HEADERS, api_client


@pytest.mark.asyncio
async def test_prompt_versions_variables_and_explicit_version_reference(tmp_path):
    upstream_bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        upstream_bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "msg_1",
                "content": [{"type": "text", "text": "ok"}],
                "stop_reason": "end_turn",
                "usage": {},
            },
        )

    async with api_client(handler, str(tmp_path / "gateway.db")) as client:
        version_one = await client.post(
            "/v1/prompts",
            headers=HEADERS,
            json={"id": "teacher", "content": "Old {{ topic }}"},
        )
        version_two = await client.post(
            "/v1/prompts",
            headers=HEADERS,
            json={"id": "teacher", "content": "Teach {{ topic }}"},
        )
        response = await client.post(
            "/v1/chat/completions",
            headers=HEADERS,
            json={
                "model": "flash",
                "messages": [{"role": "user", "content": "start"}],
                "prompt": {
                    "id": "teacher",
                    "version": 1,
                    "variables": {"topic": "Adapter"},
                },
            },
        )

    assert version_one.json()["version"] == 1
    assert version_two.json()["version"] == 2
    assert response.status_code == 200
    assert upstream_bodies[-1]["system"] == "Old Adapter"


@pytest.mark.asyncio
async def test_structured_output_is_forwarded_and_locally_validated(tmp_path):
    upstream_bodies: list[dict] = []
    response_content = ['{"name":"Ada"}', '{"name":123}']

    def handler(request: httpx.Request) -> httpx.Response:
        upstream_bodies.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "resp_1",
                "status": "completed",
                "output_text": response_content.pop(0),
                "usage": {},
            },
        )

    schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    }
    payload = {
        "model": "pro",
        "messages": [{"role": "user", "content": "extract"}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "person", "strict": True, "schema": schema},
        },
    }

    async with api_client(handler, str(tmp_path / "gateway.db")) as client:
        valid = await client.post("/v1/chat/completions", headers=HEADERS, json=payload)
        invalid = await client.post("/v1/chat/completions", headers=HEADERS, json=payload)

    assert valid.status_code == 200
    assert upstream_bodies[0]["text"]["format"]["schema"] == schema
    assert invalid.status_code == 422
    assert invalid.json()["error"]["type"] == "structured_output_error"
