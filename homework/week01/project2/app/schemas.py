from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class FlexibleModel(BaseModel):
    """保留网关暂时不认识的可选字段，便于协议继续扩展。"""

    model_config = ConfigDict(extra="allow")


class Message(FlexibleModel):
    role: Literal["system", "developer", "user", "assistant"]
    content: str


class PromptReference(BaseModel):
    id: str
    version: int | None = Field(default=None, ge=1)
    variables: dict[str, Any] = Field(default_factory=dict)


class CompletionRequest(FlexibleModel):
    """客户端交给 Gateway 的统一请求，与具体供应商协议无关。"""

    model: str
    messages: list[Message] = Field(min_length=1)
    stream: bool = False
    max_tokens: int = Field(default=1024, ge=1)
    temperature: float | None = Field(default=None, ge=0, le=2)
    response_format: dict[str, Any] | None = None
    prompt: PromptReference | None = None


class TokenUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    cache_creation_input_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


class AdapterResult(BaseModel):
    """不同协议的普通响应经过 Adapter 翻译后的统一结果。"""

    content: str
    finish_reason: str | None = None
    usage: TokenUsage = Field(default_factory=TokenUsage)
    upstream_id: str | None = None


class StreamEvent(BaseModel):
    """不同协议的流式事件经过 Adapter 翻译后的统一事件。"""

    delta: str = ""
    finish_reason: str | None = None
    usage: TokenUsage | None = None

