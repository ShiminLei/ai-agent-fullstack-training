from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, SecretStr, model_validator


class ProviderConfig(BaseModel):
    """描述一个上游服务，以及它使用哪一种 API 协议。"""

    protocol: Literal["responses", "anthropic"]
    base_url: str
    api_key: SecretStr
    timeout_seconds: float = Field(default=120, gt=0)
    anthropic_version: str = "2023-06-01"


class ModelConfig(BaseModel):
    """把网关公开模型名映射到供应商和真实模型名。"""

    provider: str
    upstream_model: str


class Settings(BaseModel):
    providers: dict[str, ProviderConfig]
    models: dict[str, ModelConfig]

    @model_validator(mode="after")
    def models_must_reference_existing_providers(self) -> Settings:
        unknown = {
            model.provider
            for model in self.models.values()
            if model.provider not in self.providers
        }
        if unknown:
            raise ValueError(f"Unknown providers: {sorted(unknown)}")
        return self

