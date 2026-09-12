from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, SecretStr, model_validator

_ENV = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?}")


def _expand(value: object) -> object:
    if isinstance(value, str):
        return _ENV.sub(lambda m: os.getenv(m.group(1), m.group(2) or ""), value)
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    return value


class ProviderConfig(BaseModel):
    protocol: Literal["responses", "anthropic"]
    base_url: str
    api_key: SecretStr = SecretStr("")
    timeout_seconds: float = Field(default=120, gt=0)
    anthropic_version: str = "2023-06-01"

    @model_validator(mode="after")
    def normalize_url(self) -> ProviderConfig:
        self.base_url = self.base_url.rstrip("/")
        return self


class ModelConfig(BaseModel):
    provider: str
    upstream_model: str
    requests_per_minute: int = Field(default=60, ge=1)
    burst: int = Field(default=10, ge=1)


class RetryConfig(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=3)
    base_delay_seconds: float = Field(default=0.25, ge=0)
    max_delay_seconds: float = Field(default=4, ge=0)
    statuses: set[int] = {408, 409, 429, 500, 502, 503, 504}


class Settings(BaseModel):
    api_keys: list[SecretStr] = Field(default_factory=list)
    database_url: str = "data/gateway.db"
    providers: dict[str, ProviderConfig]
    models: dict[str, ModelConfig]
    retry: RetryConfig = Field(default_factory=RetryConfig)

    @model_validator(mode="after")
    def validate_models(self) -> Settings:
        unknown = {item.provider for item in self.models.values() if item.provider not in self.providers}
        if unknown:
            raise ValueError(f"Unknown providers: {sorted(unknown)}")
        return self


def load_settings(path: str | Path | None = None) -> Settings:
    config_path = Path(path or os.getenv("GATEWAY_CONFIG", "gateway.yaml"))
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    settings = Settings.model_validate(_expand(raw))
    database_path = Path(settings.database_url)
    if not database_path.is_absolute():
        settings.database_url = str((config_path.parent / database_path).resolve())
    return settings
