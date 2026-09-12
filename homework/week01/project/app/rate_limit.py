from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from app.config import Settings
from app.errors import GatewayError


@dataclass
class Bucket:
    tokens: float
    updated_at: float


class ModelRateLimiter:
    """按照公开模型名分别维护一个进程内令牌桶。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.buckets: dict[str, Bucket] = {}
        self.lock = asyncio.Lock()

    async def check(self, model: str) -> None:
        config = self.settings.models.get(model)
        if config is None:
            return
        now = time.monotonic()
        capacity = float(config.burst)
        refill_per_second = config.requests_per_minute / 60
        async with self.lock:
            bucket = self.buckets.setdefault(model, Bucket(tokens=capacity, updated_at=now))
            elapsed = now - bucket.updated_at
            bucket.tokens = min(capacity, bucket.tokens + elapsed * refill_per_second)
            bucket.updated_at = now
            if bucket.tokens < 1:
                raise GatewayError(
                    f"Rate limit exceeded for model {model}",
                    status_code=429,
                    error_type="rate_limit_error",
                    code="model_rate_limit_exceeded",
                )
            bucket.tokens -= 1

