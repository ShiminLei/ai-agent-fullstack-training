from __future__ import annotations

from typing import Any


class UpstreamError(Exception):
    """上游模型服务调用失败，并标记该错误是否适合重试。"""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        retryable: bool = False,
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.details = details

