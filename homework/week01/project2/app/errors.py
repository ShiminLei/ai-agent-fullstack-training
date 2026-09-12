from __future__ import annotations

from typing import Any


class GatewayError(Exception):
    """网关向调用方暴露的统一错误。"""

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
    """上游模型服务调用失败，并标记该错误是否适合重试。"""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        retryable: bool = False,
        details: Any = None,
    ) -> None:
        super().__init__(
            message,
            status_code=status_code,
            error_type="upstream_error",
            code="upstream_error",
            details=details,
        )
        self.retryable = retryable

