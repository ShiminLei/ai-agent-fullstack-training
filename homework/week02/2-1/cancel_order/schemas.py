"""第 1 步：定义 cancel_order 的输入、输出和错误协议。"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    """拒绝额外字段，也拒绝偷偷进行类型转换。"""

    model_config = ConfigDict(extra="forbid", strict=True)


class CancelOrderInput(StrictModel):
    """模型唯一可以填写的业务参数。"""

    order_id: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=1, max_length=200)


class CancelOrderOutput(StrictModel):
    """业务 Handler 成功后必须返回的稳定结构。"""

    order_id: str
    status: Literal["cancelled"]
    operation_id: str


class ToolError(StrictModel):
    """与课程示例相同的统一错误协议。"""

    code: Literal[
        "INVALID_ARGUMENT",
        "TOOL_NOT_FOUND",
        "PERMISSION_DENIED",
        "APPROVAL_REQUIRED",
        "TIMEOUT",
        "UPSTREAM_ERROR",
        "INVALID_OUTPUT",
    ]
    message: str
    retryable: bool = False
