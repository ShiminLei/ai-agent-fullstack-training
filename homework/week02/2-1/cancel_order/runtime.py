"""第 5 步：让候选 Tool Call 经过受控 Runtime 才能执行。"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from time import perf_counter

from pydantic import BaseModel, ValidationError

from contracts import ExecutionContext, ToolCall, ToolDefinition, ToolResultMessage
from schemas import ToolError


AuditWriter = Callable[[dict], Awaitable[None]]


def success_message(call: ToolCall, output: BaseModel) -> ToolResultMessage:
    return ToolResultMessage(
        tool_call_id=call.id,
        name=call.name,
        content=output.model_dump_json(),
    )


def error_message(call: ToolCall, error: ToolError) -> ToolResultMessage:
    return ToolResultMessage(
        tool_call_id=call.id,
        name=call.name,
        content=json.dumps({"error": error.model_dump()}, ensure_ascii=False),
        is_error=True,
    )


class ToolRuntime:
    def __init__(self, audit_writer: AuditWriter):
        self._tools: dict[str, ToolDefinition] = {}
        self._audit_writer = audit_writer

    def register(self, tool: ToolDefinition) -> None:
        if tool.name in self._tools:
            raise ValueError(f"duplicate tool: {tool.name}")
        self._tools[tool.name] = tool

    def model_tools(self, ctx: ExecutionContext) -> list[dict]:
        """模型只看到当前身份有资格使用的工具及其公开 Schema。"""

        return [
            tool.to_model_tool()
            for tool in self._tools.values()
            if tool.permission in ctx.permissions
        ]

    async def execute(
        self,
        call: ToolCall,
        ctx: ExecutionContext,
    ) -> ToolResultMessage:
        started = perf_counter()
        tool = self._tools.get(call.name)

        if tool is None:
            return await self._reject(
                call, ctx, started, "tool_lookup",
                ToolError(code="TOOL_NOT_FOUND", message="工具不存在或当前不可用"),
            )

        try:
            args = tool.input_model.model_validate_json(call.arguments_json)
        except ValidationError as exc:
            issues = [
                {
                    "path": ".".join(str(part) for part in item["loc"]),
                    "message": item["msg"],
                }
                for item in exc.errors(include_url=False, include_input=False)
            ]
            return await self._reject(
                call, ctx, started, "input_validation",
                ToolError(
                    code="INVALID_ARGUMENT",
                    message=json.dumps(issues, ensure_ascii=False),
                ),
            )

        if tool.permission not in ctx.permissions:
            return await self._reject(
                call, ctx, started, "authorization",
                ToolError(
                    code="PERMISSION_DENIED",
                    message="当前身份没有 order:write 权限",
                ),
            )

        if tool.risk == "high" and call.id not in ctx.approved_call_ids:
            return await self._reject(
                call, ctx, started, "approval",
                ToolError(
                    code="APPROVAL_REQUIRED",
                    message="取消订单属于高风险操作，需要用户确认",
                ),
            )

        # 非幂等工具强制只执行一次，即使配置被误设为允许重试。
        attempts_allowed = 1 if not tool.idempotent else tool.max_retries + 1
        for attempt in range(1, attempts_allowed + 1):
            try:
                async with asyncio.timeout(tool.timeout_seconds):
                    raw_output = await tool.handler(args, ctx)
                output = tool.output_model.model_validate(raw_output)
            except TimeoutError:
                return await self._fail(
                    call, ctx, started, attempt,
                    ToolError(
                        code="TIMEOUT",
                        message="取消请求超时；结果未知，禁止自动重试",
                        retryable=False,
                    ),
                )
            except ValidationError:
                return await self._fail(
                    call, ctx, started, attempt,
                    ToolError(
                        code="INVALID_OUTPUT",
                        message="工具返回结果不符合 Output Schema",
                    ),
                )
            except Exception:
                return await self._fail(
                    call, ctx, started, attempt,
                    ToolError(code="UPSTREAM_ERROR", message="取消订单失败"),
                )

            await self._write_audit(
                call=call,
                ctx=ctx,
                started=started,
                outcome="success",
                stage="handler",
                handler_invoked=True,
                attempt=attempt,
            )
            return success_message(call, output)

        raise AssertionError("unreachable")

    async def _reject(
        self,
        call: ToolCall,
        ctx: ExecutionContext,
        started: float,
        stage: str,
        error: ToolError,
    ) -> ToolResultMessage:
        await self._write_audit(
            call=call,
            ctx=ctx,
            started=started,
            outcome="rejected",
            stage=stage,
            handler_invoked=False,
            attempt=0,
            error_code=error.code,
        )
        return error_message(call, error)

    async def _fail(
        self,
        call: ToolCall,
        ctx: ExecutionContext,
        started: float,
        attempt: int,
        error: ToolError,
    ) -> ToolResultMessage:
        await self._write_audit(
            call=call,
            ctx=ctx,
            started=started,
            outcome="failed",
            stage="handler",
            handler_invoked=True,
            attempt=attempt,
            error_code=error.code,
        )
        return error_message(call, error)

    async def _write_audit(
        self,
        *,
        call: ToolCall,
        ctx: ExecutionContext,
        started: float,
        outcome: str,
        stage: str,
        handler_invoked: bool,
        attempt: int,
        error_code: str | None = None,
    ) -> None:
        await self._audit_writer({
            "trace_id": ctx.trace_id,
            "tool_call_id": call.id,
            "tool_name": call.name,
            "user_id": ctx.user_id,
            "tenant_id": ctx.tenant_id,
            "outcome": outcome,
            "stage": stage,
            "handler_invoked": handler_invoked,
            "attempt": attempt,
            "latency_ms": int((perf_counter() - started) * 1000),
            "error_code": error_code,
        })
