import asyncio
from dataclasses import dataclass, field
from typing import Any, Literal

from app.events import EventType, RunEvent


RunStatus = Literal[
    # Run 刚创建，还没有进入 Agent Loop。
    "created",
    # Agent Loop 正在消费模型流。
    "running",
    # 以下三种状态都是终态：一旦进入就不能再产生新事件。
    "completed",
    "failed",
    "cancelled",
]

# 单独保存终态集合，append() 可以快速判断 Run 是否已经结束。
TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


@dataclass
class RunState:
    """一次 Agent 运行在内存中的全部可变状态。"""

    # 每次运行的唯一标识，RunEvent 也会携带同一个 run_id。
    run_id: str
    # Run 当前位于生命周期中的哪个阶段。
    status: RunStatus = "created"
    # 下一个 RunEvent 应使用的序号；从 0 开始并且只能递增。
    next_seq: int = 0
    # 已产生的完整事件历史，用于查询、审计和断线重放。
    # default_factory 确保每个 RunState 都获得自己独立的列表。
    events: list[RunEvent] = field(default_factory=list)
    # 用户请求取消时会被 set()；Agent Loop 会主动检查这个信号。
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    # 既是保护本 Run 状态的锁，也能在新增事件时唤醒 SSE 订阅者。
    changed: asyncio.Condition = field(default_factory=asyncio.Condition)


class InMemoryRunStore:
    """将所有 Run 保存在当前 Python 进程内的简单 Store。"""

    def __init__(self) -> None:
        # run_id -> RunState，作用类似一张以内存字典实现的 runs 表。
        self._runs: dict[str, RunState] = {}
        # 保护 _runs 字典，避免两个并发请求同时创建或查询时发生竞争。
        self._lock = asyncio.Lock()

    async def create(self, run_id: str) -> RunState:
        """创建并保存一个 Run；重复的 run_id 会被拒绝。"""

        async with self._lock:
            if run_id in self._runs:
                raise ValueError(f"run already exists: {run_id}")

            state = RunState(run_id=run_id)
            self._runs[run_id] = state
            return state

    async def get(self, run_id: str) -> RunState | None:
        """查询 Run；找不到时返回 None，让调用方自行决定如何处理。"""

        async with self._lock:
            return self._runs.get(run_id)

    async def require(self, run_id: str) -> RunState:
        """查询必须存在的 Run；找不到时直接抛出 KeyError。"""

        state = await self.get(run_id)
        if state is None:
            raise KeyError(f"run not found: {run_id}")
        return state

    async def append(
        self,
        run_id: str,
        event_type: EventType,
        data: dict[str, Any] | None = None,
    ) -> RunEvent:
        """为 Run 创建并保存下一个事件，同时推进序号和运行状态。"""

        state = await self.require(run_id)

        # 同一个 Run 的序号分配、事件保存和状态更新必须作为一个整体完成。
        # 否则两个协程同时 append 时，可能拿到相同的 seq。
        async with state.changed:
            # 终态之后禁止再追加事件，从而保证一个 Run 只有一个最终结果。
            if state.status in TERMINAL_STATUSES:
                raise RuntimeError(f"run is already terminal: {run_id}")

            # Store 集中分配 seq，调用方不允许自己决定事件序号。
            event = RunEvent(
                run_id=run_id,
                seq=state.next_seq,
                type=event_type,
                data=data or {},
            )
            state.next_seq += 1
            state.events.append(event)

            # 生命周期事件到来时，同步更新 RunState.status。
            if event_type == "run.started":
                state.status = "running"
            elif event_type == "run.completed":
                state.status = "completed"
            elif event_type == "run.failed":
                state.status = "failed"
            elif event_type == "run.cancelled":
                state.status = "cancelled"

            # 通知正在等待这个 Run 新事件的 SSE 订阅者。
            state.changed.notify_all()
            return event

    async def list_after(
        self,
        run_id: str,
        after_seq: int,
    ) -> list[RunEvent]:
        """返回 seq 大于 after_seq 的事件，供客户端断线后补收。"""

        state = await self.require(run_id)

        async with state.changed:
            return [event for event in state.events if event.seq > after_seq]

    async def request_cancel(self, run_id: str) -> None:
        """发出协作式取消信号；真正停止模型消费由 Agent Loop 完成。"""

        state = await self.require(run_id)
        # asyncio.Event.set() 不会强行杀死任务，只是把信号变为“已触发”。
        state.cancel_event.set()
