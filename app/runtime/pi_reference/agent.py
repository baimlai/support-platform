"""Pi v1.5.0 agent.ts 的选择性 Python 移植：状态、订阅、取消和两类输入队列。

来源：earendil-works/pi，f9a26365568127461c47a07a37dcb42c140a3ac5。
许可见同目录 NOTICE.md。Loop 负责执行，这个薄包装层负责“怎样使用 Loop”。
HTTP 的任务 id、数据库和后台任务仍归 TaskService，不在内核重复建一套。

教学入口：prompt -> _run -> run_agent_loop -> _receive -> 订阅者。
运行中调用 steer/follow_up 只入队，Loop 在安全点通过配置里的函数取走消息。
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Literal

from app.runtime.contracts import (
    AgentContext,
    AgentEvent,
    AgentLoopConfig,
    AssistantMessage,
    CancellationToken,
    Message,
    MessageEnd,
    MessageStart,
    MessageUpdate,
    StreamFn,
    ToolExecutionEnd,
    ToolExecutionStart,
)
from app.runtime.pi_reference.loop import LoopResult, run_agent_loop, run_agent_loop_continue

QueueMode = Literal["all", "one-at-a-time"]
Listener = Callable[[AgentEvent], None | Awaitable[None]]


@dataclass(slots=True)
class PendingMessageQueue:
    """参考 Pi 的输入队列；列表是每个对象独有的，不能写成类级别的 []。"""

    mode: QueueMode = "one-at-a-time"
    _messages: list[Message] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Literal 是类型提示；运行时仍显式检查，拼错配置才能及时报错。"""
        if self.mode not in ("all", "one-at-a-time"):
            raise ValueError("队列模式只能是 all 或 one-at-a-time")

    def put(self, message: Message) -> None:
        """入队不会调用模型，也不会打断正在执行的工具。"""
        self._messages.append(message)

    def peek(self) -> list[Message]:
        """只看下一批；切片创建新列表，调用方清空返回值不会清空队列。"""
        return self._messages[:] if self.mode == "all" else self._messages[:1]

    def drain(self) -> list[Message]:
        """先取下一批，再删除对应前缀；一轮只取一条或全部取走由 mode 决定。"""
        batch = self.peek()
        del self._messages[: len(batch)]
        return batch

    def clear(self) -> None:
        """显式丢弃排队输入；取消运行本身不自动丢掉用户尚未处理的要求。"""
        self._messages.clear()

    def restore_front(self, message: Message) -> None:
        """本项目扩展：轮数保护/取消导致已取出的输入未生效时，放回队首。"""
        self._messages.insert(0, message)


class Agent:
    """可重复使用的单 Agent；同一个对象同一时刻只允许一次运行。

    不创建后台任务、不隐式开线程。微服务可用 asyncio.create_task(agent.prompt(...))
    自己决定后台任务的生命周期；等待者取消和进程重启也仍由服务层管理。
    """

    def __init__(self, context: AgentContext, config: AgentLoopConfig, stream_fn: StreamFn) -> None:
        self.context = context
        self.config = config
        self.stream_fn = stream_fn
        self.steering_queue = PendingMessageQueue()
        self.follow_up_queue = PendingMessageQueue()
        self.is_running = False
        self.streaming_message: AssistantMessage | None = None
        self.pending_tool_calls: set[str] = set()
        self._listeners: list[Listener] = []
        self._token: CancellationToken | None = None
        self._borrowed: list[tuple[PendingMessageQueue, Message]] = []

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        """登记回调并返回取消订阅函数；函数也能作为返回值，这也是 Hook 的基础。"""
        self._listeners.append(listener)

        def unsubscribe() -> None:
            # 重复取消订阅不报错，方便 HTTP 连接关闭时统一清理。
            if listener in self._listeners:
                self._listeners.remove(listener)

        return unsubscribe

    def steer(self, message: Message) -> None:
        """运行中纠正调查方向；在下一安全点使用，例如“先查锁等待”。"""
        self.steering_queue.put(message)

    def follow_up(self, message: Message) -> None:
        """原调查自然停下后再处理的追问，例如“再给一份验证步骤”。"""
        self.follow_up_queue.put(message)

    def abort(self) -> None:
        """协作式取消。令牌可重复标记，工具和模型自行在 await 附近响应。"""
        if self._token is not None:
            self._token.cancel()

    def peek_queued_messages(self) -> list[Message]:
        """查看下一批输入但不消费，方便 UI 显示还有什么要求在排队。"""
        return self.steering_queue.peek() or self.follow_up_queue.peek()

    async def prompt(
        self,
        prompts: list[Message],
        cancellation: CancellationToken | None = None,
    ) -> LoopResult:
        """追加新输入并运行；传列表以支持一次输入多条消息。"""
        return await self._run(prompts, cancellation, continuing=False)

    async def continue_run(self, cancellation: CancellationToken | None = None) -> LoopResult:
        """对应 Pi 的 continue()；Python 的 continue 是关键字，方法改名加 _run。"""
        return await self._run([], cancellation, continuing=True)

    async def _receive(self, event: AgentEvent) -> None:
        """把事件转成轻量运行状态，再按登记顺序通知订阅者。"""
        if isinstance(event, MessageStart):
            # Loop 已把输入写进历史才发开始事件，此时才确认队列消费成功。
            for index, (_, message) in enumerate(self._borrowed):
                if message is event.message:
                    del self._borrowed[index]
                    break
        if isinstance(event, (MessageStart, MessageUpdate)) and isinstance(event.message, AssistantMessage):
            self.streaming_message = event.message
        elif isinstance(event, MessageEnd) and isinstance(event.message, AssistantMessage):
            self.streaming_message = None
        elif isinstance(event, ToolExecutionStart):
            self.pending_tool_calls.add(event.tool_call_id)
        elif isinstance(event, ToolExecutionEnd):
            self.pending_tool_calls.discard(event.tool_call_id)
        # 遍历快照：订阅者可以在回调里退订，不会破坏正在遍历的原列表。
        for listener in list(self._listeners):
            result = listener(event)
            if inspect.isawaitable(result):
                await result

    async def _run(
        self,
        prompts: list[Message],
        cancellation: CancellationToken | None,
        *,
        continuing: bool,
    ) -> LoopResult:
        """统一生命周期；try/finally 保证失败或硬取消后对象不会永远显示 running。"""
        if self.is_running:
            raise RuntimeError("Agent 正在运行；请用 steer/follow_up 排队，或等待结束")
        self.is_running = True
        self._token = cancellation if cancellation is not None else CancellationToken()
        def drain(queue: PendingMessageQueue) -> list[Message]:
            """先借出消息，直到 message_start 才确认消费；避免轮数耗尽时丢追问。"""
            messages = queue.drain()
            self._borrowed.extend((queue, message) for message in messages)
            return messages

        config = replace(
            self.config,
            get_steering_messages=lambda: drain(self.steering_queue),
            get_follow_up_messages=lambda: drain(self.follow_up_queue),
        )
        try:
            if continuing and self.peek_queued_messages():
                # 上轮结束在助手消息时，排队输入可以作为新的 prompt 启动。
                prompts = drain(self.steering_queue) or drain(self.follow_up_queue)
                continuing = False
            if continuing:
                result = await run_agent_loop_continue(
                    self.context,
                    config,
                    self._receive,
                    self.stream_fn,
                    self._token,
                )
            else:
                result = await run_agent_loop(
                    prompts,
                    self.context,
                    config,
                    self._receive,
                    self.stream_fn,
                    self._token,
                )
            self.context = result.context
            return result
        finally:
            # 倒序插回队首可恢复原顺序；已进入历史的输入已被 _receive 移除，不会重复。
            for queue, message in reversed(self._borrowed):
                queue.restore_front(message)
            self._borrowed.clear()
            self.is_running = False
            self.streaming_message = None
            self.pending_tool_calls.clear()
            self._token = None
