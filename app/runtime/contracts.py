"""平台运行接口和公共类型。

这一层是内核与业务之间的“插座”：
- 内核（`app/runtime/pi_reference/`）只认识这里的消息、工具、事件和回调接口；
- DBA 业务（`app/harness/`、`app/adapters/`）通过实现这些接口把上下文、模型和工具接进来。

为什么要把类型独立出来：
内核不应该 import 慢 SQL 规则、SQLite 表结构或具体某个模型 SDK。否则以后换上下文策略、
换工具结果处理方式、甚至换内核时，业务代码会被迫大改。这里的类型只描述“Agent 运行需要什么”，
不描述“DBA 业务怎么做”。

来源说明：
- 消息、工具调用、工具结果、运行事件的形状参考了 Pi 的
  `packages/agent/src/types.ts` 和 `packages/ai/src/types.ts`；
- 这里是 Python 重写，不是 Pi 官方 SDK；字段做了简化，具体差异见 `docs/pi-mapping.md`。

## 类型设计说明

1. `@dataclass`：自动生成 `__init__`、`__repr__`、`__eq__`，适合“主要用来装数据”的类。
2. `slots=True`：告诉 Python 只为声明的字段预留属性槽，写错字段名会立刻报错，
   也不会因为随手加属性导致对象悄悄变胖。
3. `field(default_factory=...)`：默认值如果可变（list/dict/Event），必须用工厂函数，
   否则所有实例会共用同一个对象。这是 Python 新手最常踩的坑之一。
4. `StrEnum`：枚举成员本身也是字符串，写 `StopReason.STOP == "stop"` 成立，
   JSON 序列化时也更省事。
5. `Protocol`：只描述“对象要长什么样”，不要求继承。只要函数签名对得上，就能当工具处理器。
6. `TypeAlias`：给复杂类型起一个好读的名字，例：`Content = TextContent | ToolCall`。
7. `from __future__ import annotations`：让类型注解延迟求值，写 `list[Message]` 这类
   新语法时不必担心运行顺序，也方便以后做循环引用。
"""

# 这行必须放在文件最上面（docstring 之后）。它让下面所有类型注解
# 变成“字符串形式保存”，运行时不会立刻解析，类型检查工具仍然能看懂。
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum, StrEnum
from typing import Any, Literal, Protocol, TypeAlias


def now_ms() -> int:
    """返回当前时间的毫秒时间戳。

    Pi 的消息时间戳也是毫秒，这里保持一致，方便以后对照源码和排查时序。
    例子：2026-10-29 18:30:00 UTC 会得到类似 1798645800000 的整数。
    """
    return int(time.time() * 1000)


# `StrEnum` 是 Python 3.11 加入的“字符串枚举”。
# 普通 Enum 的成员打印出来是 `StopReason.STOP`，传给 JSON 还需要手动取 `.value`；
# StrEnum 成员本身就可以当字符串用，写日志和序列化都更直观。
class StopReason(StrEnum):
    """助手这一轮为什么停下来。

    - `stop`：模型正常说完，没有工具调用；
    - `tool_use`：模型要求调用工具，循环还要继续；
    - `length`：输出被最大 token 截断，工具参数可能不完整；
    - `error`：模型或适配器报错；
    - `aborted`：用户取消或超时中止。

    用字符串枚举而不是裸字符串，是为了避免拼写错误。比如写成 `StopReason.TOOL_USE`
    比写 `"toolUse"` 更容易被 IDE 和类型检查发现。
    """

    STOP = "stop"
    TOOL_USE = "toolUse"
    LENGTH = "length"
    ERROR = "error"
    ABORTED = "aborted"


# `@dataclass` 会根据下面的字段自动生成构造函数。
# 例如 `TextContent(text="你好")` 会自动得到一个带 `type="text"` 的对象。
# `slots=True` 让实例只能有 text/type 两个属性，写错属性名会抛 AttributeError。
@dataclass(slots=True)
class TextContent:
    """一段文本内容。

    例子：模型最终回答、工具返回的 JSON 字符串、错误说明，都是一段文本。
    `type` 字段保留在序列化结果里，前端可以按 `type` 决定怎么渲染。
    """

    text: str
    # `init=False` 表示这个字段不出现在构造函数参数里；
    # 它固定为 "text"，专门给 JSON 序列化和前端判断类型用。
    type: Literal["text"] = field(default="text", init=False)


@dataclass(slots=True)
class ToolCall:
    """模型提出的一次工具调用请求。

    `id` 是关联键：模型返回的 ToolCall、我们执行工具后生成的 ToolResultMessage，
    以及前端时间线上的工具开始/结束事件，都靠这个 id 对上。
    例子：模型说“调用 slow_log_query，id=call-1”，工具返回“call-1 的结果是 ...”，
    即使中间有并发或重试，也不会把结果配错。
    """

    id: str
    name: str
    arguments: dict[str, Any]
    type: Literal["toolCall"] = field(default="toolCall", init=False)


# 助手消息里的内容块目前只支持文本和工具调用。
# 图片、思考过程等 Pi 支持的内容先不移植，原因是本轮 DBA 场景用不到，移植进来反而增加复杂度。
Content: TypeAlias = TextContent | ToolCall


@dataclass(slots=True)
class SystemMessage:
    """系统提示词消息。

    例子：告诉模型“你是 DBA 助手，必须先取证再下结论”。
    本轮先由 Harness 拼好一条系统消息放进上下文；Pi 中更复杂的
    “系统消息里动态增删工具定义”机制没有完整移植，原因见 `docs/pi-mapping.md`。
    """

    content: str
    timestamp: int = field(default_factory=now_ms)
    role: Literal["system"] = field(default="system", init=False)


@dataclass(slots=True)
class UserMessage:
    """用户消息。

    例子：工单正文、用户补充的“实例是 db-mysql-001”。本轮内容一般是字符串；
    保留 `list[TextContent]` 是为了以后接多段文本或图片时不改类型。
    """

    content: str | list[TextContent]
    timestamp: int = field(default_factory=now_ms)
    role: Literal["user"] = field(default="user", init=False)


@dataclass(slots=True)
class AssistantMessage:
    """助手回复。

    `content` 里可能同时有文本和工具调用：
    - 只有文本：模型给出了阶段性说明或最终报告，循环通常可以结束；
    - 有 ToolCall：模型要求平台去查证据，循环执行完工具后继续下一轮。

    `error_message` 只在 `stop_reason` 为 `error` 或 `aborted` 时有意义。
    """

    content: list[Content]
    stop_reason: StopReason = StopReason.STOP
    error_message: str | None = None
    timestamp: int = field(default_factory=now_ms)
    provider: str = "mock"
    model: str = "mock-dba"
    role: Literal["assistant"] = field(default="assistant", init=False)


@dataclass(slots=True)
class ToolResultMeta:
    """工具结果的平台元信息。

    为什么单独拆一个小对象：
    - `details` 是工具自己的业务字段，不同工具完全不一样；
    - `meta` 是平台统一补充的字段，记录器、前端和模型都不用理解每个工具的业务 Schema。

    例子：慢日志原始结果有 15000 个字符，平台保存成证据 `ev-000001`，
    模型只收到 4000 字符以内的预览。这时 `meta` 里会写：
        evidence_id="ev-000001"
        truncated=True
        raw_size_chars=15000
        max_inline_chars=4000
    """

    # 原始结果在证据目录里的编号；平台工具（如 search_tools）不产生证据，保持 None。
    evidence_id: str | None = None
    # True 表示模型看到的 content 被平台截断过，完整内容在证据文件里。
    truncated: bool = False
    # 原始内容的字符数，用来在报告和前端里说明“到底有多大”。
    raw_size_chars: int | None = None
    # 平台允许放进模型上下文的最大字符数，方便解释为什么截断。
    max_inline_chars: int | None = None


@dataclass(slots=True)
class ToolResultMessage:
    """工具结果消息，会作为下一轮模型输入的一部分。

    注意：`content` 是“给模型看的有界内容”，不是原始工具返回的全部数据。
    第 2 步接入证据存储后，完整原始结果会落在文件里，这里只放片段和引用。
    `meta` 是平台统一补充的证据引用；`details` 仍然是工具自己的业务摘要。
    """

    tool_call_id: str
    tool_name: str
    content: list[TextContent]
    is_error: bool = False
    details: Any = None
    duration_ms: int | None = None
    meta: ToolResultMeta | None = None
    timestamp: int = field(default_factory=now_ms)
    role: Literal["toolResult"] = field(default="toolResult", init=False)


# 模型和内核只认识这四种消息。自定义消息类型（例如 UI 通知）以后可以通过转换函数过滤掉。
Message: TypeAlias = SystemMessage | UserMessage | AssistantMessage | ToolResultMessage


@dataclass(slots=True)
class ToolResult:
    """一次工具执行的运行时结果。

    和 `ToolResultMessage` 的区别：
    - `ToolResult` 是执行层的结果，带有 `terminate` 这种“只给运行时看”的标记；
    - `ToolResultMessage` 是要写进对话、下一轮发给模型的消息。

    例子：工具执行成功返回一段 JSON，`is_error=False`；
    工具抛异常，内核会把它转成 `is_error=True` 的结果，模型看到的是错误说明，
    而不是直接让整个任务崩掉。
    """

    content: list[TextContent]
    details: Any = None
    is_error: bool = False
    terminate: bool = False
    duration_ms: int | None = None
    # 平台保存原始结果后补充的证据信息；平台工具或未接入证据层时为 None。
    meta: ToolResultMeta | None = None


@dataclass(slots=True)
class CancellationToken:
    """协作式取消令牌。

    用法：
    - 任务层（以后的 FastAPI 接口或 CLI）在用户点“取消”时调用 `token.cancel()`；
    - 模型适配器和工具在执行过程中检查 `token.cancelled`，尽早停下；
    - 内核在每轮开始、每次工具执行前后检查，避免继续做无用功。

    为什么不用直接 `task.cancel()`：
    直接取消会让 `asyncio.CancelledError` 从任意 await 点冒出来，资源清理和事件记录更难写。
    协作式令牌可以先产生正常的“aborted”结果和 `agent_end` 事件；需要强制停止时，
    `task.cancel()` 仍然可用，内核不会吞掉 `CancelledError`。
    """

    # 这里必须用 `default_factory`，不能写 `default=asyncio.Event()`。
    # 原因：默认值在“定义类时”只算一次，所有令牌会共享同一个 Event；
    # 工厂函数则是在“每次创建对象时”调用，每个令牌拿到独立的事件对象。
    # `repr=False` 让打印对象时不必显示内部事件，调试输出更干净。
    _event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def cancel(self) -> None:
        """把令牌标记为已取消。重复调用没有副作用。"""
        self._event.set()

    @property
    def cancelled(self) -> bool:
        """是否已经取消。"""
        return self._event.is_set()

    async def wait(self) -> None:
        """等待取消发生；主要给需要同时等待“工具完成”和“取消”的场景用。"""
        await self._event.wait()


# `Protocol` 是“结构化类型”的写法，类似其他语言的 interface，
# 但不要求显式继承。只要一个函数能接受这三个参数并返回 ToolResult，它就符合要求。
# 好处：测试里写一个普通 async 函数就能传进来，不需要为了类型系统额外包一层类。
class ToolHandler(Protocol):
    """工具执行函数接口。

    参数：
    - `tool_call_id`：模型给的调用 id，原样带回，用于和工具结果配对；
    - `arguments`：已经过 JSON Schema 校验的参数；
    - `cancellation`：取消令牌，长耗时工具应定期检查。

    实现方式：任何 `async def handler(tool_call_id, arguments, cancellation)` 都可以。
    例子：
        async def echo(tool_call_id, arguments, cancellation):
            return ToolResult(content=[TextContent(text=arguments["text"])])
    """

    async def __call__(
        self,
        tool_call_id: str,
        arguments: dict[str, Any],
        cancellation: CancellationToken,
    ) -> ToolResult: ...


@dataclass(slots=True)
class ToolSpec:
    """工具定义。

    `parameters` 用 JSON Schema 描述参数，模型适配器会把它转成各家模型能理解的工具定义。
    例子：
        {
            "type": "object",
            "properties": {"instance_id": {"type": "string"}},
            "required": ["instance_id"],
        }

    `handler` 不参与序列化，也不会发给模型；模型只看到名称、描述和参数 Schema。
    `timeout_s` 为空时使用循环配置里的默认超时。

    `annotations` 是 MCP 的工具注解（例如 `readOnlyHint=True`），用来告诉客户端
    “这个工具只读、不会改动数据”。本地工具可以留空；从 MCP `tools/list`
    映射过来的工具会把它一起带过来。它不参与模型调用，但会写进能力目录和运行记录，
    方便审计“这次到底允许了哪些操作”。
    """

    name: str
    label: str
    description: str
    parameters: dict[str, Any]
    handler: ToolHandler
    timeout_s: float | None = None
    annotations: dict[str, Any] | None = None

    def declaration(self) -> dict[str, Any]:
        """返回可以发给模型或写入执行记录的工具声明。

        这里特意不返回 `handler`：函数对象既不能 JSON 序列化，也不应该暴露给模型。
        `annotations` 只在有值时才写，避免本地工具的能力目录里出现一堆 null。
        """
        declaration = {
            "name": self.name,
            "label": self.label,
            "description": self.description,
            "parameters": self.parameters,
        }
        if self.annotations is not None:
            declaration["annotations"] = self.annotations
        return declaration


@dataclass(slots=True)
class AgentContext:
    """一次运行的可变上下文。

    - `messages`：完整对话历史，包含系统消息、用户消息、助手消息和工具结果；
    - `tools`：本次运行可执行的工具集合。

    “完整历史”不等于“每次发给模型的全部内容”。真正的模型输入由
    `transform_context` 和 `convert_to_llm` 在每轮请求前决定，这一步是第 2 步做
    Topic 和有界上下文的关键位置。

    `metadata` 是平台自己用的附加信息，不会直接发给模型。例子：上下文裁剪钩子
    会把“本轮最多允许多少字符、实际用了多少字符”写进 `metadata["context_budget"]`，
    记录器再把它写进 `model_requests.jsonl`，方便排查。
    """

    messages: list[Message] = field(default_factory=list)
    tools: list[ToolSpec] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BeforeToolCallContext:
    """`before_tool_call` 钩子的输入。"""

    assistant_message: AssistantMessage
    tool_call: ToolCall
    arguments: dict[str, Any]
    messages: list[Message]


@dataclass(slots=True)
class BeforeToolCallResult:
    """`before_tool_call` 钩子的返回值。

    返回 `block=True` 可以阻止工具执行，内核会把 `reason` 作为错误结果交给模型。
    例子：权限校验发现当前租户不能查这个实例，就返回
    `BeforeToolCallResult(block=True, reason="无权访问该实例")`。
    """

    block: bool = False
    reason: str | None = None
    terminate: bool = False


@dataclass(slots=True)
class AfterToolCallContext:
    """`after_tool_call` 钩子的输入。"""

    assistant_message: AssistantMessage
    tool_call: ToolCall
    arguments: dict[str, Any]
    result: ToolResult
    is_error: bool
    messages: list[Message]


# 用一个特殊对象区分“调用方没改这个字段”和“调用方显式传了 None”。
# 例子：`details=None` 表示把 details 覆盖成 null；不传这个字段则保留工具原值。
_UNSET: Any = object()


@dataclass(slots=True)
class AfterToolCallResult:
    """`after_tool_call` 钩子的返回值，按字段覆盖工具结果。

    `meta` 和 `details` 一样，用 `_UNSET` 区分“没改”和“显式改成 None”。
    例子：证据层只保存原始结果，然后返回 `AfterToolCallResult(meta=...)`，
    这时 content、details、is_error 都保持工具原来的值。
    """

    content: list[TextContent] | None = None
    details: Any = _UNSET
    is_error: bool | None = None
    terminate: bool | None = None
    meta: Any = _UNSET


@dataclass(slots=True)
class TurnContext:
    """一轮结束后传给 `finish_turn` 的信息。"""

    message: AssistantMessage
    tool_results: list[ToolResultMessage]
    messages: list[Message]
    new_messages: list[Message]


@dataclass(slots=True)
class TurnDecision:
    """`finish_turn` 的决定：继续下一轮，或在 `turn_end` 后结束。"""

    action: Literal["continue", "end"]


# 钩子统一用异步函数，避免工具执行过程中出现“半同步半异步”的分支。
BeforeToolCallHook: TypeAlias = Callable[
    [BeforeToolCallContext], Awaitable[BeforeToolCallResult | None]
]
AfterToolCallHook: TypeAlias = Callable[
    [AfterToolCallContext], Awaitable[AfterToolCallResult | None]
]
TransformContextHook: TypeAlias = Callable[
    [list[Message], CancellationToken], Awaitable[list[Message]]
]
ConvertToLlmHook: TypeAlias = Callable[[list[Message]], list[Message]]
FinishTurnHook: TypeAlias = Callable[[TurnContext], Awaitable[TurnDecision | None]]


def default_convert_to_llm(messages: list[Message]) -> list[Message]:
    """默认不做转换，原样返回。

    本轮的消息类型已经都是模型能理解的四种角色。以后如果加了“UI 通知”这类自定义消息，
    就在这里过滤掉，避免它们被发给模型。
    """
    return list(messages)


@dataclass(slots=True)
class AgentLoopConfig:
    """一次 Agent 循环的配置。

    - `model`：模型标识，实际由适配器解释；
    - `max_turns`：最多允许几轮“模型回复 + 工具执行”。这是我们在 Pi 低层循环之外加的保护，
      Pi 的低层 `agentLoop` 本身没有轮数上限，`maxTurns` 在它的 durable harness 里；
    - `tool_timeout_s`：单个工具的默认超时。Pi 低层循环也没有内置工具超时，这里补上；
    - `tools_provider`：每轮请求前返回“当前可见、可执行”的工具列表。这是平台为了
      按需加载工具加的扩展：初始只暴露少量工具，模型调用 `search_tools` 后再把
      匹配到的工具放进后续请求。Pi 的 `toolsAdded/toolsRemoved` 增量机制没有完整移植；
    - 三个钩子对应 Pi 的 `transformContext`、`beforeToolCall`、`afterToolCall`、`finishTurn`。
    """

    model: str = "mock-dba"
    max_turns: int = 8
    tool_timeout_s: float = 10.0
    tools_provider: Callable[[], list[ToolSpec]] | None = None
    transform_context: TransformContextHook | None = None
    convert_to_llm: ConvertToLlmHook = default_convert_to_llm
    before_tool_call: BeforeToolCallHook | None = None
    after_tool_call: AfterToolCallHook | None = None
    finish_turn: FinishTurnHook | None = None


@dataclass(slots=True)
class ModelRequest:
    """真正发给模型适配器的请求。

    `messages` 是经过 `transform_context` 和 `convert_to_llm` 之后的结果，
    可能比 `AgentContext.messages` 少（例如第 2 步裁剪旧消息），这正是“完整记录和模型输入分开保存”的边界。
    """

    messages: list[Message]
    tools: list[ToolSpec]
    model: str
    cancellation: CancellationToken
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------- 模型流式事件 ----------
# 这组事件描述“模型这一轮是怎么生成出来的”。Pi 的事件更细（text_start/text_delta/...），
# 本轮只保留循环真正需要区分的几类，先让流程能跑通。


@dataclass(slots=True)
class AssistantStart:
    """模型开始回复。`partial` 是当前已经生成的部分消息。"""

    partial: AssistantMessage
    type: Literal["start"] = field(default="start", init=False)


@dataclass(slots=True)
class AssistantTextDelta:
    """模型新增了一段文本。"""

    delta: str
    partial: AssistantMessage
    type: Literal["text_delta"] = field(default="text_delta", init=False)


@dataclass(slots=True)
class AssistantToolCallEvent:
    """模型生成了一个完整的工具调用。"""

    tool_call: ToolCall
    partial: AssistantMessage
    type: Literal["tool_call"] = field(default="tool_call", init=False)


@dataclass(slots=True)
class AssistantDone:
    """模型这一轮正常结束。`message` 是最终完整消息。"""

    message: AssistantMessage
    type: Literal["done"] = field(default="done", init=False)


@dataclass(slots=True)
class AssistantError:
    """模型这一轮失败或被取消。`message.stop_reason` 会标明原因。"""

    message: AssistantMessage
    type: Literal["error"] = field(default="error", init=False)


AssistantStreamEvent: TypeAlias = (
    AssistantStart | AssistantTextDelta | AssistantToolCallEvent | AssistantDone | AssistantError
)

# 模型适配器就是一个函数：给请求，返回异步事件流。
# 例子：`adapter.stream` 或 `recorder.wrap_stream_fn(adapter.stream)` 都符合这个形状。
StreamFn: TypeAlias = Callable[[ModelRequest], AsyncIterator[AssistantStreamEvent]]


# ---------- 运行事件 ----------
# 这些事件会写进执行记录、以后也会通过 HTTP/SSE 给前端。


@dataclass(slots=True)
class AgentStart:
    """一次运行开始。"""

    type: Literal["agent_start"] = field(default="agent_start", init=False)


@dataclass(slots=True)
class TurnStart:
    """一轮开始。`turn` 从 1 开始计数。"""

    turn: int
    type: Literal["turn_start"] = field(default="turn_start", init=False)


@dataclass(slots=True)
class MessageStart:
    """一条消息开始进入对话。"""

    message: Message
    turn: int = 0
    type: Literal["message_start"] = field(default="message_start", init=False)


@dataclass(slots=True)
class MessageUpdate:
    """助手消息流式更新。"""

    message: Message
    assistant_event: AssistantStreamEvent
    turn: int = 0
    type: Literal["message_update"] = field(default="message_update", init=False)


@dataclass(slots=True)
class MessageEnd:
    """一条消息完成。"""

    message: Message
    turn: int = 0
    type: Literal["message_end"] = field(default="message_end", init=False)


@dataclass(slots=True)
class ToolExecutionStart:
    """工具开始执行。"""

    tool_call_id: str
    tool_name: str
    arguments: dict[str, Any]
    turn: int = 0
    type: Literal["tool_execution_start"] = field(default="tool_execution_start", init=False)


@dataclass(slots=True)
class ToolExecutionUpdate:
    """工具执行过程中的进度更新（本轮模拟工具不产生，但接口先留着）。"""

    tool_call_id: str
    tool_name: str
    arguments: dict[str, Any]
    partial_result: ToolResult
    turn: int = 0
    type: Literal["tool_execution_update"] = field(default="tool_execution_update", init=False)


@dataclass(slots=True)
class ToolExecutionEnd:
    """工具执行结束，无论成功还是失败都会发这个事件。"""

    tool_call_id: str
    tool_name: str
    result: ToolResult
    is_error: bool
    duration_ms: int | None = None
    turn: int = 0
    type: Literal["tool_execution_end"] = field(default="tool_execution_end", init=False)


@dataclass(slots=True)
class TurnEnd:
    """一轮结束：助手这一轮的消息和它触发的工具结果。"""

    turn: int
    message: AssistantMessage
    tool_results: list[ToolResultMessage]
    type: Literal["turn_end"] = field(default="turn_end", init=False)


@dataclass(slots=True)
class TopicUpdated:
    """平台维护的调查 Topic 发生变化。

    这个事件不是 Pi 内核产生的，而是 Harness 在工具结果落盘后更新 Topic 时补发的。
    放进 `AgentEvent` 联合类型后，记录器和以后的前端事件流就能像处理其它运行事件一样处理它。

    例子：慢日志工具结束后，平台把“最慢指纹 a1b2c3d4e5f6，平均 2380 ms”写进 Topic，
    这时会发一条 `topic_updated`，`data` 里带完整的 `topic` 对象。
    """

    topic: dict[str, Any]
    turn: int = 0
    type: Literal["topic_updated"] = field(default="topic_updated", init=False)


@dataclass(slots=True)
class AgentEnd:
    """一次运行结束。

    `reason` 是我们在 Pi 事件形状上补的字段，方便前端和记录区分：
    - `completed`：模型正常结束，或工具要求终止；
    - `max_turns`：达到最大轮数；
    - `cancelled`：用户取消；
    - `error`：模型或运行出错。
    """

    messages: list[Message]
    reason: str = "completed"
    error: str | None = None
    type: Literal["agent_end"] = field(default="agent_end", init=False)


AgentEvent: TypeAlias = (
    AgentStart
    | TurnStart
    | MessageStart
    | MessageUpdate
    | MessageEnd
    | ToolExecutionStart
    | ToolExecutionUpdate
    | ToolExecutionEnd
    | TurnEnd
    | TopicUpdated
    | AgentEnd
)

EventSink: TypeAlias = Callable[[AgentEvent], Awaitable[None]]


def to_jsonable(value: Any) -> Any:
    """把 dataclass、枚举和容器递归转成能 `json.dumps` 的普通对象。

    为什么需要它：
    执行记录和以后的 HTTP 响应都要求 JSON，但代码里用的是 dataclass 和 `StopReason` 枚举。
    例子：
        to_jsonable(TextContent(text="hi")) -> {"text": "hi", "type": "text"}
        to_jsonable(StopReason.TOOL_USE) -> "toolUse"

    注意：`ToolSpec` 里带函数，不要直接对整个 `ToolSpec` 调用；用 `declaration()`。
    """
    # `is` 判断“是不是同一个对象”，适合和 `_UNSET` 这类哨兵值比较；
    # `==` 判断“值是否相等”，适合业务数据。两者不要混用。
    if value is _UNSET:
        return None
    # 枚举成员序列化时取 `.value`，例如 StopReason.TOOL_USE 变成 "toolUse"。
    if isinstance(value, Enum):
        return value.value
    # `is_dataclass(value) and not isinstance(value, type)` 用来排除“类本身”：
    # 我们想转换的是 `TextContent(text="hi")` 这样的实例，而不是 TextContent 这个类。
    if is_dataclass(value) and not isinstance(value, type):
        result: dict[str, Any] = {}
        for item in fields(value):
            field_value = getattr(value, item.name)
            # 内部哨兵值不写进 JSON。
            if field_value is _UNSET:
                continue
            result[item.name] = to_jsonable(field_value)
        return result
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    return value


def content_text(content: str | list[TextContent]) -> str:
    """把用户/系统消息的 content 统一成字符串。

    例子：`content_text([TextContent(text="你好"), TextContent(text="世界")])` 返回 `"你好世界"`。
    """
    if isinstance(content, str):
        return content
    return "".join(block.text for block in content)


def assistant_text(message: AssistantMessage) -> str:
    """抽取助手消息里的全部文本，忽略工具调用块。"""
    return "".join(block.text for block in message.content if isinstance(block, TextContent))


def tool_result_text(message: ToolResultMessage) -> str:
    """抽取工具结果消息里的全部文本。"""
    return "".join(block.text for block in message.content)
