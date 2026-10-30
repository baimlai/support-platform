"""从 Pi 的 Agent 循环选择性移植出来的最小 Python 循环。

## 来源

- Pi 仓库：https://github.com/earendil-works/pi
- 参考提交：`babe30a7e6c185346c85859da30b58183e5964f0`（2026-10-29）
- 主要参考文件：`packages/agent/src/agent-loop.ts`
- 配套阅读：`packages/agent/src/types.ts`、`packages/agent/src/agent.ts`、
  `packages/ai/src/types.ts`、`packages/ai/src/utils/transcript.ts`、
  `packages/ai/src/utils/event-stream.ts`、`packages/ai/src/utils/validation.ts`

## 哪些是 Pi 的，哪些是我们改的

Pi 移植部分：
- `run_agent_loop` 的整体循环结构：提示词入上下文 → 调模型 → 拿工具调用 → 执行工具 →
  工具结果入上下文 → 再调模型 → 继续或停止。
- 事件顺序：`agent_start`、`turn_start`、`message_start/end`、工具执行开始/结束、
  `turn_end`、`agent_end`。
- 消息、`toolCallId`、工具结果三者用 id 关联。
- 每轮请求前的 `transformContext`、`convertToLlm` 位置。
- 工具执行前后的 `beforeToolCall`、`afterToolCall` 位置。
- 模型流式回复的 start/delta/done/error 处理方式。

我们的改动（可能改变原行为，原因写在这里）：
- 增加 `max_turns`。Pi 低层 `agentLoop` 没有轮数上限，`maxTurns` 在 durable harness 里；
  本轮平台必须有防止无限循环的保护，所以放进循环配置。
- 增加 `tool_timeout_s`。Pi 低层循环依赖取消信号，没有统一工具超时；本轮在调用工具时用
  `asyncio.wait_for` 包一层。
- `agent_end` 增加 `reason` 字段。Pi 的 `agent_end` 只带 messages；前端和记录需要知道
  是正常结束、达到轮数还是取消，所以补一个字段。
- 工具顺序执行。Pi 支持 parallel/sequential 两种模式，本轮先只做 sequential，
  因为模拟工具少，顺序执行更容易看懂和排查。
- 硬取消（`asyncio.CancelledError`）继续向上抛出，只补一个 `agent_end` 记录再抛；
  协作式取消（`CancellationToken`）会产生正常的错误工具结果并结束本轮。
- 不移植 steering/follow-up 队列、`prepareNextTurn`、工具声明增量（`toolsAdded/toolsRemoved`）、
  并行工具、图片内容和 provider 身份等。原因：本轮单 Agent、模拟工具、单页工作台用不到，
  先保持循环小到能逐行读懂。

## 一个具体例子

假设模型第一轮说“调用 slow_log_query(id=call-1)”，循环会：
1. 发 `turn_start(1)`；
2. 调模型，收到助手消息（带 `ToolCall(id="call-1")`）；
3. 发 `tool_execution_start(tool_call_id="call-1")`；
4. 执行模拟工具，发 `tool_execution_end`；
5. 生成 `ToolResultMessage(tool_call_id="call-1")`，发 `message_start/end`；
6. 发 `turn_end(1)`；
7. 因为还有工具调用，发 `turn_start(2)`，把工具结果带给模型；
8. 模型给出最终文本，发 `turn_end(2)` 和 `agent_end(reason="completed")`。

## 异步实现说明

1. `async def` 定义的是“协程函数”，调用它不会立刻执行，只会得到一个协程对象；
   必须 `await` 它，或者用 `asyncio.run()` 从最外层启动。
2. `await` 的意思是“这里可能要等 IO，先把控制权交回事件循环，等结果好了再继续”。
   模型请求、工具执行、文件/网络 IO 都应该用 `await`，不要用 `time.sleep()` 卡住整个线程。
3. `async for` 用来消费“异步生成器”。模型适配器一边生成一边 `yield`，
   内核就能边收边处理，不需要等整段回复生成完。
4. `asyncio.CancelledError` 是取消信号，不是普通错误。捕获后如果处理不了，
   必须继续 `raise`，否则外层会以为任务正常结束，资源可能泄漏。
5. `asyncio.wait_for(coro, timeout)` 会在超时后取消内部任务并抛 `TimeoutError`；
   我们把它转成错误工具结果，交给模型继续判断。
6. `try/except/else/finally` 的执行顺序：`try` 里没出错才走 `else`，
   无论是否出错 `finally` 都会执行。清理信号处理器这类工作适合放 `finally`。
"""

from __future__ import annotations

# 导入顺序约定为“标准库 → 第三方库 → 本项目模块”，三组之间空一行。
# 好处是扫一眼就知道依赖来自哪里；ruff 的 I（isort）规则会自动帮你排序。
import asyncio
import copy
import inspect
import time
from dataclasses import dataclass, field
from typing import Any

import jsonschema

from app.runtime.contracts import (
    _UNSET,
    AfterToolCallContext,
    AfterToolCallResult,
    AgentContext,
    AgentEnd,
    AgentLoopConfig,
    AgentStart,
    AssistantDone,
    AssistantError,
    AssistantMessage,
    AssistantStart,
    AssistantTextDelta,
    AssistantToolCallEvent,
    BeforeToolCallContext,
    CancellationToken,
    EventSink,
    Message,
    MessageEnd,
    MessageStart,
    MessageUpdate,
    ModelRequest,
    StopReason,
    StreamFn,
    TextContent,
    ToolCall,
    ToolExecutionEnd,
    ToolExecutionStart,
    ToolResult,
    ToolResultMessage,
    ToolSpec,
    TurnContext,
    TurnDecision,
    TurnEnd,
    TurnStart,
)


class ToolArgumentError(ValueError):
    """工具参数不符合 JSON Schema 时抛出的错误。

    内核会捕获它并转成错误工具结果交给模型。例子：模型只传了 `instance_id`，
    但 Schema 还要求 `start_time`，这里就会报“缺少 start_time”。
    """


@dataclass(slots=True)
class LoopResult:
    """一次循环的返回值。

    - `messages`：本次运行新增的消息（提示词、助手回复、工具结果），对应 Pi 的 `newMessages`；
    - `context`：运行结束时的完整上下文，包含运行前已有的历史；
    - `reason`：`completed`、`max_turns`、`cancelled` 或 `error`；
    - `turns`：实际执行了几轮“模型回复 + 工具执行”；
    - `error`：失败时的错误说明。
    """

    messages: list[Message]
    context: AgentContext
    reason: str
    turns: int = 0
    error: str | None = None


@dataclass(slots=True)
class _ExecutedToolBatch:
    """一批工具调用的执行结果。"""

    # 可变默认值（list/dict/set）必须用 `default_factory=list`。
    # 如果写成 `messages: list[...] = []`，所有批次会共用同一个列表，
    # 第二批工具结果会“神秘地”出现在第一批里。这是非常经典的坑。
    messages: list[ToolResultMessage] = field(default_factory=list)
    terminate: bool = False


@dataclass(slots=True)
class _ToolExecutionOutcome:
    """单个工具调用的执行结果和“是否真的执行了”。

    `executed=False` 表示工具没找到、参数不合法或被 before 钩子拦截，
    这时不调用 after 钩子，和 Pi 的行为一致。
    """

    result: ToolResult
    executed: bool


# `async def` 定义协程函数。调用 `run_agent_loop(...)` 只会创建协程对象，
# 真正开始执行是在外层 `await` 它的时候。参数里的 `X | None = None` 表示可选参数，
# 传 None 就用默认值，这也是 Python 里最常见的“可选值”写法。
async def run_agent_loop(
    prompts: list[Message],
    context: AgentContext,
    config: AgentLoopConfig,
    emit: EventSink,
    stream_fn: StreamFn,
    cancellation: CancellationToken | None = None,
) -> LoopResult:
    """启动一次 Agent 循环，处理给定的新提示词。

    参数：
    - `prompts`：这次要追加给模型的新消息，通常是工单正文；
    - `context`：运行前已有的上下文（本轮包含系统提示词和工具）；
    - `config`：轮数、超时、钩子等配置；
    - `emit`：事件接收器，记录器和前端通过它观察过程；
    - `stream_fn`：模型适配器，负责把请求变成流式助手事件；
    - `cancellation`：取消令牌，不传则新建一个不会被取消的令牌。

    返回 `LoopResult`。注意：即使模型报错，本函数通常也会正常返回，
    用 `reason="error"` 表示失败，这样调用方还能保存执行记录。
    """
    if config.max_turns < 1:
        raise ValueError("max_turns 必须大于等于 1")

    token = cancellation or CancellationToken()

    # Pi 的 agentLoop 会把新提示词追加到 context 后面，并将新增消息单独返回。
    # 这里保持同样行为：current 是完整上下文，new_messages 只放本次新增的内容。
    new_messages: list[Message] = list(prompts)
    # 注意 `metadata=context.metadata`：这里故意传同一个字典，而不是复制。
    # 原因：`transform_context` 钩子通常通过闭包拿到调用方原来的 `context`，
    # 它把“本轮上下文预算”写进 `context.metadata` 后，循环里的 `current.metadata`
    # 必须能立刻看到同一份数据。如果复制一份，两个对象就各写各的，记录里会丢信息。
    current = AgentContext(
        messages=[*context.messages, *prompts],
        tools=list(context.tools),
        metadata=context.metadata,
    )

    # `emit` 是异步函数：调用时要写 `await emit(...)`，不能只写 `emit(...)`。
    # 这样事件接收器可以安全地做磁盘写入或网络推送，循环会等它完成再继续。
    await emit(AgentStart())

    # 第一轮在第一句提示词之前就开始。Pi 也是先发 turn_start，再发提示词的 message 事件。
    turn_no = 1
    await emit(TurnStart(turn=turn_no))
    for message in prompts:
        await emit(MessageStart(message=message, turn=turn_no))
        await emit(MessageEnd(message=message, turn=turn_no))

    try:
        reason, error, turns = await _run_turns(
            current=current,
            new_messages=new_messages,
            config=config,
            emit=emit,
            stream_fn=stream_fn,
            token=token,
            turn_no=turn_no,
        )
    except asyncio.CancelledError:
        # 这是 Python 层面的硬取消（例如接口层 task.cancel()）。
        # 我们只补一个记录事件，然后把取消信号继续向上抛，绝不吞掉它。
        await emit(AgentEnd(messages=new_messages, reason="cancelled"))
        raise
    except Exception as exc:  # noqa: BLE001 - 运行入口要把意外错误变成可记录的结果
        # 平台钩子或事件接收器出错时，不让进程直接崩掉；返回 error，调用方仍能保存记录。
        await emit(AgentEnd(messages=new_messages, reason="error", error=str(exc)))
        return LoopResult(
            messages=new_messages,
            context=current,
            reason="error",
            turns=turn_no,
            error=str(exc),
        )

    await emit(AgentEnd(messages=new_messages, reason=reason, error=error))
    return LoopResult(
        messages=new_messages,
        context=current,
        reason=reason,
        turns=turns,
        error=error,
    )


async def _run_turns(
    current: AgentContext,
    new_messages: list[Message],
    config: AgentLoopConfig,
    emit: EventSink,
    stream_fn: StreamFn,
    token: CancellationToken,
    turn_no: int,
) -> tuple[str, str | None, int]:
    """真正跑“模型 → 工具 → 模型”的循环。

    返回值是 `(reason, error, turns)`。这一层每轮做四件事：
    1. 调模型，拿到助手消息；
    2. 如果模型要调工具，按顺序执行，并把结果写回上下文；
    3. 发 `turn_end`；
    4. 判断是继续下一轮，还是结束。
    """

    while True:
        # 每轮开始前先看取消。已经取消就不再发起新的模型请求，避免浪费。
        if token.cancelled:
            return "cancelled", None, turn_no

        # 1) 调模型。这个函数内部会处理流式事件，并把最终助手消息写进 current.messages。
        assistant_message = await _stream_assistant_response(
            current=current,
            config=config,
            emit=emit,
            stream_fn=stream_fn,
            token=token,
            turn_no=turn_no,
        )
        new_messages.append(assistant_message)

        # 模型报错或被取消：本轮没有工具结果，直接结束。
        # 注意：Pi 在这种情况下仍然会调用 finishTurn，但会忽略它的决定；这里照做。
        if assistant_message.stop_reason in (StopReason.ERROR, StopReason.ABORTED):
            turn_context = TurnContext(
                message=assistant_message,
                tool_results=[],
                messages=current.messages,
                new_messages=new_messages,
            )
            await _call_finish_turn(config, turn_context)
            await emit(TurnEnd(turn=turn_no, message=assistant_message, tool_results=[]))
            reason = "error" if assistant_message.stop_reason is StopReason.ERROR else "cancelled"
            return reason, assistant_message.error_message, turn_no

        # 2) 找出这一轮模型提出的所有工具调用。
        tool_calls = [block for block in assistant_message.content if isinstance(block, ToolCall)]
        batch = _ExecutedToolBatch()

        if tool_calls:
            if assistant_message.stop_reason is StopReason.LENGTH:
                # 输出被 token 上限截断时，工具参数可能是不完整的。
                # Pi 的做法是“全部不执行，全部返回错误”，让模型重新发一次完整调用。
                batch = await _fail_truncated_tool_calls(
                    tool_calls=tool_calls,
                    emit=emit,
                    turn_no=turn_no,
                )
            else:
                batch = await _execute_tool_calls(
                    current=current,
                    assistant_message=assistant_message,
                    tool_calls=tool_calls,
                    config=config,
                    emit=emit,
                    token=token,
                    turn_no=turn_no,
                )

            # 工具结果进入上下文，下一轮模型就能看到。
            current.messages.extend(batch.messages)
            new_messages.extend(batch.messages)

        # 3) 一轮结束。先把完整信息交给 finish_turn，再发 turn_end。
        turn_context = TurnContext(
            message=assistant_message,
            tool_results=batch.messages,
            messages=current.messages,
            new_messages=new_messages,
        )
        decision = await _call_finish_turn(config, turn_context)
        await emit(TurnEnd(turn=turn_no, message=assistant_message, tool_results=batch.messages))

        # finish_turn 明确要求结束，就不再管后面有没有工具调用。
        if decision is not None and decision.action == "end":
            return "completed", None, turn_no

        # 没有工具调用时，模型通常是在给最终回答。除非 finish_turn 明确要求继续，
        # 否则本轮就是正常结束。
        wants_continue = decision is not None and decision.action == "continue"
        if not tool_calls and not wants_continue:
            return "completed", None, turn_no

        # 所有工具结果都要求终止时，循环也结束。
        # 例子：某个“提交最终报告”的工具返回 terminate=True，模型不需要再说话。
        if batch.terminate:
            return "completed", None, turn_no

        # 工具执行过程中用户取消，本轮结束后停止。
        if token.cancelled:
            return "cancelled", None, turn_no

        # 4) 进入下一轮。先检查轮数上限，避免发出一个实际上不会执行的 turn_start。
        # 例子：max_turns=2，第 2 轮工具执行完后这里就返回 max_turns，不会再发 turn_start(3)。
        if turn_no >= config.max_turns:
            return "max_turns", f"达到最大轮数 {config.max_turns}", turn_no
        turn_no += 1
        await emit(TurnStart(turn=turn_no))


async def _stream_assistant_response(
    current: AgentContext,
    config: AgentLoopConfig,
    emit: EventSink,
    stream_fn: StreamFn,
    token: CancellationToken,
    turn_no: int,
) -> AssistantMessage:
    """调一次模型，并把流式事件转成运行事件。

    这里就是 Pi README 里的：
        AgentMessage[] → transformContext() → convertToLlm() → 模型
    区别是 Pi 的低层循环把系统提示词和工具声明放在 transcript 的系统消息里；
    本轮由 Harness 负责拼系统消息，`tools` 单独挂在 `ModelRequest` 上。
    """

    # 每轮请求前先让平台有机会裁剪/注入上下文。第 2 步的 Topic 和有界上下文会挂在这里。
    messages = current.messages
    if config.transform_context is not None:
        try:
            messages = await config.transform_context(messages, token)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 上下文准备失败也要变成可记录的模型错误
            return await _append_stream_error(
                current=current,
                emit=emit,
                turn_no=turn_no,
                message=f"上下文准备失败：{exc}",
            )

    # 把平台内部消息转成模型真正能理解的消息。默认原样返回。
    try:
        converted = config.convert_to_llm(messages)
        # 钩子可能写成普通函数，也可能写成 async 函数。
        # `inspect.isawaitable` 先判断返回值是不是“可等待对象”，是才 await。
        # 这样调用方两种写法都能用，不必为了统一而强制多包一层 async。
        if inspect.isawaitable(converted):
            converted = await converted
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - 同上
        return await _append_stream_error(
            current=current,
            emit=emit,
            turn_no=turn_no,
            message=f"消息转换失败：{exc}",
        )

    request = ModelRequest(
        messages=list(converted),
        # 工具列表不直接取 `current.tools`，而是走 `tools_provider`。
        # 这样 Harness 可以按需加载：第一轮只给 slow_log_query，模型搜索后再把
        # execution_plan 等工具放进后续请求，不必一开始就把所有定义塞给模型。
        tools=_resolve_tools(current, config),
        model=config.model,
        cancellation=token,
        # 复制一份 metadata，避免模型适配器或记录器无意中改到循环内部状态。
        # 例子：`context_budget` 描述本轮上下文用了多少字符，记录器只读它。
        metadata=dict(current.metadata),
    )

    # 记录 partial：Pi 的流式事件每一帧都带“到目前为止的完整消息”，
    # 循环用最新 partial 覆盖上下文里的临时助手消息。
    partial: AssistantMessage | None = None
    added_partial = False

    try:
        stream = stream_fn(request)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - 适配器同步抛错，也变成模型错误
        return await _append_stream_error(
            current=current,
            emit=emit,
            turn_no=turn_no,
            message=f"模型请求失败：{exc}",
        )

    # `async for` 每次从异步生成器取一个事件。模型可以“说一个字就 yield 一次”，
    # 循环收到就立刻发 `message_update`；不需要把整段回复攒在内存里再处理。
    try:
        async for event in stream:
            if isinstance(event, AssistantStart):
                # 模型开始回复：先把“空消息”放进上下文，后续帧不断替换它。
                # 这样事件顺序和 Pi 一致：message_start 先出现，message_update 随后。
                partial = event.partial
                current.messages.append(partial)
                added_partial = True
                await emit(MessageStart(message=partial, turn=turn_no))

            elif isinstance(event, (AssistantTextDelta, AssistantToolCallEvent)):
                # 文本增量或工具调用生成：用事件里的 partial 覆盖上一条临时消息。
                if partial is not None:
                    partial = event.partial
                    current.messages[-1] = partial
                    await emit(
                        MessageUpdate(
                            message=partial,
                            assistant_event=event,
                            turn=turn_no,
                        )
                    )

            elif isinstance(event, (AssistantDone, AssistantError)):
                # 这一轮结束：用最终消息替换临时消息；如果适配器没发 start，就补一个。
                final_message = event.message
                if added_partial:
                    current.messages[-1] = final_message
                else:
                    current.messages.append(final_message)
                    await emit(MessageStart(message=final_message, turn=turn_no))
                await emit(MessageEnd(message=final_message, turn=turn_no))
                return final_message

    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - 流中途抛错也要留下错误消息
        return await _append_stream_error(
            current=current,
            emit=emit,
            turn_no=turn_no,
            message=f"模型流读取失败：{exc}",
        )

    # 流结束但既没有 done 也没有 error：按错误处理，避免循环拿着半截消息继续跑。
    return await _append_stream_error(
        current=current,
        emit=emit,
        turn_no=turn_no,
        message="模型流在没有 done/error 事件的情况下结束",
    )


async def _append_stream_error(
    current: AgentContext,
    emit: EventSink,
    turn_no: int,
    message: str,
) -> AssistantMessage:
    """生成一条错误助手消息，放进上下文并发出开始/结束事件。"""
    error_message = AssistantMessage(
        content=[],
        stop_reason=StopReason.ERROR,
        error_message=message,
    )
    current.messages.append(error_message)
    await emit(MessageStart(message=error_message, turn=turn_no))
    await emit(MessageEnd(message=error_message, turn=turn_no))
    return error_message


async def _fail_truncated_tool_calls(
    tool_calls: list[ToolCall],
    emit: EventSink,
    turn_no: int,
) -> _ExecutedToolBatch:
    """模型输出被截断时，所有工具调用都不执行，统一返回错误。

    为什么不能执行：
    流式解析器可能会把截断的 JSON “尽量补全”，参数看起来合法，但其实少了条件。
    例子：本应是 `{"instance_id": "db-1", "start": "10:00", "end": "11:00"}`，
    截断后可能只剩 `{"instance_id": "db-1"}`，直接执行会查错范围。
    """
    batch = _ExecutedToolBatch()
    for tool_call in tool_calls:
        await emit(
            ToolExecutionStart(
                tool_call_id=tool_call.id,
                tool_name=tool_call.name,
                arguments=tool_call.arguments,
                turn=turn_no,
            )
        )
        result = _error_tool_result(
            f"工具 {tool_call.name} 未执行：模型输出达到 token 上限，参数可能被截断，请重新发起完整调用。"
        )
        await emit(
            ToolExecutionEnd(
                tool_call_id=tool_call.id,
                tool_name=tool_call.name,
                result=result,
                is_error=True,
                duration_ms=None,
                turn=turn_no,
            )
        )
        message = _create_tool_result_message(tool_call, result)
        await emit(MessageStart(message=message, turn=turn_no))
        await emit(MessageEnd(message=message, turn=turn_no))
        batch.messages.append(message)
    return batch


async def _execute_tool_calls(
    current: AgentContext,
    assistant_message: AssistantMessage,
    tool_calls: list[ToolCall],
    config: AgentLoopConfig,
    emit: EventSink,
    token: CancellationToken,
    turn_no: int,
) -> _ExecutedToolBatch:
    """按模型给出的顺序执行一批工具调用。

    Pi 默认是 parallel，本轮先做 sequential：
    - 一次只跑一个工具，时间线更容易看懂；
    - 模拟工具之间没有依赖，但顺序执行已经能验证“调用 id 配对”和“失败继续”。
    """
    batch = _ExecutedToolBatch()
    finalized_results: list[ToolResult] = []

    for tool_call in tool_calls:
        # 已经取消时，不再真正执行工具，但仍然给每个调用生成错误结果。
        # 这样助手消息里的每个 toolCall 都有对应的 toolResult，配对关系不会断。
        if token.cancelled:
            await emit(
                ToolExecutionStart(
                    tool_call_id=tool_call.id,
                    tool_name=tool_call.name,
                    arguments=tool_call.arguments,
                    turn=turn_no,
                )
            )
            result = _error_tool_result("操作已取消")
            await emit(
                ToolExecutionEnd(
                    tool_call_id=tool_call.id,
                    tool_name=tool_call.name,
                    result=result,
                    is_error=True,
                    duration_ms=None,
                    turn=turn_no,
                )
            )
            message = _create_tool_result_message(tool_call, result)
            await emit(MessageStart(message=message, turn=turn_no))
            await emit(MessageEnd(message=message, turn=turn_no))
            batch.messages.append(message)
            finalized_results.append(result)
            continue

        # Pi 在准备工具之前就发 tool_execution_start，此时参数还没校验。
        # 这样即使工具名不存在，时间线上也能看到模型确实请求了这个工具。
        await emit(
            ToolExecutionStart(
                tool_call_id=tool_call.id,
                tool_name=tool_call.name,
                arguments=tool_call.arguments,
                turn=turn_no,
            )
        )

        outcome = await _prepare_and_execute_tool(
            current=current,
            assistant_message=assistant_message,
            tool_call=tool_call,
            config=config,
            token=token,
        )
        result = outcome.result

        # after 钩子只在工具真的执行过之后调用；找不到工具、参数错误、被拦截的调用不调用。
        # 这与 Pi 的 finalizeExecutedToolCall 行为一致。
        if outcome.executed and config.after_tool_call is not None:
            try:
                after_result = await config.after_tool_call(
                    AfterToolCallContext(
                        assistant_message=assistant_message,
                        tool_call=tool_call,
                        arguments=tool_call.arguments,
                        result=result,
                        is_error=result.is_error,
                        messages=current.messages,
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - after 钩子出错时，把结果改成错误
                result = _error_tool_result(f"工具结果后处理失败：{exc}")
                after_result = None
            if after_result is not None:
                result = _apply_after_hook(result, after_result)

        await emit(
            ToolExecutionEnd(
                tool_call_id=tool_call.id,
                tool_name=tool_call.name,
                result=result,
                is_error=result.is_error,
                duration_ms=result.duration_ms,
                turn=turn_no,
            )
        )

        message = _create_tool_result_message(tool_call, result)
        await emit(MessageStart(message=message, turn=turn_no))
        await emit(MessageEnd(message=message, turn=turn_no))
        batch.messages.append(message)
        finalized_results.append(result)

    # Pi 只有在“这一批每个工具结果都 terminate”时才提前结束。
    # 例子：一个工具要求结束，另一个工具没要求，循环仍然继续。
    batch.terminate = bool(finalized_results) and all(
        result.terminate for result in finalized_results
    )
    return batch


async def _prepare_and_execute_tool(
    current: AgentContext,
    assistant_message: AssistantMessage,
    tool_call: ToolCall,
    config: AgentLoopConfig,
    token: CancellationToken,
) -> _ToolExecutionOutcome:
    """校验、前置钩子、执行、超时处理的完整单工具流程。"""

    # 1) 找到工具。找不到就返回错误结果，让模型知道要改用别的工具。
    # 这里和模型请求使用同一个 `_resolve_tools`：模型没看到的工具，执行阶段也找不到，
    # 避免“模型猜到一个没加载的工具名就偷偷执行”的情况。
    tool = next(
        (item for item in _resolve_tools(current, config) if item.name == tool_call.name),
        None,
    )
    if tool is None:
        return _ToolExecutionOutcome(
            result=_error_tool_result(f"Tool {tool_call.name} not found"),
            executed=False,
        )

    # 2) 参数校验。Pi 用 TypeBox/JSON Schema 校验并做类型转换；
    # 本轮用 jsonschema 做基础校验，不做类型转换，差异已记录在 docs/pi-mapping.md。
    try:
        arguments = _validate_tool_arguments(tool.name, tool.parameters, tool_call.arguments)
    except ToolArgumentError as exc:
        return _ToolExecutionOutcome(result=_error_tool_result(str(exc)), executed=False)

    # 3) before 钩子。平台可以在这里做权限、预算、危险操作拦截。
    if config.before_tool_call is not None:
        try:
            before_result = await config.before_tool_call(
                BeforeToolCallContext(
                    assistant_message=assistant_message,
                    tool_call=tool_call,
                    arguments=arguments,
                    messages=current.messages,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 前置钩子出错，按拦截处理
            return _ToolExecutionOutcome(
                result=_error_tool_result(f"工具前置检查失败：{exc}"),
                executed=False,
            )

        # 钩子跑完后再看一次取消，和 Pi 在 beforeToolCall 后的检查一致。
        if token.cancelled:
            return _ToolExecutionOutcome(result=_error_tool_result("操作已取消"), executed=False)

        if before_result is not None and before_result.block:
            result = _error_tool_result(before_result.reason or "工具执行被拦截")
            if before_result.terminate:
                result.terminate = True
            return _ToolExecutionOutcome(result=result, executed=False)

    if token.cancelled:
        return _ToolExecutionOutcome(result=_error_tool_result("操作已取消"), executed=False)

    # 4) 执行工具，并用 asyncio.wait_for 加超时。
    # 工具自己抛异常时，不把整个任务炸掉，而是变成 is_error=True 的结果交给模型。
    timeout = tool.timeout_s if tool.timeout_s is not None else config.tool_timeout_s
    started_at = time.perf_counter()
    try:
        if timeout is not None and timeout > 0:
            # `asyncio.wait_for` 会等工具完成，最多等 timeout 秒。
            # 一旦超时，它会取消工具内部的协程并抛 TimeoutError；
            # 注意这个取消只对“能被取消的 await 点”有效，纯 CPU 死循环是停不下来的，
            # 所以真实工具里也要主动检查 cancellation.cancelled。
            result = await asyncio.wait_for(
                tool.handler(tool_call.id, arguments, token),
                timeout=timeout,
            )
        else:
            result = await tool.handler(tool_call.id, arguments, token)
    except asyncio.CancelledError:
        # CancelledError 在 3.8+ 继承自 BaseException，不是 Exception。
        # 所以它不会被上面的 `except Exception` 吞掉；这里显式再抛一次，表达“取消必须上浮”。
        # 硬取消继续向上传播，不转成普通工具错误。
        raise
    except TimeoutError:
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        result = _error_tool_result(f"工具 {tool.name} 执行超时（超过 {timeout} 秒）")
        result.duration_ms = duration_ms
        return _ToolExecutionOutcome(result=result, executed=True)
    except Exception as exc:  # noqa: BLE001 - 工具失败要回报给模型，而不是终止运行
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        result = _error_tool_result(str(exc) or exc.__class__.__name__)
        result.duration_ms = duration_ms
        return _ToolExecutionOutcome(result=result, executed=True)

    result.duration_ms = int((time.perf_counter() - started_at) * 1000)
    return _ToolExecutionOutcome(result=result, executed=True)


def _resolve_tools(current: AgentContext, config: AgentLoopConfig) -> list[ToolSpec]:
    """返回这一轮真正可用的工具列表。

    两种模式：
    - 没配置 `tools_provider`：沿用 `AgentContext.tools`，行为和第 1 步完全一样；
    - 配置了：每轮向 Harness 要一份“当前已加载”的工具列表，支持按需加载。

    为什么每次都重新调用，而不是缓存：
    `search_tools` 执行完会改变已加载集合，下一轮模型请求必须看到新工具。
    这个函数返回的是新列表，调用方改动它不会影响 ToolSession 内部状态。
    """
    if config.tools_provider is not None:
        return list(config.tools_provider())
    return list(current.tools)


def _validate_tool_arguments(
    tool_name: str,
    schema: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """用 JSON Schema 校验参数，并返回一份拷贝。

    返回拷贝是为了避免工具修改参数时影响模型原始消息（Pi 也会先 clone）。
    """
    try:
        # `jsonschema.validate` 失败会抛异常，而不是返回 True/False。
        # 这种“失败就抛”的写法适合参数校验：调用方必须显式处理，不会忘记检查返回值。
        jsonschema.validate(instance=arguments, schema=schema)
    except jsonschema.ValidationError as exc:
        path = ".".join(str(part) for part in exc.absolute_path) or "root"
        raise ToolArgumentError(
            f"工具 {tool_name} 参数校验失败：{path}: {exc.message}"
        ) from exc
    except jsonschema.SchemaError as exc:
        raise ToolArgumentError(f"工具 {tool_name} 的 Schema 定义不合法：{exc.message}") from exc
    return copy.deepcopy(arguments)


def _apply_after_hook(result: ToolResult, after_result: AfterToolCallResult) -> ToolResult:
    """按字段覆盖工具结果。

    例子：after 钩子只返回 `terminate=True`，那么 content、details、is_error 都保持原样，
    只有 terminate 被改掉。
    """
    content = after_result.content if after_result.content is not None else result.content
    details = result.details if after_result.details is _UNSET else after_result.details
    is_error = after_result.is_error if after_result.is_error is not None else result.is_error
    terminate = (
        after_result.terminate if after_result.terminate is not None else result.terminate
    )
    # `meta` 也按同样规则处理：钩子没设置就保留工具原值；
    # 显式设置（包括设置成 None）才覆盖。
    meta = result.meta if after_result.meta is _UNSET else after_result.meta
    return ToolResult(
        content=content,
        details=details,
        is_error=is_error,
        terminate=terminate,
        duration_ms=result.duration_ms,
        meta=meta,
    )


def _error_tool_result(message: str) -> ToolResult:
    """构造一个错误工具结果。"""
    return ToolResult(
        content=[TextContent(text=message)],
        details={},
        is_error=True,
    )


def _create_tool_result_message(tool_call: ToolCall, result: ToolResult) -> ToolResultMessage:
    """把运行时结果转成对话里的工具结果消息。

    关键点：`tool_call_id` 必须原样带回，这是消息、工具调用、结果三者的关联键。
    """
    return ToolResultMessage(
        tool_call_id=tool_call.id,
        tool_name=tool_call.name,
        content=result.content,
        details=result.details,
        is_error=result.is_error,
        duration_ms=result.duration_ms,
        # 把证据引用一起带进对话消息，下一轮模型和记录器都能看到。
        meta=result.meta,
    )


async def _call_finish_turn(
    config: AgentLoopConfig,
    turn_context: TurnContext,
) -> TurnDecision | None:
    """调用可选的 finish_turn 钩子。"""
    if config.finish_turn is None:
        return None
    return await config.finish_turn(turn_context)
