"""参考 Pi v1.5.0 ai/src/utils/retry.ts、provider-retry.ts、overflow.ts。

来源提交 f9a26365568127461c47a07a37dcb42c140a3ac5，许可见 NOTICE.md。
选择性移植：错误分类、有上限指数退避、取消等待。流式包装是我们的适配：
失败尝试写在模型记录里，但不给消息历史追加一串失败助手消息。
不重试工具，不把上下文溢出当临时网络错误，不承诺断点续传模型输出。
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace

from app.runtime.contracts import (
    AssistantDone,
    AssistantError,
    AssistantMessage,
    AssistantStart,
    AssistantStreamEvent,
    AssistantTextDelta,
    EventSink,
    ModelRequest,
    ModelRetry,
    StopReason,
    StreamFn,
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """初次调用不算 retry；max_retries=2 表示最多总共发三次请求。"""

    max_retries: int = 2
    base_delay_s: float = 0.5
    max_delay_s: float = 10.0

    def __post_init__(self) -> None:
        """配置错误在装配时暴露，避免任务已经运行才发现负数重试次数。"""
        if self.max_retries < 0 or self.base_delay_s < 0 or self.max_delay_s < 0:
            raise ValueError("重试次数和等待时间不能为负数")


def is_context_overflow(message: AssistantMessage) -> bool:
    """只识别错误文本；尚无真实 usage，暂不移植上游的静默溢出检测。"""
    if message.stop_reason is not StopReason.ERROR:
        return False
    text = message.error_message or ""
    if re.search(r"rate.?limit|too many requests|throttl", text, re.I):
        return False
    return bool(
        re.search(
            r"context[_ ](?:length|window).*exceed|contextwindowexceeded|prompt.*too long|"
            r"maximum context length|input.*exceeds.*context|too many tokens|"
            r"(?:^|\b)413(?:\b|:)|request.*too large",
            text,
            re.I,
        )
    )


def is_retryable_error(message: AssistantMessage) -> bool:
    """先排除确定性失败，再看临时错误；HTTP 429 不一定值得重试。"""
    if message.stop_reason is not StopReason.ERROR or is_context_overflow(message):
        return False
    text = message.error_message or ""
    if re.search(
        r"insufficient_quota|quota exceeded|out of budget|billing|usage.?limit|"
        r'"code"\s*:\s*"?1308(?!\d)|\b(?:400|401|403|404|422)\b|'
        r"API.?key|unauthori[sz]ed|forbidden|认证|密钥",
        text,
        re.I,
    ):
        return False
    return bool(
        re.search(
            r"(?<![a-z0-9])(?:408|409|429|500|502|503|504|520|524)(?![a-z0-9])|"
            r"overloaded|server.?busy|temporarily unavailable|rate.?limit|too many requests|"
            r"connection|network|timed? out|timeout|stream.*(?:ended|interrupted|读取失败)|"
            r"连接|网络|超时",
            text,
            re.I,
        )
    )


async def _wait_for_retry(delay_s: float, request: ModelRequest) -> bool:
    """等待计时或取消，返回是否可以继续；不用 sleep 忽略用户的取消请求。"""
    if request.cancellation.cancelled:
        return False
    try:
        await asyncio.wait_for(request.cancellation.wait(), timeout=delay_s)
        return False
    except TimeoutError:
        return not request.cancellation.cancelled


def with_model_retry(stream_fn: StreamFn, policy: RetryPolicy, emit: EventSink) -> StreamFn:
    """创建模型流包装器。内层应先包记录器，让每次尝试都有真实请求/响应记录。"""

    async def stream(request: ModelRequest) -> AsyncIterator[AssistantStreamEvent]:
        """异步生成器：一边转发流，一边等待终态再决定是否重试。"""
        started = False
        turn = int(request.metadata.get("turn", 0))
        for attempt in range(policy.max_retries + 1):
            if request.cancellation.cancelled:
                yield AssistantError(AssistantMessage(content=[], stop_reason=StopReason.ABORTED))
                return
            # replace 创建新 dataclass；原请求的 metadata 不会被改写。
            attempt_request = replace(
                request,
                metadata={**request.metadata, "retry_attempt": attempt},
            )
            terminal = None
            source = None
            try:
                source = stream_fn(attempt_request)
                async for event in source:
                    if isinstance(event, (AssistantDone, AssistantError)):
                        terminal = event
                        break
                    if isinstance(event, AssistantStart):
                        if started:
                            # 重新生成时清空当前预览，不再往历史里追加第二条临时助手消息。
                            yield AssistantTextDelta(delta="", partial=event.partial)
                            continue
                        started = True
                    yield event
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 适配器也可能在网络 await 时直接抛异常。
                terminal = AssistantError(
                    AssistantMessage(
                        content=[],
                        stop_reason=StopReason.ERROR,
                        error_message=f"模型流读取失败：{exc}",
                    )
                )
            finally:
                close = getattr(source, "aclose", None)
                if close is not None:
                    await close()
            if terminal is None:
                terminal = AssistantError(
                    AssistantMessage(
                        content=[],
                        stop_reason=StopReason.ERROR,
                        error_message="模型流读取失败：stream ended without done/error",
                    )
                )
            message = terminal.message
            if attempt >= policy.max_retries or not is_retryable_error(message):
                if attempt:
                    await emit(ModelRetry(attempt, 0, message.error_message or "", "finished", turn))
                yield terminal
                return
            delay = min(policy.base_delay_s * 2 ** min(attempt, 30), policy.max_delay_s)
            await emit(ModelRetry(attempt + 1, delay, message.error_message or "", turn=turn))
            if not await _wait_for_retry(delay, request):
                await emit(ModelRetry(attempt + 1, 0, "用户取消重试", "finished", turn))
                yield AssistantError(replace(message, stop_reason=StopReason.ABORTED))
                return
            await emit(ModelRetry(attempt + 1, 0, "", "attempt", turn))

    return stream
