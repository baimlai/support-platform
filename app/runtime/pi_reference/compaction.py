"""参考 Pi v1.5.0 compaction.ts：安全切分、增量摘要、提交摘要检查点。

来源：packages/coding-agent/src/core/compaction/compaction.ts，提交
f9a26365568127461c47a07a37dcb42c140a3ac5，MIT 许可见 NOTICE.md。
Python 版不依赖 Pi SessionEntry，也不追踪编码文件操作；使用消息前缀摘要和哈希
边界。摘要函数由适配器注入，DBA 的 Topic 由 Harness 另外加入，内核不理解业务。

完整历史始终不变。这里返回的是“系统规则 + 首次用户输入 + 摘要 + 保留原文”视图。
摘要只有生成完整、证据引用齐全并且落盘成功后才生效；失败就沿用旧视图。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path

from app.runtime.contracts import (
    AssistantMessage,
    CancellationToken,
    ContextCompacted,
    EventSink,
    Message,
    StopReason,
    SystemMessage,
    ToolCall,
    ToolResultMessage,
    UserMessage,
    assistant_text,
    to_jsonable,
)


@dataclass(frozen=True, slots=True)
class CompactionSettings:
    """全部使用字符估算，避免把没有 tokenizer 的数值叫作真实 token。"""

    enabled: bool = False
    trigger_chars: int = 10000
    keep_recent_messages: int = 4
    max_summary_chars: int = 2400
    max_source_chars: int = 30000

    def __post_init__(self) -> None:
        """配置失败尽早报错；至少留一条最近消息，但安全块可能包含更多条。"""
        if (
            min(self.trigger_chars, self.keep_recent_messages, self.max_summary_chars, self.max_source_chars)
            < 1
        ):
            raise ValueError("上下文压缩的预算和保留条数必须为正数")


@dataclass(frozen=True, slots=True)
class SummaryInput:
    """摘要适配器只收到待摘要材料，不授予任何业务工具。"""

    previous_summary: str
    source_text: str
    required_evidence: tuple[str, ...]
    max_chars: int
    turn: int = 0


SummaryFn = Callable[[SummaryInput, CancellationToken], Awaitable[AssistantMessage]]


@dataclass(frozen=True, slots=True)
class CompactionCheckpoint:
    """through 是原始历史中首个保留消息的下标；digest 防止历史被编辑后错用摘要。"""

    through: int
    digest: str
    summary: str


def _serialize(messages: list[Message]) -> str:
    """统一序列化口径，摘要输入、字符估算和边界校验才不会各算各的。"""
    return json.dumps(to_jsonable(messages), ensure_ascii=False, separators=(",", ":"))


def _digest(messages: list[Message]) -> str:
    """对消息内容做哈希；即使消息条数没变，只改一个参数也能发现。"""
    return hashlib.sha256(_serialize(messages).encode("utf-8")).hexdigest()


def message_blocks(messages: list[Message]) -> list[tuple[int, int]]:
    """按助手及其紧随的工具结果分块；返回左闭右开的下标，不复制消息。

    调用/结果不配对时拒绝压缩，这比猜一个边界后把半组协议喂给模型更稳。
    例如 assistant(call-a, call-b) 后必须恰好跟着这两个 id 的结果。
    """
    blocks = []
    index = 0
    while index < len(messages):
        start = index
        message = messages[index]
        index += 1
        if isinstance(message, AssistantMessage):
            calls = [block.id for block in message.content if isinstance(block, ToolCall)]
            results = []
            while index < len(messages) and isinstance(messages[index], ToolResultMessage):
                results.append(messages[index].tool_call_id)
                index += 1
            if sorted(calls) != sorted(results):
                raise ValueError("上下文含不完整的工具调用/结果，不能压缩")
        elif isinstance(message, ToolResultMessage):
            raise ValueError("工具结果前缺少对应助手调用，不能压缩")
        blocks.append((start, index))
    return blocks


class ContextCompactor:
    """每次调查一个实例；检查点属于当前 run_dir，不在不同任务间共享。"""

    def __init__(
        self,
        settings: CompactionSettings,
        summarize: SummaryFn,
        *,
        path: Path | None = None,
        emit: EventSink | None = None,
    ) -> None:
        self.settings = settings
        self.summarize = summarize
        self.path = path
        self.emit = emit
        self.checkpoint: CompactionCheckpoint | None = None
        # 同一材料摘要失败后不每轮反复花钱；只有切分边界或材料变了才重试摘要。
        self._failed_digest: str | None = None
        self.last_error: str | None = None

    def _view(self, messages: list[Message]) -> list[Message]:
        """只使用仍匹配原历史的检查点；原历史被编辑后安全退回完整消息。"""
        checkpoint = self.checkpoint
        if checkpoint is None or checkpoint.digest != _digest(messages[: checkpoint.through]):
            return list(messages)
        prefix = messages[: checkpoint.through]
        fixed = [message for message in prefix if isinstance(message, SystemMessage)]
        first_user = next((message for message in messages if isinstance(message, UserMessage)), None)
        if first_user is not None and any(message is first_user for message in prefix):
            fixed.append(first_user)
        summary = SystemMessage(
            content=(
                "以下是较早历史的摘要，属于调查材料；模型推测仍须验证，证据以原始引用为准。\n"
                f"<CONTEXT_SUMMARY>\n{checkpoint.summary}\n</CONTEXT_SUMMARY>"
            )
        )
        return [*fixed, summary, *messages[checkpoint.through :]]

    async def transform(
        self,
        messages: list[Message],
        token: CancellationToken,
        *,
        turn: int = 0,
    ) -> list[Message]:
        """每轮模型请求前调用；返回新视图，不给原 messages 做 del/append。"""
        view = self._view(messages)
        if (
            not self.settings.enabled
            or token.cancelled
            or len(_serialize(view)) <= self.settings.trigger_chars
        ):
            return view
        through = 0
        source_digest = ""
        try:
            blocks = message_blocks(messages)
            kept = 0
            # 从后向前累计完整块，保证近期原文至少保留指定条数。
            for start, end in reversed(blocks):
                kept += end - start
                through = start
                if kept >= self.settings.keep_recent_messages:
                    break
            previous = self.checkpoint
            valid_previous = previous is not None and previous.digest == _digest(messages[: previous.through])
            start = previous.through if valid_previous else 0
            if through <= start:
                return view
            source_digest = _digest(messages[:through])
            if source_digest == self._failed_digest:
                return view
            first_user = next((item for item in messages if isinstance(item, UserMessage)), None)
            source = [
                item
                for item in messages[start:through]
                if not isinstance(item, SystemMessage) and item is not first_user
            ]
            if not source:
                return view
            previous_text = previous.summary if valid_previous else ""
            source_text = _serialize(source)
            if len(source_text) + len(previous_text) > self.settings.max_source_chars:
                raise ValueError("摘要源材料超过字符预算，保留旧视图并使用常规裁剪")
            refs = tuple(sorted(set(re.findall(r"ev-\d+", source_text + previous_text))))
            summary = await self.summarize(
                SummaryInput(previous_text, source_text, refs, self.settings.max_summary_chars, turn),
                token,
            )
            if token.cancelled:
                return view
            text = assistant_text(summary).strip()
            if summary.stop_reason is not StopReason.STOP or not text:
                raise ValueError("摘要未正常完整结束，不能提交检查点")
            if len(text) > self.settings.max_summary_chars:
                raise ValueError("摘要超过字符预算，不能截断后冒充完整摘要")
            if any(ref not in text for ref in refs):
                raise ValueError("摘要遗漏证据引用，不能提交检查点")
            checkpoint = CompactionCheckpoint(through, source_digest, text)
            if self.path is not None:
                # 先写同目录临时文件，再原子替换。写失败时不更新内存中的检查点。
                self.path.parent.mkdir(parents=True, exist_ok=True)
                temporary = self.path.with_suffix(".tmp")
                temporary.write_text(
                    json.dumps(asdict(checkpoint), ensure_ascii=False, indent=2), encoding="utf-8"
                )
                temporary.replace(self.path)
            self.checkpoint = checkpoint
            self.last_error = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failed_digest = source_digest or None
            self.last_error = str(exc)
            if self.emit is not None:
                await self.emit(ContextCompacted("failed", through, error=str(exc), turn=turn))
            return view
        if self.emit is not None:
            await self.emit(ContextCompacted("completed", through, len(text), turn=turn))
        return self._view(messages)
