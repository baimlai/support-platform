"""上下文拼装：系统提示词、Skill、工单、Topic 和有界历史。

## 这一层解决什么问题

模型每次只能收到有限长度的输入。一次 DBA 调查可能产生很多消息：
慢日志、执行计划、监控错误、锁等待、每一轮的助手说明……
如果每轮都发完整历史，上下文会越来越大，最后既贵又容易抓不住重点。

这里的策略是固定的四段式：

```text
系统规则 + 当前 Skill
Topic 摘要（已知事实、候选原因、未决问题、下一步）
工单原文
最近若干条消息（助手和工具结果，成对保留）
```

“完整历史”仍然保存在 `AgentContext.messages` 和 `messages.json` 里，
只是**发给模型的内容**经过 `build_model_context` 裁剪。两者分开后，
排查时既能回看完整过程，也能看到模型这一轮到底收到了什么。

## 有界裁剪的三条原则

1. **固定内容永远保留**：系统规则、工单、Topic 不参与“按新旧淘汰”。
2. **工具调用和结果成对保留**：助手消息里的 `ToolCall` 和后面的 `ToolResultMessage`
   必须一起出现或一起消失，否则模型会看到“只有结果没有调用”或反过来。
3. **大结果换成带证据编号的摘要**：不直接切半截 JSON，而是保留开头 + 说明
   “完整内容在 ev-xxxxxx”，模型需要时再用 `evidence_read` 读。

## 字符数 vs token 数

这里的预算全部按**字符数**估算，不是 token 精确计数。原因：
不同模型的 tokenizer 不一样，本地模拟也不想引入额外依赖。
记录和报告里会明确写“字符估算”，不假装成精确 token 数。

## 实现说明

- `@dataclass(slots=True)` 的 `ContextBudget` 只是一组配置，`__post_init__`
  可以在这里做参数兜底，避免调用方传入 0 或负数导致裁剪逻辑出错。
- `replace(message, ...)` 来自 `dataclasses`，会复制一个对象并替换指定字段。
  为什么不用 `message.content = ...`：那会改到完整历史，记录里就看不到原文了。
- `reversed(blocks)` 从最新消息往前选，保证“最近的对话优先保留”。
- `json.dumps(to_jsonable(message), ensure_ascii=False)` 用来估算一条消息的字符数，
  口径统一，虽然不是最精确，但足够做预算控制。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from app.harness.skills import Skill
from app.runtime.contracts import (
    AgentContext,
    AssistantMessage,
    CancellationToken,
    Message,
    SystemMessage,
    TextContent,
    ToolResultMessage,
    ToolSpec,
    TransformContextHook,
    UserMessage,
    content_text,
    to_jsonable,
)

BEGIN_TICKET = "<BEGIN_TICKET_JSON>"
END_TICKET = "<END_TICKET_JSON>"
BEGIN_TOPIC = "<BEGIN_TOPIC_JSON>"
END_TOPIC = "<END_TOPIC_JSON>"

# 基础系统规则。Skill 正文会追加在后面，不替换这些安全边界。
BASE_SYSTEM_PROMPT = """你是云数据库 DBA 工单调查助手，当前运行在本地只读模拟环境。

工作规则：
1. 只能使用平台提供的工具取证，不要编造工具结果、实例数据或变更记录。
2. 需要某个业务工具时，先用 search_tools 搜索；搜索只加载工具，不代表有权限，
   没有加载的工具无法执行。
3. 每条业务工具结果都会保存为证据，编号形如 ev-000001。结果被截断时，
   用 evidence_read(evidence_id, offset, limit) 分段读取原文，不要凭预览猜全文。
4. 工具失败、超时、无数据都必须如实说明；不能把“没查到”解释成“没有异常”。
5. 区分观测事实、推测和尚未验证的项；相关性不等于因果。
6. 最终回答使用 Markdown，并严格按以下顺序和二级标题输出：
   `## 结论`、`## 修复建议`、`## 证据链`、`## 已排除与待验证`、
   `## 调查过程`、`## 验证方法`、`## 当前限制`。
7. 结论必须先给唯一的主根因，并写“置信度：高/中/低”、直接证据和结论边界。
   只有证据不足、无法收敛时才列候选原因，最多 3 个，按可能性排序，并写清还缺什么证据；
   证据充分时不要把候选原因平铺成清单。修复建议按“首选方案 / 备选方案 / 临时缓解”
   分组，最多三组。
8. 不要声称已经执行任何生产变更；本轮只做只读调查和建议。
"""


@dataclass(slots=True)
class ContextBudget:
    """模型上下文预算。

    参数含义：
    - `max_chars`：整份模型输入的目标字符上限；
    - `max_recent_messages`：最多保留多少条最近消息；
    - `max_topic_chars`：Topic 摘要最多占多少字符；
    - `max_tool_result_chars`：单条工具结果进入上下文时最多保留多少字符；
    - `max_assistant_text_chars`：历史助手文本最多保留多少字符。

    例子：`max_chars=12000`、`max_tool_result_chars=1200` 时，
    一个 15000 字符的慢日志结果仍会完整落盘，但模型这一轮最多看到约 1200 字符的片段，
    并收到证据编号去读全文。
    """

    max_chars: int = 12000
    max_recent_messages: int = 10
    max_topic_chars: int = 4000
    max_tool_result_chars: int = 1200
    max_assistant_text_chars: int = 1200

    def __post_init__(self) -> None:
        """对配置做下限兜底，避免 0 或负数让裁剪逻辑失效。

        例子：调用方传 `max_tool_result_chars=0`，会被提到 200，
        至少还能看到一小段内容加截断说明，而不是空白。
        """
        self.max_chars = max(1000, int(self.max_chars))
        self.max_recent_messages = max(2, int(self.max_recent_messages))
        self.max_topic_chars = max(200, int(self.max_topic_chars))
        self.max_tool_result_chars = max(200, int(self.max_tool_result_chars))
        self.max_assistant_text_chars = max(200, int(self.max_assistant_text_chars))


def build_system_message(skill: Skill | None = None) -> SystemMessage:
    """创建系统消息：基础规则 + 当前 Skill 的正文。

    例子：加载 `slow-sql` Skill 后，系统消息里会追加“先查慢日志、再查执行计划、
    监控失败要标未验证”等调查步骤。
    """
    content = BASE_SYSTEM_PROMPT.strip()
    if skill is not None:
        content = f"{content}\n\n{skill.system_section()}"
    return SystemMessage(content=content)


def build_ticket_text(ticket: dict[str, Any]) -> str:
    """把工单对象拼成用户消息。

    用标记包住 JSON，而不是只靠花括号解析。例子：如果工单描述里本身带 `{}`，
    模型仍然能准确找到 `<BEGIN_TICKET_JSON>` 和 `<END_TICKET_JSON>` 之间的内容。
    """
    ticket_json = json.dumps(ticket, ensure_ascii=False, indent=2)
    return f"""下面是待处理的工单（mock=true，数据仅用于流程验证）：

{BEGIN_TICKET}
{ticket_json}
{END_TICKET}

请按当前 Skill 的调查方法开始调查：先确认实例、SQL 指纹和时间范围，再调用工具取证。
需要其它工具时先用 search_tools 搜索。如果某个工具失败或无数据，
请在结论中明确标注为“未验证”，不要解释为“没有异常”。
最后严格按系统提示要求的章节输出：先给 `## 结论`（唯一主根因 + 置信度 + 直接证据 +
结论边界），再给 `## 修复建议`；只有证据不足时才在结论里列最多 3 个候选，
未验证项放进 `## 已排除与待验证`，不要把所有假设平铺成候选清单。
"""


def build_user_message(ticket: dict[str, Any]) -> UserMessage:
    """创建第一条用户消息。"""
    return UserMessage(content=build_ticket_text(ticket))


def build_initial_context(
    ticket: dict[str, Any],
    tools: list[ToolSpec] | None = None,
    skill: Skill | None = None,
) -> AgentContext:
    """创建运行前的上下文。

    - `tools` 可以留空：第 2 步的工具由 `tools_provider` 按需提供；
    - 工单本身作为 `run_agent_loop` 的 prompts 传入，保证事件顺序和 Pi 一致：
      先 `agent_start`/`turn_start`，再发用户消息事件。
    """
    del ticket  # 工单正文由 build_user_message 负责，这里只需要系统消息和工具槽位
    return AgentContext(
        messages=[build_system_message(skill)],
        tools=list(tools or []),
    )


def make_transform_context(
    context: AgentContext,
    ticket: dict[str, Any],
    topic_provider: Callable[[], dict[str, Any]],
    budget: ContextBudget,
) -> TransformContextHook:
    """创建一个“每轮模型请求前裁剪上下文”的钩子。

    为什么要闭包捕获 `context`：
    内核的 transform 钩子只收到 `messages` 和取消令牌，不直接给 AgentContext。
    但记录器需要知道“这一轮用了多少字符”，所以这里提前拿到 context 对象，
    把预算信息写进 `context.metadata["context_budget"]`。
    内核在构造 `ModelRequest` 时会复制这份 metadata，记录器就能读到了。

    例子：第 3 轮请求前，钩子把 18 条历史裁成 9 条，写下
    `used_chars=8300`、`dropped_messages=9`，`model_requests.jsonl` 里能看到。
    """

    async def transform(
        messages: list[Message],
        cancellation: CancellationToken,
    ) -> list[Message]:
        # `del` 只是告诉阅读者和 lint：这个钩子不需要取消令牌。
        # 真正的取消检查由内核在每轮开始和工具执行前后完成。
        del cancellation
        built, info = build_model_context(
            messages=messages,
            ticket=ticket,
            topic=topic_provider(),
            budget=budget,
        )
        context.metadata["context_budget"] = info
        return built

    return transform


def build_model_context(
    messages: list[Message],
    ticket: dict[str, Any],
    topic: dict[str, Any] | None,
    budget: ContextBudget,
) -> tuple[list[Message], dict[str, Any]]:
    """把完整历史裁成这一轮真正发给模型的消息。

    返回 `(messages, budget_info)`：
    - `messages`：系统规则 + Topic + 工单 + 最近消息；
    - `budget_info`：字符预算、保留/丢弃数量、被裁剪的工具结果数量。

    例子：完整历史有 20 条消息，预算只允许最近 8 条，
    最早的一条助手消息和它的工具结果会被一起丢掉，不会只丢一半。
    """
    # 1) 固定头：所有系统消息（通常只有基础规则）+ 最新 Topic + 工单。
    system_messages = [message for message in messages if isinstance(message, SystemMessage)]
    ticket_message = _find_ticket_message(messages)
    if ticket_message is None and ticket:
        # 测试或以后接口直接传工单、历史里没有工单消息时，补一条，保证模型知道任务是什么。
        ticket_message = build_user_message(ticket)

    head: list[Message] = list(system_messages)
    if topic:
        head.append(_build_topic_message(topic, budget.max_topic_chars))
    if ticket_message is not None:
        head.append(ticket_message)

    # 2) 可裁剪部分：除了系统消息和工单以外的历史。
    recent = [
        message
        for message in messages
        if not isinstance(message, SystemMessage) and message is not ticket_message
    ]
    blocks = _group_message_blocks(recent)

    # 3) 从最新往前选，直到达到消息条数或字符预算。
    used_chars = sum(_message_chars(message) for message in head)
    selected: list[list[Message]] = []
    included_recent_count = 0
    dropped_blocks = 0

    for block in reversed(blocks):
        block_chars = sum(_message_chars(message) for message in block)
        too_many_messages = (
            included_recent_count + len(block) > budget.max_recent_messages
        )
        too_many_chars = used_chars + block_chars > budget.max_chars
        # 最新一块无论如何保留：如果连最后一条消息都装不下，就没法让模型回答当前问题了。
        if selected and (too_many_messages or too_many_chars):
            dropped_blocks += 1
            continue
        selected.append(block)
        used_chars += block_chars
        included_recent_count += len(block)

    # 上面是从后往前选的，这里翻回正常顺序，模型才能按时间阅读。
    selected.reverse()

    # 4) 对选中的消息做单条裁剪：大工具结果换成片段 + 证据编号。
    bounded_recent: list[Message] = []
    stubbed_tool_results = 0
    for block in selected:
        for message in block:
            bounded, stubbed = _bound_message(message, budget)
            bounded_recent.append(bounded)
            if stubbed:
                stubbed_tool_results += 1

    included = [*head, *bounded_recent]
    used_chars = sum(_message_chars(message) for message in included)

    budget_info: dict[str, Any] = {
        "max_chars": budget.max_chars,
        "used_chars": used_chars,
        "source_messages": len(messages),
        "included_messages": len(included),
        "included_recent_messages": len(bounded_recent),
        "dropped_messages": len(recent) - len(bounded_recent),
        "dropped_blocks": dropped_blocks,
        "stubbed_tool_results": stubbed_tool_results,
        "max_recent_messages": budget.max_recent_messages,
        "max_tool_result_chars": budget.max_tool_result_chars,
        "max_topic_chars": budget.max_topic_chars,
        # 最新一块可能本身就超预算。这时仍然保留它，但明确标记，方便排查。
        "over_budget": used_chars > budget.max_chars,
        "note": (
            "字符估算，不是 token 精确计数；system、工单和 Topic 固定包含；"
            "工具调用和工具结果按块保留，不会只留一半。"
        ),
    }
    return included, budget_info


def _find_ticket_message(messages: list[Message]) -> UserMessage | None:
    """在历史里找第一条带工单标记的用户消息。"""
    for message in messages:
        if not isinstance(message, UserMessage):
            continue
        text = content_text(message.content)
        if BEGIN_TICKET in text and END_TICKET in text:
            return message
    return None


def _build_topic_message(topic: dict[str, Any], max_chars: int) -> SystemMessage:
    """把 Topic 包成一条系统消息。

    为什么用 SystemMessage 而不是 UserMessage：
    Topic 是平台维护的状态，不是用户输入。放在系统消息里，模型不会把它
    当成“用户刚说了新要求”；也不会打乱工单消息和工具结果的配对。
    """
    topic_json = _bounded_topic_json(topic, max_chars)
    return SystemMessage(
        content=(
            "下面是平台维护的调查 Topic（不是用户新消息）。"
            "它只包含工具实际返回的事实和平台整理的待办；"
            "事实后的 ev-xxxxxx 是证据编号。\n"
            f"{BEGIN_TOPIC}\n{topic_json}\n{END_TOPIC}"
        )
    )


def _bounded_topic_json(topic: dict[str, Any], max_chars: int) -> str:
    """限制 Topic 的字符数，超预算时压缩成摘要。

    为什么不直接切 JSON 字符串：
    切一半的 JSON 会让模型误以为数据损坏。这里保留一份“结构相同、内容更短”的
    摘要，模型仍然能按 `facts[].key`、`candidates[].status` 读取，
    只是长句和部分数据字段被省略。如果这样还超，再退到最小说明。
    """
    text = json.dumps(topic, ensure_ascii=False, indent=2)
    if len(text) <= max_chars:
        return text

    facts = topic.get("facts") or []
    candidates = topic.get("candidates") or []
    summary = {
        "truncated": True,
        "question": topic.get("question"),
        "scope": topic.get("scope"),
        # 注意：facts 仍然保持“字典列表”的结构，而不是压成纯字符串。
        # 原因：模型和后续工具要靠 key/data 继续工作，结构一变就要另写一套解析。
        "facts": [
            _compact_fact(fact)
            for fact in facts[:6]
            if isinstance(fact, dict)
        ],
        "candidates": [
            _compact_candidate(candidate)
            for candidate in candidates[:4]
            if isinstance(candidate, dict)
        ],
        # `attempted_tools` 必须保留：模型靠它判断“哪些工具已经试过”，
        # 丢了就会反复调用同一个工具。这比保留某个事实的完整数据更关键。
        "attempted_tools": list(topic.get("attempted_tools") or []),
        "evidence_reads": list(topic.get("evidence_reads") or [])[-2:],
        "searches": list(topic.get("searches") or [])[-2:],
        "open_questions": [str(item)[:160] for item in topic.get("open_questions") or []][:4],
        "next_steps": [str(item)[:160] for item in topic.get("next_steps") or []][:4],
        "note": "Topic 超出字符预算，已压缩为摘要。",
    }
    compact = json.dumps(summary, ensure_ascii=False, indent=2)
    if len(compact) <= max_chars:
        return compact

    minimal = {
        "truncated": True,
        "question": topic.get("question"),
        "attempted_tools": list(topic.get("attempted_tools") or []),
        "note": "Topic 超出字符预算，只保留问题和已尝试工具；完整内容见 topic.json。",
    }
    return json.dumps(minimal, ensure_ascii=False, indent=2)


# 压缩 Topic 时优先保留这些数据字段。它们决定了模型下一步还能不能继续查：
# - `sql_fingerprint`：没有它就没法查执行计划；
# - `access_type`/`full_scan`：报告要判断是不是全表扫描；
# - `status`/`sample_count`：锁等待 no_data 的含义；
# - `truncated`/`raw_size_chars`：说明大结果是否被截断。
_COMPACT_FACT_KEYS = (
    "sql_fingerprint",
    "access_type",
    "key",
    "full_scan",
    "estimated_rows",
    "avg_latency_ms",
    "max_latency_ms",
    "count",
    "avg_rows_examined",
    "avg_rows_sent",
    "coverage_ratio",
    "status",
    "sample_count",
    "truncated",
    "raw_size_chars",
    "max_inline_chars",
)


def _compact_fact(fact: dict[str, Any]) -> dict[str, Any]:
    """把一条 Topic 事实压成更短的字典，保留 key 和关键数据。"""
    data = fact.get("data")
    data = data if isinstance(data, dict) else {}
    small_data = {key: data[key] for key in _COMPACT_FACT_KEYS if key in data}
    return {
        "key": fact.get("key"),
        "statement": str(fact.get("statement") or "")[:160],
        "evidence_id": fact.get("evidence_id"),
        "source_tool": fact.get("source_tool"),
        "observed": True,
        "data": small_data,
    }


def _compact_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    """把一条候选原因压短，保留状态和证据编号。"""
    return {
        "key": candidate.get("key"),
        "statement": str(candidate.get("statement") or "")[:160],
        "status": candidate.get("status"),
        "evidence": list(candidate.get("evidence") or []),
    }


def _group_message_blocks(messages: list[Message]) -> list[list[Message]]:
    """把消息按“助手消息 + 紧随其后的工具结果”分组。

    例子：
        [助手(call-1), 工具结果(call-1), 助手(call-2), 工具结果(call-2)]
    会分成两块。裁剪时按块丢弃，就不会出现“调用还在、结果没了”的情况。

    工具结果不是紧跟在助手消息后的情况（例如运行开始时就有）会单独成块。
    """
    blocks: list[list[Message]] = []
    index = 0
    while index < len(messages):
        message = messages[index]
        if isinstance(message, AssistantMessage):
            block = [message]
            index += 1
            while index < len(messages) and isinstance(messages[index], ToolResultMessage):
                block.append(messages[index])
                index += 1
            blocks.append(block)
        else:
            blocks.append([message])
            index += 1
    return blocks


def _bound_message(message: Message, budget: ContextBudget) -> tuple[Message, bool]:
    """裁剪单条消息，返回 `(新消息, 是否裁剪了工具结果)`。

    只处理“文本太长”的情况：
    - 工具结果：保留开头 + 截断说明 + 证据编号；
    - 助手文本 / 用户文本：保留开头 + 截断说明；
    - 工具调用本身不裁剪，参数通常很小，而且裁剪会破坏调用语义。
    """
    if isinstance(message, AssistantMessage):
        changed = False
        new_content: list[Any] = []
        for block in message.content:
            if isinstance(block, TextContent) and len(block.text) > budget.max_assistant_text_chars:
                changed = True
                new_content.append(
                    TextContent(
                        text=_truncate_text(
                            block.text,
                            budget.max_assistant_text_chars,
                            "助手文本超出上下文预算，已截断",
                        )
                    )
                )
            else:
                new_content.append(block)
        if not changed:
            return message, False
        return replace(message, content=new_content), False

    if isinstance(message, UserMessage):
        if isinstance(message.content, str):
            if len(message.content) <= budget.max_assistant_text_chars:
                return message, False
            return (
                replace(
                    message,
                    content=_truncate_text(
                        message.content,
                        budget.max_assistant_text_chars,
                        "用户消息超出上下文预算，已截断",
                    ),
                ),
                False,
            )
        changed = False
        new_blocks: list[TextContent] = []
        for block in message.content:
            if len(block.text) > budget.max_assistant_text_chars:
                changed = True
                new_blocks.append(
                    TextContent(
                        text=_truncate_text(
                            block.text,
                            budget.max_assistant_text_chars,
                            "用户消息超出上下文预算，已截断",
                        )
                    )
                )
            else:
                new_blocks.append(block)
        if not changed:
            return message, False
        return replace(message, content=new_blocks), False

    if isinstance(message, ToolResultMessage):
        text = "".join(block.text for block in message.content)
        if len(text) <= budget.max_tool_result_chars:
            return message, False
        evidence_id = message.meta.evidence_id if message.meta else None
        hint = f"原 {len(text)} 字符"
        if evidence_id:
            hint += f"，完整内容见证据 {evidence_id}"
        else:
            hint += "，完整内容见运行记录"
        new_text = _truncate_text(
            text,
            budget.max_tool_result_chars,
            f"工具结果超出上下文预算（{hint}）",
        )
        return replace(message, content=[TextContent(text=new_text)]), True

    return message, False


def _truncate_text(text: str, limit: int, reason: str) -> str:
    """保留开头 `limit` 个字符，并追加一句明确的截断说明。

    例子：`_truncate_text("abcdef", 3, "太长")` 返回 `"abc\\n…（太长）"`。
    说明里写清原因，模型才不会把截断当成完整内容。
    """
    return f"{text[:limit]}\n…（{reason}）"


def _message_chars(message: Message) -> int:
    """估算一条消息占用的字符数。

    用统一的 JSON 序列化长度做口径，虽然比“只数文本”略大，
    但对预算控制来说更保守。报告里会注明这是字符估算，不是 token 数。
    """
    return len(json.dumps(to_jsonable(message), ensure_ascii=False))
