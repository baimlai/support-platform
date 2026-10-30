"""第 2 步使用的模拟模型。

它不连接任何大模型服务，而是按固定剧本走完一次“带 Skill、工具搜索、证据读取、
Topic 和有界上下文”的慢 SQL 调查。

## 为什么不再只看完整消息

第 1 步的模拟模型靠“完整消息里有没有某个工具结果”决定下一步。第 2 步加入
有界上下文后，旧消息会被裁掉：第 6 轮时，第 1 轮的慢日志消息已经不在模型输入里。
如果还按消息判断，模型会误以为慢日志没查过，重新调用工具，陷入循环。

因此这里优先读平台注入的 Topic：

- `attempted_tools` 告诉模型“哪些工具已经试过”；
- `facts[].data` 保留关键结构化数据，例如最慢 SQL 指纹；
- `candidates` 和 `open_questions` 用于生成报告。

这更接近真实 Agent 的做法：上下文可以裁剪，但“调查状态”要单独维护。

## 剧本顺序

1. `slow_log_query(limit=50)`：查慢日志，制造一个超过内联上限的大结果；
2. `evidence_read(offset=4000)`：演示原始结果分页读取；
3. `search_tools(...)`：按需加载执行计划、实例监控、锁等待；
4. `execution_plan(...)`：用 Topic 里的指纹查执行计划；
5. `instance_metrics` + `lock_wait_query`：演示接口失败和无数据；
6. 生成 Markdown 报告：只依据实际工具结果和 Topic，不写死结论。

## 实现说明

- `async def` 里只要有 `yield`，它就是一个“异步生成器”。调用它不会立刻执行，
  而是返回一个可以 `async for` 的对象。
- `stream` 里直接 `return self._stream(request)`，是把异步生成器对象交给调用方；
  不是返回一个普通值，也不是提前执行。
- `dict.setdefault(key, []).append(...)` 是“分组收集”的常用写法：
  key 不存在时先放一个空列表，再往里追加。
- `re.search(r"指纹\\s*([A-Za-z0-9_-]+)", text)` 用来从自然语言里提取指纹。
  为什么需要它：如果 Topic 被压缩成摘要，结构化 `data` 可能被省略，
  但摘要里的 `statement` 仍然包含“指纹 a1b2c3d4e5f6”，可以作为兜底。
- `"\n".join([...])` 把多个字符串用换行拼起来，比在循环里反复 `+=` 更清晰。
- `*lines` 是列表解包，把列表里的每一项作为独立元素放进外层列表。
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from typing import Any

from app.adapters.scenario_mock import ScenarioEngine
from app.runtime.contracts import (
    AssistantDone,
    AssistantError,
    AssistantMessage,
    AssistantStart,
    AssistantStreamEvent,
    AssistantTextDelta,
    AssistantToolCallEvent,
    Content,
    ModelRequest,
    StopReason,
    SystemMessage,
    TextContent,
    ToolCall,
    ToolResultMessage,
    UserMessage,
    content_text,
)

BEGIN_TICKET = "<BEGIN_TICKET_JSON>"
END_TICKET = "<END_TICKET_JSON>"
BEGIN_TOPIC = "<BEGIN_TOPIC_JSON>"
END_TOPIC = "<END_TOPIC_JSON>"

# 工具名到中文标签的映射，只用于报告里的“调查过程”表格。
_TOOL_LABELS: dict[str, str] = {
    "slow_log_query": "慢日志查询",
    "evidence_read": "证据读取",
    "search_tools": "工具搜索",
    "execution_plan": "执行计划",
    "instance_metrics": "实例监控",
    "lock_wait_query": "锁等待",
}

# 每个业务工具成功后，会在 Topic 里写哪个事实。
# 报告和过程表格用这张表判断“这个工具到底成功了没有”。
_FACT_KEY_BY_TOOL: dict[str, str] = {
    "slow_log_query": "slow_log.top",
    "execution_plan": "execution_plan.access",
    "instance_metrics": "instance_metrics.available",
    "lock_wait_query": "lock_wait.samples",
}

# Topic 候选原因的状态 -> 报告里的中文标签。
_STATUS_LABELS: dict[str, str] = {
    "supported": "有观测支持",
    "candidate": "候选",
    "unverified": "未验证",
}

# 第 3 步需要的全部业务工具。搜索时用它们判断“是不是已经都加载了”。
_ALL_BUSINESS_TOOLS = frozenset(
    {"execution_plan", "instance_metrics", "lock_wait_query"}
)


class MockDbaModel:
    """按剧本执行的模拟模型。

    参数：
    - `delay_s`：每段文本之间的停顿，让 CLI 能看到流式效果；测试里传 0 可以跑得更快；
    - `chunk_size`：每次流出的字符数，模拟真实模型的分段输出。
    """

    def __init__(self, delay_s: float = 0.005, chunk_size: int = 16) -> None:
        self._delay_s = delay_s
        self._chunk_size = max(1, chunk_size)
        # 工具调用 id 自增，保证同一次运行里每个调用都有唯一 id。
        self._call_seq = 0
        # 多数据库场景使用数据驱动的 ScenarioEngine；旧慢 SQL 工单继续走
        # 下面写死的六步剧本。`_scenario_checked` 防止每个模型轮次都去读一次
        # 数据集文件：第一次判断完就把结果缓存下来。
        self._scenario_checked = False
        self._scenario_engine: ScenarioEngine | None = None

    def stream(self, request: ModelRequest) -> AsyncIterator[AssistantStreamEvent]:
        """模型适配器入口，返回异步事件流。

        说明：`_stream` 是异步生成器函数，调用 `self._stream(request)` 只创建
        生成器对象，并不会执行里面的代码；真正开始跑是在内核 `async for` 的时候。
        """
        return self._stream(request)

    async def _stream(self, request: ModelRequest) -> AsyncIterator[AssistantStreamEvent]:
        if request.cancellation.cancelled:
            yield AssistantError(
                message=AssistantMessage(
                    content=[],
                    stop_reason=StopReason.ABORTED,
                    error_message="用户已取消",
                )
            )
            return

        messages = request.messages
        ticket = _extract_ticket(messages)
        topic = _extract_topic(messages)
        facts = _facts_by_key(topic)

        text, calls = self._next_action(
            request=request,
            ticket=ticket,
            topic=topic,
            facts=facts,
            messages=messages,
        )

        async for event in self._stream_message(request, text, calls):
            yield event

    def _next_action(
        self,
        *,
        request: ModelRequest,
        ticket: dict[str, Any],
        topic: dict[str, Any],
        facts: dict[str, dict[str, Any]],
        messages: list[Any],
    ) -> tuple[str, list[ToolCall]]:
        """根据 Topic 和“当前可见工具”决定这一轮做什么。

        返回 `(说明文本, 工具调用列表)`。调用列表为空表示这一轮输出最终报告。
        """
        # 场景工单的步骤、工具参数和诊断结论都在 `fixtures/mock_dba/*.json` 里。
        # 这里先做一次“是不是场景工单”的判断：
        # - 是：交给 ScenarioEngine，按数据文件里的 steps 执行；
        # - 否：继续执行下面的慢 SQL 六步剧本，保证旧样例行为不变。
        if not self._scenario_checked:
            self._scenario_engine = ScenarioEngine.from_ticket(ticket)
            self._scenario_checked = True
        if self._scenario_engine is not None:
            return self._scenario_engine.next_action(
                request=request,
                ticket=ticket,
                topic=topic,
                messages=messages,
                visible_tools={tool.name for tool in request.tools},
                make_call=self._new_call,
            )

        visible = {tool.name for tool in request.tools}
        attempted = _attempted_tools(topic, messages)

        instance_id = str(ticket.get("instance_id", "db-mysql-001"))
        time_range = ticket.get("time_range", {})
        start_time = str(time_range.get("start", "2026-10-29T10:00:00+08:00"))
        end_time = str(time_range.get("end", "2026-10-29T11:00:00+08:00"))

        slow_fact = facts.get("slow_log.top") or {}
        slow_data = slow_fact.get("data") or {}
        slow_evidence_id = str(slow_fact.get("evidence_id") or "") or _last_evidence_id(
            messages, "slow_log_query"
        )
        slow_truncated = bool(slow_data.get("truncated")) or _last_truncated(
            messages, "slow_log_query"
        )

        # 1) 第一步永远是查慢日志。limit=50 是为了让结果超过内联上限，
        #    从而触发“保存证据 + 上下文只给预览”的分支。
        if "slow_log_query" not in attempted and "slow_log_query" in visible:
            return (
                "我先按工单里的实例和时间范围查询慢日志。为了看到完整分布，本次取 50 条；"
                "原始结果可能较大，平台会保存为证据，模型只看到有界预览。",
                [
                    self._new_call(
                        "slow_log_query",
                        {
                            "instance_id": instance_id,
                            "start_time": start_time,
                            "end_time": end_time,
                            "limit": 50,
                        },
                    )
                ],
            )

        # 2) 慢日志结果被截断时，读一段原文，演示 evidence_read 的分页能力。
        #    这里故意从 offset=4000 开始，而不是从 0 开始，说明“预览之外还有内容”。
        if slow_evidence_id and slow_truncated and "evidence_read" not in attempted:
            if "evidence_read" in visible:
                raw_size = slow_data.get("raw_size_chars") or "未知"
                return (
                    f"慢日志原始结果约 {raw_size} 字符，已保存为证据 {slow_evidence_id}。"
                    "我先从第 4001 个字符开始读 800 字，确认分页读取可用，"
                    "而不是只凭预览猜全文。",
                    [
                        self._new_call(
                            "evidence_read",
                            {
                                "evidence_id": slow_evidence_id,
                                "offset": 4000,
                                "limit": 800,
                            },
                        )
                    ],
                )

        # 3) 初始只开放慢日志。需要其它工具时先搜索，命中后才在下一轮可见。
        if (
            "search_tools" not in attempted
            and "search_tools" in visible
            and not _ALL_BUSINESS_TOOLS.issubset(visible)
        ):
            return (
                "慢日志已经看过。接下来用 search_tools 搜索执行计划、实例监控和锁等待，"
                "按需加载后续工具；搜索只加载工具，不代表获得权限。",
                [
                    self._new_call(
                        "search_tools",
                        {"query": "执行计划 实例监控 锁等待", "limit": 5},
                    )
                ],
            )

        # 4) 指纹优先从 Topic 的结构化数据里取。
        #    如果 Topic 被压缩成摘要，就从 statement 或最近一条慢日志结果里兜底。
        fingerprint = _extract_fingerprint(facts, messages)
        if fingerprint and "execution_plan" not in attempted and "execution_plan" in visible:
            return (
                f"按 Topic 里的最慢 SQL 指纹 {fingerprint} 查询执行计划，"
                "确认访问类型、是否命中索引以及扫描行数。",
                [
                    self._new_call(
                        "execution_plan",
                        {"instance_id": instance_id, "sql_fingerprint": fingerprint},
                    )
                ],
            )

        # 5) 同时补实例监控和锁等待。它们一个会失败、一个会返回 no_data，
        #    正好用来验证“失败不等于正常、无数据不等于没有”。
        calls: list[ToolCall] = []
        if "instance_metrics" not in attempted and "instance_metrics" in visible:
            calls.append(
                self._new_call(
                    "instance_metrics",
                    {
                        "instance_id": instance_id,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
            )
        if "lock_wait_query" not in attempted and "lock_wait_query" in visible:
            calls.append(
                self._new_call(
                    "lock_wait_query",
                    {
                        "instance_id": instance_id,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
            )
        if calls:
            return (
                "最后补两类辅助证据：实例监控用于判断资源瓶颈，"
                "锁等待用于判断是否存在等待样本。",
                calls,
            )

        # 6) 没有可做的工具了，生成最终报告。
        #    注意：报告只使用 Topic 事实、候选原因和当前上下文里还留着的工具结果。
        return (
            self._build_report(
                request=request,
                ticket=ticket,
                topic=topic,
                facts=facts,
                messages=messages,
            ),
            [],
        )

    def _new_call(self, name: str, arguments: dict[str, Any]) -> ToolCall:
        """创建一个工具调用，id 形如 `call-1`、`call-2`。"""
        self._call_seq += 1
        return ToolCall(id=f"call-{self._call_seq}", name=name, arguments=arguments)

    async def _stream_message(
        self,
        request: ModelRequest,
        text: str,
        tool_calls: list[ToolCall],
    ) -> AsyncIterator[AssistantStreamEvent]:
        """把一段文本和若干工具调用按流式事件发出去。"""
        stop_reason = StopReason.TOOL_USE if tool_calls else StopReason.STOP
        partial = AssistantMessage(content=[], stop_reason=stop_reason)
        yield AssistantStart(partial=partial)

        accumulated = ""
        for index in range(0, len(text), self._chunk_size):
            chunk = text[index : index + self._chunk_size]
            if request.cancellation.cancelled:
                yield AssistantError(
                    message=AssistantMessage(
                        content=[TextContent(text=accumulated + "\n\n[操作已取消]")],
                        stop_reason=StopReason.ABORTED,
                        error_message="用户已取消",
                    )
                )
                return

            accumulated += chunk
            partial = AssistantMessage(
                content=[TextContent(text=accumulated)],
                stop_reason=stop_reason,
            )
            yield AssistantTextDelta(delta=chunk, partial=partial)
            if self._delay_s > 0:
                await asyncio.sleep(self._delay_s)

        # 工具调用逐个发出。每个事件都带“当前完整 partial”，和 Pi 的流式形状一致。
        content: list[Content] = []
        if accumulated:
            content.append(TextContent(text=accumulated))
        for tool_call in tool_calls:
            if request.cancellation.cancelled:
                yield AssistantError(
                    message=AssistantMessage(
                        content=list(content),
                        stop_reason=StopReason.ABORTED,
                        error_message="用户已取消",
                    )
                )
                return
            content.append(tool_call)
            partial = AssistantMessage(content=list(content), stop_reason=stop_reason)
            yield AssistantToolCallEvent(tool_call=tool_call, partial=partial)

        final_message = AssistantMessage(
            content=list(content),
            stop_reason=stop_reason,
        )
        yield AssistantDone(message=final_message)

    def _build_report(
        self,
        *,
        request: ModelRequest,
        ticket: dict[str, Any],
        topic: dict[str, Any],
        facts: dict[str, dict[str, Any]],
        messages: list[Any],
    ) -> str:
        """根据 Topic 和实际工具结果拼出最终报告。

        这里的“事实”全部来自 Topic；Topic 又只根据工具真实返回更新。
        因此即使慢日志消息已经被上下文裁掉，报告里的指纹、耗时和证据编号仍然准确。
        """
        attempted = _attempted_tools(topic, messages)
        candidates = _candidates(topic)
        candidate_map = {
            str(item.get("key")): item
            for item in candidates
            if isinstance(item, dict) and item.get("key")
        }
        open_questions = [str(item) for item in topic.get("open_questions") or []]

        # ---------- 现象（作为结论里的观测摘要，不再单独占一个章节） ----------
        symptom = ticket.get("symptom") or ticket.get("title") or "工单未填写现象"
        phenomenon_lines = [
            f"- 工单 `{ticket.get('ticket_id', '未知')}`：{symptom}",
            f"- 实例：`{ticket.get('instance_id', '未知')}`；"
            f"业务库：`{ticket.get('database', '未知')}`",
        ]
        slow_fact = facts.get("slow_log.top")
        if slow_fact:
            phenomenon_lines.append(
                f"- 慢日志观测：{slow_fact.get('statement')}"
                f"（证据 `{slow_fact.get('evidence_id')}`）"
            )
        else:
            phenomenon_lines.append("- 慢日志：没有拿到可用摘要，最慢 SQL 未确认。")

        plan_fact = facts.get("execution_plan.access")
        if plan_fact:
            phenomenon_lines.append(
                f"- 执行计划观测：{plan_fact.get('statement')}"
                f"（证据 `{plan_fact.get('evidence_id')}`）"
            )

        # ---------- 调查过程 ----------
        process_lines = [
            "| # | 工具 | 结果 | 证据 |",
            "| --- | --- | --- | --- |",
        ]
        for index, tool_name in enumerate(attempted, start=1):
            status, evidence = _describe_tool(tool_name, facts, messages, topic)
            label = _TOOL_LABELS.get(tool_name, tool_name)
            process_lines.append(
                f"| {index} | `{tool_name}`（{label}） | {status} | {evidence} |"
            )
        process_lines.extend(
            [
                "",
                "> Skill：`slow-sql`（慢 SQL 调查）。初始只开放 `slow_log_query`，"
                "其余工具通过 `search_tools` 按需加载；大结果保存到 `evidence/`，"
                "本次还调用了 `evidence_read` 分段读取。",
                f"> {_context_budget_line(request)}",
            ]
        )

        # ---------- 证据链：只列支撑结论的关键证据 ----------
        # 监控超时、锁等待 no_data 这类“未验证”事实不属于证据链，
        # 它们放在“已排除与待验证”里，避免把“没有数据”说成“有证据”。
        evidence_lines: list[str] = []
        conclusion_fact_keys = (
            "slow_log.top",
            "slow_log.coverage",
            "execution_plan.access",
        )
        for key in conclusion_fact_keys:
            fact = facts.get(key)
            if not fact:
                continue
            evidence_id = fact.get("evidence_id")
            ref = f"（证据 `{evidence_id}`）" if evidence_id else ""
            evidence_lines.append(
                f"- **{fact.get('key')}**：{fact.get('statement')}{ref}"
            )
        if not evidence_lines:
            evidence_lines.append("- 没有形成可引用的事实，关键证据待补充。")

        # ---------- 结论：先给唯一主根因，证据不足才列候选 ----------
        conclusion_lines = _conclusion_lines(phenomenon_lines, facts, candidates)

        # ---------- 建议：首选 / 备选 / 临时缓解 ----------
        advice_lines = _recommendation_lines(facts)

        # ---------- 已排除与待验证 ----------
        ruled_lines = _ruled_out_lines(facts, candidate_map, open_questions)

        # ---------- 验证方法 ----------
        verify_lines = [
            "1. 用同一时间范围重新采集慢日志，对比平均耗时和扫描行数是否稳定复现。",
            "2. 在只读环境对最慢 SQL 执行 `EXPLAIN ANALYZE`，"
            "把真实执行计划与本次模拟计划对照。",
            "3. 补采实例监控和更长窗口的锁等待样本，复核本次未验证项。",
            "4. 如果调整索引，先在测试库对比调整前后的执行计划和慢日志统计。",
        ]

        # ---------- 当前限制 ----------
        limitation_lines = _limitation_lines(facts, candidate_map, attempted, request)

        # 这里用 `join` 一次性拼出整段 Markdown。
        # `*conclusion_lines` 前面的星号是“解包”：把列表里的每个字符串当成独立元素
        # 放进外层列表，而不是把整个列表塞成一个元素。
        return "\n".join(
            [
                "# 慢 SQL 工单调查报告（模拟 · 第 2 步）",
                "",
                "## 结论",
                *conclusion_lines,
                "",
                "## 修复建议",
                *advice_lines,
                "",
                "## 证据链",
                *evidence_lines,
                "",
                "## 已排除与待验证",
                *ruled_lines,
                "",
                "## 调查过程",
                *process_lines,
                "",
                "## 验证方法",
                *verify_lines,
                "",
                "## 当前限制",
                *limitation_lines,
            ]
        )


def _extract_ticket(messages: list[Any]) -> dict[str, Any]:
    """从用户消息里提取工单 JSON。

    用标记而不是直接找花括号，是因为工单描述里可能本身带 `{}`。
    解析失败时返回空字典，模型会使用兜底实例和时间范围，报告里仍会标明来源不明。
    """
    for message in messages:
        if not isinstance(message, UserMessage):
            continue
        text = content_text(message.content)
        if BEGIN_TICKET not in text or END_TICKET not in text:
            continue
        # `split(separator, 1)` 的第二个参数表示“最多切几刀”。
        # 传 1 可以避免工单 JSON 里恰好出现同样的标记时把内容切碎。
        raw = text.split(BEGIN_TICKET, 1)[1].split(END_TICKET, 1)[0].strip()
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _extract_topic(messages: list[Any]) -> dict[str, Any]:
    """从系统消息里取出最新 Topic JSON。

    为什么倒着找：上下文里可能有旧 Topic（真实模型历史里）和本轮新 Topic，
    最新的那份才反映当前调查状态。找不到时返回空字典，模型会退回“看消息”模式。
    """
    for message in reversed(messages):
        if not isinstance(message, SystemMessage):
            continue
        text = content_text(message.content)
        if BEGIN_TOPIC not in text or END_TOPIC not in text:
            continue
        raw = text.split(BEGIN_TOPIC, 1)[1].split(END_TOPIC, 1)[0].strip()
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _facts_by_key(topic: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """把 Topic 的事实列表转成“key -> 事实”的字典，方便按 key 读取。"""
    result: dict[str, dict[str, Any]] = {}
    for item in topic.get("facts") or []:
        if isinstance(item, dict) and item.get("key"):
            result[str(item["key"])] = item
    return result


def _candidates(topic: dict[str, Any]) -> list[dict[str, Any]]:
    """取出 Topic 的候选原因列表，过滤掉格式异常项。"""
    return [item for item in topic.get("candidates") or [] if isinstance(item, dict)]


def _attempted_tools(topic: dict[str, Any], messages: list[Any]) -> list[str]:
    """返回已经尝试过的工具名，保持调用顺序并去重。

    优先使用 Topic 的 `attempted_tools`，因为上下文裁剪不会裁掉它。
    同时扫描消息里的 `ToolResultMessage` 作为兜底：
    测试或旧上下文可能没有 Topic，这时仍然能判断“做过什么”。
    """
    names: list[str] = []
    for item in topic.get("attempted_tools") or []:
        name = str(item)
        if name not in names:
            names.append(name)
    for message in messages:
        if isinstance(message, ToolResultMessage) and message.tool_name not in names:
            names.append(message.tool_name)
    return names


def _extract_fingerprint(
    facts: dict[str, dict[str, Any]],
    messages: list[Any],
) -> str:
    """从 Topic 或慢日志结果里找最慢 SQL 指纹。

    顺序：
    1. Topic 事实的结构化 `data`；
    2. Topic 事实的 `statement`（摘要模式可能只有文字）；
    3. 最近一条慢日志工具结果（没有被证据层改写的场景）。
    """
    fact = facts.get("slow_log.top") or {}
    data = fact.get("data") or {}
    fingerprint = data.get("sql_fingerprint")
    if fingerprint:
        return str(fingerprint)

    statement = str(fact.get("statement") or "")
    match = re.search(r"指纹\s*([A-Za-z0-9_-]{6,})", statement)
    if match:
        return match.group(1)

    payload = _last_payload(messages, "slow_log_query")
    if payload:
        top = payload.get("top")
        if isinstance(top, dict) and top.get("sql_fingerprint"):
            return str(top["sql_fingerprint"])
        rows = payload.get("rows") or []
        if rows:
            slowest = max(rows, key=lambda row: row.get("avg_latency_ms", 0))
            if slowest.get("sql_fingerprint"):
                return str(slowest["sql_fingerprint"])
    return ""


def _tool_results_by_name(messages: list[Any]) -> dict[str, list[ToolResultMessage]]:
    """按工具名分组收集工具结果消息，保留出现顺序。"""
    grouped: dict[str, list[ToolResultMessage]] = {}
    for message in messages:
        if isinstance(message, ToolResultMessage):
            grouped.setdefault(message.tool_name, []).append(message)
    return grouped


def _last_result(
    messages: list[Any],
    tool_name: str,
) -> ToolResultMessage | None:
    """取某个工具最后一次的结果；没有返回 None。"""
    grouped = _tool_results_by_name(messages)
    results = grouped.get(tool_name)
    return results[-1] if results else None


def _last_payload(messages: list[Any], tool_name: str) -> dict[str, Any] | None:
    """取某个工具最后一次结果的 JSON 业务内容。

    工具失败时 content 是错误说明，不是 JSON，这里返回 None。
    如果内容是证据层生成的截断信封，会取出其中的 `summary`，
    因为业务字段（如 `top`、`rows`）就在 summary 里。
    """
    message = _last_result(messages, tool_name)
    if message is None or message.is_error:
        return None
    text = _tool_result_text(message)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict):
        return None
    summary = value.get("summary")
    if isinstance(summary, dict):
        return summary
    return value


def _last_evidence_id(messages: list[Any], tool_name: str) -> str:
    """取某个工具最后一次结果附带的证据编号。"""
    message = _last_result(messages, tool_name)
    if message is None or message.meta is None:
        return ""
    return str(message.meta.evidence_id or "")


def _last_truncated(messages: list[Any], tool_name: str) -> bool:
    """判断某个工具最后一次结果是否被平台截断过。"""
    message = _last_result(messages, tool_name)
    return bool(message is not None and message.meta is not None and message.meta.truncated)


def _tool_result_text(message: ToolResultMessage) -> str:
    """拼出工具结果里的全部文本。"""
    return "".join(block.text for block in message.content)


def _describe_tool(
    tool_name: str,
    facts: dict[str, dict[str, Any]],
    messages: list[Any],
    topic: dict[str, Any],
) -> tuple[str, str]:
    """给“调查过程”表格生成一行：结果说明和证据引用。

    平台工具（搜索、证据读取）的详细结果可能已经被上下文裁掉，
    所以先看当前消息，取不到再读 Topic 里保存的活动记录。
    """
    if tool_name == "search_tools":
        payload = _last_payload(messages, tool_name)
        if payload is None:
            searches = topic.get("searches") or []
            if searches and isinstance(searches[-1], dict):
                payload = searches[-1]
        if payload:
            loaded = [str(item) for item in payload.get("loaded") or []]
            if loaded:
                return f"加载了 {len(loaded)} 个工具：{', '.join(loaded)}", "—"
            return "没有加载新工具", "—"
        return "已执行（搜索结果不在当前上下文中）", "—"

    if tool_name == "evidence_read":
        payload = _last_payload(messages, tool_name)
        if payload is None:
            reads = topic.get("evidence_reads") or []
            if reads and isinstance(reads[-1], dict):
                payload = reads[-1]
        if payload:
            target = str(payload.get("evidence_id", "?"))
            offset = payload.get("offset", "?")
            limit = payload.get("limit", "?")
            total = payload.get("total_chars", "?")
            tail = "后面还有内容" if payload.get("truncated") else "已读到末尾"
            return (
                f"读取证据 `{target}`：offset={offset}, limit={limit}, "
                f"总 {total} 字符，{tail}",
                "—",
            )
        return "已执行（分页结果不在当前上下文中）", "—"

    fact_key = _FACT_KEY_BY_TOOL.get(tool_name)
    fact = facts.get(fact_key) if fact_key else None
    if fact:
        data = fact.get("data") or {}
        evidence = str(fact.get("evidence_id") or "—")
        if tool_name == "slow_log_query":
            return (
                f"成功：最慢指纹 `{data.get('sql_fingerprint', '未知')}`，"
                f"平均 {data.get('avg_latency_ms', '未知')} ms",
                f"`{evidence}`",
            )
        if tool_name == "execution_plan":
            index_name = data.get("key") or "未命中索引"
            return (
                f"成功：访问类型 `{data.get('access_type', '未知')}`，"
                f"索引 `{index_name}`",
                f"`{evidence}`",
            )
        if tool_name == "instance_metrics":
            return "成功：已返回监控数据", f"`{evidence}`"
        if tool_name == "lock_wait_query":
            return (
                f"{data.get('status', 'unknown')}："
                f"样本数 {data.get('sample_count', '未知')}",
                f"`{evidence}`",
            )
        return "成功", f"`{evidence}`"

    message = _last_result(messages, tool_name)
    if message is not None and message.is_error:
        evidence = message.meta.evidence_id if message.meta is not None else "—"
        return f"失败，未验证：{_short_error(message)}", f"`{evidence}`"
    if message is not None:
        evidence = message.meta.evidence_id if message.meta is not None else "—"
        return "已执行，但当前上下文里没有可复用的摘要", f"`{evidence}`"
    return "未获取到结果", "—"


def _short_error(message: ToolResultMessage) -> str:
    """把失败结果压成一行，表格里不会因为换行而散架。"""
    text = " ".join(_tool_result_text(message).split())
    return text[:120]


def _context_budget_line(request: ModelRequest) -> str:
    """生成报告里的上下文预算说明。

    数字来自 `request.metadata["context_budget"]`，是裁剪钩子写进去的。
    这样报告能直接展示“这一轮模型到底看到了多少、丢了哪些”，
    而不是让读者去翻 `model_requests.jsonl`。
    """
    budget = request.metadata.get("context_budget")
    if not isinstance(budget, dict) or not budget:
        return "本轮没有上下文预算记录（可能未启用有界上下文）。"
    used = budget.get("used_chars", "?")
    max_chars = budget.get("max_chars", "?")
    recent = budget.get("included_recent_messages", 0)
    dropped = budget.get("dropped_messages", 0)
    stubbed = budget.get("stubbed_tool_results", 0)
    over = "，已超过上限但仍保留最新消息" if budget.get("over_budget") else ""
    return (
        f"本轮上下文预算：模型输入约 {used}/{max_chars} 字符（字符估算，不是 token）；"
        f"保留最近 {recent} 条消息，丢弃 {dropped} 条旧消息，"
        f"另有 {stubbed} 条工具结果被二次裁剪{over}。"
    )


def _conclusion_lines(
    phenomenon_lines: list[str],
    facts: dict[str, dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> list[str]:
    """生成报告最前面的“结论”。

    规则很简单：
    - 慢日志和执行计划都拿到了，并且计划确认全表扫描 -> 给唯一主根因，置信度“中”；
    - 证据不完整 -> 明确写“当前证据不足以给出唯一根因”，最多列 3 个候选。

    为什么要单独抽一个函数：
    结论的写法是产品契约，不应该散落在 `_build_report` 的字符串拼接里。
    以后加别的 Skill，也可以复用“先结论、后候选”的规则。
    """
    slow_fact = facts.get("slow_log.top") or {}
    slow_data = slow_fact.get("data") or {}
    plan_fact = facts.get("execution_plan.access") or {}
    plan_data = plan_fact.get("data") or {}
    fingerprint = slow_data.get("sql_fingerprint") or "最慢 SQL"
    full_scan = bool(plan_data.get("full_scan"))

    if slow_fact and plan_fact and full_scan:
        refs = [
            f"`{fact.get('evidence_id')}`"
            for fact in (slow_fact, plan_fact)
            if fact.get("evidence_id")
        ]
        boundary = ["尚未执行真实 `EXPLAIN ANALYZE`，访问路径仍属于模拟计划推断"]
        if any(candidate.get("key") == "instance_resource" for candidate in candidates):
            boundary.append("实例监控未验证")
        if any(candidate.get("key") == "lock_wait" for candidate in candidates):
            boundary.append("锁等待无样本，不能完全排除")
        return [
            f"**最可能根因：** 慢 SQL `{fingerprint}` 的访问路径缺少合适索引，"
            f"执行计划为全表扫描（`type=ALL`、`key` 为空），并伴随排序开销；"
            f"慢日志显示平均扫描 {slow_data.get('avg_rows_examined') or '大量'} 行、"
            f"只返回 {slow_data.get('avg_rows_sent') or '少量'} 行。",
            "",
            "**置信度：** 中",
            f"**直接证据：** {'、'.join(refs) or '见证据链'}",
            f"**结论边界：** {'；'.join(boundary)}。",
            "",
            "**现象：**",
            *phenomenon_lines,
        ]

    # 证据不足时不要硬猜。这里把“候选”降级成结论里的短列表，
    # 并明确告诉用户还缺什么证据，避免把 6 个假设平铺成 6 个“根因”。
    evidence_refs = [
        f"`{fact.get('evidence_id')}`"
        for fact in facts.values()
        if fact.get("evidence_id")
    ]
    lines = [
        "**最可能根因：** 当前证据不足以给出唯一根因，不能把下面的候选当成结论。",
        "",
        "**置信度：** 低",
        f"**直接证据：** {'、'.join(evidence_refs) or '暂无'}",
        "**结论边界：** 需要补充执行计划、实例监控或锁等待等关键证据后再判断。",
    ]
    ranked = [
        candidate
        for candidate in candidates
        if candidate.get("status") in {"supported", "candidate"}
    ][:3]
    if ranked:
        lines.extend(["", "**当前候选（按可能性排序，最多 3 个）：**"])
        for index, candidate in enumerate(ranked, start=1):
            status = str(candidate.get("status", "candidate"))
            label = _STATUS_LABELS.get(status, status)
            refs = candidate.get("evidence") or []
            ref_text = ""
            if refs:
                ref_text = "（证据：" + "、".join(f"`{item}`" for item in refs) + "）"
            lines.append(f"{index}. **[{label}]** {candidate.get('statement')}{ref_text}")
    lines.extend(["", "**现象：**", *phenomenon_lines])
    return lines


def _recommendation_lines(facts: dict[str, dict[str, Any]]) -> list[str]:
    """生成分组后的修复建议。

    顺序固定为“首选方案 / 备选方案 / 临时缓解”，和旧版平铺 1~6 条不同：
    - 首选方案针对主根因；
    - 备选方案说明什么情况下改用它；
    - 临时缓解只是止血，不冒充根因修复。
    """
    plan_fact = facts.get("execution_plan.access") or {}
    plan_data = plan_fact.get("data") or {}
    full_scan = bool(plan_data.get("full_scan"))
    lines: list[str] = []

    if full_scan:
        lines.extend(
            [
                "### 首选方案",
                "1. 先在只读副本或测试环境对最慢 SQL 执行 `EXPLAIN ANALYZE`，"
                "确认实际扫描行数、回表次数和排序开销。",
                "2. 确认全表扫描后，评估为过滤列和排序列增加联合索引；"
                "先评估写入放大、磁盘占用和变更窗口，再走变更流程。",
                "",
                "### 备选方案",
                "- 如果索引改动成本高，先尝试 SQL 改写：减少返回列、缩小过滤范围，"
                "或让排序走覆盖索引。",
                "",
                "### 临时缓解",
                "- 在业务侧限流、分页或缓存热点查询，避免高峰期放大；"
                "这只是缓解，不是根因修复。",
            ]
        )
    elif plan_data:
        lines.extend(
            [
                "### 首选方案",
                "1. 用真实 `EXPLAIN ANALYZE` 和 10:00 前后的慢日志分布复核访问路径。",
                "",
                "### 备选方案",
                "- 如果执行计划没有异常，再补采实例监控和锁等待，排查资源或阻塞因素。",
            ]
        )
    else:
        lines.extend(
            [
                "### 首选方案",
                "1. 先补齐真实执行计划，确认访问路径和扫描行数，再决定是否调整索引。",
                "",
                "### 备选方案",
                "- 同时补采实例监控和锁等待，避免只盯 SQL 文本。",
            ]
        )

    lines.extend(
        [
            "",
            "> 以上建议仅用于只读排查；任何索引、参数或 SQL 变更都必须走变更流程。",
        ]
    )
    return lines


def _ruled_out_lines(
    facts: dict[str, dict[str, Any]],
    candidate_map: dict[str, dict[str, Any]],
    open_questions: list[str],
) -> list[str]:
    """生成“已排除与待验证”章节。

    这一节是给用户展开看的透明度说明，不是结论本身：
    - 已排除写“排除了什么、依据是什么”；
    - 待验证写“还缺什么、为什么影响结论”。
    """
    excluded: list[str] = []
    pending: list[str] = []

    if facts.get("slow_log.top"):
        excluded.append("慢日志缺失或采样未命中：本次已经拿到最慢 SQL，可排除这一项。")

    plan_fact = facts.get("execution_plan.access")
    plan_data = (plan_fact or {}).get("data") or {}
    if plan_fact and not plan_data.get("full_scan"):
        excluded.append("全表扫描：执行计划没有显示 `type=ALL`。")
    if plan_fact and plan_data.get("full_scan"):
        pending.append(
            "真实执行计划：当前是模拟计划，需用 `EXPLAIN ANALYZE` 复核全表扫描和排序开销。"
        )

    metrics_candidate = candidate_map.get("instance_resource") or {}
    if metrics_candidate.get("status") == "unverified":
        pending.append("实例 CPU/IO：监控接口超时，未验证，不能写成“资源正常”。")

    lock_data = (facts.get("lock_wait.samples") or {}).get("data") or {}
    if lock_data.get("status") == "no_data" or lock_data.get("sample_count") == 0:
        pending.append(
            "锁等待：采样窗口 `no_data`，不等于没有锁等待；需要更长窗口或其它采样源。"
        )

    for question in open_questions:
        text = str(question).strip()
        if not text or text in pending:
            continue
        # 监控超时和锁等待已经有专门条目，避免同一件事说两遍。
        if "instance_metrics" in text or "锁等待" in text:
            continue
        pending.append(text)

    if not excluded:
        excluded.append("暂无可完全排除的项；现有证据只够形成主根因推断。")
    if not pending:
        pending.append("暂无影响结论的待验证项。")

    return [
        "### 已排除",
        *[f"- {item}" for item in excluded],
        "",
        "### 待验证",
        *[f"- {item}" for item in pending],
    ]


def _limitation_lines(
    facts: dict[str, dict[str, Any]],
    candidate_map: dict[str, dict[str, Any]],
    attempted: list[str],
    request: ModelRequest,
) -> list[str]:
    """生成“当前限制”列表，明确哪些结论没有验证。"""
    lines = ["- 本报告基于**模拟数据**和模拟工具，不代表真实客户、实例或业务事实。"]

    slow_fact = facts.get("slow_log.top") or {}
    slow_data = slow_fact.get("data") or {}
    if slow_data.get("truncated"):
        lines.append(
            f"- 慢日志原始结果约 {slow_data.get('raw_size_chars', '未知')} 字符，"
            f"超过单条内联上限 {slow_data.get('max_inline_chars', '未知')}；"
            f"报告依据证据 `{slow_fact.get('evidence_id')}` 的摘要和分页读取，"
            "没有逐行核对全部行。"
        )

    metrics_candidate = candidate_map.get("instance_resource") or {}
    if metrics_candidate.get("status") == "unverified":
        lines.append("- 实例监控未验证：CPU/IO 是否成为瓶颈无法判断。")
    elif "instance_metrics" not in attempted:
        lines.append("- 实例监控未调用，CPU/IO 未验证。")

    lock_fact = facts.get("lock_wait.samples")
    lock_data = (lock_fact or {}).get("data") or {}
    if lock_data.get("status") == "no_data" or lock_data.get("sample_count") == 0:
        lines.append("- 锁等待查询为 `no_data`：采样窗口内没有样本，不等于没有锁等待。")
    elif lock_fact is None and "lock_wait_query" in attempted:
        lines.append("- 锁等待查询没有形成可用事实，相关结论未验证。")

    budget = request.metadata.get("context_budget")
    if isinstance(budget, dict) and budget.get("over_budget"):
        lines.append("- 本轮模型输入超过字符预算，虽然保留了最新消息，但旧消息可能已被裁掉。")

    lines.append("- 本轮只做只读调查和建议，没有执行任何生产变更。")
    return lines


if __name__ == "__main__":
    # 这个模块本来是被 CLI 和测试 import 的，直接运行没有意义。
    # 这里显式提示一下，避免有人误以为它是一个可独立运行的程序。
    print("MockDbaModel 是模拟模型适配器，请通过 `uv run python -m app.cli run` 使用。")
