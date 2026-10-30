"""多数据库 Mock 场景的“剧本引擎”和报告生成器。

## 为什么需要这个文件

慢 SQL 的模拟模型是一套写死的六步剧本：查慢日志、读证据、搜索、查执行计划、
补监控和锁等待、生成报告。现在有十个模拟场景，涉及 MySQL、PostgreSQL、Redis、
MongoDB、Elasticsearch 和 ClickHouse。如果每个场景都复制一份 `MockDbaModel`，
会有两个明显问题：

1. 代码重复：每一套都包含“搜索工具、调用工具、读 Topic、拼报告”的骨架；
2. 难以维护：报告章节格式一改，要同步改十份。

所以这里把“场景差异”全部放进数据文件：

```text
fixtures/mock_dba/<scenario>.json
  scenario.steps        这次调查要调用哪些工具、参数是什么
  scenario.diagnosis    根因、置信度、证据 key、修复建议、限制
  tool_results          每个工具返回给模型的 payload 和给平台的 details
```

`ScenarioEngine` 只负责读数据、按步骤执行、生成报告。它不连接数据库，
也不执行写操作；数据仍然是模拟的，但 MCP 工具调用链路是真实的。

## 报告为什么要这么拼

报告格式是产品契约，不是随便写一段 Markdown：

- `## 结论` 必须先给**唯一主根因**，再给置信度、直接证据、结论边界；
- 只有证据不足时才列候选原因，最多 3 个；
- 修复建议分“首选方案 / 备选方案 / 临时缓解”；
- 已排除和待验证放进单独章节，前端默认折叠；
- 所有关键事实必须带证据编号。

这些规则在 `build_report()` 里集中实现一次，十个场景共用。

## Python 语法提示

- `@dataclass(slots=True)` 适合描述“字段固定的数据对象”；
- `Path.is_relative_to()` 用来检查路径是否在允许目录内，防止数据文件字段
  指向项目外的任意文件；
- `str.removeprefix()` 用来去掉占位符前缀，例如
  `"$fact:pg.long_transaction.xid_age".removeprefix("$fact:")`；
- 字典的 `|` 运算符会合并两个字典，右边覆盖左边。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.runtime.contracts import ModelRequest, ToolCall, ToolResultMessage

# 项目根目录：app/adapters/scenario_mock.py -> app/adapters -> app -> 项目根。
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 场景数据只允许放在这个目录下。工单里的 `dataset` 是相对路径，
# 读取前必须确认解析后的真实路径没有越界。
SCENARIO_DATASET_ROOT = PROJECT_ROOT / "fixtures" / "mock_dba"

# 候选原因状态 -> 报告里的中文标签。
_STATUS_LABELS: dict[str, str] = {
    "supported": "有观测支持",
    "candidate": "候选",
    "unverified": "未验证",
}

# 平台工具的展示名称。它们不属于某个数据库，但会出现在调查过程表格里。
_PLATFORM_LABELS: dict[str, str] = {
    "search_tools": "工具搜索",
    "evidence_read": "证据读取",
}


def resolve_ticket_dataset(
    ticket: dict[str, Any],
    *,
    fallback: Path | str | None = None,
) -> Path | str | None:
    """决定这张工单应该让 MCP Server 加载哪份数据。

    为什么需要这个函数：
    API 服务以前只读服务端配置里的一个固定数据集。多场景模式下，每张工单
    都带自己的 `dataset`，如果仍然用全局数据集，PostgreSQL 工单会连到 MySQL
    数据，模型看到的工具和执行结果全对不上。

    规则：
    - 旧工单没有 `scenario_mode`：返回 `fallback`（服务端默认数据集）；
    - 场景工单：校验路径、存在性和 `mock=true`，返回绝对路径。

    例子：
        `resolve_ticket_dataset(ticket, fallback="fixtures/mock_dba/slow_sql_001.json")`
        对旧慢 SQL 工单返回 fallback；对 Redis 场景工单返回 Redis 数据集路径。
    """
    if not ticket.get("scenario_mode"):
        return fallback

    raw_path = str(ticket.get("dataset") or "").strip()
    if not raw_path:
        raise ValueError("场景工单缺少 dataset 字段，无法加载模拟数据")

    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    resolved = candidate.resolve()
    if not resolved.is_relative_to(SCENARIO_DATASET_ROOT.resolve()):
        raise ValueError(
            f"场景数据必须位于 {SCENARIO_DATASET_ROOT} 内，当前路径：{resolved}"
        )
    if not resolved.exists():
        raise FileNotFoundError(f"场景数据文件不存在：{resolved}")

    raw = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw.get("mock"):
        raise ValueError(f"场景数据未标记 mock=true 或顶层不是对象：{resolved}")
    return resolved


@dataclass(slots=True)
class ScenarioDefinition:
    """一个已加载的场景数据。

    `raw` 保留完整 JSON，方便报告和测试读取 `tool_results` 里的 summary；
    `steps` 和 `diagnosis` 是常用字段的快捷方式。
    """

    path: Path
    raw: dict[str, Any]
    label: str
    search_query: str
    steps: list[dict[str, Any]] = field(default_factory=list)
    diagnosis: dict[str, Any] = field(default_factory=dict)

    @property
    def step_labels(self) -> dict[str, str]:
        """返回“工具名 -> 中文标签”，报告调查过程时用。"""
        labels: dict[str, str] = {}
        for step in self.steps:
            tool = str(step.get("tool") or "")
            if tool:
                labels[tool] = str(step.get("label") or tool)
        return labels


class ScenarioEngine:
    """按数据集里的步骤执行一次多数据库调查，并生成报告。

    这个类不处理模型流式输出，也不发事件。它只回答两个问题：
    - “这一轮该调用哪个工具？” -> `next_action()`
    - “调查结束后报告怎么写？” -> `build_report()`
    """

    def __init__(self, definition: ScenarioDefinition) -> None:
        self.definition = definition
        self.path = definition.path
        self.raw = definition.raw
        self.steps = definition.steps
        self.diagnosis = definition.diagnosis
        self.label = definition.label
        self.search_query = definition.search_query
        self.step_labels = definition.step_labels

    @classmethod
    def from_ticket(cls, ticket: dict[str, Any]) -> ScenarioEngine | None:
        """从工单加载场景；不是场景工单时返回 `None`。

        旧版慢 SQL 工单没有 `scenario_mode=true`，所以会返回 `None`，
        继续走 `MockDbaModel` 原来的六步剧本，行为完全不变。

        路径安全：
        - 只接受相对路径或项目根目录下的绝对路径；
        - 解析后的真实路径必须位于 `fixtures/mock_dba/` 内；
        - 文件必须标记 `mock=true`，防止有人误把真实数据放进模拟链路。

        例子：
            `"dataset": "fixtures/mock_dba/redis_hot_key_001.json"`
            解析后是项目内的文件，允许读取；
            `"dataset": "../../etc/passwd"` 会直接报错。
        """
        if not ticket.get("scenario_mode"):
            return None

        resolved = resolve_ticket_dataset(ticket)
        assert resolved is not None  # scenario_mode 为 True 时一定会返回 Path
        resolved = Path(resolved)

        raw = json.loads(resolved.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or not raw.get("mock"):
            raise ValueError(f"场景数据未标记 mock=true 或顶层不是对象：{resolved}")

        scenario = raw.get("scenario")
        if not isinstance(scenario, dict):
            raise ValueError(f"场景数据缺少 scenario 对象：{resolved}")

        steps = scenario.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError(f"场景数据 scenario.steps 不能为空：{resolved}")
        diagnosis = scenario.get("diagnosis")
        if not isinstance(diagnosis, dict):
            raise ValueError(f"场景数据 scenario.diagnosis 不能为空：{resolved}")

        return cls(
            ScenarioDefinition(
                path=resolved,
                raw=raw,
                label=str(scenario.get("label") or ticket.get("scenario_label") or "DBA 调查"),
                search_query=str(
                    scenario.get("search_query")
                    or " ".join(str(step.get("tool") or "") for step in steps)
                ),
                steps=[step for step in steps if isinstance(step, dict)],
                diagnosis=diagnosis,
            )
        )

    # ------------------------------------------------------------------
    # 下一步
    # ------------------------------------------------------------------

    def next_action(
        self,
        *,
        request: ModelRequest,
        ticket: dict[str, Any],
        topic: dict[str, Any],
        messages: list[Any],
        visible_tools: set[str],
        make_call: Callable[[str, dict[str, Any]], ToolCall],
    ) -> tuple[str, list[ToolCall]]:
        """决定这一轮的工具调用；步骤都走完后返回报告。

        顺序：
        1. 如果 `search_tools` 还没用过，先搜索本场景需要的工具；
        2. 按 `scenario.steps` 顺序，找第一个还没尝试过的工具；
        3. 如果工具还没在可见列表里，再搜一次工具名；
        4. 全部尝试完，生成最终报告。

        为什么先搜索再调用：
        新场景 Skill `dba-triage` 的 `initial_tools` 是空列表，模型一开始只能
        看到 `search_tools` / `evidence_read`。这一步演示真实 MCP 客户端
        “先 tools/list 发现工具、再 tools/call 调用”的流程，而不是把所有工具
        一股脑塞给模型。
        """
        attempted = _attempted_tools(topic, messages)
        facts = _facts_by_key(topic)

        # 第一步：搜索。搜索本身不产生证据，但会在 Topic 里留下搜索记录。
        if "search_tools" not in attempted and "search_tools" in visible_tools:
            return (
                "这是多数据库场景，初始没有直接开放业务工具。"
                "我先用 search_tools 搜索本次排查需要的诊断工具，"
                "命中后工具会在下一轮可见；搜索只加载工具，不代表获得权限。",
                [
                    make_call(
                        "search_tools",
                        {"query": self.search_query, "limit": 8},
                    )
                ],
            )

        for step in self.steps:
            tool = str(step.get("tool") or "")
            if not tool or tool in attempted:
                continue

            if tool not in visible_tools:
                # 正常数据下第一次搜索就会加载全部工具。这里再搜一次是为了
                # 容忍关键词排序导致某个工具没进前 N 名的情况。
                return (
                    f"工具 `{tool}` 还没有加载，我按工具名再搜索一次，"
                    "确认它是否属于当前 MCP Server。",
                    [make_call("search_tools", {"query": tool, "limit": 8})],
                )

            arguments = _resolve_arguments(
                step.get("args") if isinstance(step.get("args"), dict) else {},
                ticket=ticket,
                facts=facts,
                messages=messages,
            )
            label = str(step.get("label") or tool)
            reason = str(
                step.get("reason")
                or f"调用只读工具 `{tool}`（{label}）补充证据。"
            )
            return reason, [make_call(tool, arguments)]

        # 所有步骤已经执行过：输出报告。
        return (
            self.build_report(
                request=request,
                ticket=ticket,
                topic=topic,
                messages=messages,
            ),
            [],
        )

    # ------------------------------------------------------------------
    # 报告
    # ------------------------------------------------------------------

    def build_report(
        self,
        *,
        request: ModelRequest | None,
        ticket: dict[str, Any],
        topic: dict[str, Any],
        messages: list[Any],
    ) -> str:
        """按统一章节格式生成多数据库场景报告。

        报告里的所有“事实”都来自 Topic；Topic 又只根据工具实际返回更新。
        因此即使早期工具结果被上下文裁剪，报告仍然能给出准确的事实和证据编号。
        """
        attempted = _attempted_tools(topic, messages)
        facts = _facts_by_key(topic)
        open_questions = [
            str(item).strip()
            for item in topic.get("open_questions") or []
            if str(item).strip()
        ]
        evidence_keys = [
            str(item) for item in self.diagnosis.get("evidence_keys") or []
        ]

        # ---------- 现象 ----------
        symptom = str(ticket.get("symptom") or ticket.get("title") or "工单未填写现象")
        phenomenon_lines = [
            f"- 工单 `{ticket.get('ticket_id', '未知')}`：{symptom}",
            (
                f"- 实例：`{ticket.get('instance_id', '未知')}`；"
                f"数据库类型：`{ticket.get('database_type', '未知')}`；"
                f"业务库：`{ticket.get('database', '未知')}`"
            ),
        ]
        for key in evidence_keys[:3]:
            fact = facts.get(key)
            if not fact:
                continue
            ref = _evidence_ref(fact)
            phenomenon_lines.append(f"- 观测：{fact.get('statement')}{ref}")

        # ---------- 结论 ----------
        conclusion_lines = self._conclusion_lines(
            phenomenon_lines=phenomenon_lines,
            facts=facts,
            topic=topic,
            evidence_keys=evidence_keys,
        )

        # ---------- 修复建议 ----------
        advice_lines = self._recommendation_lines()

        # ---------- 证据链 ----------
        evidence_lines: list[str] = []
        for key in evidence_keys:
            fact = facts.get(key)
            if fact is None:
                evidence_lines.append(f"- **{key}**：未获取到对应事实（待验证）")
                continue
            evidence_lines.append(
                f"- **{key}**：{fact.get('statement')}{_evidence_ref(fact)}"
            )
        if not evidence_lines:
            evidence_lines.append("- 没有形成可引用的事实，关键证据待补充。")

        # ---------- 已排除与待验证 ----------
        ruled_lines = self._ruled_out_lines(facts, open_questions)

        # ---------- 调查过程 ----------
        process_lines = [
            "| # | 工具 | 结果 | 证据 |",
            "| --- | --- | --- | --- |",
        ]
        for index, tool_name in enumerate(attempted, start=1):
            status, evidence = self._describe_tool(
                tool_name,
                facts=facts,
                messages=messages,
                topic=topic,
            )
            label = (
                _PLATFORM_LABELS.get(tool_name)
                or self.step_labels.get(tool_name)
                or tool_name
            )
            safe_status = status.replace("|", "\\|")
            process_lines.append(
                f"| {index} | `{tool_name}`（{label}） | {safe_status} | {evidence} |"
            )
        loaded = topic.get("searches") or []
        loaded_names: list[str] = []
        for record in loaded:
            if isinstance(record, dict):
                loaded_names.extend(str(item) for item in record.get("loaded") or [])
        process_lines.extend(
            [
                "",
                (
                    "> Skill：`dba-triage`（通用 DBA 分诊）。初始业务工具为空，"
                    "按需通过 `search_tools` 加载；业务结果由 MCP Server 返回，"
                    "大结果会落盘为证据。"
                ),
                (
                    f"> 本次通过搜索加载：{', '.join(loaded_names) if loaded_names else '无'}。"
                ),
                f"> {_context_budget_line(request)}",
            ]
        )

        # ---------- 验证方法 ----------
        verification = _string_list(self.diagnosis.get("verification"))
        verify_lines = (
            [f"{index}. {item}" for index, item in enumerate(verification, start=1)]
            if verification
            else ["1. 补充真实监控和日志，复核报告中的关键事实。"]
        )

        # ---------- 当前限制 ----------
        limitation_lines = self._limitation_lines(request, facts)

        return "\n".join(
            [
                f"# {self.label}（模拟）",
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

    def _conclusion_lines(
        self,
        *,
        phenomenon_lines: list[str],
        facts: dict[str, dict[str, Any]],
        topic: dict[str, Any],
        evidence_keys: list[str],
    ) -> list[str]:
        """生成“先结论、后现象”的结论区。

        证据充分时只给一个主根因；`insufficient=true` 或置信度低时才列候选。
        这条规则和慢 SQL 报告一致：DBA 调查的目标是收敛，而不是罗列假设。
        """
        diagnosis = self.diagnosis
        confidence = str(diagnosis.get("confidence") or "中")
        boundary = str(diagnosis.get("boundary") or "结论仅基于当前模拟证据。")
        evidence_refs = [
            _evidence_ref(facts[key])
            for key in evidence_keys
            if key in facts and _evidence_ref(facts[key])
        ]
        ref_text = "、".join(evidence_refs) or "见证据链"

        insufficient = bool(diagnosis.get("insufficient"))
        if insufficient:
            lines = [
                "**最可能根因：** 当前证据不足以给出唯一根因，不能把候选原因当成结论。",
                "",
                f"**置信度：** {confidence}",
                f"**直接证据：** {ref_text}",
                f"**结论边界：** {boundary}",
            ]
            candidates = _report_candidates(topic)
            if candidates:
                lines.extend(["", "**当前候选（按可能性排序，最多 3 个）：**"])
                for index, candidate in enumerate(candidates, start=1):
                    status = _STATUS_LABELS.get(
                        str(candidate.get("status") or "candidate"),
                        str(candidate.get("status") or "candidate"),
                    )
                    refs = [
                        f"`{item}`" for item in candidate.get("evidence") or [] if item
                    ]
                    tail = f"（证据：{'、'.join(refs)}）" if refs else ""
                    lines.append(
                        f"{index}. **[{status}]** {candidate.get('statement')}{tail}"
                    )
            lines.extend(["", "**现象：**", *phenomenon_lines])
            return lines

        root_cause = str(diagnosis.get("root_cause") or "").strip()
        if not root_cause:
            # 数据质量保护：没有根因就不能假装有唯一结论，退化为候选模式。
            diagnosis["insufficient"] = True
            diagnosis["confidence"] = "低"
            return self._conclusion_lines(
                phenomenon_lines=phenomenon_lines,
                facts=facts,
                topic=topic,
                evidence_keys=evidence_keys,
            )

        return [
            f"**最可能根因：** {root_cause}",
            "",
            f"**置信度：** {confidence}",
            f"**直接证据：** {ref_text}",
            f"**结论边界：** {boundary}",
            "",
            "**现象：**",
            *phenomenon_lines,
        ]

    def _recommendation_lines(self) -> list[str]:
        """生成“首选 / 备选 / 临时缓解”三组建议。

        修复建议允许多个候选，因为同一根因可能有不同实施路径；
        但主结论仍然只给一个根因。
        """
        fix = self.diagnosis.get("fix")
        if not isinstance(fix, dict):
            fix = {}
        primary = _string_list(fix.get("primary"))
        alternate = _string_list(fix.get("alternate"))
        mitigation = _string_list(fix.get("mitigation"))

        lines: list[str] = []
        lines.extend(["### 首选方案"])
        if primary:
            lines.extend(
                f"{index}. {item}" for index, item in enumerate(primary, start=1)
            )
        else:
            lines.append("1. 补齐关键证据后再决定修复方案。")

        lines.extend(["", "### 备选方案"])
        if alternate:
            lines.extend(f"- {item}" for item in alternate)
        else:
            lines.append("- 暂无；首选方案不适用时，先回到证据补采。")

        lines.extend(["", "### 临时缓解"])
        if mitigation:
            lines.extend(f"- {item}" for item in mitigation)
        else:
            lines.append("- 暂无；临时缓解不能替代根因修复。")

        lines.extend(
            [
                "",
                "> 以上建议仅用于只读排查；索引、参数、扩缩容等变更必须走变更流程。",
            ]
        )
        return lines

    def _ruled_out_lines(
        self,
        facts: dict[str, dict[str, Any]],
        open_questions: list[str],
    ) -> list[str]:
        """生成“已排除”和“待验证”。

        数据里可以明确写“排除了什么”，也会自动补上工具失败留下的未决问题。
        这样报告不会把“没有数据”写成“已排除”。
        """
        excluded = _string_list(self.diagnosis.get("excluded"))
        pending = _string_list(self.diagnosis.get("pending"))

        # 如果诊断里引用了某个证据 key，但实际没拿到，必须自动标为待验证。
        for key in self.diagnosis.get("evidence_keys") or []:
            if str(key) not in facts:
                message = f"关键事实 `{key}` 未获取到，相关结论未验证。"
                if message not in pending:
                    pending.append(message)

        for question in open_questions:
            if question not in pending:
                pending.append(question)

        if not excluded:
            excluded.append("暂无可以完全排除的项；现有证据只够支持主根因推断。")
        if not pending:
            pending.append("暂无影响结论的待验证项。")

        return [
            "### 已排除",
            *[f"- {item}" for item in excluded],
            "",
            "### 待验证",
            *[f"- {item}" for item in pending],
        ]

    def _describe_tool(
        self,
        tool_name: str,
        *,
        facts: dict[str, dict[str, Any]],
        messages: list[Any],
        topic: dict[str, Any],
    ) -> tuple[str, str]:
        """为调查过程表格生成一行。

        优先用当前上下文里最近一条工具结果的 `details.summary`；
        被上下文裁掉时，退回 Topic 里同一工具产生的事实。
        """
        if tool_name == "search_tools":
            payload = _last_details(messages, tool_name)
            if not payload:
                searches = topic.get("searches") or []
                payload = searches[-1] if searches and isinstance(searches[-1], dict) else {}
            loaded = [str(item) for item in payload.get("loaded") or []]
            if loaded:
                return f"加载了 {len(loaded)} 个工具：{', '.join(loaded)}", "—"
            return "没有加载新工具", "—"

        if tool_name == "evidence_read":
            payload = _last_details(messages, tool_name)
            if not payload:
                reads = topic.get("evidence_reads") or []
                payload = reads[-1] if reads and isinstance(reads[-1], dict) else {}
            if payload:
                target = payload.get("evidence_id", "?")
                offset = payload.get("offset", "?")
                limit = payload.get("limit", "?")
                return f"读取证据 `{target}`：offset={offset}, limit={limit}", "—"
            return "已执行（分页结果不在当前上下文）", "—"

        latest = _last_details(messages, tool_name)
        summary = str(latest.get("summary") or "").strip()
        source_facts = [
            fact for fact in facts.values() if fact.get("source_tool") == tool_name
        ]
        evidence = [
            f"`{fact.get('evidence_id')}`"
            for fact in source_facts
            if fact.get("evidence_id")
        ]
        evidence_text = "、".join(dict.fromkeys(evidence)) or "—"

        message = _last_result(messages, tool_name)
        if message is not None and message.is_error:
            text = " ".join(_tool_result_text(message).split())[:120]
            return f"失败，未验证：{text}", evidence_text
        if summary:
            return f"成功：{summary}", evidence_text
        if source_facts:
            return f"成功：{source_facts[0].get('statement')}", evidence_text
        return "已执行，但当前上下文里没有可复用摘要（未验证）", evidence_text

    def _limitation_lines(
        self,
        request: ModelRequest | None,
        facts: dict[str, dict[str, Any]],
    ) -> list[str]:
        """生成“当前限制”。"""
        lines = ["- 本报告基于**模拟数据**和模拟工具，不代表真实客户、实例或业务事实。"]
        lines.extend(
            f"- {item}" for item in _string_list(self.diagnosis.get("limitations"))
        )

        # 数据截断说明：慢 SQL 用 meta.truncated，场景工具也兼容同样字段。
        for fact in facts.values():
            data = fact.get("data")
            if isinstance(data, dict) and data.get("truncated"):
                lines.append(
                    f"- 事实 `{fact.get('key')}` 的原始结果被截断，"
                    f"报告依据证据 `{fact.get('evidence_id')}` 的摘要，未逐行核对全文。"
                )

        budget = request.metadata.get("context_budget") if request is not None else None
        if isinstance(budget, dict) and budget.get("over_budget"):
            lines.append("- 本轮模型输入超过字符预算，旧消息可能已被裁剪。")

        lines.append("- 本轮只做只读调查和建议，没有执行任何生产变更。")
        return lines


# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------


def _string_list(value: Any) -> list[str]:
    """把数据里的字符串列表整理干净，非字符串项忽略。"""
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _evidence_ref(fact: dict[str, Any]) -> str:
    """生成“（证据 `ev-000001`）”这样的引用；没有证据时返回空字符串。"""
    evidence_id = fact.get("evidence_id")
    return f"（证据 `{evidence_id}`）" if evidence_id else ""


def _facts_by_key(topic: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Topic 的 facts 是列表，这里转成 key -> fact 的字典。"""
    result: dict[str, dict[str, Any]] = {}
    for item in topic.get("facts") or []:
        if isinstance(item, dict) and item.get("key"):
            result[str(item["key"])] = item
    return result


def _report_candidates(topic: dict[str, Any]) -> list[dict[str, Any]]:
    """取最多 3 个候选原因，优先有观测支持的。"""
    ranked = [
        item
        for item in topic.get("candidates") or []
        if isinstance(item, dict)
        and item.get("statement")
        and item.get("status") in {"supported", "candidate"}
    ]
    ranked.sort(key=lambda item: 0 if item.get("status") == "supported" else 1)
    return ranked[:3]


def _attempted_tools(topic: dict[str, Any], messages: list[Any]) -> list[str]:
    """返回已经尝试过的工具名，保持顺序并去重。"""
    names: list[str] = []
    for item in topic.get("attempted_tools") or []:
        name = str(item)
        if name not in names:
            names.append(name)
    for message in messages:
        if isinstance(message, ToolResultMessage) and message.tool_name not in names:
            names.append(message.tool_name)
    return names


def _last_result(messages: list[Any], tool_name: str) -> ToolResultMessage | None:
    """取某个工具最后一次结果。"""
    for message in reversed(messages):
        if isinstance(message, ToolResultMessage) and message.tool_name == tool_name:
            return message
    return None


def _last_details(messages: list[Any], tool_name: str) -> dict[str, Any]:
    """取某个工具最后一次结果的 details；没有时返回空字典。"""
    message = _last_result(messages, tool_name)
    if message is None or not isinstance(message.details, dict):
        return {}
    return message.details


def _tool_result_text(message: ToolResultMessage) -> str:
    """拼出工具结果里的全部文本。"""
    return "".join(block.text for block in message.content)


def _context_budget_line(request: ModelRequest | None) -> str:
    """生成报告里的上下文预算说明。"""
    budget = request.metadata.get("context_budget") if request is not None else None
    if not isinstance(budget, dict) or not budget:
        return "本轮没有上下文预算记录（可能未启用有界上下文）。"
    return (
        f"本轮上下文预算：模型输入约 {budget.get('used_chars', '?')}/"
        f"{budget.get('max_chars', '?')} 字符（字符估算，不是 token）；"
        f"丢弃 {budget.get('dropped_messages', 0)} 条旧消息，"
        f"另有 {budget.get('stubbed_tool_results', 0)} 条工具结果被二次裁剪。"
    )


def _resolve_arguments(
    arguments: dict[str, Any],
    *,
    ticket: dict[str, Any],
    facts: dict[str, dict[str, Any]],
    messages: list[Any],
) -> dict[str, Any]:
    """把数据文件里的参数占位符替换成真实值。

    支持的占位符：
    - `$instance_id`、`$database`、`$start_time`、`$end_time`；
    - `$last_evidence`：最近一条工具结果的证据编号；
    - `$evidence:<fact.key>`：某个事实对应的证据编号；
    - `$fact:<fact.key>.<field>`：某个事实 `data` 里的字段。

    例子：
        `{"instance_id": "$instance_id", "table": "public.orders"}`
        会变成 `{"instance_id": "db-pg-003", "table": "public.orders"}`。

    设计原因：一个数据集要能被不同工单复用（例如换实例 ID 或时间范围），
    不能把工单字段写死。参数占位符让数据文件保持“模板”形态。
    """
    time_range = ticket.get("time_range")
    if not isinstance(time_range, dict):
        time_range = {}
    base = {
        "$instance_id": str(ticket.get("instance_id") or ""),
        "$database": str(ticket.get("database") or ""),
        "$start_time": str(time_range.get("start") or ""),
        "$end_time": str(time_range.get("end") or ""),
        "$scenario_label": str(ticket.get("scenario_label") or ""),
        "$last_evidence": _last_evidence_id(messages),
    }
    return {
        str(key): _resolve_value(value, base=base, facts=facts)
        for key, value in arguments.items()
    }


def _resolve_value(
    value: Any,
    *,
    base: dict[str, str],
    facts: dict[str, dict[str, Any]],
) -> Any:
    """递归替换一个参数值里的占位符。

    字典和列表会递归处理，因为有些工具参数是对象/数组（当前模拟数据以字符串
    和数字为主，但保留这个能力方便以后扩展）。
    """
    if isinstance(value, dict):
        return {
            key: _resolve_value(item, base=base, facts=facts)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_resolve_value(item, base=base, facts=facts) for item in value]
    if not isinstance(value, str) or not value.startswith("$"):
        return value

    if value in base:
        return base[value]

    if value.startswith("$evidence:"):
        key = value.removeprefix("$evidence:")
        fact = facts.get(key)
        if fact is None:
            raise ValueError(f"参数占位符引用了不存在的事实：{key}")
        evidence_id = str(fact.get("evidence_id") or "")
        if not evidence_id:
            raise ValueError(f"事实 {key} 没有证据编号，无法用于 $evidence 占位符")
        return evidence_id

    if value.startswith("$fact:"):
        expression = value.removeprefix("$fact:")
        key, separator, field_name = expression.rpartition(".")
        if not separator:
            raise ValueError(f"$fact 占位符必须写成 $fact:<key>.<field>：{value}")
        fact = facts.get(key)
        if fact is None:
            raise ValueError(f"参数占位符引用了不存在的事实：{key}")
        data = fact.get("data")
        if not isinstance(data, dict) or field_name not in data:
            raise ValueError(f"事实 {key} 没有字段 {field_name}：{value}")
        return data[field_name]

    # 未知占位符直接报错，避免把 "$foo" 原样发给 MCP 工具。
    raise ValueError(f"未知参数占位符：{value}")


def _last_evidence_id(messages: list[Any]) -> str:
    """取最近一条带证据编号的工具结果。"""
    for message in reversed(messages):
        if isinstance(message, ToolResultMessage) and message.meta is not None:
            evidence_id = str(message.meta.evidence_id or "")
            if evidence_id:
                return evidence_id
    return ""
