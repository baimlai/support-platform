"""调查 Topic：把“查到了什么、还差什么”从长对话里提出来。

## 为什么需要 Topic

Agent 跑几轮之后，完整消息会越来越长：慢日志一大段、执行计划一大段、工具结果一大段。
如果每次都把全部历史发给模型，上下文会不断膨胀；如果直接截断，又可能把已经确认的事实丢掉。

Topic 是平台维护的一份“调查进度摘要”，只记录：
- 当前问题是什么、调查范围是什么；
- 已经确认的事实（每条都带证据编号）；
- 候选原因及其状态（candidate / supported / unverified）；
- 还没解决的问题（open questions）；
- 下一步该做什么（next steps）。

它和完整消息历史分开保存：`topic.json` 很小，可以每轮注入模型上下文；
完整原文在 `messages.json` 和 `evidence/` 里，需要时再查。

## 事实从哪来

Topic **只根据工具实际返回的结果更新**，不读取模型的推测文本。
例子：模型在报告里写“可能是索引问题”，这句话不会变成 Topic 事实；
只有执行计划工具真的返回 `type=ALL`、`key=null`，才把访问路径写成“已支持”的候选原因。
这样能避免把模型的猜测沉淀成“已确认事实”。

## 实现说明

- `dict[str, TopicFact]` 用 key 去重：同一个工具重复运行不会无限追加事实，
  后一次结果会覆盖前一次，并保留新的证据编号。
- `slots=True` 的 dataclass 适合这种“字段固定、只装数据”的对象。
- `dataclasses.asdict()` 会把嵌套 dataclass 也转成字典，但这里字段里带 list，
  手写 `to_dict()` 更直观，也能控制顺序。
- `json.dumps(..., sort_keys=True)` 用来做“内容有没有变化”的快照比较；
  比较的是去掉 `updated_at` 的内容，否则每次时间戳不同都会误判为有变化。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.runtime.contracts import ToolExecutionEnd, TopicUpdated

# 只有这四个业务工具会改变 Topic。平台工具（搜索、证据读取）不改变调查事实。
_KNOWN_TOOLS = frozenset(
    {"slow_log_query", "execution_plan", "instance_metrics", "lock_wait_query"}
)

# 所有业务工具都尝试过之后，下一步就是汇总报告。
_ALL_TOOLS = ("slow_log_query", "execution_plan", "instance_metrics", "lock_wait_query")


def _utc_now() -> str:
    """返回带时区的 ISO 时间。"""
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class TopicFact:
    """一条已确认的观测事实。

    例子：`source_tool="slow_log_query"`，`statement="最慢 SQL 指纹 ..."`,
    `evidence_id="ev-000001"`。报告里的事实应当能追到这条记录。
    """

    key: str
    statement: str
    evidence_id: str | None = None
    source_tool: str = ""
    # 结构化的事实数据，给程序和模型复用。
    # 例子：慢日志事实里放 {"sql_fingerprint": "a1b2...", "avg_latency_ms": 2380}，
    # 即使原始工具消息被上下文裁掉，模型仍然能拿到指纹继续查执行计划。
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "statement": self.statement,
            "evidence_id": self.evidence_id,
            "source_tool": self.source_tool,
            "observed": True,
            "data": dict(self.data),
        }


@dataclass(slots=True)
class TopicCandidate:
    """一条候选原因。

    `status` 的取值：
    - `candidate`：有线索，但证据不足；
    - `supported`：有观测证据支持，但仍然是推断，不等于最终结论；
    - `unverified`：相关工具失败或无数据，当前无法判断。
    """

    key: str
    statement: str
    status: str = "candidate"
    evidence: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "statement": self.statement,
            "status": self.status,
            "evidence": list(self.evidence),
        }


@dataclass(slots=True)
class Topic:
    """一次调查的 Topic 快照。"""

    question: str
    scope: dict[str, Any]
    facts: dict[str, TopicFact] = field(default_factory=dict)
    candidates: dict[str, TopicCandidate] = field(default_factory=dict)
    open_questions: list[str] = field(default_factory=list)
    next_steps: list[str] = field(default_factory=list)
    # 尝试过哪些工具（包括 search_tools、evidence_read 这类平台工具）。
    # 有它以后，即使旧的工具结果消息被上下文裁剪，模型仍然知道“这一步已经做过了”，
    # 不会反复调用同一个工具，也不会把“没查到”误当成“还没查”。
    attempted_tools: list[str] = field(default_factory=list)
    # 平台工具的活动记录。它们不产生业务事实，但报告和前端需要知道
    # “读了哪段证据、搜了什么、加载了哪些工具”。
    # 例子：`{"evidence_id": "ev-000001", "offset": 4000, "limit": 800, ...}`。
    evidence_reads: list[dict[str, Any]] = field(default_factory=list)
    searches: list[dict[str, Any]] = field(default_factory=list)
    updated_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> dict[str, Any]:
        """转成前端和 `topic.json` 使用的结构。

        facts 和 candidates 用列表而不是字典返回，原因是前端渲染列表更直接，
        顺序也稳定（Python 字典保持插入顺序）。
        """
        return {
            "question": self.question,
            "scope": dict(self.scope),
            "facts": [fact.to_dict() for fact in self.facts.values()],
            "candidates": [candidate.to_dict() for candidate in self.candidates.values()],
            "open_questions": list(self.open_questions),
            "next_steps": list(self.next_steps),
            "attempted_tools": list(self.attempted_tools),
            "evidence_reads": [dict(item) for item in self.evidence_reads],
            "searches": [dict(item) for item in self.searches],
            "updated_at": self.updated_at,
        }


class TopicManager:
    """根据工具执行事件维护 Topic，并持久化到 `topic.json`。

    用法：
        manager = TopicManager(ticket, path=run_dir / "topic.json")
        event = manager.update_from_event(tool_execution_end_event)
        if event is not None:
            await emit(event)
    """

    def __init__(self, ticket: dict[str, Any], path: Path | str | None = None) -> None:
        self.path = Path(path) if path is not None else None
        self.topic = Topic(
            question=str(ticket.get("title") or ticket.get("symptom") or "DBA 工单调查"),
            scope={
                "ticket_id": ticket.get("ticket_id"),
                "instance_id": ticket.get("instance_id"),
                "database": ticket.get("database"),
                "database_type": ticket.get("database_type"),
                "scenario_id": ticket.get("scenario_id"),
                "scenario_label": ticket.get("scenario_label"),
                "time_range": ticket.get("time_range", {}),
                "mock": bool(ticket.get("mock", False)),
            },
        )
        # 多数据库场景模式下，Topic 的“下一步”来自工单里的调查计划和每个工具
        # 返回的 `details.topic.next_steps`。慢 SQL 的旧流程继续使用固定剧本，
        # 两者互不影响。
        self._scenario_mode = bool(ticket.get("scenario_mode"))
        self._scenario_plan = [
            str(item)
            for item in ticket.get("investigation_plan") or []
            if str(item).strip()
        ]
        self._next_steps_override: list[str] = []
        # `_slow_log_ok` 只影响“下一步是否推荐查执行计划”：
        # 慢日志失败时，没有指纹可查，不应该继续推荐执行计划。
        self._slow_log_ok = False
        self._refresh_next_steps()
        self._persist()

    def topic_dict(self) -> dict[str, Any]:
        """返回当前 Topic 的普通字典，不暴露内部可变对象。"""
        return self.topic.to_dict()

    def update_from_event(self, event: ToolExecutionEnd) -> TopicUpdated | None:
        """根据一次工具执行结束事件更新 Topic。

        返回 `None` 表示没有变化（或者这个工具与 Topic 无关），调用方不用发事件。
        返回 `TopicUpdated` 表示内容变了，调用方应当把它也发给记录器和前端。
        """
        before = self._snapshot()

        # 所有工具都记录“尝试过”，包括 search_tools 和 evidence_read。
        # 为什么平台工具也要记：它们不产生调查事实，但能防止模型在上下文被裁剪后
        # 反复搜索或反复读同一个证据。
        if event.tool_name not in self.topic.attempted_tools:
            self.topic.attempted_tools.append(event.tool_name)

        evidence_id = event.result.meta.evidence_id if event.result.meta else None
        details = _event_details(event)
        if event.tool_name in _KNOWN_TOOLS:
            if event.is_error:
                self._record_failure(event, evidence_id)
            elif event.tool_name == "slow_log_query":
                self._record_slow_log(event, evidence_id)
            elif event.tool_name == "execution_plan":
                self._record_execution_plan(event, evidence_id)
            elif event.tool_name == "instance_metrics":
                self._record_metrics(event, evidence_id)
            elif event.tool_name == "lock_wait_query":
                self._record_lock_wait(event, evidence_id)
        elif isinstance(details.get("topic"), dict):
            # 多数据库场景工具统一把“事实 / 候选原因 / 下一步”放在
            # structuredContent.details.topic 里。Topic 不需要认识每一种数据库，
            # 只按这套约定读取即可。
            self._record_scenario_topic(event, details, evidence_id)
            if event.is_error:
                self._record_generic_failure(event, evidence_id)
        elif event.tool_name == "evidence_read":
            self._record_evidence_read(event)
        elif event.tool_name == "search_tools":
            self._record_search(event)
        elif event.is_error:
            # 未知业务工具失败也要留下“未验证”说明，不能静默吞掉。
            self._record_generic_failure(event, evidence_id)

        self._refresh_next_steps()

        if self._snapshot() == before:
            return None
        self.topic.updated_at = _utc_now()
        self._persist()
        return TopicUpdated(topic=self.topic.to_dict(), turn=event.turn)

    # ---------- 各类工具结果 ----------

    def _record_slow_log(self, event: ToolExecutionEnd, evidence_id: str | None) -> None:
        """慢日志成功：记录最慢指纹、扫描比例和访问路径候选原因。"""
        details = _event_details(event)
        top = details.get("top")
        if not isinstance(top, dict):
            return

        fingerprint = top.get("sql_fingerprint", "未知")
        statement = (
            f"最慢 SQL 指纹 {fingerprint}：平均 {top.get('avg_latency_ms', '未知')} ms，"
            f"最大 {top.get('max_latency_ms', '未知')} ms，出现 {top.get('count', '未知')} 次，"
            f"平均扫描 {top.get('avg_rows_examined', '未知')} 行、返回 "
            f"{top.get('avg_rows_sent', '未知')} 行"
        )
        # 工具结果的平台元信息里有“是否截断、原文多大”。
        # 把它一并写进 Topic，后面即使原始消息被上下文裁掉，
        # 报告仍然能说明“这次看到的是预览，不是完整原文”。
        meta = event.result.meta
        self._set_fact(
            "slow_log.top",
            statement,
            evidence_id,
            "slow_log_query",
            data={
                "sql_fingerprint": fingerprint,
                "avg_latency_ms": top.get("avg_latency_ms"),
                "max_latency_ms": top.get("max_latency_ms"),
                "count": top.get("count"),
                "avg_rows_examined": top.get("avg_rows_examined"),
                "avg_rows_sent": top.get("avg_rows_sent"),
                "truncated": bool(meta.truncated) if meta is not None else False,
                "raw_size_chars": meta.raw_size_chars if meta is not None else None,
                "max_inline_chars": meta.max_inline_chars if meta is not None else None,
            },
        )
        self._slow_log_ok = True

        coverage = details.get("coverage_ratio")
        self._set_fact(
            "slow_log.coverage",
            f"慢日志采样覆盖率约 {coverage}，不代表全量请求",
            evidence_id,
            "slow_log_query",
            data={"coverage_ratio": coverage},
        )
        self._set_candidate(
            "access_path",
            "最慢 SQL 可能因访问路径或索引导致大量扫描（等待执行计划确认，仍是推断）",
            "candidate",
            evidence_id,
        )

    def _record_execution_plan(self, event: ToolExecutionEnd, evidence_id: str | None) -> None:
        """执行计划成功：把访问路径候选原因升级为“有证据支持”或保持候选。"""
        details = _event_details(event)
        access_type = details.get("access_type", "未知")
        key_name = details.get("key") or "未命中索引"
        full_scan = bool(details.get("full_scan"))
        estimated_rows = details.get("estimated_rows", "未知")
        extra = details.get("extra") or []

        statement = (
            f"执行计划访问类型 {access_type}，{key_name}，"
            f"估算扫描 {estimated_rows} 行，Extra：{', '.join(extra) if extra else '无'}"
        )
        self._set_fact(
            "execution_plan.access",
            statement,
            evidence_id,
            "execution_plan",
            data={
                "access_type": access_type,
                "key": details.get("key"),
                "full_scan": full_scan,
                "estimated_rows": estimated_rows,
                "actual_rows_sent": details.get("actual_rows_sent"),
                "extra": list(extra),
            },
        )

        if full_scan:
            self._set_candidate(
                "access_path",
                "最慢 SQL 执行计划显示全表扫描，可能无法有效利用现有索引；"
                "这是有观测支持的推断，仍需真实 EXPLAIN ANALYZE 复核",
                "supported",
                evidence_id,
            )
        else:
            self._set_candidate(
                "access_path",
                "执行计划没有显示全表扫描，慢的原因仍需结合真实执行计划复核",
                "candidate",
                evidence_id,
            )

    def _record_metrics(self, event: ToolExecutionEnd, evidence_id: str | None) -> None:
        """实例监控成功：记录已返回数据，避免把“没展开”写成“没有异常”。"""
        del event
        self._set_fact(
            "instance_metrics.available",
            "实例监控已返回数据（模拟报告未展开具体指标）",
            evidence_id,
            "instance_metrics",
            data={},
        )
        self._set_candidate(
            "instance_resource",
            "实例 CPU/IO 是否成为瓶颈：已有监控数据，但尚未分析",
            "candidate",
            evidence_id,
        )

    def _record_lock_wait(self, event: ToolExecutionEnd, evidence_id: str | None) -> None:
        """锁等待结果：no_data 也是一种观测，但不能推出“没有锁等待”。"""
        details = _event_details(event)
        status = str(details.get("status", "unknown"))
        sample_count = _as_int(details.get("sample_count"))
        if status == "no_data" or sample_count == 0:
            self._set_fact(
                "lock_wait.samples",
                "锁等待查询返回 no_data：采样窗口内没有样本",
                evidence_id,
                "lock_wait_query",
                data={"status": status, "sample_count": sample_count},
            )
            self._add_open_question("无锁等待样本不等于没有锁等待，需要更长窗口或其它采样源复核")
            self._set_candidate(
                "lock_wait",
                "锁等待影响：采样窗口内无样本，当前无法确认或排除",
                "unverified",
                evidence_id,
            )
        else:
            self._set_fact(
                "lock_wait.samples",
                f"锁等待查询返回 {sample_count} 个样本",
                evidence_id,
                "lock_wait_query",
                data={"status": status, "sample_count": sample_count},
            )
            self._set_candidate(
                "lock_wait",
                "锁等待样本存在，可能影响响应时间，需要结合等待对象和事务分析",
                "candidate",
                evidence_id,
            )

    def _record_scenario_topic(
        self,
        event: ToolExecutionEnd,
        details: dict[str, Any],
        evidence_id: str | None,
    ) -> None:
        """读取多数据库场景工具返回的 `details.topic`。

        约定格式（由数据集提供）：

        ```json
        {
          "facts": [
            {"key": "pg.long_transaction", "statement": "...", "data": {...}}
          ],
          "candidates": [
            {"key": "autovacuum_blocked", "statement": "...", "status": "supported"}
          ],
          "open_questions": ["还需要确认应用事务边界"],
          "next_steps": ["查询表统计", "汇总证据并输出报告"]
        }
        ```

        为什么把格式放在工具结果里，而不是让 Topic 为每种数据库写一个分支：
        数据库类型会继续增加，平台不应该每接一种数据库就改一次 Topic。
        只要工具结果遵守这套结构，Topic、前端和报告都能复用同一条链路。

        注意：这里只记录工具**实际返回**的内容。模型在文本里说“可能是锁”
        不会进入事实；它只能引用这里已经存在的事实。
        """
        topic_data = details.get("topic")
        if not isinstance(topic_data, dict):
            return

        facts = topic_data.get("facts")
        if isinstance(facts, list):
            for item in facts:
                if not isinstance(item, dict) or not item.get("key"):
                    continue
                data = item.get("data")
                self._set_fact(
                    str(item["key"]),
                    str(item.get("statement") or item["key"]),
                    evidence_id,
                    event.tool_name,
                    data=dict(data) if isinstance(data, dict) else {},
                )

        candidates = topic_data.get("candidates")
        if isinstance(candidates, list):
            for item in candidates:
                if not isinstance(item, dict) or not item.get("key"):
                    continue
                status = str(item.get("status") or "candidate")
                if status not in {"candidate", "supported", "unverified"}:
                    status = "candidate"
                self._set_candidate(
                    str(item["key"]),
                    str(item.get("statement") or item["key"]),
                    status,
                    evidence_id,
                )

        questions = topic_data.get("open_questions")
        if isinstance(questions, list):
            for question in questions:
                text = str(question).strip()
                if text:
                    self._add_open_question(text)

        # 下一步由数据给出，报告/前端能看到“查完这一项之后还差什么”。
        # 放到 override 里，`_refresh_next_steps` 在场景模式下直接采用。
        next_steps = topic_data.get("next_steps")
        if isinstance(next_steps, list):
            self._next_steps_override = [
                str(item).strip() for item in next_steps if str(item).strip()
            ]

    def _record_generic_failure(
        self,
        event: ToolExecutionEnd,
        evidence_id: str | None,
    ) -> None:
        """记录任意业务工具的失败。

        慢 SQL 工具有专门的候选原因，比如监控失败对应 `instance_resource`。
        多数据库场景工具的数量会继续增加，不可能每加一个工具就改一次 Topic。
        因此这里只做两件通用的事：
        1. 写一条未决问题，说明哪个工具失败、错误是什么；
        2. 如果有候选原因，升级成 `unverified`，避免报告把“没查到”当成“正常”。

        例子：`pg_table_stats` 超时，报告必须写“表统计未验证”，
        不能写“表没有膨胀”。
        """
        message = " ".join(tool_result_text_from_event(event).split())[:160]
        ref = f"（证据 `{evidence_id}`）" if evidence_id else ""
        self._add_open_question(
            f"工具 {event.tool_name} 执行失败，相关结论未验证：{message}{ref}"
        )

    def _record_evidence_read(self, event: ToolExecutionEnd) -> None:
        """记录一次证据分页读取。

        平台工具不产生业务事实，但“读了哪条证据、读了哪一段”是调查过程的一部分。
        把它写进 Topic 后，即使分页结果消息被上下文裁掉，报告仍然能展示这一段。
        """
        details = _event_details(event)
        record = {
            "evidence_id": details.get("evidence_id"),
            "offset": details.get("offset"),
            "limit": details.get("limit"),
            "total_chars": details.get("total_chars"),
            "truncated": details.get("truncated"),
            "is_error": event.is_error,
        }
        if event.is_error:
            record["error"] = tool_result_text_from_event(event)[:160]
        self.topic.evidence_reads.append(record)

    def _record_search(self, event: ToolExecutionEnd) -> None:
        """记录一次工具搜索：搜了什么、命中了多少、加载了哪些工具。"""
        details = _event_details(event)
        record = {
            "query": details.get("query"),
            "match_count": details.get("match_count"),
            "loaded": list(details.get("loaded") or []),
            "is_error": event.is_error,
        }
        if event.is_error:
            record["error"] = tool_result_text_from_event(event)[:160]
        self.topic.searches.append(record)

    def _record_failure(self, event: ToolExecutionEnd, evidence_id: str | None) -> None:
        """工具失败：不加“事实”，只加未决问题和未验证候选原因。"""
        message = tool_result_text_from_event(event)[:160]
        self._add_open_question(
            f"工具 {event.tool_name} 执行失败，相关结论未验证：{message}"
        )
        if event.tool_name == "instance_metrics":
            self._set_candidate(
                "instance_resource",
                "实例 CPU/IO 是否成为瓶颈：监控接口失败，尚未验证",
                "unverified",
                evidence_id,
            )
        elif event.tool_name == "lock_wait_query":
            self._set_candidate(
                "lock_wait",
                "锁等待影响：查询失败，尚未验证",
                "unverified",
                evidence_id,
            )
        elif event.tool_name == "execution_plan":
            self._set_candidate(
                "access_path",
                "访问路径：执行计划获取失败，尚未验证",
                "unverified",
                evidence_id,
            )

    # ---------- 内部工具 ----------

    def _set_fact(
        self,
        key: str,
        statement: str,
        evidence_id: str | None,
        source_tool: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """写入或覆盖一条事实。

        用 key 覆盖而不是追加，保证同一个工具跑多次时 Topic 不会无限变长。
        """
        self.topic.facts[key] = TopicFact(
            key=key,
            statement=statement,
            evidence_id=evidence_id,
            source_tool=source_tool,
            data=dict(data or {}),
        )

    def _set_candidate(
        self,
        key: str,
        statement: str,
        status: str,
        evidence_id: str | None,
    ) -> None:
        """写入或更新一条候选原因，证据编号去重后追加。"""
        candidate = self.topic.candidates.get(key)
        if candidate is None:
            candidate = TopicCandidate(key=key, statement=statement, status=status)
            self.topic.candidates[key] = candidate
        candidate.statement = statement
        candidate.status = status
        if evidence_id and evidence_id not in candidate.evidence:
            candidate.evidence.append(evidence_id)

    def _add_open_question(self, question: str) -> None:
        """追加未决问题；相同问题只保留一条。"""
        if question not in self.topic.open_questions:
            self.topic.open_questions.append(question)

    def _refresh_next_steps(self) -> None:
        """根据“哪些工具已经尝试过”重算下一步。

        为什么不手动 add/remove：
        手动维护很容易忘记删除已经完成的步骤。这里每次从状态重算，
        结果稳定，也不会出现重复步骤。
        """
        attempted = set(self.topic.attempted_tools)
        if self._scenario_mode:
            # 场景模式：优先使用工具结果里给的下一步；还没执行任何业务工具时，
            # 使用工单携带的调查计划。这样前端一开始就能看到完整排查路线。
            if self._next_steps_override:
                self.topic.next_steps = list(self._next_steps_override)
            else:
                self.topic.next_steps = list(self._scenario_plan)
            return

        steps: list[str] = []
        if "slow_log_query" not in attempted:
            steps.append("查询慢日志，确认最慢 SQL 指纹")
        elif self._slow_log_ok and "execution_plan" not in attempted:
            steps.append("按最慢指纹查询执行计划")
        if "instance_metrics" not in attempted:
            steps.append("补充实例监控（CPU/IO）")
        if "lock_wait_query" not in attempted:
            steps.append("补充锁等待样本")
        if all(name in attempted for name in _ALL_TOOLS):
            steps.append("汇总证据并输出报告")
        self.topic.next_steps = steps

    def _snapshot(self) -> str:
        """生成“忽略时间戳”的内容快照，用来判断 Topic 是否真的变化。"""
        data = self.topic.to_dict()
        data.pop("updated_at", None)
        return json.dumps(data, ensure_ascii=False, sort_keys=True)

    def _persist(self) -> None:
        """把 Topic 写到磁盘。

        即使没有配置路径也允许运行（测试里可能只关心内存状态），
        但正式运行都会传 `run_dir / "topic.json"`。
        """
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self.topic.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _as_int(value: Any) -> int:
    """尽量把值转成整数，转不了就返回 0。

    为什么要这个函数：工具结果来自外部接口，`sample_count` 可能是 None、
    字符串 "3" 或者干脆没有。直接 `int(...)` 遇到 None 会抛 TypeError，
    而 Topic 更新不应该因为一个可选字段格式不对就中断整次运行。
    例子：`_as_int("3")` 返回 3，`_as_int(None)` 返回 0。
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _event_text(event: ToolExecutionEnd) -> str:
    """取出工具结果里的文本内容。"""
    return "".join(block.text for block in event.result.content)


def _event_details(event: ToolExecutionEnd) -> dict[str, Any]:
    """取工具结果的业务摘要。

    正常情况下看 `details`；如果 `details` 为空，例如结果被替换成了截断信封，
    就尝试解析 content 里的 JSON，并把 `summary` 当作摘要。
    例子：`{"truncated": true, "summary": {"top": {...}}}` 会返回 `{"top": {...}}`。
    """
    details = event.result.details
    if isinstance(details, dict) and details:
        return details
    text = _event_text(event)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    summary = parsed.get("summary")
    if isinstance(summary, dict):
        return summary
    return parsed


def tool_result_text_from_event(event: ToolExecutionEnd) -> str:
    """返回工具执行事件里的全部文本。

    事件里装的是 `ToolResult`，不是 `ToolResultMessage`，但两者都有
    `content: list[TextContent]`。这里直接拼文本，避免为了复用一个小函数
    去构造一个假的消息对象。
    """
    return "".join(block.text for block in event.result.content)
