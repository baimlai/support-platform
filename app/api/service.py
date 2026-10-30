"""HTTP 后台任务的生命周期管理。

## 这一层解决什么问题

FastAPI 的接口只负责“收请求、返回 JSON”。真正耗时的 Agent 调查必须放到后台跑，
否则一个请求会阻塞十几秒甚至更久。`TaskService` 负责：

1. 创建任务并立刻返回 `queued`；
2. 用 `asyncio.create_task` 在后台跑 `AgentRunner`；
3. 把状态、轮次、报告和能力快照写进 SQLite；
4. 提供取消、事件分页、证据读取、轮次上下文等查询；
5. 服务启动/停止时，把没跑完的任务标成 `interrupted`，不假装恢复。

## 为什么取消用“协作式”

`CancellationToken` 是一个普通标记，循环在每轮开始和工具执行前检查它。
接口调用 `cancel()` 只是把标记翻成 True，不会粗暴杀掉正在写文件的协程。
这样已经产生的消息、事件和证据还能完整落盘，报告里也能写“调查未完成”。

## 实现说明

- `asyncio.create_task(coro)` 会把协程排进当前事件循环，立即返回一个 Task 对象；
  不要写成 `asyncio.create_task(coro())` 后忘记保存，否则任务可能被垃圾回收。
- `asyncio.gather(..., return_exceptions=True)` 等待多个任务结束；
  加 `return_exceptions=True` 是为了“一个任务取消/报错，不影响其它任务的清理”。
- 文件读取是同步的。证据和事件文件都不大，当前实现直接读；
  以后接入大文件再换成 `asyncio.to_thread`，接口形状不变。
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import re
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.adapters.deepseek_model import ModelConfigurationError, resolve_api_key
from app.adapters.models import (
    SUPPORTED_MODEL_PROVIDERS,
    available_model_names,
    normalize_provider,
    resolve_model_name,
)
from app.adapters.scenario_mock import resolve_ticket_dataset
from app.api.settings import ApiSettings
from app.api.store import TaskRecord, TaskStore, utc_now
from app.harness.context import ContextBudget
from app.harness.evidence import EvidenceStore
from app.harness.runner import AgentRunner, RunOptions
from app.harness.skills import (
    DEFAULT_SKILL_NAME,
    SkillNotFound,
    list_skills,
    load_skill,
)
from app.harness.tool_search import PLATFORM_TOOL_DECLARATIONS
from app.mcp.dba_tools import business_tool_declarations
from app.runtime.contracts import AgentEvent, CancellationToken, TurnStart

logger = logging.getLogger(__name__)

# 任务进入这些状态后，不会再产生新事件，前端可以停止轮询。
TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})

# 循环结束原因 -> 对外任务状态。
# 达到最大轮数没有最终报告，按 failed 记录，但 stop_reason 保留 max_turns，
# 这样前端既能统一处理“失败”，又能显示具体原因。
_STATUS_BY_REASON = {
    "completed": "completed",
    "cancelled": "cancelled",
    "error": "failed",
    "max_turns": "failed",
}


class TaskNotFound(LookupError):
    """任务 id 不存在。"""


class SampleNotFound(LookupError):
    """样例 id 不存在。"""


class TurnContextNotFound(LookupError):
    """指定轮次的模型输入还没产生或不存在。"""


class ReportNotReady(LookupError):
    """任务还没到终态，或者报告文件不存在。"""


class InvalidTaskRequest(ValueError):
    """请求内容不合法，例如既没给样例也没给工单文本。"""


@dataclass(slots=True)
class _TaskRuntime:
    """只存在内存里的运行时状态。

    为什么不全放数据库：取消令牌和 asyncio.Task 是进程内对象，没法序列化。
    任务 id 是两边共同的 key，SQLite 存“结果”，这里存“正在跑的东西”。
    """

    cancellation: CancellationToken
    task: asyncio.Task[None] | None = None


def _read_json(path: Path) -> dict[str, Any] | None:
    """读一个 JSON 对象；文件不存在或内容坏了都返回 `None`。

    查询接口不应该因为一个损坏的 topic.json 就整个 500。
    真正需要严格校验的地方（创建任务、读取样例）会单独抛错。
    """
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("读取 JSON 失败：%s", path)
        return None
    return data if isinstance(data, dict) else None


def _safe_sample_id(stem: str) -> str:
    """把文件名转成前端使用的样例 id。

    例子：`slow_sql_001.json` -> `slow-sql-001`。
    只做最简单的小写和下划线替换，不引入额外映射表。
    """
    return re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-")


def _as_int(value: Any) -> int:
    """把可选的 `scenario_order` 转成整数，坏了就当作 0。

    为什么要容错：样例文件是人工维护的，偶尔漏字段或写成字符串不应该让
    整个 `/api/samples` 接口报 500。排序错了可以修数据，接口挂了影响更大。
    例子：`_as_int("20")` -> 20；`_as_int(None)` -> 0。
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class TaskService:
    """任务创建、后台执行和查询的统一入口。"""

    def __init__(self, settings: ApiSettings) -> None:
        self.settings = settings
        self.store = TaskStore(settings.db_path)
        self._runtime: dict[str, _TaskRuntime] = {}
        self._samples: list[tuple[str, Path, dict[str, Any]]] | None = None
        self._shutting_down = False

    # ---------- 启动与停止 ----------

    async def startup(self) -> None:
        """服务启动：建表，并把上次没跑完的任务标成中断。"""
        self.store.initialize()
        interrupted = self.store.mark_unfinished_interrupted(
            "服务重启，上一次运行未完成；本版本不支持进程重启后精确续跑。"
        )
        if interrupted:
            logger.warning("启动时把 %s 个未完成任务标记为 interrupted", interrupted)

    async def shutdown(self) -> None:
        """服务停止：尽量让后台任务先收尾，再把剩余任务标成中断。

        顺序：
        1. 先发协作式取消，让循环有机会写 `agent_end` 和报告；
        2. 再硬取消还没退出的 asyncio 任务；
        3. 最后把数据库里仍非终态的任务统一标成 interrupted。
        """
        self._shutting_down = True
        runtimes = list(self._runtime.values())
        for runtime in runtimes:
            runtime.cancellation.cancel()

        # `await asyncio.sleep(0)` 让出一次事件循环，给取消标记一个被检查的机会。
        await asyncio.sleep(0)
        tasks = [runtime.task for runtime in runtimes if runtime.task and not runtime.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._runtime.clear()
        interrupted = self.store.mark_unfinished_interrupted("服务停止，任务被中断。")
        if interrupted:
            logger.warning("停止时把 %s 个未完成任务标记为 interrupted", interrupted)

    # ---------- 创建任务 ----------

    async def create_task(
        self,
        *,
        sample_id: str | None,
        ticket_text: str | None,
        tenant_id: str | None,
        skill_name: str | None,
        max_turns: int,
        model_provider: str | None = None,
        model_name: str | None = None,
    ) -> dict[str, Any]:
        """创建一个后台调查任务。

        参数二选一：
        - `sample_id`：使用 `fixtures/tickets/` 里的样例；
        - `ticket_text`：用户直接输入工单文本，实例和时间范围沿用样例模板。

        Skill 的优先级：
        - 请求里显式传了 `skill` -> 用它；
        - 没传 -> 用样例工单里的 `skill`（例如多数据库样例是 `dba-triage`）；
        - 工单里也没有 -> 退回默认 `slow-sql`。

        模型也二选一（不传就用服务端默认）：
        - `model_provider="mock"`：本地剧本，不需要网络和 Key；
        - `model_provider="deepseek"`：真实模型，服务端必须配置 Key。

        为什么自由文本也沿用样例：模拟工具只认识样例里的实例和时间范围。
        如果不补这些字段，任务会在第一次工具调用时失败，演示不了完整流程。
        响应里仍会保留用户原文，并明确标记 mock=true。
        """
        provider = self._resolve_provider(model_provider)
        resolved_model = self._resolve_model_name(provider, model_name)
        # DeepSeek 没有 Key 时直接拒绝创建任务，并给出可操作的提示。
        # 不在这里“悄悄退回 mock”：用户明明选了真实模型，却拿到模拟结果，
        # 比报错更难发现。
        if provider == "deepseek":
            try:
                resolve_api_key(self.settings.model_api_key)
            except ModelConfigurationError as exc:
                raise InvalidTaskRequest(str(exc)) from exc

        ticket = self._build_ticket(
            sample_id=sample_id,
            ticket_text=ticket_text,
            tenant_id=tenant_id,
        )
        resolved_skill = self._resolve_skill(skill_name, ticket)

        # 场景数据路径在创建任务时就校验，避免任务先返回 202、几秒后才失败。
        # 旧工单没有 scenario_mode，这里会原样返回服务端默认数据集。
        try:
            resolve_ticket_dataset(
                ticket,
                fallback=self.settings.mcp_dataset_path,
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise InvalidTaskRequest(f"场景数据不可用：{exc}") from exc

        task_id = str(uuid.uuid4())
        run_dir = self.settings.run_root / task_id
        self.store.create(
            task_id=task_id,
            ticket=ticket,
            skill_name=resolved_skill,
            run_dir=str(run_dir),
            model_provider=provider,
            model_name=resolved_model,
        )

        runtime = _TaskRuntime(cancellation=CancellationToken())
        self._runtime[task_id] = runtime
        # 注意：协程对象传进去后，事件循环会在合适的时候开始执行它。
        # 这里不 await，接口才能立刻返回 202。
        runtime.task = asyncio.create_task(
            self._run_task(
                task_id=task_id,
                ticket=ticket,
                skill_name=resolved_skill,
                max_turns=max_turns,
                model_provider=provider,
                model_name=resolved_model,
            ),
            name=f"dba-task-{task_id}",
        )
        return self.get_task(task_id)

    def _resolve_skill(self, requested: str | None, ticket: dict[str, Any]) -> str:
        """确定这次任务用哪个 Skill，并提前校验它存在。

        把它单独抽出来，是因为“样例自带 Skill”和“用户显式选择 Skill”都可能
        出现。优先级写在一个地方，接口、CLI 和测试就不会各有一套规则。
        """
        candidate = (
            (requested or "").strip()
            or str(ticket.get("skill") or "").strip()
            or DEFAULT_SKILL_NAME
        )
        try:
            load_skill(candidate)
        except (SkillNotFound, ValueError) as exc:
            raise InvalidTaskRequest(str(exc)) from exc
        return candidate

    def _resolve_provider(self, requested: str | None) -> str:
        """确定这次任务的 provider：请求值 > 服务端默认值 > mock。

        例子：请求没传 -> 用 `SUPPORT_PLATFORM_MODEL_PROVIDER`；
        请求传了 `" DeepSeek "` -> 规范成 `"deepseek"`。
        """
        provider = normalize_provider(requested or self.settings.model_provider)
        if provider not in SUPPORTED_MODEL_PROVIDERS:
            raise InvalidTaskRequest(
                f"不支持的模型 provider：{requested!r}。"
                f"可选值：{', '.join(sorted(SUPPORTED_MODEL_PROVIDERS))}"
            )
        return provider

    def _resolve_model_name(self, provider: str, requested: str | None) -> str:
        """确定实际模型名：请求值 > 服务端配置 > 环境变量 > 代码默认值。"""
        configured = self.settings.model_name if provider == "deepseek" else None
        return resolve_model_name(provider, requested or configured)

    def _build_ticket(
        self,
        *,
        sample_id: str | None,
        ticket_text: str | None,
        tenant_id: str | None,
    ) -> dict[str, Any]:
        """按请求生成工单字典。"""
        if sample_id is not None and sample_id.strip():
            matched = self._find_sample(sample_id)
            if matched is None:
                raise SampleNotFound(f"样例不存在：{sample_id}")
            _, _, ticket = matched
            result = copy.deepcopy(ticket)
            if tenant_id:
                result["tenant_id"] = tenant_id
            return result

        if ticket_text is None or not ticket_text.strip():
            raise InvalidTaskRequest("sample_id 和 ticket_text 必须提供一个")

        samples = self._load_samples()
        if not samples:
            raise InvalidTaskRequest("fixtures/tickets 下没有可用的样例模板")
        template = copy.deepcopy(samples[0][2])
        text = ticket_text.strip()
        first_line = text.splitlines()[0].strip() or "用户提交的 DBA 工单"
        stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        template.update(
            {
                "ticket_id": f"TCK-{stamp}-{secrets.token_hex(2)}",
                "tenant_id": tenant_id or template.get("tenant_id") or "tenant-demo",
                "title": first_line[:80],
                "description": text,
                "symptom": text[:160],
                "mock": True,
                "mock_note": (
                    "工单文本由用户输入；实例、时间范围和工具数据仍来自模拟样例，"
                    "仅用于验证流程，不代表真实业务事实。"
                ),
                "source": "user_text",
            }
        )
        return template

    # ---------- 后台执行 ----------

    async def _run_task(
        self,
        *,
        task_id: str,
        ticket: dict[str, Any],
        skill_name: str,
        max_turns: int,
        model_provider: str,
        model_name: str,
    ) -> None:
        """真正跑 Agent 的协程。

        任何情况下都要把结果写回 SQLite；否则前端会一直看到 running。
        """
        runtime = self._runtime.get(task_id)
        if runtime is None:  # 正常不会发生，防御性判断
            return

        self.store.update_fields(task_id, status="running")
        run_dir = self.settings.run_root / task_id

        options = RunOptions(
            ticket=ticket,
            skill_name=skill_name,
            max_turns=max_turns,
            tool_timeout_s=self.settings.tool_timeout_s,
            run_root=self.settings.run_root,
            # 任务 id 直接当运行目录名，前端拿到 task_id 就能定位所有材料，
            # 不用再维护“任务 id -> 运行 id”的映射。
            run_id=task_id,
            model_delay_s=self.settings.model_delay_s,
            max_inline_chars=self.settings.max_inline_chars,
            context_budget=ContextBudget(
                max_chars=self.settings.context_budget,
                max_recent_messages=self.settings.context_messages,
            ),
            # 模型选择在创建任务时已经写进数据库；这里原样传给执行链，
            # 保证“页面显示用了什么模型”和“实际请求用了什么模型”一致。
            model_provider=model_provider,
            model_name=model_name,
            model_base_url=self.settings.model_base_url,
            model_api_key=self.settings.model_api_key,
            model_timeout_s=self.settings.model_timeout_s,
            model_max_tokens=self.settings.model_max_tokens,
            model_temperature=self.settings.model_temperature,
            # 每张工单使用自己的场景数据集；旧工单没有 scenario_mode 时，
            # 这里返回服务端配置的默认慢 SQL 数据集，行为保持不变。
            dataset_path=resolve_ticket_dataset(
                ticket,
                fallback=self.settings.mcp_dataset_path,
            ),
            mcp_transport=self.settings.mcp_transport,
            mcp_url=self.settings.mcp_url,
            mcp_command=self.settings.mcp_command,
            mcp_timeout_s=self.settings.mcp_timeout_s,
        )

        try:
            outcome = await AgentRunner(options).run(
                cancellation=runtime.cancellation,
                on_event=lambda event: self._on_event(task_id, event),
            )
            status = _STATUS_BY_REASON.get(outcome.result.reason, "failed")
            capabilities = _read_json(run_dir / "run.json") or {}
            self.store.update_fields(
                task_id,
                status=status,
                stop_reason=outcome.result.reason,
                turns=outcome.result.turns,
                current_turn=outcome.result.turns,
                evidence_count=outcome.evidence.count,
                capabilities_json=json.dumps(
                    capabilities.get("capabilities"),
                    ensure_ascii=False,
                ),
                error=outcome.result.error,
                finished_at=utc_now(),
            )
        except asyncio.CancelledError:
            # 这是服务停止时的硬取消，和用户点“取消”的协作式取消不同。
            if not self._shutting_down:
                self._mark_interrupted(task_id, "任务被服务中断")
            raise
        except Exception as exc:  # noqa: BLE001 - 后台任务必须把异常转成任务状态
            logger.exception("任务执行失败：%s", task_id)
            self._write_failure_report(run_dir, exc)
            self.store.update_fields(
                task_id,
                status="failed",
                stop_reason="error",
                error=str(exc) or exc.__class__.__name__,
                finished_at=utc_now(),
            )
        finally:
            self._runtime.pop(task_id, None)

    def _on_event(self, task_id: str, event: AgentEvent) -> None:
        """事件回调：只在轮次开始时更新一次数据库。

        为什么不每个事件都写库：`message_update` 每几个字符就一条，
        每条都写 SQLite 既慢又没必要。轮次信息对前端已经足够。
        """
        if not isinstance(event, TurnStart):
            return
        try:
            self.store.update_fields(
                task_id,
                current_turn=event.turn,
                turns=event.turn,
            )
        except Exception:  # noqa: BLE001 - 更新进度失败不能影响调查本身
            logger.exception("更新任务轮次失败：%s", task_id)

    def _mark_interrupted(self, task_id: str, reason: str) -> None:
        """把任务标成中断，并保留已有错误信息。"""
        try:
            self.store.update_fields(
                task_id,
                status="interrupted",
                stop_reason="interrupted",
                error=reason,
                finished_at=utc_now(),
            )
        except Exception:  # noqa: BLE001 - 清理阶段尽量不抛新异常
            logger.exception("标记任务中断失败：%s", task_id)

    @staticmethod
    def _write_failure_report(run_dir: Path, exc: Exception) -> None:
        """后台任务在建好记录目录前就失败时，补一份最小失败报告。"""
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
            report_path = run_dir / "report.md"
            if report_path.exists():
                return
            report_path.write_text(
                "# 运行失败\n\n"
                "调查在产生完整报告前失败。\n\n"
                f"错误信息：{exc}\n",
                encoding="utf-8",
            )
        except OSError:
            logger.exception("写入失败报告失败：%s", run_dir)

    # ---------- 任务查询 ----------

    def get_task(self, task_id: str) -> dict[str, Any]:
        """返回任务详情，包含当前 Topic、报告和能力快照。"""
        record = self._require_task(task_id)
        run_dir = self._run_dir(record)
        topic = _read_json(run_dir / "topic.json")
        report = None
        if record.status in TERMINAL_STATUSES:
            report_path = run_dir / "report.md"
            if report_path.exists():
                report = report_path.read_text(encoding="utf-8")

        evidence_count = record.evidence_count
        if evidence_count is None:
            evidence_count = self._count_evidence(record)

        capabilities = record.capabilities
        if capabilities is None:
            run_meta = _read_json(run_dir / "run.json") or {}
            capabilities = run_meta.get("capabilities")

        return {
            "task_id": record.task_id,
            "status": record.status,
            "stop_reason": record.stop_reason,
            "ticket": record.ticket,
            "model_provider": record.model_provider,
            "model_name": record.model_name or resolve_model_name(record.model_provider),
            "current_turn": record.current_turn,
            "turns": record.turns,
            "topic": topic,
            "report": report,
            "error": record.error,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "finished_at": record.finished_at,
            "run_id": record.run_id,
            "evidence_count": evidence_count,
            "capabilities": capabilities,
        }

    async def cancel_task(self, task_id: str) -> dict[str, Any]:
        """请求取消任务，立即返回任务快照。

        注意：返回时状态可能仍是 running。取消是协作式的，要等循环跑到检查点。
        前端应该继续轮询任务详情，直到看到 cancelled。
        """
        record = self._require_task(task_id)
        if record.status in TERMINAL_STATUSES:
            return self.get_task(task_id)

        runtime = self._runtime.get(task_id)
        if runtime is not None:
            runtime.cancellation.cancel()
        else:
            # 理论上只有启动恢复阶段会走到这里；直接标记取消，避免任务卡住。
            self.store.update_fields(
                task_id,
                status="cancelled",
                stop_reason="cancelled",
                finished_at=utc_now(),
            )
        return self.get_task(task_id)

    def _require_task(self, task_id: str) -> TaskRecord:
        record = self.store.get(task_id)
        if record is None:
            raise TaskNotFound(f"任务不存在：{task_id}")
        return record

    def ensure_task(self, task_id: str) -> None:
        """只检查任务是否存在，不存在就抛 `TaskNotFound`。

        SSE 接口在开始流式响应前先调用它。否则客户端会先收到 200 和
        `text/event-stream`，然后在流里才发现任务不存在，错误很难处理。
        """
        self._require_task(task_id)

    def _run_dir(self, record: TaskRecord) -> Path:
        """任务对应的运行目录。"""
        return Path(record.run_dir) if record.run_dir else self.settings.run_root / record.task_id

    def _count_evidence(self, record: TaskRecord) -> int:
        """证据条数以索引文件行数为准。"""
        index_path = self._run_dir(record) / "evidence" / "index.jsonl"
        if not index_path.exists():
            return 0
        return sum(1 for line in index_path.read_text(encoding="utf-8").splitlines() if line.strip())

    # ---------- 事件 ----------

    def read_events(
        self,
        task_id: str,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> dict[str, Any]:
        """按 `seq > after_seq` 读取事件，返回一页。

        `has_more` 只表示“服务端此刻还有更多可读事件”，
        不代表任务以后不会再产生事件；是否继续轮询要看任务状态。
        """
        record = self._require_task(task_id)
        limit = max(1, min(1000, int(limit)))
        events_path = self._run_dir(record) / "events.jsonl"
        if not events_path.exists():
            return {"events": [], "next_seq": after_seq, "has_more": False}

        events: list[dict[str, Any]] = []
        has_more = False
        for line in events_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                # 文件正在追加时，最后一行可能还没写完。跳过它，下次再读。
                continue
            seq = int(event.get("seq", 0))
            if seq <= after_seq:
                continue
            if len(events) >= limit:
                has_more = True
                break
            events.append(event)

        next_seq = int(events[-1]["seq"]) if events else after_seq
        return {"events": events, "next_seq": next_seq, "has_more": has_more}

    async def stream_events(
        self,
        task_id: str,
        after_seq: int,
        request: Any,
    ) -> Any:
        """SSE 事件流：先补历史事件，再持续等待新事件，任务终态后关闭。

        每个 SSE 消息长这样：

        ```text
        id: 12
        event: tool_execution_end
        data: {"seq": 12, ...}

        ```
        空行是两个事件之间的分隔符，不能省。
        """
        record = self._require_task(task_id)
        del record  # 只用于提前确认任务存在；后面会重新读状态

        yield "retry: 1500\n\n"
        last_seq = max(0, int(after_seq))
        last_keepalive = asyncio.get_running_loop().time()

        while True:
            if await request.is_disconnected():
                return

            page = self.read_events(task_id, after_seq=last_seq, limit=200)
            if page["events"]:
                for event in page["events"]:
                    payload = json.dumps(event, ensure_ascii=False)
                    yield f"id: {event['seq']}\nevent: {event['type']}\ndata: {payload}\n\n"
                    last_seq = int(event["seq"])
                # 可能还有下一页，立刻继续，不 sleep。
                continue

            current = self._require_task(task_id)
            if current.status in TERMINAL_STATUSES:
                # 终态时事件已经写完；发一个结束事件让前端可以主动关闭连接。
                end_payload = json.dumps(
                    {"status": current.status, "stop_reason": current.stop_reason},
                    ensure_ascii=False,
                )
                yield f"event: stream_end\ndata: {end_payload}\n\n"
                return

            now = asyncio.get_running_loop().time()
            if now - last_keepalive >= 15:
                # SSE 注释行（以冒号开头）不会触发前端事件，只用于保活。
                yield ": keep-alive\n\n"
                last_keepalive = now
            await asyncio.sleep(0.5)

    # ---------- 轮次上下文 ----------

    def read_turn_context(self, task_id: str, turn: int) -> dict[str, Any]:
        """读取第 N 轮真正发给模型的输入。

        数据来自 `model_requests.jsonl`，每行一次模型请求。
        它可能和 `messages.json` 不一样：`messages.json` 是完整历史，
        这里记录的是经过 Topic 和有界上下文裁剪后的实际输入。
        """
        record = self._require_task(task_id)
        path = self._run_dir(record) / "model_requests.jsonl"
        if not path.exists():
            raise TurnContextNotFound(f"任务还没有模型请求记录：turn={turn}")

        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            if int(data.get("request_seq", 0)) != turn:
                continue
            metadata = data.get("metadata") or {}
            return {
                "task_id": task_id,
                "turn": turn,
                "request_seq": data.get("request_seq"),
                "model": data.get("model"),
                "messages": data.get("messages") or [],
                "tools": data.get("tools") or [],
                "budget": metadata.get("context_budget"),
            }
        raise TurnContextNotFound(f"第 {turn} 轮还没有模型请求记录")

    # ---------- 证据与报告 ----------

    def list_evidence(self, task_id: str) -> list[dict[str, Any]]:
        """返回证据目录卡片，不读取证据全文。"""
        record = self._require_task(task_id)
        index_path = self._run_dir(record) / "evidence" / "index.jsonl"
        if not index_path.exists():
            return []

        items: list[dict[str, Any]] = []
        for line in index_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                items.append(item)
        return items

    def read_evidence(
        self,
        task_id: str,
        evidence_id: str,
        *,
        offset: int = 0,
        limit: int = 4000,
    ) -> dict[str, Any]:
        """按证据编号和字符偏移读取原文。

        这里复用 `EvidenceStore.read`，它的编号校验会挡住 `../` 这类路径穿越。
        """
        record = self._require_task(task_id)
        store = EvidenceStore(
            run_dir=self._run_dir(record),
            run_id=record.run_id,
            max_inline_chars=self.settings.max_inline_chars,
        )
        return store.read(evidence_id, offset=offset, limit=limit)

    def get_report(self, task_id: str) -> str:
        """返回最终 Markdown 报告；任务没结束或文件不存在时抛 `ReportNotReady`。"""
        record = self._require_task(task_id)
        if record.status not in TERMINAL_STATUSES:
            raise ReportNotReady("报告尚未生成")
        path = self._run_dir(record) / "report.md"
        if not path.exists():
            raise ReportNotReady("报告文件不存在")
        return path.read_text(encoding="utf-8")

    # ---------- 能力目录与样例 ----------

    def list_models(self) -> dict[str, Any]:
        """返回页面可选的模型来源。

        响应示例（DeepSeek 没有配置 Key 时）：

        ```json
        {
          "default_provider": "mock",
          "providers": [
            {"provider": "mock", "available": true, ...},
            {"provider": "deepseek", "available": false,
             "reason": "缺少 DeepSeek API Key。..."}
          ]
        }
        ```

        `available=false` 时页面应该禁用这个选项，而不是让用户选了以后
        创建任务才收到 422。`reason` 直接展示给用户即可。
        """
        default_provider = normalize_provider(self.settings.model_provider)

        mock_models = available_model_names("mock")
        mock_entry = {
            "provider": "mock",
            "label": "模拟模型",
            "description": "本地剧本驱动，不联网、不需要 Key；用于演示和自动化测试。",
            "default_model": mock_models[0],
            "models": mock_models,
            "available": True,
            "reason": None,
        }

        configured_deepseek = self.settings.model_name
        deepseek_models = available_model_names("deepseek", configured_deepseek)
        try:
            resolve_api_key(self.settings.model_api_key)
        except ModelConfigurationError as exc:
            deepseek_available = False
            deepseek_reason = str(exc)
        else:
            deepseek_available = True
            deepseek_reason = None
        deepseek_entry = {
            "provider": "deepseek",
            "label": "DeepSeek",
            "description": "调用 DeepSeek 的 OpenAI 兼容接口；需要服务端配置 API Key。",
            "default_model": deepseek_models[0],
            "models": deepseek_models,
            "available": deepseek_available,
            "reason": deepseek_reason,
        }

        return {
            "default_provider": default_provider,
            "providers": [mock_entry, deepseek_entry],
        }

    def default_model_info(self) -> dict[str, Any]:
        """返回当前服务端默认模型，供 `/api/health` 展示。"""
        catalog = self.list_models()
        provider = str(catalog["default_provider"])
        entry = next(
            item for item in catalog["providers"] if item["provider"] == provider
        )
        return {
            "provider": provider,
            "name": entry["default_model"],
            "available": entry["available"],
        }

    def capabilities(self) -> dict[str, Any]:
        """返回前端能力目录：所有 Skill、工具声明和可选模型。"""
        skills = [skill.declaration() for skill in list_skills()]
        # 业务工具声明是静态的，不启动 MCP 进程也能展示能力目录。
        # 真正执行时，AgentRunner 会连接 MCP Server，用 `tools/list` 返回的
        # 实时定义覆盖同样的工具名和参数 Schema。
        business_tools = [
            {**declaration, "kind": "business"}
            for declaration in business_tool_declarations()
        ]
        platform_tools = [
            {**declaration, "kind": "platform"}
            for declaration in PLATFORM_TOOL_DECLARATIONS
        ]
        return {
            "skills": skills,
            "tools": [*business_tools, *platform_tools],
            # 前端和排查人员可以据此确认：工具不是本地函数，而是通过 MCP 接入。
            "mcp": {
                "protocol": "JSON-RPC 2.0",
                "transport": self.settings.mcp_transport,
                "endpoint": self.settings.mcp_endpoint(),
                "server": "support-platform-dba-mcp",
                "note": (
                    "业务工具通过真实 MCP Server 调用；数据仍来自 fixtures 模拟数据。"
                ),
            },
            # 页面打开时只调一次 capabilities 也能拿到模型下拉框数据；
            # 单独需要刷新模型可用性时再调 GET /api/models。
            "models": self.list_models(),
        }

    def samples(self) -> list[dict[str, Any]]:
        """返回可选的模拟工单样例。

        多数据库场景的样例除了标题和实例，还带数据库类型、场景标识和 Skill。
        前端据此显示标签，并在创建任务时自动选择正确的 Skill；不依赖前端
        自己从 `sample_id` 猜数据库类型。

        例子（Redis 样例的返回片段）：
            {
              "sample_id": "redis-hot-key-001",
              "database_type": "redis",
              "scenario_label": "Redis 单分片热 Key 导致延迟升高",
              "skill": "dba-triage",
              ...
            }
        """
        return [
            {
                "sample_id": sample_id,
                "title": str(ticket.get("title", "")),
                "description": str(ticket.get("description", "")),
                "symptom": str(ticket.get("symptom", "")),
                "instance_id": str(ticket.get("instance_id", "")),
                "database": str(ticket.get("database", "")),
                "database_type": str(ticket.get("database_type", "mysql")),
                "scenario_id": str(ticket.get("scenario_id", "")),
                "scenario_label": str(ticket.get("scenario_label", "")),
                "scenario_order": _as_int(ticket.get("scenario_order")),
                "skill": str(ticket.get("skill") or DEFAULT_SKILL_NAME),
                "mock": bool(ticket.get("mock", True)),
            }
            for sample_id, _, ticket in self._load_samples()
        ]

    def _load_samples(self) -> list[tuple[str, Path, dict[str, Any]]]:
        """加载样例目录，按 `scenario_order` 排序；没有该字段的旧样例排最前。

        为什么不在文件名前加 01/02：
        文件名就是样例 id（`slow_sql_001` -> `slow-sql-001`），已经被测试和前端
        引用。用工单里的 `scenario_order` 控制展示顺序，比改文件名安全。
        """
        if self._samples is not None:
            return self._samples

        samples: list[tuple[str, Path, dict[str, Any]]] = []
        if not self.settings.samples_dir.exists():
            self._samples = samples
            return samples

        for path in sorted(self.settings.samples_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                logger.exception("读取样例失败：%s", path)
                continue
            if not isinstance(data, dict):
                continue
            sample_id = _safe_sample_id(path.stem)
            if not sample_id:
                continue
            samples.append((sample_id, path, data))

        # `scenario_order` 相同时按样例 id 排，保证接口输出稳定。
        samples.sort(
            key=lambda item: (
                _as_int(item[2].get("scenario_order")),
                item[0],
            )
        )
        self._samples = samples
        return samples

    def _find_sample(self, sample_id: str) -> tuple[str, Path, dict[str, Any]] | None:
        """按样例 id 查找；同时接受横线和下划线两种写法。"""
        normalized = _safe_sample_id(sample_id)
        for item in self._load_samples():
            if item[0] == normalized:
                return item
        return None
