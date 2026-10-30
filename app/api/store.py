"""任务元信息的 SQLite 存储。

## 为什么任务列表要放 SQLite

运行记录（事件、模型输入输出、报告、证据）仍然放文件，因为它们是“大块材料”，
直接用文本工具就能看。任务本身只有十几个字段，但必须满足：

- 服务重启后还能查到历史任务；
- 页面刷新不会丢任务；
- 服务重启时能把上次没跑完的任务标成 `interrupted`，而不是假装还在运行。

SQLite 是 Python 标准库自带的轻量数据库，不需要额外安装服务，适合本地小平台。

## 表结构

```text
tasks
  task_id        任务 id（UUID）
  status         queued / running / completed / failed / cancelled / interrupted
  stop_reason    completed / max_turns / cancelled / error / interrupted
  ticket_json    工单完整 JSON
  skill_name     使用的 Skill
  model_provider 模型来源：mock / deepseek
  model_name     实际模型名，例如 mock-dba / deepseek-flash
  current_turn   当前轮次，运行中由事件回调更新
  turns          已执行轮数
  run_id         对应 var/runs/ 下的目录名，本实现直接复用 task_id
  run_dir        运行目录绝对或相对路径
  evidence_count 证据条数
  capabilities   Skill/工具能力快照 JSON
  error          失败原因
  created_at / updated_at / finished_at
```

## 实现说明

- `sqlite3.connect(path)` 每次方法调用都新开连接。任务量很小，这样最省心；
  连接不是线程安全的，短连接可以避免多线程共享同一个连接。
- `with closing(sqlite3.connect(...)) as conn, conn:`：
  第一个 `with` 负责关闭连接，第二个 `with` 负责提交或回滚事务。
  这种写法把“资源释放”和“事务边界”分开，排查时不容易漏。
- `?` 是参数占位符，值通过元组传入。**不要用字符串拼接 SQL**，否则会有注入风险。
- 列名更新走白名单 `_UPDATABLE_COLUMNS`，调用方传错字段会直接报错，
  不会把任意字段拼进 SQL。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# 允许通过 `update_fields` 修改的列。集中放在这里，既是白名单，也方便对照表结构。
_UPDATABLE_COLUMNS = frozenset(
    {
        "status",
        "stop_reason",
        "current_turn",
        "turns",
        "evidence_count",
        "capabilities_json",
        "error",
        "updated_at",
        "finished_at",
    }
)


def utc_now() -> str:
    """返回带时区的 ISO 时间，例：`2026-10-30T03:00:00.123456+00:00`。"""
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class TaskRecord:
    """从 SQLite 读出来的一行任务。

    `ticket` 和 `capabilities` 在数据库里是 JSON 字符串，读出时已经转回字典，
    上层不用再关心存储格式。
    """

    task_id: str
    status: str
    stop_reason: str | None
    ticket: dict[str, Any]
    skill_name: str
    model_provider: str
    model_name: str | None
    current_turn: int
    turns: int
    run_id: str
    run_dir: str
    evidence_count: int | None
    capabilities: dict[str, Any] | None
    error: str | None
    created_at: str
    updated_at: str
    finished_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> TaskRecord:
        """把 SQLite 行转成 dataclass。

        `json.loads` 只接受字符串/字节；数据库里的 JSON 列一定是字符串，
        但历史数据或手工改库可能写坏，所以这里用 `or "{}"` 兜底。
        """
        # `row.keys()` 返回这一行有哪些列。老版本数据库如果还没执行迁移，
        # 这里退回到 mock，避免查询历史任务时因为少一列直接 500。
        columns = row.keys()
        return cls(
            task_id=str(row["task_id"]),
            status=str(row["status"]),
            stop_reason=row["stop_reason"],
            ticket=json.loads(row["ticket_json"] or "{}"),
            skill_name=str(row["skill_name"]),
            model_provider=(
                str(row["model_provider"] or "mock")
                if "model_provider" in columns
                else "mock"
            ),
            model_name=row["model_name"] if "model_name" in columns else None,
            current_turn=int(row["current_turn"] or 0),
            turns=int(row["turns"] or 0),
            run_id=str(row["run_id"] or row["task_id"]),
            run_dir=str(row["run_dir"] or ""),
            evidence_count=(
                int(row["evidence_count"]) if row["evidence_count"] is not None else None
            ),
            capabilities=(
                json.loads(row["capabilities_json"]) if row["capabilities_json"] else None
            ),
            error=row["error"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            finished_at=row["finished_at"],
        )


class TaskStore:
    """任务表的读写入口。"""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        """打开连接并设置行工厂。

        `row_factory = sqlite3.Row` 让查询结果可以按列名读取（`row["status"]`），
        比按数字下标（`row[1]`）可读得多，也不怕以后调整列顺序。
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        """建表。重复调用是安全的（`IF NOT EXISTS`）。

        WAL（Write-Ahead Logging）让读写可以更好地并行：
        一个任务在写状态时，另一个查询请求仍然可以读。
        本地小平台用不上复杂调优，这一行已经够用。
        """
        with closing(self._connect()) as connection, connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    stop_reason TEXT,
                    ticket_json TEXT NOT NULL,
                    skill_name TEXT NOT NULL,
                    model_provider TEXT NOT NULL DEFAULT 'mock',
                    model_name TEXT,
                    current_turn INTEGER NOT NULL DEFAULT 0,
                    turns INTEGER NOT NULL DEFAULT 0,
                    run_id TEXT NOT NULL,
                    run_dir TEXT NOT NULL,
                    evidence_count INTEGER,
                    capabilities_json TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT
                )
                """
            )
            # 表已经存在时，`CREATE TABLE IF NOT EXISTS` 不会补新列。
            # 这里做一次很小的“向前迁移”：老数据库补上模型字段，
            # 历史任务默认算作 mock，不会因为升级后缺列而查询失败。
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(tasks)")
            }
            if "model_provider" not in columns:
                connection.execute(
                    "ALTER TABLE tasks ADD COLUMN model_provider TEXT NOT NULL DEFAULT 'mock'"
                )
            if "model_name" not in columns:
                connection.execute("ALTER TABLE tasks ADD COLUMN model_name TEXT")

    def create(
        self,
        *,
        task_id: str,
        ticket: dict[str, Any],
        skill_name: str,
        run_dir: str,
        model_provider: str = "mock",
        model_name: str | None = None,
        status: str = "queued",
    ) -> TaskRecord:
        """插入一条新任务，返回刚写入的记录。"""
        now = utc_now()
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT INTO tasks (
                    task_id, status, stop_reason, ticket_json, skill_name,
                    model_provider, model_name, current_turn, turns, run_id,
                    run_dir, evidence_count, capabilities_json, error,
                    created_at, updated_at, finished_at
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, 0, 0, ?, ?, NULL, NULL, NULL, ?, ?, NULL)
                """,
                (
                    task_id,
                    status,
                    json.dumps(ticket, ensure_ascii=False),
                    skill_name,
                    model_provider,
                    model_name,
                    task_id,
                    run_dir,
                    now,
                    now,
                ),
            )
        record = self.get(task_id)
        if record is None:  # 理论上刚插入一定能查到，防御性判断
            raise RuntimeError(f"任务写入后读取失败：{task_id}")
        return record

    def get(self, task_id: str) -> TaskRecord | None:
        """按 id 查一条任务；不存在返回 `None`。"""
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return TaskRecord.from_row(row) if row is not None else None

    def update_fields(self, task_id: str, **fields: Any) -> None:
        """更新指定列，并把 `updated_at` 自动带上。

        例子：任务跑完时调用
        `update_fields(task_id, status="completed", stop_reason="completed", turns=6)`。
        """
        unknown = set(fields) - _UPDATABLE_COLUMNS
        if unknown:
            raise ValueError(f"不允许更新的字段：{sorted(unknown)}")

        fields["updated_at"] = utc_now()
        assignments = ", ".join(f"{name} = ?" for name in fields)
        values = list(fields.values()) + [task_id]
        with closing(self._connect()) as connection, connection:
            connection.execute(
                f"UPDATE tasks SET {assignments} WHERE task_id = ?",
                values,
            )

    def mark_unfinished_interrupted(self, reason: str) -> int:
        """把上次进程留下的 queued/running 任务标成 interrupted。

        为什么必须这样做：
        Agent 在内存里跑，进程退出后不会自己继续。如果数据库里还写着 running，
        前端会一直轮询一个永远不会更新的任务。明确标成中断，符合任务书
        “未完成任务不得假装已经恢复”的要求。

        返回被标记的行数，启动日志可以打印出来。
        """
        now = utc_now()
        with closing(self._connect()) as connection, connection:
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = 'interrupted',
                    stop_reason = 'interrupted',
                    error = COALESCE(error, ?),
                    updated_at = ?,
                    finished_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (reason, now, now),
            )
            return int(cursor.rowcount)
