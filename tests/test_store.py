"""任务表结构测试（SQLite）。

这里重点验证“升级不丢历史任务”：

- 老版本数据库没有 `model_provider` / `model_name` 两列；
- 新版本启动时 `initialize()` 会自动补列；
- 老任务读出来默认是 `mock`，不会因为缺列直接报错。

## 实现说明

- 测试直接手写一份老表结构，再调用新代码的 `initialize()`，
  这比“假装升级”更接近真实场景。
- `TemporaryDirectory()` 会自动清理临时文件，不污染项目的 `var/` 目录。
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.api.store import TaskStore


class TaskStoreMigrationTests(unittest.TestCase):
    def _create_old_database(self, path: Path) -> None:
        """建一张没有模型字段的老表，并插入一条历史任务。"""
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                """
                CREATE TABLE tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    stop_reason TEXT,
                    ticket_json TEXT NOT NULL,
                    skill_name TEXT NOT NULL,
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
            connection.execute(
                """
                INSERT INTO tasks (
                    task_id, status, stop_reason, ticket_json, skill_name,
                    current_turn, turns, run_id, run_dir, evidence_count,
                    capabilities_json, error, created_at, updated_at, finished_at
                ) VALUES (
                    'old-task', 'completed', 'completed', '{"ticket_id":"TCK-OLD"}',
                    'slow-sql', 6, 6, 'old-task', '/tmp/old-task', 4,
                    NULL, NULL, '2026-10-01T00:00:00+00:00',
                    '2026-10-01T00:01:00+00:00', '2026-10-01T00:01:00+00:00'
                )
                """
            )
            connection.commit()
        finally:
            connection.close()

    def test_initialize_adds_model_columns_without_breaking_old_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "tasks.db"
            self._create_old_database(db_path)

            store = TaskStore(db_path)
            store.initialize()

            old = store.get("old-task")
            self.assertIsNotNone(old)
            assert old is not None  # 给类型检查器看的，运行时前面已断言
            self.assertEqual(old.model_provider, "mock")
            self.assertIsNone(old.model_name)

            created = store.create(
                task_id="new-task",
                ticket={"ticket_id": "TCK-NEW"},
                skill_name="slow-sql",
                run_dir=str(Path(tmp) / "new-task"),
                model_provider="deepseek",
                model_name="deepseek-flash",
            )
            self.assertEqual(created.model_provider, "deepseek")
            self.assertEqual(created.model_name, "deepseek-flash")

    def test_create_defaults_to_mock(self) -> None:
        """不传模型字段时，新建任务默认记成 mock，和旧接口保持兼容。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = TaskStore(Path(tmp) / "tasks.db")
            store.initialize()
            record = store.create(
                task_id="default-task",
                ticket={"ticket_id": "TCK-DEFAULT"},
                skill_name="slow-sql",
                run_dir=str(Path(tmp) / "default-task"),
            )
            self.assertEqual(record.model_provider, "mock")
            self.assertIsNone(record.model_name)


if __name__ == "__main__":
    unittest.main()
