"""多数据库 Mock 场景的数据质量与路径安全测试。

## 为什么单独测“数据”

十个场景共享同一套 `ScenarioEngine`，代码只写一份，场景差异全在
`fixtures/mock_dba/*.json` 里。这样做的代价是：数据写错时，Python 代码
仍然能正常运行，只是报告会说错话，或者引用一个不存在的事实。

所以这组测试专门检查数据契约：

1. 每张场景工单都有 `scenario_mode=true`、`skill=dba-triage`、`dataset`；
2. 数据集顶层和每个工具结果都标记 `mock=true`；
3. `scenario.steps` 用到的工具，必须和 `tool_results` 的 key 一一对应；
4. 工具名必须在静态能力目录里，否则 MCP Server 注册时会失败；
5. `diagnosis.evidence_keys` 引用的每个事实，必须真的在工具结果的
   `details.topic.facts` 里出现，不能只写一个漂亮但拿不到证据的结论；
6. 数据集路径只能落在 `fixtures/mock_dba/` 内，防止 `../` 读到项目外的文件。

## Python 语法提示

- `Path.glob("*.json")` 只匹配当前目录，不会递归；
- `set(a) == set(b)` 比较“元素是否完全一样”，不关心顺序；
- `unittest.mock.patch(...)` 是上下文管理器，退出时自动恢复原值；
- `with self.subTest(...)` 让一个用例里的多个场景分别报错，不会第一个失败就停。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.adapters.scenario_mock import ScenarioEngine, resolve_ticket_dataset
from app.mcp.dba_tools import static_tool_specs

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TICKETS_DIR = PROJECT_ROOT / "fixtures" / "tickets"
DATASETS_DIR = PROJECT_ROOT / "fixtures" / "mock_dba"

# 九个多数据库场景。慢 SQL 是旧流程，单独检查，不放进这个映射。
SCENARIO_TICKETS = {
    "mysql_row_lock_001.json": (20, "mysql"),
    "pg_vacuum_bloat_001.json": (30, "postgresql"),
    "pg_ddl_lock_001.json": (40, "postgresql"),
    "redis_hot_key_001.json": (50, "redis"),
    "redis_memory_eviction_001.json": (60, "redis"),
    "mongo_collscan_001.json": (70, "mongodb"),
    "mongo_replica_lag_001.json": (80, "mongodb"),
    "es_disk_watermark_001.json": (90, "elasticsearch"),
    "clickhouse_too_many_parts_001.json": (100, "clickhouse"),
}


def _load_json(path: Path) -> dict:
    """读一个 JSON 文件，并确认顶层是对象。

    顶层是列表或字符串时，后面 `.get()` 会报 AttributeError；这里提前给出
    更清楚的错误，排查数据时一眼能看懂。
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise AssertionError(f"{path} 顶层必须是 JSON 对象")
    return data


class ScenarioDataTests(unittest.TestCase):
    """九个场景数据集必须满足同一份契约。"""

    def test_ticket_and_dataset_contract(self) -> None:
        """每个场景工单的 Skill、数据集和步骤都要对得上。"""
        catalog = {tool.name for tool in static_tool_specs()}
        ticket_files = sorted(TICKETS_DIR.glob("*.json"))
        self.assertEqual(len(ticket_files), 10)

        for filename, (order, database_type) in SCENARIO_TICKETS.items():
            with self.subTest(ticket=filename):
                ticket = _load_json(TICKETS_DIR / filename)
                self.assertTrue(ticket["mock"])
                self.assertTrue(ticket["scenario_mode"])
                self.assertEqual(ticket["skill"], "dba-triage")
                self.assertEqual(ticket["database_type"], database_type)
                self.assertEqual(ticket["scenario_order"], order)
                self.assertTrue(ticket["scenario_label"])

                engine = ScenarioEngine.from_ticket(ticket)
                self.assertIsNotNone(engine)
                assert engine is not None  # 类型收窄：下面可以放心访问属性

                step_tools = {
                    str(step["tool"]) for step in engine.steps if step.get("tool")
                }
                result_tools = set(engine.raw["tool_results"])
                self.assertEqual(
                    step_tools,
                    result_tools,
                    "scenario.steps 和 tool_results 的工具集合必须一致",
                )
                self.assertTrue(step_tools <= catalog)

                diagnosis = engine.diagnosis
                self.assertTrue(str(diagnosis.get("root_cause") or "").strip())
                self.assertIn(diagnosis.get("confidence"), {"高", "中", "低"})
                self.assertTrue(diagnosis.get("boundary"))
                self.assertTrue(diagnosis.get("fix", {}).get("primary"))
                self.assertTrue(diagnosis.get("verification"))
                evidence_keys = set(diagnosis.get("evidence_keys") or [])
                self.assertTrue(evidence_keys)

                # 每个证据 key 必须真的由某个工具的 details.topic.facts 产生。
                produced_keys: set[str] = set()
                for tool_name, result in engine.raw["tool_results"].items():
                    self.assertFalse(result.get("is_error"), f"{tool_name} 不应预设失败")
                    payload = result.get("payload")
                    self.assertIsInstance(payload, dict)
                    self.assertTrue(payload.get("mock"), f"{tool_name}.payload 未标记 mock")
                    topic_data = (result.get("details") or {}).get("topic") or {}
                    for fact in topic_data.get("facts") or []:
                        if fact.get("key"):
                            produced_keys.add(str(fact["key"]))
                self.assertTrue(
                    evidence_keys <= produced_keys,
                    f"缺少事实：{evidence_keys - produced_keys}",
                )

    def test_legacy_slow_sql_ticket_stays_on_old_flow(self) -> None:
        """旧慢 SQL 工单不加 `scenario_mode`，继续走原来的六步剧本。"""
        ticket = _load_json(TICKETS_DIR / "slow_sql_001.json")
        self.assertNotIn("scenario_mode", ticket)
        self.assertEqual(ticket["database_type"], "mysql")
        self.assertEqual(ticket["skill"], "slow-sql")
        self.assertEqual(ticket["scenario_order"], 10)
        self.assertIsNone(ScenarioEngine.from_ticket(ticket))


class DatasetResolutionTests(unittest.TestCase):
    """数据集路径解析和 `mock=true` 校验。"""

    def test_non_scenario_ticket_uses_fallback(self) -> None:
        """旧工单没有 `scenario_mode`，应原样使用服务端默认数据集。"""
        self.assertEqual(
            resolve_ticket_dataset({"scenario_mode": False}, fallback="default.json"),
            "default.json",
        )

    def test_rejects_missing_or_escaping_dataset(self) -> None:
        """缺少路径、路径穿越、文件不存在都必须报错。"""
        with self.assertRaises(ValueError):
            resolve_ticket_dataset({"scenario_mode": True})

        with self.assertRaises(ValueError):
            resolve_ticket_dataset(
                {
                    "scenario_mode": True,
                    "dataset": "../tickets/slow_sql_001.json",
                }
            )

        with self.assertRaises(FileNotFoundError):
            resolve_ticket_dataset(
                {
                    "scenario_mode": True,
                    "dataset": "fixtures/mock_dba/not_exists_001.json",
                }
            )

    def test_rejects_non_mock_dataset(self) -> None:
        """即使文件在允许目录内，没有 `mock=true` 也不能进入模拟链路。

        用 `patch` 把允许目录临时换成临时目录，避免往仓库里塞测试垃圾文件。
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake = root / "not_mock.json"
            fake.write_text(
                json.dumps({"mock": False, "tool_results": {}}),
                encoding="utf-8",
            )
            with patch(
                "app.adapters.scenario_mock.SCENARIO_DATASET_ROOT",
                root,
            ):
                with self.assertRaises(ValueError):
                    resolve_ticket_dataset(
                        {
                            "scenario_mode": True,
                            "dataset": str(fake),
                        }
                    )


if __name__ == "__main__":
    unittest.main()
