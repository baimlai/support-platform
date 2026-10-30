"""第 2 步 Harness 能力的单元测试。

覆盖任务书第 2 步的验收点：
- Skill 能加载、声明初始工具、拒绝非法名称；
- 关键词工具搜索能命中工具，并按需加载到 ToolSession；
- 证据能完整落盘、按 offset/limit 分页读取、挡住路径穿越；
- 大工具结果进入上下文时被换成“预览 + 证据编号”；
- Topic 根据工具结果更新事实、候选原因和未决问题，并持久化；
- 有界上下文按块裁剪消息，工具调用和工具结果不会只留一半；
- Topic 被压缩时仍保留结构化事实和 `attempted_tools`。

说明：
- `unittest` 是标准库自带的测试框架，不用额外装依赖；
- 继承 `IsolatedAsyncioTestCase` 后，`async def test_xxx` 会被自动放进独立事件循环执行；
- 每个测试用 `self.enterContext(tempfile.TemporaryDirectory())` 建临时目录，
  测试结束时自动删除，不会在仓库里留下证据文件。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from app.harness.context import (
    BEGIN_TOPIC,
    ContextBudget,
    build_model_context,
    build_user_message,
)
from app.harness.evidence import EvidenceNotFound, EvidenceProcessor, EvidenceStore
from app.harness.skills import SkillNotFound, list_skills, load_skill
from app.harness.tool_search import ToolSession, search_tool_specs
from app.harness.topic import TopicManager
from app.mcp.dba_tools import static_tool_specs
from app.runtime.contracts import (
    AfterToolCallContext,
    AssistantMessage,
    SystemMessage,
    TextContent,
    ToolCall,
    ToolExecutionEnd,
    ToolResult,
    ToolResultMessage,
    ToolResultMeta,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TICKET_PATH = PROJECT_ROOT / "fixtures" / "tickets" / "slow_sql_001.json"


def load_ticket() -> dict[str, Any]:
    """读取测试用模拟工单。"""
    return json.loads(TICKET_PATH.read_text(encoding="utf-8"))


class EvidenceStoreTests(unittest.TestCase):
    """证据存储：落盘、索引、分页和非法编号。"""

    def test_save_read_and_restore_index(self) -> None:
        """保存后能按编号读取；重新打开同一个目录时，编号要继续递增。"""
        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        run_dir = Path(temp_dir)
        store = EvidenceStore(
            run_dir=run_dir,
            run_id="run-1",
            max_inline_chars=1200,
        )

        meta = store.save(
            tool_call_id="call-1",
            tool_name="slow_log_query",
            content="x" * 1500,
            details={"top": {"sql_fingerprint": "a1b2c3d4e5f6"}},
            is_error=False,
            duration_ms=12,
        )
        self.assertEqual(meta.evidence_id, "ev-000001")
        self.assertTrue(meta.truncated)
        self.assertEqual(meta.size_chars, 1500)

        chunk = store.read("ev-000001", offset=1000, limit=300)
        self.assertEqual(chunk["content"], "x" * 300)
        self.assertEqual(chunk["offset"], 1000)
        self.assertEqual(chunk["total_chars"], 1500)
        self.assertTrue(chunk["truncated"])
        self.assertEqual(store.describe("ev-000001")["tool_name"], "slow_log_query")
        self.assertEqual(store.count, 1)

        # 模拟“进程重启后重新打开同一个运行目录”。
        reopened = EvidenceStore(
            run_dir=run_dir,
            run_id="run-1",
            max_inline_chars=1200,
        )
        self.assertEqual(reopened.count, 1)
        second = reopened.save(
            tool_call_id="call-2",
            tool_name="execution_plan",
            content="ok",
            details={},
            is_error=False,
        )
        self.assertEqual(second.evidence_id, "ev-000002")
        self.assertFalse(second.truncated)
        self.assertEqual(reopened.read("ev-000002")["content"], "ok")

    def test_invalid_evidence_id_is_rejected(self) -> None:
        """带路径分隔符或格式不对的编号必须直接拒绝，不能拼成文件路径。"""
        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        store = EvidenceStore(run_dir=Path(temp_dir), run_id="run-1")
        for bad_id in ("../secret", "ev-1", "ev-00001", "ev-000001/../../x"):
            with self.subTest(evidence_id=bad_id):
                with self.assertRaises(EvidenceNotFound):
                    store.read(bad_id)


class EvidenceProcessorTests(unittest.IsolatedAsyncioTestCase):
    """证据处理器：大结果换成预览，小结果原样保留。"""

    async def test_large_result_becomes_bounded_envelope(self) -> None:
        """原始结果超过内联上限时，模型只看到预览、摘要和 evidence_id。"""
        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        store = EvidenceStore(
            run_dir=Path(temp_dir),
            run_id="run-1",
            max_inline_chars=1200,
        )
        processor = EvidenceProcessor(store=store)
        raw_text = json.dumps(
            {
                "rows": [
                    {
                        "sql_fingerprint": "a1b2c3d4e5f6",
                        "sample_sql": "select * from orders",
                        "padding": "x" * 1600,
                    }
                ]
            },
            ensure_ascii=False,
        )
        details = {"top": {"sql_fingerprint": "a1b2c3d4e5f6"}}
        context = AfterToolCallContext(
            assistant_message=AssistantMessage(content=[]),
            tool_call=ToolCall(id="call-1", name="slow_log_query", arguments={}),
            arguments={},
            result=ToolResult(
                content=[TextContent(text=raw_text)],
                details=details,
                is_error=False,
                duration_ms=5,
            ),
            is_error=False,
            messages=[],
        )

        processed = await processor.after_tool_call(context)
        self.assertIsNotNone(processed)
        self.assertIsNotNone(processed.meta)
        self.assertEqual(processed.meta.evidence_id, "ev-000001")
        self.assertTrue(processed.meta.truncated)
        self.assertEqual(processed.meta.raw_size_chars, len(raw_text))

        visible_text = processed.content[0].text
        self.assertLessEqual(len(visible_text), 1200)
        envelope = json.loads(visible_text)
        self.assertTrue(envelope["truncated"])
        self.assertEqual(envelope["evidence_id"], "ev-000001")
        self.assertEqual(envelope["summary"], details)

        # 原始结果仍然在证据文件里，没有被改写。
        stored = store.read("ev-000001")
        self.assertEqual(stored["content"], raw_text)

    async def test_small_result_and_platform_tool_are_not_changed(self) -> None:
        """短结果原样返回；平台工具跳过证据保存，避免 evidence_read 套娃。"""
        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        store = EvidenceStore(run_dir=Path(temp_dir), run_id="run-1")
        processor = EvidenceProcessor(
            store=store,
            skip_tool_names={"evidence_read"},
        )

        small_context = AfterToolCallContext(
            assistant_message=AssistantMessage(content=[]),
            tool_call=ToolCall(id="call-1", name="lock_wait_query", arguments={}),
            arguments={},
            result=ToolResult(
                content=[TextContent(text="ok")],
                details={"status": "no_data"},
                is_error=False,
            ),
            is_error=False,
            messages=[],
        )
        small = await processor.after_tool_call(small_context)
        self.assertIsNotNone(small)
        self.assertEqual(small.content[0].text, "ok")
        self.assertEqual(small.meta.evidence_id, "ev-000001")

        platform_context = AfterToolCallContext(
            assistant_message=AssistantMessage(content=[]),
            tool_call=ToolCall(id="call-2", name="evidence_read", arguments={}),
            arguments={},
            result=ToolResult(content=[TextContent(text="page")]),
            is_error=False,
            messages=[],
        )
        skipped = await processor.after_tool_call(platform_context)
        self.assertIsNone(skipped)
        self.assertEqual(store.count, 1)


class SkillTests(unittest.TestCase):
    """Skill 加载与声明。"""

    def test_load_default_skill(self) -> None:
        """默认 Skill 应包含调查正文和初始工具。"""
        skill = load_skill("slow-sql")
        self.assertEqual(skill.name, "slow-sql")
        self.assertEqual(skill.initial_tools, ["slow_log_query"])
        self.assertIn("慢 SQL", skill.content)
        self.assertIn("evidence_read", skill.content)

        declaration = skill.declaration()
        self.assertEqual(declaration["name"], "slow-sql")
        self.assertIn("content", declaration)
        self.assertIn("slow_log_query", declaration["initial_tools"])

        names = [item.name for item in list_skills()]
        self.assertIn("slow-sql", names)

    def test_invalid_skill_name_is_rejected(self) -> None:
        """Skill 名称不能带路径分隔符，避免读到 skills 目录之外。"""
        with self.assertRaises(SkillNotFound):
            load_skill("../secret")


class ToolSearchTests(unittest.TestCase):
    """关键词搜索与按需加载。"""

    def test_search_matches_business_tools(self) -> None:
        """中文关键词应命中对应的英文工具名。

        工具目录已经扩展到多数据库场景（MySQL、PostgreSQL、Redis、MongoDB、
        Elasticsearch、ClickHouse），同一次搜索会命中更多工具。这里只断言
        慢 SQL 的四个老工具仍在结果里，并单独用下面的用例验证场景工具。
        """
        tools = static_tool_specs()
        matches = search_tool_specs(tools, "执行计划 实例监控 锁等待", limit=5)
        names = [tool.name for tool, _score in matches]
        self.assertTrue(
            {"execution_plan", "instance_metrics", "lock_wait_query"} <= set(names)
        )
        self.assertTrue(all(score > 0 for _tool, score in matches))

    def test_search_matches_scenario_tools(self) -> None:
        """多数据库场景的中文关键词应命中各自的工具。

        例子：搜“MongoDB 执行计划”时，`mongo_explain` 必须排进前 8 名；
        否则 Agent 会先搜一次工具名，虽然也能恢复，但多浪费一轮。
        """
        tools = static_tool_specs()
        cases = {
            "MongoDB 执行计划": "mongo_explain",
            "Redis 内存 淘汰": "redis_info",
            "Elasticsearch 分片 水位": "es_shard_allocation",
            "ClickHouse Too many parts": "clickhouse_query_log",
            "PostgreSQL 表膨胀": "pg_table_stats",
        }
        for query, expected in cases.items():
            names = [
                tool.name for tool, _score in search_tool_specs(tools, query, limit=8)
            ]
            self.assertIn(expected, names, f"查询 {query!r} 没有命中 {expected}")

    def test_session_loads_tools_on_demand(self) -> None:
        """初始只能看到 Skill 声明的工具，搜索后才加载其它工具。"""
        tools = static_tool_specs()
        session = ToolSession.create(tools, ["slow_log_query"])
        self.assertEqual(session.visible_names(), ["slow_log_query"])

        payload = session.search("执行计划 实例监控 锁等待")
        loaded = set(payload["loaded"])
        self.assertTrue(
            {"execution_plan", "instance_metrics", "lock_wait_query"} <= loaded
        )
        self.assertEqual(
            set(session.business_visible_names()),
            {"slow_log_query", *loaded},
        )
        self.assertEqual(len(session.searches), 1)

    def test_invalid_initial_tools_are_rejected(self) -> None:
        """Skill 声明不存在的工具，或工具目录里重名，都应当直接报错。"""
        tools = static_tool_specs()
        with self.assertRaises(ValueError):
            ToolSession.create(tools, ["missing_tool"])
        with self.assertRaises(ValueError):
            ToolSession.create([tools[0], tools[0]], [tools[0].name])


class TopicTests(unittest.TestCase):
    """Topic 根据工具结果更新并持久化。"""

    def test_topic_tracks_facts_failures_and_platform_calls(self) -> None:
        """成功、失败、no_data 和平台工具活动要分别落到 Topic 的不同位置。"""
        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        run_dir = Path(temp_dir)
        topic_path = run_dir / "topic.json"
        manager = TopicManager(load_ticket(), path=topic_path)

        slow_event = ToolExecutionEnd(
            tool_call_id="call-1",
            tool_name="slow_log_query",
            result=ToolResult(
                content=[TextContent(text='{"mock": true}')],
                details={
                    "coverage_ratio": 0.92,
                    "top": {
                        "sql_fingerprint": "a1b2c3d4e5f6",
                        "count": 1240,
                        "avg_latency_ms": 2380,
                        "max_latency_ms": 8120,
                        "avg_rows_examined": 185000,
                        "avg_rows_sent": 20,
                    },
                },
                meta=ToolResultMeta(
                    evidence_id="ev-000001",
                    truncated=True,
                    raw_size_chars=16658,
                    max_inline_chars=4000,
                ),
            ),
            is_error=False,
            turn=1,
        )
        update = manager.update_from_event(slow_event)
        self.assertIsNotNone(update)
        self.assertEqual(update.turn, 1)

        topic = manager.topic_dict()
        facts = {fact["key"]: fact for fact in topic["facts"]}
        self.assertIn("slow_log.top", facts)
        self.assertIn("slow_log.coverage", facts)
        self.assertEqual(facts["slow_log.top"]["data"]["sql_fingerprint"], "a1b2c3d4e5f6")
        self.assertTrue(facts["slow_log.top"]["data"]["truncated"])
        self.assertEqual(topic["attempted_tools"], ["slow_log_query"])

        # 同一个结果重复上报时，内容没有变化，不应该再发 topic_updated。
        self.assertIsNone(manager.update_from_event(slow_event))

        metrics_event = ToolExecutionEnd(
            tool_call_id="call-2",
            tool_name="instance_metrics",
            result=ToolResult(
                content=[TextContent(text="实例监控接口超时（模拟）")],
                is_error=True,
                meta=ToolResultMeta(evidence_id="ev-000002"),
            ),
            is_error=True,
            turn=2,
        )
        self.assertIsNotNone(manager.update_from_event(metrics_event))
        topic = manager.topic_dict()
        facts = {fact["key"]: fact for fact in topic["facts"]}
        self.assertNotIn("instance_metrics.available", facts)
        self.assertTrue(any("instance_metrics" in item for item in topic["open_questions"]))
        candidates = {item["key"]: item for item in topic["candidates"]}
        self.assertEqual(candidates["instance_resource"]["status"], "unverified")

        lock_event = ToolExecutionEnd(
            tool_call_id="call-3",
            tool_name="lock_wait_query",
            result=ToolResult(
                content=[TextContent(text='{"status": "no_data"}')],
                details={"status": "no_data", "sample_count": 0},
                meta=ToolResultMeta(evidence_id="ev-000003"),
            ),
            is_error=False,
            turn=3,
        )
        self.assertIsNotNone(manager.update_from_event(lock_event))
        topic = manager.topic_dict()
        facts = {fact["key"]: fact for fact in topic["facts"]}
        self.assertEqual(facts["lock_wait.samples"]["data"]["status"], "no_data")
        candidates = {item["key"]: item for item in topic["candidates"]}
        self.assertEqual(candidates["lock_wait"]["status"], "unverified")

        read_event = ToolExecutionEnd(
            tool_call_id="call-4",
            tool_name="evidence_read",
            result=ToolResult(
                content=[TextContent(text='{"evidence_id": "ev-000001"}')],
                details={
                    "evidence_id": "ev-000001",
                    "offset": 4000,
                    "limit": 800,
                    "total_chars": 16658,
                    "truncated": True,
                },
            ),
            is_error=False,
            turn=4,
        )
        self.assertIsNotNone(manager.update_from_event(read_event))
        topic = manager.topic_dict()
        self.assertEqual(topic["evidence_reads"][0]["evidence_id"], "ev-000001")
        self.assertEqual(topic["evidence_reads"][0]["offset"], 4000)

        search_event = ToolExecutionEnd(
            tool_call_id="call-5",
            tool_name="search_tools",
            result=ToolResult(
                content=[TextContent(text='{"loaded": ["execution_plan"]}')],
                details={
                    "query": "执行计划",
                    "match_count": 1,
                    "loaded": ["execution_plan"],
                },
            ),
            is_error=False,
            turn=5,
        )
        self.assertIsNotNone(manager.update_from_event(search_event))
        topic = manager.topic_dict()
        self.assertEqual(topic["searches"][0]["loaded"], ["execution_plan"])
        self.assertEqual(
            topic["attempted_tools"],
            [
                "slow_log_query",
                "instance_metrics",
                "lock_wait_query",
                "evidence_read",
                "search_tools",
            ],
        )

        # topic.json 每一步都落盘，内容和内存中的 Topic 一致。
        self.assertTrue(topic_path.exists())
        persisted = json.loads(topic_path.read_text(encoding="utf-8"))
        self.assertEqual(persisted["facts"], topic["facts"])
        self.assertEqual(persisted["attempted_tools"], topic["attempted_tools"])


class ContextBudgetTests(unittest.TestCase):
    """有界上下文：固定头保留，最近消息按块裁剪。"""

    def _build_history(self, ticket: dict[str, Any]) -> list[Any]:
        """构造系统消息、工单消息和 6 个“助手 + 工具结果”块。"""
        messages: list[Any] = [
            SystemMessage(content="系统规则"),
            build_user_message(ticket),
        ]
        for index in range(6):
            call = ToolCall(
                id=f"call-{index}",
                name="slow_log_query",
                arguments={"limit": 1},
            )
            messages.append(
                AssistantMessage(
                    content=[
                        TextContent(text=f"第 {index} 轮说明"),
                        call,
                    ]
                )
            )
            messages.append(
                ToolResultMessage(
                    tool_call_id=call.id,
                    tool_name="slow_log_query",
                    content=[TextContent(text="x" * 500)],
                    meta=ToolResultMeta(evidence_id=f"ev-{index:06d}"),
                )
            )
        return messages

    def test_drops_old_blocks_but_keeps_pairs(self) -> None:
        """最多保留 4 条最近消息时，只留下最新 2 个完整块。"""
        ticket = load_ticket()
        manager = TopicManager(ticket)
        messages = self._build_history(ticket)
        budget = ContextBudget(
            max_chars=5000,
            max_recent_messages=4,
            max_topic_chars=2000,
            max_tool_result_chars=200,
            max_assistant_text_chars=200,
        )

        included, info = build_model_context(
            messages=messages,
            ticket=ticket,
            topic=manager.topic_dict(),
            budget=budget,
        )

        self.assertIsInstance(included[0], SystemMessage)
        self.assertTrue(
            any(
                isinstance(message, SystemMessage) and BEGIN_TOPIC in message.content
                for message in included
            )
        )
        self.assertEqual(info["included_recent_messages"], 4)
        self.assertEqual(info["dropped_messages"], 8)

        call_ids = {
            block.id
            for message in included
            if isinstance(message, AssistantMessage)
            for block in message.content
            if isinstance(block, ToolCall)
        }
        result_ids = {
            message.tool_call_id
            for message in included
            if isinstance(message, ToolResultMessage)
        }
        self.assertEqual(call_ids, result_ids)

        # 500 字的工具结果被压到 200 字左右，并带上证据编号提示。
        tool_messages = [
            message for message in included if isinstance(message, ToolResultMessage)
        ]
        self.assertEqual(len(tool_messages), 2)
        self.assertTrue(all(len(message.content[0].text) < 320 for message in tool_messages))
        self.assertTrue(all("证据" in message.content[0].text for message in tool_messages))

    def test_compressed_topic_keeps_structured_facts(self) -> None:
        """Topic 超预算被压缩时，仍要保留事实 key、关键数据和 attempted_tools。"""
        ticket = load_ticket()
        topic = {
            "question": "订单查询变慢",
            "scope": {"ticket_id": ticket["ticket_id"]},
            "facts": [
                {
                    "key": "slow_log.top",
                    "statement": "很长的说明" * 200,
                    "evidence_id": "ev-000001",
                    "source_tool": "slow_log_query",
                    "observed": True,
                    "data": {"sql_fingerprint": "a1b2c3d4e5f6"},
                }
            ],
            "candidates": [
                {
                    "key": "access_path",
                    "statement": "可能是索引问题" * 50,
                    "status": "candidate",
                    "evidence": ["ev-000001"],
                }
            ],
            "open_questions": [],
            "next_steps": ["查执行计划"],
            "attempted_tools": ["slow_log_query"],
            "evidence_reads": [],
            "searches": [],
        }
        budget = ContextBudget(
            max_chars=10000,
            max_recent_messages=10,
            max_topic_chars=1200,
        )

        included, _info = build_model_context(
            messages=[SystemMessage(content="系统规则"), build_user_message(ticket)],
            ticket=ticket,
            topic=topic,
            budget=budget,
        )
        topic_message = next(
            message
            for message in included
            if isinstance(message, SystemMessage) and BEGIN_TOPIC in message.content
        )
        self.assertIn('"truncated": true', topic_message.content)
        self.assertIn('"key": "slow_log.top"', topic_message.content)
        self.assertIn('"sql_fingerprint": "a1b2c3d4e5f6"', topic_message.content)
        self.assertIn('"attempted_tools"', topic_message.content)
        self.assertIn("slow_log_query", topic_message.content)


if __name__ == "__main__":
    # 允许直接运行这个文件：python tests/test_harness.py
    # 平时更常用的是 `python -m unittest discover -s tests -v`，一次跑完整个 tests 目录。
    unittest.main()
