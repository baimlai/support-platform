"""HTTP 接口测试（第 3 步）。

测试直接使用 FastAPI 的 `TestClient`。`with TestClient(app)` 会真正执行
lifespan（启动建表、停止清理），比手工调用函数更接近实际服务。

## 为什么后台任务能跑完

`TestClient` 内部有一个常驻事件循环。`POST /api/tasks` 返回后，后台
`asyncio.create_task` 仍在这个循环里继续执行；测试通过轮询 `GET /api/tasks/{id}`
等它进入终态。轮询上限 5 秒，避免测试本身死等。

## 测试覆盖

- 样例、能力目录；
- 样例任务完整流程、事件分页、轮次上下文、证据列表/分页、报告；
- 自由文本工单；
- 取消（协作式）和重复取消（幂等）；
- 报告未生成、未知任务/样例/证据、非法请求；
- SSE 流；
- 服务重启后未完成任务标记为 interrupted。
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.api.main import create_app
from app.api.settings import ApiSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TERMINAL = {"completed", "failed", "cancelled", "interrupted"}


class ApiTestBase(unittest.TestCase):
    """公共初始化：临时目录、应用、TestClient。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.settings = ApiSettings(
            run_root=self.tmp / "runs",
            db_path=self.tmp / "tasks.db",
            samples_dir=PROJECT_ROOT / "fixtures" / "tickets",
            model_delay_s=0.0,
        )
        self._client_context = TestClient(create_app(self.settings))
        self.client = self._client_context.__enter__()

    def tearDown(self) -> None:
        self._client_context.__exit__(None, None, None)
        self._tmp.cleanup()

    def wait_terminal(self, task_id: str, timeout_s: float = 5.0) -> dict[str, Any]:
        """轮询任务，直到进入终态；超时就失败。

        例子：任务正常完成时，这个函数最多每 20ms 查一次，通常 0.1 秒内返回。
        """
        deadline = time.monotonic() + timeout_s
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            response = self.client.get(f"/api/tasks/{task_id}")
            self.assertEqual(response.status_code, 200)
            last = response.json()
            if last["status"] in TERMINAL:
                return last
            time.sleep(0.02)
        self.fail(f"任务 {task_id} 在 {timeout_s} 秒内没有结束，最后状态：{last}")

    def all_events(self, task_id: str) -> list[dict[str, Any]]:
        """按分页把所有事件读出来。"""
        events: list[dict[str, Any]] = []
        after_seq = 0
        while True:
            response = self.client.get(
                f"/api/tasks/{task_id}/events",
                params={"after_seq": after_seq, "limit": 1000},
            )
            self.assertEqual(response.status_code, 200)
            page = response.json()
            events.extend(page["events"])
            if not page["has_more"]:
                return events
            after_seq = page["next_seq"]


class ApiFlowTests(ApiTestBase):
    def test_samples_and_capabilities(self) -> None:
        """样例目录和能力目录能被前端直接使用。"""
        samples = self.client.get("/api/samples")
        self.assertEqual(samples.status_code, 200)
        sample_list = samples.json()
        self.assertEqual(sample_list[0]["sample_id"], "slow-sql-001")
        self.assertTrue(sample_list[0]["mock"])

        capabilities = self.client.get("/api/capabilities")
        self.assertEqual(capabilities.status_code, 200)
        data = capabilities.json()
        self.assertEqual([skill["name"] for skill in data["skills"]], ["dba-triage", "slow-sql"])
        tool_names = {tool["name"] for tool in data["tools"]}
        self.assertTrue({"slow_log_query", "search_tools", "evidence_read"} <= tool_names)
        platform_tools = [tool for tool in data["tools"] if tool["kind"] == "platform"]
        self.assertEqual({tool["name"] for tool in platform_tools}, {"search_tools", "evidence_read"})

    def test_sample_task_full_flow(self) -> None:
        """一次完整调查：任务、事件、上下文、证据、报告都能读到。"""
        created = self.client.post("/api/tasks", json={"sample_id": "slow-sql-001"})
        self.assertEqual(created.status_code, 202)
        initial = created.json()
        self.assertEqual(initial["status"], "queued")
        self.assertIsNone(initial["report"])
        task_id = initial["task_id"]

        final = self.wait_terminal(task_id)
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["stop_reason"], "completed")
        self.assertEqual(final["turns"], 6)
        self.assertEqual(final["evidence_count"], 4)
        self.assertEqual(final["capabilities"]["skill"]["name"], "slow-sql")
        self.assertIn("## 结论", final["report"])
        self.assertIn("**置信度：** 中", final["report"])
        self.assertIn("## 修复建议", final["report"])
        self.assertIn("## 证据链", final["report"])
        self.assertIn("## 已排除与待验证", final["report"])
        self.assertNotIn("## 候选原因", final["report"])
        self.assertIn("ev-000001", final["report"])
        self.assertEqual(
            [fact["key"] for fact in final["topic"]["facts"]],
            [
                "slow_log.top",
                "slow_log.coverage",
                "execution_plan.access",
                "lock_wait.samples",
            ],
        )
        self.assertEqual(
            [candidate["status"] for candidate in final["topic"]["candidates"]],
            ["supported", "unverified", "unverified"],
        )

        # 事件分页：第一页按 limit 截断，next_seq 指向最后一条。
        first_page = self.client.get(
            f"/api/tasks/{task_id}/events",
            params={"after_seq": 0, "limit": 5},
        ).json()
        self.assertEqual(len(first_page["events"]), 5)
        self.assertTrue(first_page["has_more"])
        self.assertEqual(first_page["next_seq"], 5)

        events = self.all_events(task_id)
        self.assertEqual(events[0]["type"], "agent_start")
        self.assertEqual(events[-1]["type"], "agent_end")
        topic_events = [event for event in events if event["type"] == "topic_updated"]
        self.assertEqual(len(topic_events), 6)

        slow_log_end = next(
            event
            for event in events
            if event["type"] == "tool_execution_end"
            and event["data"]["tool_name"] == "slow_log_query"
        )
        self.assertEqual(slow_log_end["data"]["evidence_id"], "ev-000001")
        self.assertTrue(slow_log_end["data"]["result"]["meta"]["truncated"])

        # 轮次上下文记录的是真正发给模型的消息，不是完整历史。
        context = self.client.get(f"/api/tasks/{task_id}/turns/1/context")
        self.assertEqual(context.status_code, 200)
        context_data = context.json()
        self.assertEqual(context_data["turn"], 1)
        self.assertIsNotNone(context_data["budget"])
        self.assertIn("used_chars", context_data["budget"])
        self.assertIn("dropped_messages", context_data["budget"])
        self.assertIn("slow_log_query", {tool["name"] for tool in context_data["tools"]})
        self.assertIn("search_tools", {tool["name"] for tool in context_data["tools"]})

        # 证据列表与分页读取。
        evidence = self.client.get(f"/api/tasks/{task_id}/evidence").json()
        self.assertEqual(len(evidence), 4)
        self.assertEqual(evidence[0]["evidence_id"], "ev-000001")
        self.assertEqual(evidence[0]["size_chars"], 16658)
        self.assertFalse(evidence[0]["is_error"])
        self.assertTrue(evidence[2]["is_error"])

        chunk = self.client.get(
            f"/api/tasks/{task_id}/evidence/ev-000001",
            params={"offset": 4000, "limit": 800},
        )
        self.assertEqual(chunk.status_code, 200)
        chunk_data = chunk.json()
        self.assertEqual(chunk_data["total_chars"], 16658)
        self.assertEqual(chunk_data["offset"], 4000)
        self.assertEqual(chunk_data["limit"], 800)
        self.assertTrue(chunk_data["truncated"])
        self.assertTrue(chunk_data["content"])

        # 非法证据编号被拒绝，不能靠猜测编号读到证据目录外的文件。
        bad = self.client.get(f"/api/tasks/{task_id}/evidence/ev-1")
        self.assertEqual(bad.status_code, 404)

        report = self.client.get(f"/api/tasks/{task_id}/report")
        self.assertEqual(report.status_code, 200)
        self.assertIsNone(report.json()["structured"])
        self.assertIn("## 当前限制", report.json()["markdown"])

    def test_free_text_ticket(self) -> None:
        """自由文本工单沿用样例的实例和时间范围，但保留用户原文。"""
        text = "订单查询从今天 10 点开始明显变慢，部分请求超时。"
        created = self.client.post(
            "/api/tasks",
            json={"ticket_text": text, "tenant_id": "tenant-x"},
        )
        self.assertEqual(created.status_code, 202)
        task_id = created.json()["task_id"]
        final = self.wait_terminal(task_id)
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["ticket"]["description"], text)
        self.assertEqual(final["ticket"]["tenant_id"], "tenant-x")
        self.assertEqual(final["ticket"]["instance_id"], "db-mysql-001")
        self.assertTrue(final["ticket"]["mock"])
        self.assertEqual(final["ticket"]["source"], "user_text")

    def test_model_catalog_defaults_and_selection(self) -> None:
        """模型目录、健康检查和任务快照要能对上，默认继续走 mock。"""
        catalog_response = self.client.get("/api/models")
        self.assertEqual(catalog_response.status_code, 200)
        catalog = catalog_response.json()
        self.assertEqual(catalog["default_provider"], "mock")

        providers = {item["provider"]: item for item in catalog["providers"]}
        self.assertTrue(providers["mock"]["available"])
        self.assertEqual(providers["mock"]["default_model"], "mock-dba")
        self.assertIn("deepseek", providers)
        self.assertIn("deepseek-flash", providers["deepseek"]["models"])
        # 本机可能配置了 Key，也可能没有；只断言字段形状，避免测试依赖开发机环境。
        self.assertIsInstance(providers["deepseek"]["available"], bool)
        if not providers["deepseek"]["available"]:
            self.assertTrue(providers["deepseek"]["reason"])

        # 服务端显式配置 Key 时，同一接口应把 deepseek 标成可用。
        service = self.client.app.state.service
        service.settings.model_api_key = "test-key"
        available = self.client.get("/api/models").json()
        deepseek = next(
            item for item in available["providers"] if item["provider"] == "deepseek"
        )
        self.assertTrue(deepseek["available"])
        self.assertIsNone(deepseek["reason"])

        # capabilities 里带同一份目录，页面打开一次就能渲染模型下拉框。
        capabilities = self.client.get("/api/capabilities").json()
        self.assertEqual(capabilities["models"]["default_provider"], "mock")

        # health 展示服务端默认模型，不代表某个具体任务的选择。
        health = self.client.get("/api/health").json()
        self.assertEqual(health["model"]["provider"], "mock")
        self.assertEqual(health["model"]["name"], "mock-dba")

        # 不传 model_provider/model_name：使用服务端默认 mock。
        created = self.client.post("/api/tasks", json={"sample_id": "slow-sql-001"})
        self.assertEqual(created.status_code, 202)
        initial = created.json()
        self.assertEqual(initial["model_provider"], "mock")
        self.assertEqual(initial["model_name"], "mock-dba")

        final = self.wait_terminal(initial["task_id"])
        self.assertEqual(final["model_provider"], "mock")
        self.assertEqual(final["model_name"], "mock-dba")

    def test_deepseek_selection_is_persisted_without_network(self) -> None:
        """显式选 DeepSeek 时，provider/model 要一路传到后台执行参数。

        这里把 `_run_task` 换成假函数，只验证参数传递，不真的调用 DeepSeek。
        真实接口的行为已经在 `test_deepseek_model.py` 里用 MockTransport 覆盖。
        """
        service = self.client.app.state.service
        service.settings.model_api_key = "test-key"
        captured: dict[str, Any] = {}

        async def fake_run(**kwargs: Any) -> None:
            captured.update(kwargs)

        with patch.object(service, "_run_task", new=fake_run):
            created = self.client.post(
                "/api/tasks",
                json={
                    "sample_id": "slow-sql-001",
                    # 前后空格和大小写都应该被规范化。
                    "model_provider": " DeepSeek ",
                    "model_name": "deepseek-v4-pro",
                },
            )

        self.assertEqual(created.status_code, 202)
        snapshot = created.json()
        self.assertEqual(snapshot["model_provider"], "deepseek")
        self.assertEqual(snapshot["model_name"], "deepseek-v4-pro")

        # 后台协程和 HTTP 请求在不同任务里执行，给它一点时间记录参数。
        deadline = time.monotonic() + 2.0
        while "model_provider" not in captured and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(captured.get("model_provider"), "deepseek")
        self.assertEqual(captured.get("model_name"), "deepseek-v4-pro")

    def test_cancel_is_cooperative_and_idempotent(self) -> None:
        """取消先返回 202，随后任务进入 cancelled；再次取消返回 200。"""
        service = self.client.app.state.service
        service.settings.model_delay_s = 0.05

        created = self.client.post("/api/tasks", json={"sample_id": "slow-sql-001"})
        task_id = created.json()["task_id"]
        cancelled = self.client.post(f"/api/tasks/{task_id}/cancel")
        self.assertEqual(cancelled.status_code, 202)
        self.assertIn(cancelled.json()["status"], {"queued", "running"})

        final = self.wait_terminal(task_id)
        self.assertEqual(final["status"], "cancelled")
        self.assertEqual(final["stop_reason"], "cancelled")
        self.assertIn("调查未完成", final["report"])

        again = self.client.post(f"/api/tasks/{task_id}/cancel")
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.json()["status"], "cancelled")

    def test_sse_stream_ends_after_terminal(self) -> None:
        """SSE 会补历史事件，并在任务终态后发送 stream_end 关闭。"""
        created = self.client.post("/api/tasks", json={"sample_id": "slow-sql-001"})
        task_id = created.json()["task_id"]
        self.wait_terminal(task_id)

        with self.client.stream(
            "GET",
            f"/api/tasks/{task_id}/events/stream",
            params={"after_seq": 0},
        ) as response:
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
            body = "".join(response.iter_text())

        self.assertIn("event: agent_start", body)
        self.assertIn("event: topic_updated", body)
        self.assertIn("event: agent_end", body)
        self.assertIn("event: stream_end", body)
        self.assertIn('"status": "completed"', body)


class ApiErrorTests(ApiTestBase):
    def test_unknown_ids_and_bad_requests(self) -> None:
        """未知任务/证据/轮次返回 404，请求体冲突返回 422。"""
        self.assertEqual(self.client.get("/api/tasks/nope").status_code, 404)
        self.assertEqual(self.client.post("/api/tasks/nope/cancel").status_code, 404)
        self.assertEqual(self.client.get("/api/tasks/nope/events").status_code, 404)
        self.assertEqual(self.client.get("/api/tasks/nope/report").status_code, 404)
        self.assertEqual(self.client.get("/api/tasks/nope/evidence").status_code, 404)

        both = self.client.post(
            "/api/tasks",
            json={"sample_id": "slow-sql-001", "ticket_text": "也给了文本"},
        )
        self.assertEqual(both.status_code, 422)

        neither = self.client.post("/api/tasks", json={})
        self.assertEqual(neither.status_code, 422)

        unknown_sample = self.client.post("/api/tasks", json={"sample_id": "nope"})
        self.assertEqual(unknown_sample.status_code, 404)

        unknown_skill = self.client.post(
            "/api/tasks",
            json={"sample_id": "slow-sql-001", "skill": "../secret"},
        )
        self.assertEqual(unknown_skill.status_code, 422)

        extra_field = self.client.post(
            "/api/tasks",
            json={"sample_id": "slow-sql-001", "ticketText": "拼错字段"},
        )
        self.assertEqual(extra_field.status_code, 422)

    def test_report_is_404_before_terminal_and_turn_context_404(self) -> None:
        """任务还没跑完时报告返回 404；不存在的轮次也返回 404。"""
        service = self.client.app.state.service
        service.settings.model_delay_s = 0.05
        created = self.client.post("/api/tasks", json={"sample_id": "slow-sql-001"})
        task_id = created.json()["task_id"]

        self.assertEqual(self.client.get(f"/api/tasks/{task_id}/report").status_code, 404)
        self.assertEqual(
            self.client.get(f"/api/tasks/{task_id}/turns/99/context").status_code,
            404,
        )
        self.client.post(f"/api/tasks/{task_id}/cancel")
        self.wait_terminal(task_id)

    def test_unknown_model_provider_and_missing_key_return_422(self) -> None:
        """选了不支持或不可用的模型时，创建任务前就给出明确错误。"""
        unsupported = self.client.post(
            "/api/tasks",
            json={"sample_id": "slow-sql-001", "model_provider": "openai"},
        )
        self.assertEqual(unsupported.status_code, 422)
        self.assertIn("不支持的模型 provider", unsupported.json()["detail"])

        # 把环境变量清空，确保“没有 Key”这条分支和开发机是否配了 Key 无关。
        with patch.dict(
            os.environ,
            {"DEEPSEEK_API_KEY": "", "SUPPORT_PLATFORM_DEEPSEEK_API_KEY": ""},
        ):
            catalog = self.client.get("/api/models").json()
            deepseek = next(
                item for item in catalog["providers"] if item["provider"] == "deepseek"
            )
            self.assertFalse(deepseek["available"])
            self.assertIn("API Key", deepseek["reason"])

            response = self.client.post(
                "/api/tasks",
                json={"sample_id": "slow-sql-001", "model_provider": "deepseek"},
            )
        self.assertEqual(response.status_code, 422)
        self.assertIn("API Key", response.json()["detail"])

    def test_restart_marks_unfinished_tasks_interrupted(self) -> None:
        """第二个应用实例启动时，会把上一个进程留下的 running 任务标成中断。"""
        service = self.client.app.state.service
        service.store.create(
            task_id="stale-task",
            ticket={"ticket_id": "TCK-STALE", "mock": True},
            skill_name="slow-sql",
            run_dir=str(self.tmp / "runs" / "stale-task"),
            status="running",
        )

        with TestClient(create_app(self.settings)) as second_client:
            stale = second_client.get("/api/tasks/stale-task")
            self.assertEqual(stale.status_code, 200)
            self.assertEqual(stale.json()["status"], "interrupted")
            self.assertEqual(stale.json()["stop_reason"], "interrupted")
            self.assertIn("未完成", stale.json()["error"])


class ScenarioApiTests(ApiTestBase):
    """十个 Mock 场景的端到端验收。

    这组测试不是只检查“任务能跑完”，而是检查产品契约：

    - 10 个样例（1 个慢 SQL + 9 个多数据库场景）都能通过 HTTP 创建并完成；
    - 每个场景自动选用正确的 Skill 和数据集；
    - 报告都有固定章节，且证据充分时只给一个主根因；
    - 数据集声明的证据 key 必须真的进入证据链，不能只写在 JSON 里；
    - 场景规划的每个工具都要在 Topic 的 `attempted_tools` 里出现。
    """

    REQUIRED_SECTIONS = [
        "## 结论",
        "## 修复建议",
        "## 证据链",
        "## 已排除与待验证",
        "## 调查过程",
        "## 验证方法",
        "## 当前限制",
    ]

    def test_sample_catalog_contains_ten_cases_and_all_complete(self) -> None:
        """样例目录应覆盖六种数据库，并且 10 个任务都能跑出报告。"""
        samples = self.client.get("/api/samples").json()
        self.assertEqual(len(samples), 10)
        self.assertEqual(
            {sample["database_type"] for sample in samples},
            {"mysql", "postgresql", "redis", "mongodb", "elasticsearch", "clickhouse"},
        )
        self.assertEqual(samples[0]["sample_id"], "slow-sql-001")
        self.assertEqual(samples[0]["scenario_order"], 10)

        for sample in samples:
            with self.subTest(sample=sample["sample_id"]):
                created = self.client.post(
                    "/api/tasks",
                    json={"sample_id": sample["sample_id"]},
                )
                self.assertEqual(created.status_code, 202)
                final = self.wait_terminal(created.json()["task_id"], timeout_s=10.0)

                self.assertEqual(final["status"], "completed")
                self.assertEqual(
                    final["capabilities"]["skill"]["name"],
                    sample["skill"],
                )
                report = final["report"]
                for section in self.REQUIRED_SECTIONS:
                    self.assertEqual(
                        report.count(section),
                        1,
                        f"{sample['sample_id']} 缺少或重复章节：{section}",
                    )
                # 证据充分时只能有一个主根因；候选原因不能平铺出来抢结论。
                self.assertEqual(
                    report.count("**最可能根因：**"),
                    1,
                    f"{sample['sample_id']} 必须只给一个主根因",
                )
                self.assertNotIn("当前证据不足以给出唯一根因", report)
                self.assertIn("（证据 `ev-", report)

                attempted = set(final["topic"]["attempted_tools"])
                expected_tools = self._expected_tools(final)
                self.assertTrue(
                    expected_tools <= attempted,
                    f"{sample['sample_id']} 缺少工具：{expected_tools - attempted}",
                )

    def _expected_tools(self, final: dict[str, Any]) -> set[str]:
        """从工单或数据集里读出这个场景应该调用哪些业务工具。"""
        ticket = final["ticket"]
        dataset_path = str(ticket.get("dataset") or "").strip()
        if dataset_path:
            dataset = json.loads(
                (PROJECT_ROOT / dataset_path).read_text(encoding="utf-8")
            )
            return {
                str(step["tool"])
                for step in dataset["scenario"]["steps"]
                if step.get("tool")
            }
        # 旧慢 SQL 工单没有 scenario.dataset，仍按原来的六步剧本检查四个工具。
        return {
            "slow_log_query",
            "execution_plan",
            "instance_metrics",
            "lock_wait_query",
        }


if __name__ == "__main__":
    unittest.main()
