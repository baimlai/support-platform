"""Agent 循环和模拟流程的自动化测试。

这一组测试对应任务书第 1 步的验收点：
- 模拟工单能从开始跑到结束；
- 工具调用和工具结果能按 id 配对；
- 工具失败、无数据、超时、未知工具、参数错误都不会让整个任务崩掉；
- 达到最大轮数、用户取消能正常停下；
- 运行记录（事件、请求、响应、消息、报告）真的写到了磁盘。

说明：
- `unittest` 是标准库自带的测试框架，不用额外装依赖；
- 继承 `IsolatedAsyncioTestCase` 后，`async def test_xxx` 会被自动放进独立事件循环执行，
  不需要自己写 `asyncio.run`；
- `assertEqual`、`assertTrue`、`assertIn` 这些断言失败时，会打印实际值和期望值，比手写
  `if ...: raise` 更容易定位问题。
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from app.harness.runner import AgentRunner, RunOptions
from app.harness.tool_search import ToolSession
from app.mcp.dba_tools import static_tool_specs
from app.runtime.contracts import (
    AgentContext,
    AgentEnd,
    AgentEvent,
    AgentLoopConfig,
    AgentStart,
    AssistantDone,
    AssistantMessage,
    AssistantStart,
    AssistantTextDelta,
    AssistantToolCallEvent,
    CancellationToken,
    ModelRequest,
    StopReason,
    TextContent,
    ToolCall,
    ToolExecutionEnd,
    ToolResult,
    ToolResultMessage,
    ToolSpec,
    TopicUpdated,
    TurnStart,
)
from app.runtime.pi_reference.loop import run_agent_loop

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TICKET_PATH = PROJECT_ROOT / "fixtures" / "tickets" / "slow_sql_001.json"


def load_ticket() -> dict[str, Any]:
    """读取测试用模拟工单。

    测试里也读真实 fixtures，而不是另外写一份小 JSON：
    这样一旦样例数据被改坏，测试会第一时间发现。
    """
    return json.loads(TICKET_PATH.read_text(encoding="utf-8"))


class ScriptedModel:
    """测试专用的“按剧本说话”模型。

    剧本的每一项是 `(文本, [工具调用, ...])`，每被调用一次就播下一项。
    例子：
        ScriptedModel([
            ("先查一下", [("some_tool", {"id": "1"})]),
            ("查完了，结论是……", []),
        ])

    为什么不用 `MockDbaModel`：
    很多异常场景只需要一两个固定工具调用，用剧本模型更短、更可控；
    完整业务流程的测试仍然使用 `MockDbaModel`。
    """

    def __init__(self, turns: list[tuple[str, list[tuple[str, dict[str, Any]]]]]) -> None:
        self._turns = turns
        self._index = 0

    def stream(self, request: ModelRequest) -> AsyncIterator[Any]:
        """返回异步事件流，形状和真实模型适配器一致。"""
        return self._stream(request)

    async def _stream(self, request: ModelRequest) -> AsyncIterator[Any]:
        # 这是一个“异步生成器”：函数里有 yield，调用时不会立刻执行，
        # 而是每次 `async for` 取下一个事件。模型流式输出正适合这种写法。
        del request  # 剧本模型不读取请求内容，删掉变量避免 lint 报“未使用”
        if self._index >= len(self._turns):
            raise AssertionError("剧本已经播完，但循环又请求了一次模型")
        text, calls = self._turns[self._index]
        self._index += 1

        stop_reason = StopReason.TOOL_USE if calls else StopReason.STOP
        yield AssistantStart(partial=AssistantMessage(content=[], stop_reason=stop_reason))

        content: list[Any] = []
        if text:
            content.append(TextContent(text=text))
            yield AssistantTextDelta(
                delta=text,
                partial=AssistantMessage(content=list(content), stop_reason=stop_reason),
            )

        for name, arguments in calls:
            tool_call = ToolCall(id=f"test-call-{self._index}-{len(content)}", name=name, arguments=arguments)
            content.append(tool_call)
            yield AssistantToolCallEvent(
                tool_call=tool_call,
                partial=AssistantMessage(content=list(content), stop_reason=stop_reason),
            )

        yield AssistantDone(message=AssistantMessage(content=list(content), stop_reason=stop_reason))


def make_tool(
    name: str,
    handler: Any,
    timeout_s: float | None = None,
    required: list[str] | None = None,
) -> ToolSpec:
    """快速创建一个测试工具。

    参数 Schema 默认只要求 `required` 里的字段，类型统一先写成 string；
    测试关心的是循环行为，不需要为每个工具写完整 Schema。
    """
    required_fields = required or []
    properties = {field: {"type": "string"} for field in required_fields}
    return ToolSpec(
        name=name,
        label=name,
        description=f"测试工具 {name}",
        parameters={
            "type": "object",
            "properties": properties,
            "required": required_fields,
            "additionalProperties": False,
        },
        handler=handler,
        timeout_s=timeout_s,
    )


class MockFlowTests(unittest.IsolatedAsyncioTestCase):
    """完整模拟流程的测试：通过 AgentRunner 走第 2 步的真实装配。"""

    async def test_full_mock_run_completes_and_writes_records(self) -> None:
        """一张慢 SQL 工单应跑完 6 轮，覆盖 Skill、搜索、证据、Topic 和有界上下文。"""
        ticket = load_ticket()

        # `enterContext` 会把临时目录登记到测试清理列表：测试结束时自动删除，
        # 但在测试方法内部一直可读，适合“先 run 再读文件断言”的场景。
        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        events: list[AgentEvent] = []

        def on_event(event: AgentEvent) -> None:
            """AgentRunner 的事件回调是同步函数，这里只把事件收进列表。"""
            events.append(event)

        outcome = await AgentRunner(
            RunOptions(
                ticket=ticket,
                run_root=Path(temp_dir),
                run_id="test-run",
                model_delay_s=0,
                max_turns=10,
            )
        ).run(on_event=on_event)

        result = outcome.result

        # 1) 6 轮：慢日志 → 证据分页 → 工具搜索 → 执行计划 → 监控 + 锁等待 → 报告。
        self.assertEqual(result.reason, "completed")
        self.assertEqual(result.turns, 6)

        # 2) 事件以 agent_start 开始、agent_end 结束，6 次工具执行都走完。
        self.assertIsInstance(events[0], AgentStart)
        self.assertIsInstance(events[-1], AgentEnd)
        tool_ends = [event for event in events if isinstance(event, ToolExecutionEnd)]
        self.assertEqual(
            [event.tool_name for event in tool_ends],
            [
                "slow_log_query",
                "evidence_read",
                "search_tools",
                "execution_plan",
                "instance_metrics",
                "lock_wait_query",
            ],
        )
        self.assertEqual(
            [event.turn for event in events if isinstance(event, TurnStart)],
            [1, 2, 3, 4, 5, 6],
        )
        topic_events = [event for event in events if isinstance(event, TopicUpdated)]
        self.assertEqual(len(topic_events), 6)

        # 3) 每个 ToolCall 都有同 id 的 ToolResultMessage，配对关系不能断。
        #    这是 Agent 循环里最容易出错的地方之一，所以单独断言。
        assistant_ids = [
            block.id
            for message in result.context.messages
            if isinstance(message, AssistantMessage)
            for block in message.content
            if isinstance(block, ToolCall)
        ]
        result_ids = [
            message.tool_call_id
            for message in result.context.messages
            if isinstance(message, ToolResultMessage)
        ]
        self.assertEqual(sorted(assistant_ids), sorted(result_ids))

        # 4) 监控工具是故意失败的，失败结果同样要保存为证据。
        metrics_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage) and message.tool_name == "instance_metrics"
        ]
        self.assertEqual(len(metrics_results), 1)
        self.assertTrue(metrics_results[0].is_error)
        self.assertIsNotNone(metrics_results[0].meta)
        self.assertIsNotNone(metrics_results[0].meta.evidence_id)

        # 5) 证据、工具搜索和 Topic 都按第 2 步的设计工作。
        self.assertEqual(outcome.evidence.count, 4)
        self.assertEqual(
            set(outcome.session.loaded_by_search),
            {"execution_plan", "instance_metrics", "lock_wait_query"},
        )
        topic = outcome.topic.topic_dict()
        fact_keys = {fact["key"] for fact in topic["facts"]}
        self.assertIn("slow_log.top", fact_keys)
        self.assertIn("execution_plan.access", fact_keys)
        self.assertIn("lock_wait.samples", fact_keys)
        # 失败的工具不产生事实，只产生未决问题和未验证候选原因。
        self.assertNotIn("instance_metrics.available", fact_keys)
        self.assertEqual(
            topic["attempted_tools"],
            [
                "slow_log_query",
                "evidence_read",
                "search_tools",
                "execution_plan",
                "instance_metrics",
                "lock_wait_query",
            ],
        )
        access_candidate = next(
            item for item in topic["candidates"] if item["key"] == "access_path"
        )
        self.assertEqual(access_candidate["status"], "supported")

        # 6) 报告必须先把结论放最前，再给证据编号、未验证项和上下文预算说明。
        report = outcome.recorder.report_path.read_text(encoding="utf-8")
        self.assertIn("# 慢 SQL 工单调查报告（模拟 · 第 2 步）", report)
        self.assertIn("## 结论", report)
        self.assertIn("**置信度：** 中", report)
        self.assertIn("## 修复建议", report)
        self.assertIn("### 首选方案", report)
        self.assertIn("## 证据链", report)
        self.assertIn("## 已排除与待验证", report)
        self.assertIn("### 待验证", report)
        self.assertIn("## 调查过程", report)
        self.assertIn("## 当前限制", report)
        self.assertIn("ev-000001", report)
        self.assertIn("未验证", report)
        self.assertIn("上下文", report)
        self.assertIn("a1b2c3d4e5f6", report)
        # 证据充分时不要再平铺“候选原因”清单。
        self.assertNotIn("## 候选原因", report)

        # 7) 运行元信息、事件序列、模型请求和响应都真的写到了磁盘。
        meta = json.loads(outcome.recorder.run_path.read_text(encoding="utf-8"))
        self.assertEqual(meta["status"], "completed")
        self.assertEqual(meta["stop_reason"], "completed")
        self.assertEqual(meta["evidence_count"], 4)
        self.assertEqual(meta["capabilities"]["skill"]["name"], "slow-sql")
        self.assertTrue((outcome.recorder.run_dir / "topic.json").exists())

        event_lines = outcome.recorder.events_path.read_text(encoding="utf-8").splitlines()
        parsed_events = [json.loads(line) for line in event_lines]
        self.assertEqual(
            [item["seq"] for item in parsed_events],
            list(range(1, len(parsed_events) + 1)),
        )
        self.assertEqual(parsed_events[-1]["type"], "agent_end")

        request_lines = outcome.recorder.model_requests_path.read_text(
            encoding="utf-8"
        ).splitlines()
        self.assertEqual(len(request_lines), 6)  # 6 轮 = 6 次模型请求
        last_request = json.loads(request_lines[-1])
        budget = last_request["metadata"]["context_budget"]
        self.assertGreater(budget["dropped_messages"], 0)
        self.assertGreater(budget["source_messages"], budget["included_messages"])
        self.assertTrue(outcome.recorder.model_responses_path.exists())
        self.assertTrue(outcome.recorder.messages_path.exists())

    async def test_max_turns_stops_without_fake_report(self) -> None:
        """轮数上限为 1 时，完成第一次工具调用后必须停下，不能伪造最终报告。"""
        ticket = load_ticket()

        temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        events: list[AgentEvent] = []

        def on_event(event: AgentEvent) -> None:
            events.append(event)

        outcome = await AgentRunner(
            RunOptions(
                ticket=ticket,
                run_root=Path(temp_dir),
                run_id="max-turns",
                model_delay_s=0,
                max_turns=1,
            )
        ).run(on_event=on_event)

        result = outcome.result
        self.assertEqual(result.reason, "max_turns")
        self.assertEqual(result.turns, 1)
        # 只应该有一个 turn_start，不能出现“发起了但没执行”的轮次。
        self.assertEqual(len([event for event in events if isinstance(event, TurnStart)]), 1)
        self.assertIn(
            "达到最大轮数",
            outcome.recorder.report_path.read_text(encoding="utf-8"),
        )

        meta = json.loads(outcome.recorder.run_path.read_text(encoding="utf-8"))
        self.assertEqual(meta["status"], "failed")
        self.assertEqual(meta["stop_reason"], "max_turns")


class ToolVisibilityTests(unittest.IsolatedAsyncioTestCase):
    """工具可见性：没有加载的工具不能执行，搜索后才可用。"""

    async def test_unloaded_tool_is_not_executable(self) -> None:
        """模型直接点名未加载的工具时，内核应返回 not found，而不是偷偷执行。"""
        # 这个测试只关心“未加载工具不能执行”，不会真的调用 handler，
        # 所以用 MCP 的静态声明即可，不必启动 MCP 子进程。
        session = ToolSession.create(static_tool_specs(), ["slow_log_query"])
        model = ScriptedModel(
            [
                (
                    "我直接调执行计划",
                    [
                        (
                            "execution_plan",
                            {
                                "instance_id": "db-mysql-001",
                                "sql_fingerprint": "a1b2c3d4e5f6",
                            },
                        )
                    ],
                ),
                ("工具没加载，应该先搜索。", []),
            ]
        )

        async def emit(event: AgentEvent) -> None:
            del event

        result = await run_agent_loop(
            prompts=[],
            context=AgentContext(messages=[], tools=[]),
            config=AgentLoopConfig(
                max_turns=3,
                tools_provider=session.visible_specs,
            ),
            emit=emit,
            stream_fn=model.stream,
        )

        self.assertEqual(result.reason, "completed")
        error_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage) and message.is_error
        ]
        self.assertEqual(len(error_results), 1)
        self.assertIn("not found", error_results[0].content[0].text)

    async def test_search_loads_tool_for_next_request(self) -> None:
        """搜索命中后，工具会在下一次模型请求的工具声明里出现并可执行。"""
        tools = static_tool_specs()

        async def fake_execution_plan(
            tool_call_id: str,
            arguments: dict[str, Any],
            cancellation: CancellationToken,
        ) -> ToolResult:
            """只验证可见性，不重复启动 MCP；真实 MCP 调用在 test_mcp.py 覆盖。"""
            del tool_call_id, cancellation
            return ToolResult(
                content=[
                    TextContent(text=json.dumps({"sql_fingerprint": arguments["sql_fingerprint"]}))
                ],
                details={"full_scan": True},
            )

        for tool in tools:
            if tool.name == "execution_plan":
                tool.handler = fake_execution_plan
        session = ToolSession.create(tools, ["slow_log_query"])
        session.search("执行计划")
        model = ScriptedModel(
            [
                (
                    "现在查执行计划",
                    [
                        (
                            "execution_plan",
                            {
                                "instance_id": "db-mysql-001",
                                "sql_fingerprint": "a1b2c3d4e5f6",
                            },
                        )
                    ],
                ),
                ("拿到了执行计划。", []),
            ]
        )

        async def emit(event: AgentEvent) -> None:
            del event

        result = await run_agent_loop(
            prompts=[],
            context=AgentContext(messages=[], tools=[]),
            config=AgentLoopConfig(
                max_turns=3,
                tools_provider=session.visible_specs,
            ),
            emit=emit,
            stream_fn=model.stream,
        )

        self.assertEqual(result.reason, "completed")
        tool_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage)
        ]
        self.assertEqual(len(tool_results), 1)
        self.assertFalse(tool_results[0].is_error)
        self.assertIn("a1b2c3d4e5f6", tool_results[0].content[0].text)


class LoopFailureTests(unittest.IsolatedAsyncioTestCase):
    """循环的异常分支：工具失败、超时、未知工具、参数错误。"""

    async def test_tool_exception_becomes_error_result(self) -> None:
        """工具内部抛异常时，模型应收到 is_error 结果并继续，而不是整个任务崩掉。"""

        async def boom(
            tool_call_id: str,
            arguments: dict[str, Any],
            cancellation: CancellationToken,
        ) -> ToolResult:
            del tool_call_id, arguments, cancellation
            raise RuntimeError("模拟数据库连接失败")

        model = ScriptedModel(
            [
                ("我先查一下", [("boom", {})]),
                ("查询失败了，我把失败原因写进报告。", []),
            ]
        )
        context = AgentContext(messages=[], tools=[make_tool("boom", boom)])
        events: list[AgentEvent] = []

        async def emit(event: AgentEvent) -> None:
            events.append(event)

        result = await run_agent_loop(
            prompts=[],
            context=context,
            config=AgentLoopConfig(max_turns=3),
            emit=emit,
            stream_fn=model.stream,
        )

        self.assertEqual(result.reason, "completed")
        error_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage)
        ]
        self.assertEqual(len(error_results), 1)
        self.assertTrue(error_results[0].is_error)
        self.assertIn("模拟数据库连接失败", error_results[0].content[0].text)

    async def test_tool_timeout_becomes_error_result(self) -> None:
        """工具执行超过配置时间时，内核应主动中断并返回超时说明。"""

        async def slow(
            tool_call_id: str,
            arguments: dict[str, Any],
            cancellation: CancellationToken,
        ) -> ToolResult:
            del tool_call_id, arguments, cancellation
            await asyncio.sleep(0.2)
            return ToolResult(content=[TextContent(text="这条结果不应该出现")])

        model = ScriptedModel(
            [
                ("调用一个很慢的工具", [("slow", {})]),
                ("超时了，标记为未验证。", []),
            ]
        )
        context = AgentContext(messages=[], tools=[make_tool("slow", slow, timeout_s=0.02)])

        async def emit(event: AgentEvent) -> None:
            del event  # 这个测试不关心事件，只关心最终结果

        result = await run_agent_loop(
            prompts=[],
            context=context,
            config=AgentLoopConfig(max_turns=3, tool_timeout_s=1.0),
            emit=emit,
            stream_fn=model.stream,
        )

        error_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage) and message.is_error
        ]
        self.assertEqual(len(error_results), 1)
        self.assertIn("执行超时", error_results[0].content[0].text)
        self.assertEqual(result.reason, "completed")

    async def test_unknown_tool_returns_error_result(self) -> None:
        """模型点名一个不存在的工具时，应返回“未找到”，并让模型有机会改正。"""
        model = ScriptedModel(
            [
                ("调用不存在的工具", [("missing_tool", {"x": "1"})]),
                ("工具不存在，我改用其他方式。", []),
            ]
        )
        context = AgentContext(messages=[], tools=[])

        async def emit(event: AgentEvent) -> None:
            del event

        result = await run_agent_loop(
            prompts=[],
            context=context,
            config=AgentLoopConfig(max_turns=3),
            emit=emit,
            stream_fn=model.stream,
        )

        error_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage) and message.is_error
        ]
        self.assertEqual(len(error_results), 1)
        self.assertIn("not found", error_results[0].content[0].text)
        self.assertEqual(result.reason, "completed")

    async def test_invalid_arguments_are_rejected_before_execution(self) -> None:
        """参数缺字段时应直接拒绝，工具函数不能被调用。"""
        calls: list[dict[str, Any]] = []

        async def echo(
            tool_call_id: str,
            arguments: dict[str, Any],
            cancellation: CancellationToken,
        ) -> ToolResult:
            del tool_call_id, cancellation
            calls.append(arguments)
            return ToolResult(content=[TextContent(text="ok")])

        model = ScriptedModel(
            [
                ("少传一个必填参数", [("echo", {})]),
                ("参数不对，我重新组织调用。", []),
            ]
        )
        context = AgentContext(
            messages=[],
            tools=[make_tool("echo", echo, required=["instance_id"])],
        )

        async def emit(event: AgentEvent) -> None:
            del event

        result = await run_agent_loop(
            prompts=[],
            context=context,
            config=AgentLoopConfig(max_turns=3),
            emit=emit,
            stream_fn=model.stream,
        )

        self.assertEqual(calls, [])  # 参数不合法，工具一次都没执行
        error_results = [
            message
            for message in result.context.messages
            if isinstance(message, ToolResultMessage) and message.is_error
        ]
        self.assertEqual(len(error_results), 1)
        self.assertIn("参数校验失败", error_results[0].content[0].text)
        self.assertIn("instance_id", error_results[0].content[0].text)

    async def test_cooperative_cancellation_stops_after_tool(self) -> None:
        """工具执行中调用取消令牌后，本轮结束就停，不再请求下一次模型。"""
        cancellation = CancellationToken()

        async def cancel_me(
            tool_call_id: str,
            arguments: dict[str, Any],
            token: CancellationToken,
        ) -> ToolResult:
            del tool_call_id, arguments
            token.cancel()  # 模拟用户在执行途中点了“取消”
            return ToolResult(content=[TextContent(text="执行到一半被取消")])

        model = ScriptedModel(
            [
                ("调用一个会触发取消的工具", [("cancel_me", {})]),
                ("这轮不应该出现。", []),
            ]
        )
        context = AgentContext(messages=[], tools=[make_tool("cancel_me", cancel_me)])
        events: list[AgentEvent] = []

        async def emit(event: AgentEvent) -> None:
            events.append(event)

        result = await run_agent_loop(
            prompts=[],
            context=context,
            config=AgentLoopConfig(max_turns=3),
            emit=emit,
            stream_fn=model.stream,
            cancellation=cancellation,
        )

        self.assertEqual(result.reason, "cancelled")
        # 取消也要保证工具调用和结果配对完整：助手消息里有 1 个调用，结果里也应有 1 个。
        self.assertEqual(
            len([message for message in result.context.messages if isinstance(message, ToolResultMessage)]),
            1,
        )
        self.assertIsInstance(events[-1], AgentEnd)
        self.assertEqual(events[-1].reason, "cancelled")

    async def test_stream_without_done_is_reported_as_error(self) -> None:
        """模型流没有 done/error 就结束时，不能拿半截消息继续跑。"""

        async def broken_stream(request: ModelRequest) -> AsyncIterator[Any]:
            del request
            # 只发一个 start，然后就结束。内核应识别为异常。
            yield AssistantStart(partial=AssistantMessage(content=[]))

        async def emit(event: AgentEvent) -> None:
            del event

        result = await run_agent_loop(
            prompts=[],
            context=AgentContext(messages=[], tools=[]),
            config=AgentLoopConfig(max_turns=2),
            emit=emit,
            stream_fn=broken_stream,
        )

        self.assertEqual(result.reason, "error")
        self.assertIn("没有 done/error", result.error or "")


if __name__ == "__main__":
    # 允许直接运行这个文件：python tests/test_agent_loop.py
    # 平时更常用的是 `python -m unittest discover -s tests -v`，一次跑完整个 tests 目录。
    unittest.main()
