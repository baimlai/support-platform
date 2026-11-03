"""Pi 扩展的行为测试：跨轮调用顺序、重试副作用、摘要提交和检索排名。

这些测试关心可观察行为，既检查正常路径，也检查取消、半截摘要和协议配对。
测试里的模型是本地剧本，真实 MCP 集成另由 Runner 用例验证。
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.adapters.context_summary import extractive_mock_summary, make_model_summarizer
from app.adapters.run_recorder import RunRecorder
from app.api.service import TaskService
from app.api.settings import ApiSettings
from app.harness.runner import AgentRunner, RunOptions
from app.runtime.contracts import (
    AgentContext,
    AgentLoopConfig,
    AssistantDone,
    AssistantError,
    AssistantMessage,
    AssistantStart,
    CancellationToken,
    ContextCompacted,
    MessageEnd,
    ModelRequest,
    ModelRetry,
    NextTurnUpdate,
    RequestUpdate,
    StopReason,
    SystemMessage,
    TextContent,
    ToolCall,
    ToolExecutionEnd,
    ToolResult,
    ToolResultMessage,
    ToolResultMeta,
    TurnDecision,
    UserMessage,
    assistant_text,
    to_jsonable,
)
from app.runtime.pi_reference.agent import Agent, PendingMessageQueue
from app.runtime.pi_reference.compaction import CompactionSettings, ContextCompactor, SummaryInput
from app.runtime.pi_reference.loop import run_agent_loop
from app.runtime.pi_reference.retry import (
    RetryPolicy,
    is_context_overflow,
    is_retryable_error,
    with_model_retry,
)
from app.runtime.pi_reference.search import SearchDocument, rank_documents
from app.runtime.pi_reference.truncate import truncate_text
from tests.test_agent_loop import ScriptedModel, load_ticket, make_tool


class AgentLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """用真正 Loop 验证 Hook 的创建/传递/调用关系。"""

    def test_queue_modes_are_independent(self):
        """一个队列消费消息不能影响另一个队列，peek 也不应消费输入。"""
        one = PendingMessageQueue()
        all_messages = PendingMessageQueue(mode="all")
        for queue in (one, all_messages):
            queue.put(UserMessage(content="第一条"))
            queue.put(UserMessage(content="第二条"))
        self.assertEqual(len(one.peek()), 1)
        self.assertEqual(len(one.drain()), 1)
        self.assertEqual(len(all_messages.drain()), 2)
        self.assertEqual(one.peek()[0].content, "第二条")

    async def test_max_turns_does_not_lose_unconsumed_followup(self):
        """平台新增的轮数保护不能把已取出却没执行的用户追问弄丢。"""
        agent = Agent(AgentContext(), AgentLoopConfig(max_turns=1), ScriptedModel([("报告", [])]).stream)
        agent.follow_up(UserMessage(content="下一问"))
        result = await agent.prompt([UserMessage(content="工单")])
        self.assertEqual(result.reason, "max_turns")
        self.assertEqual(agent.peek_queued_messages()[0].content, "下一问")

    async def test_prepare_hooks_replace_context_and_follow_turn_order(self):
        """首轮只执行 prepare_request，下一轮先准备再请求；替换历史要真正返回。"""
        order = []
        requests = []
        calls = 0

        async def prepare(request):
            nonlocal calls
            calls += 1
            order.append(f"request-{calls}")
            if calls == 1:
                return RequestUpdate(AgentContext(messages=[UserMessage(content="规范化工单")]), "new-model")

        async def finish(turn):
            order.append("finish")
            return TurnDecision("continue") if calls == 1 else None

        async def next_turn(turn):
            order.append("next")
            return NextTurnUpdate(messages=[UserMessage(content="追加验证要求")])

        async def transform(messages, token):
            order.append("transform")
            return list(messages)

        async def stream(request):
            requests.append(request)
            yield AssistantDone(AssistantMessage(content=[TextContent(text="回答")]))

        async def emit(event):
            if event.type == "turn_start":
                order.append("start")

        result = await run_agent_loop(
            [UserMessage(content="原始工单")],
            AgentContext(),
            AgentLoopConfig(
                prepare_request=prepare,
                prepare_next_turn=next_turn,
                transform_context=transform,
                finish_turn=finish,
            ),
            emit,
            stream,
        )
        self.assertEqual(
            order,
            [
                "start",
                "request-1",
                "transform",
                "finish",
                "next",
                "start",
                "request-2",
                "transform",
                "finish",
            ],
        )
        self.assertEqual([request.model for request in requests], ["new-model", "new-model"])
        users = [message.content for message in result.context.messages if isinstance(message, UserMessage)]
        self.assertEqual(users, ["规范化工单", "追加验证要求"])

    async def test_steering_precedes_tools_next_turn_and_follow_up_waits(self):
        """工具结束后插入 steering；follow-up 必须等到无工具的最终回答后。"""
        events = []
        model = ScriptedModel([("取证", [("read", {})]), ("报告", []), ("补充回答", [])])

        async def tool(call_id, arguments, token):
            return ToolResult(content=[TextContent(text="证据")])

        agent = Agent(AgentContext(tools=[make_tool("read", tool)]), AgentLoopConfig(), model.stream)
        agent.follow_up(UserMessage(content="再给验证步骤"))

        def on_event(event):
            events.append(event)
            if isinstance(event, ToolExecutionEnd):
                agent.steer(UserMessage(content="保留证据编号"))

        unsubscribe = agent.subscribe(on_event)
        result = await agent.prompt([UserMessage(content="查询变慢")])
        user_turns = [
            (event.message.content, event.turn)
            for event in events
            if isinstance(event, MessageEnd) and isinstance(event.message, UserMessage)
        ]
        self.assertEqual(user_turns, [("查询变慢", 1), ("保留证据编号", 2), ("再给验证步骤", 3)])
        self.assertEqual(result.turns, 3)
        self.assertFalse(agent.is_running)
        self.assertIsNone(agent.streaming_message)
        self.assertFalse(agent.pending_tool_calls)
        unsubscribe()
        unsubscribe()

    async def test_busy_guard_and_abort_clean_up_state(self):
        """并发 prompt 必须报错；协作取消后仍可复用包装对象。"""
        entered = asyncio.Event()

        async def stream(request):
            entered.set()
            await request.cancellation.wait()
            yield AssistantError(AssistantMessage(content=[], stop_reason=StopReason.ABORTED))

        agent = Agent(AgentContext(), AgentLoopConfig(), stream)
        task = asyncio.create_task(agent.prompt([UserMessage(content="开始")]))
        await entered.wait()
        with self.assertRaises(RuntimeError):
            await agent.prompt([UserMessage(content="重复开始")])
        agent.abort()
        result = await task
        self.assertEqual(result.reason, "cancelled")
        self.assertFalse(agent.is_running)

    async def test_continue_uses_existing_history_and_queued_input(self):
        """继续已有用户输入不重复追加；助手结尾时可用排队的新消息继续。"""
        agent = Agent(
            AgentContext(messages=[UserMessage(content="原始问题")]),
            AgentLoopConfig(),
            ScriptedModel([("第一答", []), ("第二答", [])]).stream,
        )
        first = await agent.continue_run()
        self.assertEqual(len(first.messages), 1)
        agent.follow_up(UserMessage(content="追加问题"))
        second = await agent.continue_run()
        self.assertEqual(assistant_text(second.context.messages[-1]), "第二答")
        self.assertEqual(len([msg for msg in second.context.messages if isinstance(msg, UserMessage)]), 2)

    async def test_finish_end_skips_preparation_and_preserves_queued_followup(self):
        """finish_turn 主动结束优先于队列和下一轮准备。"""

        async def finish(turn):
            return TurnDecision("end")

        async def forbidden(turn):
            self.fail("主动结束不应调用下一轮准备")

        agent = Agent(
            AgentContext(),
            AgentLoopConfig(finish_turn=finish, prepare_next_turn=forbidden),
            ScriptedModel([("报告", [])]).stream,
        )
        agent.follow_up(UserMessage(content="待处理追问"))
        await agent.prompt([UserMessage(content="工单")])
        self.assertEqual(agent.peek_queued_messages()[0].content, "待处理追问")


class RetryTests(unittest.IsolatedAsyncioTestCase):
    """请求级重试必须保持一条助手消息，也不能重复跑工具。"""

    async def test_each_attempt_is_recorded_and_closes_its_stream(self):
        """提前读到终态后关闭每一层生成器，确保网络响应的 finally 及时执行。"""
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        recorder = RunRecorder(root)
        closed = []

        async def model(request):
            attempt = request.metadata["retry_attempt"]
            try:
                if attempt == 0:
                    yield AssistantError(
                        AssistantMessage(
                            content=[],
                            stop_reason=StopReason.ERROR,
                            error_message="HTTP 503",
                        )
                    )
                else:
                    yield AssistantDone(AssistantMessage(content=[TextContent(text="成功")]))
            finally:
                closed.append(attempt)

        wrapped = with_model_retry(
            recorder.wrap_stream_fn(model), RetryPolicy(base_delay_s=0), recorder.on_event
        )
        request = ModelRequest([], [], "mock", CancellationToken(), {"turn": 1})
        events = [event async for event in wrapped(request)]
        self.assertEqual(closed, [0, 1])
        self.assertEqual(assistant_text(events[-1].message), "成功")
        self.assertEqual(len(recorder.model_requests_path.read_text().splitlines()), 2)

    async def test_partial_failure_retries_without_duplicate_history_or_tool(self):
        requests = []
        tool_calls = []
        events = []

        async def model(request):
            requests.append(request)
            yield AssistantStart(AssistantMessage(content=[TextContent(text="预览")]))
            if len(requests) == 1:
                yield AssistantError(
                    AssistantMessage(
                        content=[],
                        stop_reason=StopReason.ERROR,
                        error_message="HTTP 503 service unavailable",
                    )
                )
            elif len(requests) == 2:
                yield AssistantDone(
                    AssistantMessage(
                        content=[ToolCall("call-1", "read", {})],
                        stop_reason=StopReason.TOOL_USE,
                    )
                )
            else:
                yield AssistantDone(AssistantMessage(content=[TextContent(text="最终报告")]))

        async def tool(call_id, args, token):
            tool_calls.append(call_id)
            return ToolResult(content=[TextContent(text="实际证据")])

        async def emit(event):
            events.append(event)

        result = await run_agent_loop(
            [UserMessage(content="工单")],
            AgentContext(tools=[make_tool("read", tool)]),
            AgentLoopConfig(),
            emit,
            with_model_retry(model, RetryPolicy(base_delay_s=0), emit),
        )
        self.assertEqual(result.reason, "completed")
        self.assertEqual([request.metadata["retry_attempt"] for request in requests], [0, 1, 0])
        self.assertEqual(tool_calls, ["call-1"])
        self.assertEqual(
            len([msg for msg in result.context.messages if isinstance(msg, AssistantMessage)]), 2
        )
        self.assertTrue(any(isinstance(event, ModelRetry) for event in events))

    async def test_abort_during_backoff_does_not_wait_or_retry(self):
        token = CancellationToken()
        requests = []

        async def model(request):
            requests.append(request)
            yield AssistantError(
                AssistantMessage(
                    content=[],
                    stop_reason=StopReason.ERROR,
                    error_message="connection reset",
                )
            )

        async def emit(event):
            if isinstance(event, ModelRetry) and event.phase == "scheduled":
                token.cancel()

        wrapped = with_model_retry(model, RetryPolicy(base_delay_s=60, max_delay_s=60), emit)
        request = ModelRequest([], [], "mock", token)
        async with asyncio.timeout(1):
            events = [event async for event in wrapped(request)]
        self.assertEqual(len(requests), 1)
        self.assertEqual(events[-1].message.stop_reason, StopReason.ABORTED)

    def test_classifies_quota_overflow_and_transient_errors(self):
        """同为 429，额度耗尽不应按临时限流处理；溢出另交压缩策略处理。"""
        cases = {
            "HTTP 429 too many requests": True,
            'HTTP 429 {"code":1308}': False,
            "HTTP 429 insufficient_quota": False,
            "HTTP 401 unauthorized": False,
            "HTTP 503 service unavailable": True,
            "context_length_exceeded": False,
            "litellm.ContextWindowExceededError": False,
        }
        for text, expected in cases.items():
            message = AssistantMessage(content=[], stop_reason=StopReason.ERROR, error_message=text)
            self.assertEqual(is_retryable_error(message), expected, text)
        self.assertTrue(
            is_context_overflow(
                AssistantMessage(
                    content=[],
                    stop_reason=StopReason.ERROR,
                    error_message="context_length_exceeded",
                )
            )
        )


def history(count=5):
    """构造几轮取证；引用和调用 id 都不同，才能发现压缩切分配错的问题。"""
    messages = [SystemMessage(content="规则"), UserMessage(content="原始工单")]
    for index in range(count):
        call = ToolCall(f"call-{index}", "read", {})
        messages.extend(
            [
                AssistantMessage(content=[call], stop_reason=StopReason.TOOL_USE),
                ToolResultMessage(
                    call.id,
                    call.name,
                    [TextContent(text="原始返回 " + "x" * 200)],
                    meta=ToolResultMeta(evidence_id=f"ev-{index:06d}"),
                ),
            ]
        )
    return messages


class CompactionTests(unittest.IsolatedAsyncioTestCase):
    """检查点提交必须像一次小事务：先验证、再写盘，最后才替换内存状态。"""

    async def test_safe_prefix_persisted_and_original_history_unchanged(self):
        temp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        messages = history()
        original = to_jsonable(messages)
        path = temp / "compaction.json"
        compactor = ContextCompactor(
            CompactionSettings(enabled=True, trigger_chars=1, keep_recent_messages=2),
            extractive_mock_summary,
            path=path,
        )
        view = await compactor.transform(messages, CancellationToken())
        self.assertEqual(to_jsonable(messages), original)
        self.assertLess(len(view), len(messages))
        self.assertEqual(view[-2:], messages[-2:])
        checkpoint = json.loads(path.read_text())
        self.assertEqual(checkpoint["through"], len(messages) - 2)
        self.assertIn("ev-000000", checkpoint["summary"])
        self.assertIn("ev-000003", checkpoint["summary"])

    async def test_incremental_summary_receives_previous_checkpoint(self):
        requests = []

        async def summarize(source, token):
            requests.append(source)
            return AssistantMessage(content=[TextContent(text="摘要 " + " ".join(source.required_evidence))])

        compactor = ContextCompactor(
            CompactionSettings(enabled=True, trigger_chars=1, keep_recent_messages=2),
            summarize,
        )
        messages = history(3)
        await compactor.transform(messages, CancellationToken())
        previous = compactor.checkpoint.summary
        # 继续追加两条消息，相当于下一轮工具执行完成，而不是新建一份不同时间戳的历史。
        messages.extend(history(4)[-2:])
        await compactor.transform(messages, CancellationToken())
        self.assertEqual(requests[1].previous_summary, previous)
        self.assertNotIn('"id":"call-0"', requests[1].source_text)
        self.assertIn("ev-000000", compactor.checkpoint.summary)

    async def test_incomplete_or_missing_evidence_summary_is_not_committed(self):
        for reason, text in [(StopReason.LENGTH, "半截"), (StopReason.STOP, "遗漏证据的摘要")]:

            async def summarize(source, token, text=text, reason=reason):
                return AssistantMessage(content=[TextContent(text=text)], stop_reason=reason)

            compactor = ContextCompactor(CompactionSettings(enabled=True, trigger_chars=1), summarize)
            messages = history()
            self.assertEqual(await compactor.transform(messages, CancellationToken()), messages)
            self.assertIsNone(compactor.checkpoint)
            self.assertIsNotNone(compactor.last_error)

    async def test_unpaired_tools_and_disk_failure_preserve_original(self):
        messages = history()
        broken = messages[:-1]
        compactor = ContextCompactor(
            CompactionSettings(enabled=True, trigger_chars=1),
            extractive_mock_summary,
        )
        self.assertEqual(await compactor.transform(broken, CancellationToken()), broken)
        self.assertIsNone(compactor.checkpoint)
        temp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        blocker = temp / "not-a-directory"
        blocker.write_text("占位")
        compactor.path = blocker / "compaction.json"
        self.assertEqual(await compactor.transform(messages, CancellationToken()), messages)
        self.assertIsNone(compactor.checkpoint)

    async def test_cancelled_summary_keeps_previous_view(self):
        async def summarize(source, token):
            token.cancel()
            return await extractive_mock_summary(source, token)

        compactor = ContextCompactor(CompactionSettings(enabled=True, trigger_chars=1), summarize)
        messages = history()
        self.assertEqual(await compactor.transform(messages, CancellationToken()), messages)
        self.assertIsNone(compactor.checkpoint)

    async def test_real_model_summary_has_no_tools_and_separate_purpose(self):
        requests = []

        async def stream(request):
            requests.append(request)
            yield AssistantDone(AssistantMessage(content=[TextContent(text="摘要 ev-000001")]))

        summarize = make_model_summarizer(stream, "real-model")
        result = await summarize(
            SummaryInput("旧摘要", "新材料", ("ev-000001",), 1000, 4), CancellationToken()
        )
        self.assertEqual(assistant_text(result), "摘要 ev-000001")
        self.assertEqual(requests[0].tools, [])
        self.assertEqual(requests[0].metadata, {"purpose": "compaction", "turn": 4})


class SearchAndTruncationTests(unittest.TestCase):
    """纯函数测试用很小的反例验证排序规则和中文边界。"""

    def test_exact_name_beats_mentions_and_more_terms_beat_single_term(self):
        documents = [
            SearchDocument("other", "app " * 100),
            SearchDocument("mcp__peekaboo__app", "打开应用"),
            SearchDocument("window", "window"),
            SearchDocument("capture", "screenshot window"),
        ]
        self.assertEqual(rank_documents("app", documents, 4)[0].name, "mcp__peekaboo__app")
        self.assertEqual(rank_documents("screenshot window", documents, 4)[0].name, "capture")

    def test_unicode_and_long_namespace_do_not_hide_tools(self):
        documents = [SearchDocument("read", "查询锁等待", "服务器说明 " * 1000)]
        self.assertEqual(rank_documents("锁等待", documents, 3)[0].name, "read")

    def test_utf8_and_complete_lines_head_and_tail(self):
        result = truncate_text("锁\n等待\n结束", max_lines=2, max_bytes=7)
        self.assertEqual(result.content, "锁")
        self.assertEqual(result.truncated_by, "bytes")
        self.assertLessEqual(result.output_bytes, 7)
        self.assertTrue(truncate_text("锁" * 10, max_bytes=4).first_line_exceeds_limit)
        tail = truncate_text("锁" * 10, max_bytes=4, tail=True)
        self.assertEqual(tail.content, "锁")
        self.assertTrue(tail.last_line_partial)
        self.assertEqual(truncate_text("a\nb\nc\n", max_lines=2, tail=True).content, "b\nc")


class TurnContextTests(unittest.TestCase):
    """轮次与请求流水号分开后，摘要和重试不能让 HTTP 上下文读串轮。"""

    def test_turn_lookup_skips_summary_and_selects_latest_attempt(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        service = TaskService(ApiSettings(run_root=root))
        rows = [
            {"request_seq": 1, "metadata": {"turn": 1}, "messages": ["第一轮首次"]},
            {"request_seq": 2, "metadata": {"turn": 1, "retry_attempt": 1}, "messages": ["第一轮重试"]},
            {"request_seq": 3, "metadata": {"turn": 2, "purpose": "compaction"}, "messages": ["摘要请求"]},
            {"request_seq": 4, "metadata": {"turn": 2}, "messages": ["第二轮"]},
        ]
        (root / "model_requests.jsonl").write_text("\n".join(json.dumps(row) for row in rows))
        with (
            patch.object(service, "_require_task", return_value=SimpleNamespace()),
            patch.object(
                service,
                "_run_dir",
                return_value=root,
            ),
        ):
            first = service.read_turn_context("task-1", 1)
            second = service.read_turn_context("task-1", 2)
        self.assertEqual(first["messages"], ["第一轮重试"])
        self.assertEqual(first["attempt_request_seqs"], [1, 2])
        self.assertEqual(second["messages"], ["第二轮"])


class RunnerExtensionTests(unittest.IsolatedAsyncioTestCase):
    """接真实 MCP，证明新模块参与完整工单，而不是只有单独的演示函数。"""

    async def test_compaction_and_user_followup_use_real_runner(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        holder = []
        events = []
        queued = False

        def observe(event):
            nonlocal queued
            events.append(event)
            if isinstance(event, ToolExecutionEnd) and not queued:
                holder[0].follow_up(UserMessage(content="请补充验证步骤，并保留失败和证据编号。"))
                queued = True

        outcome = await AgentRunner(
            RunOptions(
                ticket=load_ticket(),
                run_root=root,
                model_delay_s=0,
                compaction=CompactionSettings(enabled=True, trigger_chars=4000, keep_recent_messages=2),
            )
        ).run(on_event=observe, on_agent=holder.append)
        self.assertEqual(outcome.result.reason, "completed")
        self.assertEqual(outcome.result.turns, 7)
        self.assertTrue(
            any(isinstance(event, ContextCompacted) and event.phase == "completed" for event in events)
        )
        self.assertTrue((outcome.recorder.run_dir / "compaction.json").exists())
        requests = [
            json.loads(line) for line in outcome.recorder.model_requests_path.read_text().splitlines()
        ]
        self.assertEqual([item["metadata"]["turn"] for item in requests], list(range(1, 8)))
        self.assertIn("补充验证步骤", json.dumps(requests[-1], ensure_ascii=False))
        self.assertTrue(any("CONTEXT_SUMMARY" in json.dumps(item) for item in requests))
        self.assertEqual(outcome.evidence.count, 4)


if __name__ == "__main__":
    unittest.main()
