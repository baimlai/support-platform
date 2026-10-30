"""DeepSeek 模型适配器测试。

这里**不会真的联网**：用 `httpx.MockTransport` 伪造 HTTP 响应和 SSE 分片，
所以测试在离线环境和 CI 里都能跑。

## 为什么要测这一层

Agent 循环只认识平台自己的事件（`AssistantStart` / `AssistantTextDelta` /
`AssistantToolCallEvent` / `AssistantDone` / `AssistantError`）。
DeepSeek 返回的是 OpenAI 格式的 SSE，中间这层翻译如果写错，
表现可能是“模型明明回答了，页面却是空的”，或者“工具参数被截断”。
这些测试就是盯住翻译边界。

## 实现说明

- `unittest.IsolatedAsyncioTestCase` 会自动为每个测试创建独立事件循环，
  可以直接写 `async def test_...`，不用手工 `asyncio.run`。
- `httpx.MockTransport(handler)` 把网络请求交给一个普通函数处理，
  请求不会真的发出去；`handler` 返回什么，适配器就收到什么。
- `json.dumps(..., ensure_ascii=False)` 让中文保持原样，断言时更容易读。
"""

from __future__ import annotations

import json
import os
import unittest
from typing import Any
from unittest.mock import patch

import httpx

from app.adapters.deepseek_model import (
    DeepSeekModel,
    ModelConfigurationError,
    _parse_sse_data,
)
from app.runtime.contracts import (
    AssistantDone,
    AssistantError,
    AssistantMessage,
    AssistantStart,
    AssistantTextDelta,
    AssistantToolCallEvent,
    CancellationToken,
    ModelRequest,
    StopReason,
    SystemMessage,
    TextContent,
    ToolCall,
    ToolResultMessage,
    ToolSpec,
    UserMessage,
)

BASE_URL = "https://deepseek.test"


def _sse(*chunks: str) -> bytes:
    """把若干段 JSON 拼成 SSE 响应体。

    每个事件是 `data: {...}` 加一个空行，最后以 `data: [DONE]` 结束。
    例子：`_sse('{"choices":[]}')` 会得到两行数据和结束标记。
    """
    lines = [f"data: {chunk}\n\n" for chunk in chunks]
    lines.append("data: [DONE]\n\n")
    return "".join(lines).encode("utf-8")


def _chunk(delta: dict[str, Any], finish_reason: str | None = None) -> str:
    """构造一个 OpenAI 风格的流式分片。"""
    return json.dumps(
        {"choices": [{"delta": delta, "finish_reason": finish_reason}]},
        ensure_ascii=False,
    )


def _request(messages: list[Any] | None = None, tools: list[ToolSpec] | None = None) -> ModelRequest:
    """构造最小模型请求，测试只关心适配器怎么翻译。"""
    return ModelRequest(
        messages=messages or [UserMessage(content="工单：查询变慢")],
        tools=tools or [],
        model="deepseek-flash",
        cancellation=CancellationToken(),
    )


async def _noop_handler(tool_call_id: str, arguments: dict[str, Any], cancellation: Any) -> Any:
    """占位的工具函数；本文件只测试“声明怎么转换”，不会真的执行工具。"""
    return None


class DeepSeekStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_text_stream_becomes_platform_events(self) -> None:
        """文本分片应该拼成完整回复，并标记 provider/model/stop_reason。"""
        captured: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            body = _sse(
                _chunk({"content": "先查慢日志"}),
                _chunk({"content": "，再下结论。"}, finish_reason="stop"),
            )
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            model = DeepSeekModel(
                api_key="test-key",
                base_url=BASE_URL,
                model="deepseek-flash",
                client=client,
            )
            events = [event async for event in model.stream(_request())]

        self.assertIsInstance(events[0], AssistantStart)
        deltas = [event for event in events if isinstance(event, AssistantTextDelta)]
        self.assertEqual([item.delta for item in deltas], ["先查慢日志", "，再下结论。"])

        done = events[-1]
        self.assertIsInstance(done, AssistantDone)
        self.assertEqual(done.message.stop_reason, StopReason.STOP)
        self.assertEqual(done.message.provider, "deepseek")
        self.assertEqual(done.message.model, "deepseek-flash")
        self.assertEqual(
            "".join(
                block.text
                for block in done.message.content
                if isinstance(block, TextContent)
            ),
            "先查慢日志，再下结论。",
        )

        # 请求本身也要正确：路径、鉴权头、流式开关和模型名。
        self.assertEqual(str(captured[0].url), f"{BASE_URL}/chat/completions")
        self.assertEqual(captured[0].headers["authorization"], "Bearer test-key")
        payload = json.loads(captured[0].content)
        self.assertTrue(payload["stream"])
        self.assertEqual(payload["model"], "deepseek-flash")

    async def test_tool_call_fragments_are_merged(self) -> None:
        """同一个工具调用的分片要按 index 合并，参数拼成完整 JSON。"""
        async def handler(request: httpx.Request) -> httpx.Response:
            del request
            body = _sse(
                _chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-1",
                                "type": "function",
                                "function": {
                                    "name": "slow_log_query",
                                    "arguments": '{"instance_id":"db-mysql-001",',
                                },
                            }
                        ]
                    }
                ),
                _chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {"arguments": '"limit":20}'},
                            }
                        ]
                    },
                    finish_reason="tool_calls",
                ),
            )
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            model = DeepSeekModel(api_key="test-key", base_url=BASE_URL, client=client)
            events = [event async for event in model.stream(_request())]

        tool_events = [event for event in events if isinstance(event, AssistantToolCallEvent)]
        self.assertEqual(len(tool_events), 1)
        call = tool_events[0].tool_call
        self.assertEqual(call.id, "call-1")
        self.assertEqual(call.name, "slow_log_query")
        self.assertEqual(call.arguments, {"instance_id": "db-mysql-001", "limit": 20})
        self.assertEqual(tool_events[0].partial.stop_reason, StopReason.TOOL_USE)

        done = events[-1]
        self.assertIsInstance(done, AssistantDone)
        self.assertEqual(done.message.stop_reason, StopReason.TOOL_USE)
        self.assertIsInstance(done.message.content[0], ToolCall)

    async def test_http_error_becomes_assistant_error(self) -> None:
        """401 不应该抛异常穿透循环，而应该变成可展示的错误事件。"""
        async def handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(
                401,
                json={"error": {"message": "invalid api key"}},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            model = DeepSeekModel(api_key="bad-key", base_url=BASE_URL, client=client)
            events = [event async for event in model.stream(_request())]

        self.assertEqual(len(events), 1)
        error_event = events[0]
        self.assertIsInstance(error_event, AssistantError)
        self.assertEqual(error_event.message.stop_reason, StopReason.ERROR)
        self.assertIn("HTTP 401", error_event.message.error_message or "")
        self.assertIn("DEEPSEEK_API_KEY", error_event.message.error_message or "")

    async def test_cancel_before_request_does_not_call_network(self) -> None:
        """请求发出前已经取消，就不应该浪费一次网络调用。"""
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, content=b"")

        token = CancellationToken()
        token.cancel()
        request = ModelRequest(
            messages=[UserMessage(content="工单")],
            tools=[],
            model="deepseek-flash",
            cancellation=token,
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            model = DeepSeekModel(api_key="test-key", base_url=BASE_URL, client=client)
            events = [event async for event in model.stream(request)]

        self.assertEqual(calls, [])
        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], AssistantError)
        self.assertEqual(events[0].message.stop_reason, StopReason.ABORTED)

    async def test_malformed_tool_arguments_become_error(self) -> None:
        """工具参数不是合法 JSON 时，要明确报错，不能把坏参数交给执行层。"""
        async def handler(request: httpx.Request) -> httpx.Response:
            del request
            body = _sse(
                _chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-bad",
                                "function": {
                                    "name": "slow_log_query",
                                    "arguments": '{"instance_id":',
                                },
                            }
                        ]
                    },
                    finish_reason="tool_calls",
                )
            )
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            model = DeepSeekModel(api_key="test-key", base_url=BASE_URL, client=client)
            events = [event async for event in model.stream(_request())]

        self.assertIsInstance(events[-1], AssistantError)
        self.assertIn("不是合法 JSON", events[-1].message.error_message or "")


class DeepSeekPayloadTests(unittest.TestCase):
    """不联网，只验证“平台消息/工具 -> OpenAI 格式”的转换。"""

    def test_messages_and_tools_are_converted(self) -> None:
        tool = ToolSpec(
            name="slow_log_query",
            label="慢日志查询",
            description="查询慢日志",
            parameters={
                "type": "object",
                "properties": {"instance_id": {"type": "string"}},
                "required": ["instance_id"],
            },
            handler=_noop_handler,
        )
        messages = [
            SystemMessage(content="你是 DBA 助手"),
            UserMessage(content=[TextContent(text="工单：查询变慢")]),
            AssistantMessage(
                content=[
                    TextContent(text="我先查慢日志。"),
                    ToolCall(
                        id="call-1",
                        name="slow_log_query",
                        arguments={"instance_id": "db-mysql-001"},
                    ),
                ],
                stop_reason=StopReason.TOOL_USE,
            ),
            ToolResultMessage(
                tool_call_id="call-1",
                tool_name="slow_log_query",
                content=[TextContent(text='{"rows":[]}')],
            ),
        ]
        model = DeepSeekModel(
            api_key="test-key",
            base_url=BASE_URL,
            model="deepseek-flash",
        )
        payload = model.build_payload(_request(messages=messages, tools=[tool]))

        self.assertEqual(payload["messages"][0], {"role": "system", "content": "你是 DBA 助手"})
        self.assertEqual(payload["messages"][1], {"role": "user", "content": "工单：查询变慢"})

        assistant = payload["messages"][2]
        self.assertEqual(assistant["role"], "assistant")
        self.assertEqual(assistant["content"], "我先查慢日志。")
        self.assertEqual(len(assistant["tool_calls"]), 1)
        self.assertEqual(assistant["tool_calls"][0]["id"], "call-1")
        self.assertEqual(assistant["tool_calls"][0]["type"], "function")
        self.assertEqual(
            json.loads(assistant["tool_calls"][0]["function"]["arguments"]),
            {"instance_id": "db-mysql-001"},
        )

        tool_message = payload["messages"][3]
        self.assertEqual(tool_message["role"], "tool")
        self.assertEqual(tool_message["tool_call_id"], "call-1")
        self.assertEqual(tool_message["content"], '{"rows":[]}')

        self.assertEqual(
            payload["tools"],
            [
                {
                    "type": "function",
                    "function": {
                        "name": "slow_log_query",
                        "description": "查询慢日志",
                        "parameters": tool.parameters,
                    },
                }
            ],
        )
        self.assertEqual(payload["tool_choice"], "auto")


class DeepSeekConfigTests(unittest.TestCase):
    def test_missing_api_key_has_clear_error(self) -> None:
        """没有 Key 时要在构造阶段报错，不能等到发请求才收到 401。"""
        with patch.dict(
            os.environ,
            {"DEEPSEEK_API_KEY": "", "SUPPORT_PLATFORM_DEEPSEEK_API_KEY": ""},
        ):
            with self.assertRaises(ModelConfigurationError) as context:
                DeepSeekModel()
        self.assertIn("DEEPSEEK_API_KEY", str(context.exception))

    def test_sse_parser_handles_comments_and_data(self) -> None:
        """SSE 注释行和空行要被忽略，只返回真正的 JSON 数据。"""
        self.assertEqual(_parse_sse_data('data: {"a":1}'), '{"a":1}')
        self.assertIsNone(_parse_sse_data(": keep-alive"))
        self.assertIsNone(_parse_sse_data("event: message"))
        self.assertIsNone(_parse_sse_data(""))


if __name__ == "__main__":
    unittest.main()
