"""Pi 风格手写 MCP 客户端的自动化测试。

这组测试和 `tests/test_mcp.py` 的区别：

- `test_mcp.py` 测的是官方 Python SDK 客户端（运行时默认路线）；
- 本文件测的是 `app/mcp/pi_style/` 里手写的 JSON-RPC 状态机（教学路线）。

两条路线连接的是同一个 FastMCP Server、同一组 DBA 工具、同一份 fixtures。
最重要的断言不是“能跑”，而是：

```text
官方客户端结果 == Pi 风格客户端结果
```

如果哪天两条路线对同一个 `tools/call` 返回了不同的文本或 details，
说明其中一条对协议的理解出了问题。测试会立刻暴露。

## 学习提示

- `IsolatedAsyncioTestCase` 会给每个测试创建独立事件循环；
- `async with` 退出时客户端会回收子进程/HTTP 连接；
- HTTP 测试自己挑空闲端口，避免 8765 被占用导致偶发失败；
- 原始 JSON-RPC 追踪写在临时目录，测试结束自动删除。
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from app.harness.tools import ToolExecutionError
from app.mcp.client import DbaMcpClient, McpClientConfig
from app.mcp.pi_style import PiStyleMcpClient
from tests.test_mcp import _free_port, _running_http_server

# 这些参数在多个测试里重复出现，提出来避免复制粘贴时漏字段。
SLOW_LOG_ARGS = {
    "instance_id": "db-mysql-001",
    "start_time": "2026-10-29T10:00:00+08:00",
    "end_time": "2026-10-29T11:00:00+08:00",
    "limit": 50,
}
METRICS_ARGS = {
    "instance_id": "db-mysql-001",
    "start_time": "2026-10-29T10:00:00+08:00",
    "end_time": "2026-10-29T11:00:00+08:00",
}
LOCK_WAIT_ARGS = {
    "instance_id": "db-mysql-001",
    "start_time": "2026-10-29T10:00:00+08:00",
    "end_time": "2026-10-29T11:00:00+08:00",
}


class StdioPiStyleTests(unittest.IsolatedAsyncioTestCase):
    """stdio 模式：手写客户端启动子进程并完成真实 JSON-RPC 往返。"""

    async def test_handshake_list_tools_call_and_trace(self) -> None:
        """握手、发现工具、调用工具、协议追踪都要能跑通。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            trace_path = Path(temp_dir) / "pi_protocol.jsonl"
            config = McpClientConfig(
                transport="stdio",
                timeout_s=10,
                trace_path=trace_path,
            )

            async with PiStyleMcpClient(config) as client:
                info = client.info
                assert info is not None
                self.assertEqual(info.server_name, "support-platform-dba-mcp")
                self.assertEqual(info.transport, "stdio")
                self.assertEqual(
                    set(info.tools),
                    {
                        "slow_log_query",
                        "execution_plan",
                        "instance_metrics",
                        "lock_wait_query",
                    },
                )
                self.assertEqual(info.protocol_version, "2025-11-25")

                specs = {spec.name: spec for spec in client.tool_specs()}
                self.assertEqual(specs["slow_log_query"].label, "慢日志查询")
                self.assertTrue(specs["slow_log_query"].annotations["readOnlyHint"])

                result = await client.call_tool("slow_log_query", SLOW_LOG_ARGS)
                self.assertFalse(result.is_error)
                self.assertEqual(len(result.content[0].text), 16658)
                self.assertEqual(
                    result.details["top"]["sql_fingerprint"],
                    "a1b2c3d4e5f6",
                )

                failed = await client.call_tool("instance_metrics", METRICS_ARGS)
                self.assertTrue(failed.is_error)
                self.assertIn("监控接口超时", failed.content[0].text)

            records = [
                json.loads(line)
                for line in trace_path.read_text(encoding="utf-8").splitlines()
            ]
            sent_methods = [
                record["message"].get("method")
                for record in records
                if record["direction"] == "send"
            ]
            self.assertIn("initialize", sent_methods)
            self.assertIn("notifications/initialized", sent_methods)
            self.assertIn("tools/list", sent_methods)
            self.assertIn("tools/call", sent_methods)
            self.assertTrue(any(record["direction"] == "recv" for record in records))

            # 追踪里的请求 id 必须能对上响应 id，这是手写状态机最核心的检查。
            sent_ids = {
                record["message"]["id"]
                for record in records
                if record["direction"] == "send" and "id" in record["message"]
            }
            recv_ids = {
                record["message"]["id"]
                for record in records
                if record["direction"] == "recv"
                and "id" in record["message"]
                and ("result" in record["message"] or "error" in record["message"])
            }
            self.assertTrue(sent_ids)
            self.assertEqual(sent_ids, recv_ids)

    async def test_matches_official_client_for_same_tool_call(self) -> None:
        """两条客户端路线必须对同一工具调用返回相同文本和 details。"""
        async with DbaMcpClient(McpClientConfig(transport="stdio", timeout_s=10)) as official:
            official_result = await official.call_tool("slow_log_query", SLOW_LOG_ARGS)

        async with PiStyleMcpClient(McpClientConfig(transport="stdio", timeout_s=10)) as pi_style:
            pi_result = await pi_style.call_tool("slow_log_query", SLOW_LOG_ARGS)

        self.assertEqual(official_result.is_error, pi_result.is_error)
        self.assertEqual(
            official_result.content[0].text,
            pi_result.content[0].text,
        )
        self.assertEqual(official_result.details, pi_result.details)

    async def test_unknown_tool_returns_error_result(self) -> None:
        """未知工具应返回 `isError=true`，而不是让客户端崩溃。"""
        async with PiStyleMcpClient(McpClientConfig(transport="stdio", timeout_s=10)) as client:
            result = await client.call_tool("not_a_real_tool", {})
        self.assertTrue(result.is_error)
        self.assertIn("not_a_real_tool", result.content[0].text)


class HttpPiStyleTests(unittest.IsolatedAsyncioTestCase):
    """Streamable HTTP 模式：POST + SSE + Mcp-Session-Id。"""

    async def test_streamable_http_call_and_session_id(self) -> None:
        """手写 HTTP 传输应拿到 session id，并能从 SSE 帧里读回结果。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            port = _free_port()
            log_path = Path(temp_dir) / "server.log"
            trace_path = Path(temp_dir) / "pi_protocol.jsonl"
            async with _running_http_server("streamable-http", port, log_path):
                config = McpClientConfig(
                    transport="streamable-http",
                    url=f"http://127.0.0.1:{port}/mcp",
                    timeout_s=10,
                    trace_path=trace_path,
                )
                async with PiStyleMcpClient(config) as client:
                    info = client.info
                    assert info is not None
                    self.assertTrue(info.session_id, "Streamable HTTP 应返回 Mcp-Session-Id")

                    result = await client.call_tool("lock_wait_query", LOCK_WAIT_ARGS)
                    self.assertFalse(result.is_error)
                    self.assertEqual(result.details["status"], "no_data")

            records = [
                json.loads(line)
                for line in trace_path.read_text(encoding="utf-8").splitlines()
            ]
            # 原始追踪里能看到 initialize、initialized、tools/list 和 tools/call。
            methods = [
                record["message"].get("method")
                for record in records
                if record["direction"] == "send"
            ]
            self.assertIn("initialize", methods)
            self.assertIn("notifications/initialized", methods)
            self.assertIn("tools/list", methods)
            self.assertIn("tools/call", methods)


class PiStyleTransportSelectionTests(unittest.TestCase):
    """传输选择的静态检查。"""

    def test_legacy_sse_is_rejected(self) -> None:
        """Pi 本身不支持旧版 SSE，我们的教学客户端也应明确拒绝。"""
        with self.assertRaises(ValueError) as context:
            PiStyleMcpClient(McpClientConfig(transport="sse", timeout_s=10))
        self.assertIn("旧版 SSE", str(context.exception))

    def test_streamable_http_endpoint_requires_url_or_host_port(self) -> None:
        """HTTP 配置没有 URL 时应按 host/port 自动拼 `/mcp`。"""
        config = McpClientConfig(transport="streamable-http", host="127.0.0.1", port=9999)
        self.assertEqual(config.endpoint, "http://127.0.0.1:9999/mcp")


class _SilentToolCallTransport:
    """测试专用传输：握手和 tools/list 正常返回，但 tools/call 永远不回复。

    这样可以在不依赖真实服务端耗时的情况下，稳定复现“客户端调用超时”。
    它实现的是 `PiTransport` 协议要求的 start/send/receive/close，
    不需要继承任何基类——这正是 Python `Protocol` 的“结构化类型”思想。
    """

    session_id = None

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self._inbox: asyncio.Queue[dict | None] = asyncio.Queue()

    async def start(self) -> None:
        return None

    async def send(self, message: dict) -> None:
        self.sent.append(message)
        method = message.get("method")
        if method == "initialize":
            await self._inbox.put(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {"name": "fake-server", "version": "0.1.0"},
                    },
                }
            )
        elif method == "tools/list":
            await self._inbox.put(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {
                        "tools": [
                            {
                                "name": "slow_log_query",
                                "description": "fake",
                                "inputSchema": {"type": "object"},
                            }
                        ]
                    },
                }
            )
        # tools/call 故意不回复，让 request() 走到超时分支。

    async def receive(self):
        while True:
            item = await self._inbox.get()
            if item is None:
                return
            yield item

    def set_protocol_version(self, version: str) -> None:
        return None

    async def close(self) -> None:
        await self._inbox.put(None)


class PiStyleTimeoutTests(unittest.IsolatedAsyncioTestCase):
    """超时和取消行为。"""

    async def test_timeout_raises_tool_execution_error_and_sends_cancel(self) -> None:
        """单次调用超时后应抛 `ToolExecutionError`，并尽力发送取消通知。

        这个测试不启动真实子进程，而是替换传输层。这样超时是确定性的，
        不会因为机器快慢而偶发失败；同时还能检查客户端确实发了
        `notifications/cancelled`。
        """
        fake = _SilentToolCallTransport()
        client = PiStyleMcpClient(McpClientConfig(transport="stdio", timeout_s=0.2))
        # 实例属性会遮蔽类方法，测试里用它注入假传输。
        client._build_transport = lambda: fake  # type: ignore[method-assign]

        async with client:
            with self.assertRaises(ToolExecutionError):
                await client.call_tool("slow_log_query", {})

        methods = [message.get("method") for message in fake.sent]
        self.assertIn("notifications/cancelled", methods)
        cancelled = next(
            message for message in fake.sent
            if message.get("method") == "notifications/cancelled"
        )
        self.assertEqual(cancelled["params"]["reason"], "timeout")


if __name__ == "__main__":
    unittest.main()
