"""真实 MCP 接入的自动化测试。

这组测试和以前的“工具函数测试”不同：它真的会：

1. 启动独立的 MCP Server 进程；
2. 完成 `initialize` 握手；
3. 调用 `tools/list` 发现工具；
4. 用 `tools/call` 执行工具；
5. 检查 `mcp_protocol.jsonl` 里的原始 JSON-RPC；
6. 分别覆盖 stdio、Streamable HTTP、旧版 HTTP+SSE 三种传输。

数据仍然是 fixtures 模拟数据，但协议、进程边界和传输都是真实的。

## 为什么 HTTP/SSE 测试要自己挑端口

固定端口（例如 8765）很容易被上一次没退干净的进程占用，导致测试时好时坏。
这里让操作系统分配一个空闲端口，再把它传给 MCP Server。
测试结束一定会 `terminate` 子进程，避免留下孤儿进程。

## 学习提示

- `asyncio.create_subprocess_exec` 启动的是独立进程，不是线程；
- `await process.wait()` 等进程退出，`terminate()` 发 SIGTERM；
- `async with DbaMcpClient(...)` 负责关闭 MCP 会话和传输；
- SSE 响应不是普通 JSON，而是 `event:` / `data:` 组成的事件流，
  原始测试会直接读取这些行，确认服务端真的用了 SSE。
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import tempfile
import unittest
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from app.mcp.client import DbaMcpClient, McpClientConfig
from app.mcp.dba_tools import business_tool_declarations

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    """让操作系统分配一个当前空闲的 TCP 端口。

    例子：绑定 `127.0.0.1:0` 时，端口 0 表示“随便给我一个”；
    `getsockname()` 返回实际分配到的端口。关闭 socket 后端口释放，
    紧接着启动的服务通常能拿到它。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _wait_for_port(port: int, timeout_s: float = 10.0) -> None:
    """轮询 TCP 端口，直到 MCP Server 开始监听。

    为什么不直接用 `sleep(1)`：
    机器快的时候浪费时间，机器慢的时候一秒又不够。轮询能兼顾速度和稳定性。
    """
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            await asyncio.sleep(0.05)
            continue
        del reader
        writer.close()
        await writer.wait_closed()
        return
    raise TimeoutError(f"MCP Server 在 {timeout_s} 秒内没有监听端口 {port}")


@asynccontextmanager
async def _running_http_server(
    transport: str,
    port: int,
    log_path: Path,
) -> AsyncIterator[None]:
    """启动一个 HTTP/SSE 模式的 MCP Server 子进程，退出时确保回收。

    参数：
    - `transport`：`streamable-http` 或 `sse`；
    - `port`：监听端口；
    - `log_path`：服务端 stderr 写到文件，测试失败时可以打开排查。

    为什么 stderr 写文件而不是 PIPE：
    如果测试不读 PIPE，日志多了可能把管道缓冲区写满，导致子进程卡住。
    写文件没有这个问题。
    """
    log_file = log_path.open("wb")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "app.mcp.server",
        "--transport",
        transport,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        cwd=str(PROJECT_ROOT),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=log_file,
    )
    try:
        await _wait_for_port(port)
        yield
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                # 子进程不响应 SIGTERM 时兜底，避免测试进程退出后还留着服务。
                process.kill()
                await process.wait()
        log_file.close()


def _parse_sse_json(text: str) -> dict:
    """从 SSE 响应文本里取出第一条 JSON 消息。

    Streamable HTTP 返回的内容长这样：

    ```text
    event: message
    data: {"jsonrpc":"2.0","id":1,"result":{...}}

    ```

    `data:` 可能跨多行；按 SSE 规范，多行 data 用换行拼起来。
    这里当前每个事件只有一行，但解析逻辑仍然按规范处理。
    """
    event_lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("data:"):
            event_lines.append(line[5:].lstrip())
        elif not line.strip() and event_lines:
            return json.loads("\n".join(event_lines))
    if event_lines:
        return json.loads("\n".join(event_lines))
    raise AssertionError(f"响应里没有 SSE data 帧：{text[:500]}")


class StdioMcpTests(unittest.IsolatedAsyncioTestCase):
    """stdio：Agent 启动 MCP 子进程，用 stdin/stdout 传 JSON-RPC。"""

    async def test_handshake_list_tools_call_and_trace(self) -> None:
        """一次真实 stdio 调查应完成握手、发现工具、调用工具并留下协议追踪。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            trace_path = Path(temp_dir) / "mcp_protocol.jsonl"
            config = McpClientConfig(
                transport="stdio",
                trace_path=trace_path,
                timeout_s=10,
            )
            async with DbaMcpClient(config) as client:
                self.assertIsNotNone(client.info)
                info = client.info
                assert info is not None  # 给类型检查器和读者一个明确信号

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
                self.assertTrue(info.protocol_version)

                specs = {spec.name: spec for spec in client.tool_specs()}
                self.assertEqual(specs["slow_log_query"].label, "慢日志查询")
                # 注解来自 MCP tools/list，不是本地手写死的。
                self.assertTrue(specs["slow_log_query"].annotations["readOnlyHint"])

                result = await client.call_tool(
                    "slow_log_query",
                    {
                        "instance_id": "db-mysql-001",
                        "start_time": "2026-10-29T10:00:00+08:00",
                        "end_time": "2026-10-29T11:00:00+08:00",
                        "limit": 50,
                    },
                )
                self.assertFalse(result.is_error)
                self.assertEqual(len(result.content[0].text), 16658)
                self.assertEqual(result.details["top"]["sql_fingerprint"], "a1b2c3d4e5f6")

                failed = await client.call_tool(
                    "instance_metrics",
                    {
                        "instance_id": "db-mysql-001",
                        "start_time": "2026-10-29T10:00:00+08:00",
                        "end_time": "2026-10-29T11:00:00+08:00",
                    },
                )
                self.assertTrue(failed.is_error)
                self.assertIn("监控接口超时", failed.content[0].text)

            records = [
                json.loads(line)
                for line in trace_path.read_text(encoding="utf-8").splitlines()
            ]
            methods = [
                record["message"].get("method")
                for record in records
                if record["direction"] == "send"
            ]
            self.assertIn("initialize", methods)
            self.assertIn("notifications/initialized", methods)
            self.assertIn("tools/list", methods)
            self.assertIn("tools/call", methods)
            self.assertTrue(any(record["direction"] == "recv" for record in records))

    async def test_tool_schema_matches_static_capability_catalog(self) -> None:
        """MCP `tools/list` 的字段名和必填项，要与静态能力目录一致。

        为什么不要求两份 Schema 完全相同：
        FastMCP 会自动补充 `title` 等展示字段，静态目录不需要这些；
        真正影响模型调用的是属性名、类型和 required。

        注意：默认数据集是旧慢 SQL 数据，Server 只注册 4 个慢 SQL 工具；
        静态目录则包含全部 20 个工具（4 个旧工具 + 16 个场景工具）。
        所以这里只比较当前 Server 真正暴露的工具。
        """
        legacy_names = {
            "slow_log_query",
            "execution_plan",
            "instance_metrics",
            "lock_wait_query",
        }
        static = {
            item["name"]: item
            for item in business_tool_declarations()
            if item["name"] in legacy_names
        }
        async with DbaMcpClient(McpClientConfig(transport="stdio", timeout_s=10)) as client:
            live = {spec.name: spec for spec in client.tool_specs()}

        self.assertEqual(set(static), set(live))
        for name, declaration in static.items():
            static_schema = declaration["parameters"]
            live_schema = live[name].parameters
            self.assertEqual(
                set(static_schema["properties"]),
                set(live_schema["properties"]),
                f"{name} 的参数名不一致",
            )
            self.assertEqual(
                set(static_schema["required"]),
                set(live_schema["required"]),
                f"{name} 的必填参数不一致",
            )

    async def test_scenario_datasets_expose_only_their_tools(self) -> None:
        """每个场景数据集只注册自己用到的工具，Schema 与静态目录一致。

        为什么值得逐个启动 Server：
        场景工具是动态注册的（`scenario_registry.py` 根据数据集里的
        `tool_results` 生成 FastMCP 工具签名）。如果某个数据集的参数名写错，
        只有真正 `tools/list` 才能发现；只测一个数据集覆盖不到全部数据库。
        """
        datasets = {
            "mysql_row_lock_001.json": {"mysql_innodb_status", "mysql_processlist"},
            "pg_vacuum_bloat_001.json": {"pg_activity", "pg_table_stats"},
            "pg_ddl_lock_001.json": {"pg_locks", "pg_activity"},
            "redis_hot_key_001.json": {
                "redis_key_stats",
                "redis_info",
                "redis_slowlog",
            },
            "redis_memory_eviction_001.json": {
                "redis_info",
                "redis_key_stats",
                "redis_slowlog",
            },
            "mongo_collscan_001.json": {
                "mongo_profiler",
                "mongo_explain",
                "mongo_server_status",
            },
            "mongo_replica_lag_001.json": {
                "mongo_rs_status",
                "mongo_server_status",
            },
            "es_disk_watermark_001.json": {
                "es_cluster_health",
                "es_shard_allocation",
            },
            "clickhouse_too_many_parts_001.json": {
                "clickhouse_query_log",
                "clickhouse_parts",
            },
        }
        static = {item["name"]: item for item in business_tool_declarations()}

        for filename, expected in datasets.items():
            with self.subTest(dataset=filename):
                config = McpClientConfig(
                    transport="stdio",
                    dataset_path=PROJECT_ROOT / "fixtures" / "mock_dba" / filename,
                    timeout_s=10,
                )
                async with DbaMcpClient(config) as client:
                    live = {spec.name: spec for spec in client.tool_specs()}

                self.assertEqual(set(live), expected)
                for name in expected:
                    static_schema = static[name]["parameters"]
                    live_schema = live[name].parameters
                    self.assertEqual(
                        set(static_schema["properties"]),
                        set(live_schema["properties"]),
                        f"{filename}: {name} 的参数名不一致",
                    )
                    self.assertEqual(
                        set(static_schema["required"]),
                        set(live_schema["required"]),
                        f"{filename}: {name} 的必填参数不一致",
                    )

    async def test_unknown_tool_returns_error_result(self) -> None:
        """工具名不存在时，Server 应返回 `isError=true`，客户端不能当成正常结果。

        注意：FastMCP 把“未知工具”当作工具调用错误返回，而不是 JSON-RPC
        协议错误。对 Agent 来说这更友好：模型能看到错误说明并换一个工具，
        整个任务不会崩掉。真正的协议/连接错误才会抛 `ToolExecutionError`。
        """
        async with DbaMcpClient(McpClientConfig(transport="stdio", timeout_s=10)) as client:
            result = await client.call_tool("not_a_real_tool", {})
        self.assertTrue(result.is_error)
        self.assertIn("not_a_real_tool", result.content[0].text)


class HttpTransportTests(unittest.IsolatedAsyncioTestCase):
    """HTTP 系列传输：Streamable HTTP（推荐）和旧版 HTTP+SSE。"""

    async def test_streamable_http_client_can_call_tool(self) -> None:
        """Streamable HTTP 客户端应能连接 `/mcp` 并完成工具调用。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            port = _free_port()
            log_path = Path(temp_dir) / "server.log"
            async with _running_http_server("streamable-http", port, log_path):
                config = McpClientConfig(
                    transport="streamable-http",
                    url=f"http://127.0.0.1:{port}/mcp",
                    timeout_s=10,
                )
                async with DbaMcpClient(config) as client:
                    info = client.info
                    assert info is not None
                    self.assertTrue(info.session_id, "Streamable HTTP 应返回 Mcp-Session-Id")
                    result = await client.call_tool(
                        "lock_wait_query",
                        {
                            "instance_id": "db-mysql-001",
                            "start_time": "2026-10-29T10:00:00+08:00",
                            "end_time": "2026-10-29T11:00:00+08:00",
                        },
                    )
                    self.assertFalse(result.is_error)
                    self.assertEqual(result.details["status"], "no_data")

    async def test_legacy_sse_client_can_call_tool(self) -> None:
        """旧版 SSE 传输应能连接 `/sse`，再把请求 POST 到 `/messages/`。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            port = _free_port()
            log_path = Path(temp_dir) / "server.log"
            async with _running_http_server("sse", port, log_path):
                config = McpClientConfig(
                    transport="sse",
                    url=f"http://127.0.0.1:{port}/sse",
                    timeout_s=10,
                )
                async with DbaMcpClient(config) as client:
                    result = await client.call_tool(
                        "execution_plan",
                        {
                            "instance_id": "db-mysql-001",
                            "sql_fingerprint": "a1b2c3d4e5f6",
                        },
                    )
                    self.assertFalse(result.is_error)
                    self.assertTrue(result.details["full_scan"])
                    self.assertEqual(result.details["access_type"], "ALL")

    async def test_streamable_http_raw_json_rpc_and_sse_frames(self) -> None:
        """不经过 MCP SDK 客户端，直接看 HTTP 请求和 SSE 响应长什么样。

        这一步最接近“手写 MCP 客户端”：
        1. POST `initialize`，响应 Content-Type 是 `text/event-stream`；
        2. 从响应头拿 `mcp-session-id`；
        3. POST `notifications/initialized`；
        4. 带 session id POST `tools/list`，从 SSE `data:` 帧里读 JSON-RPC 结果。
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            port = _free_port()
            log_path = Path(temp_dir) / "server.log"
            async with _running_http_server("streamable-http", port, log_path):
                url = f"http://127.0.0.1:{port}/mcp"
                headers = {
                    "Accept": "application/json, text/event-stream",
                    "Content-Type": "application/json",
                }
                async with httpx.AsyncClient(timeout=10) as http:
                    init_response = await http.post(
                        url,
                        headers=headers,
                        json={
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "protocolVersion": "2025-06-18",
                                "capabilities": {},
                                "clientInfo": {"name": "raw-test", "version": "0.1.0"},
                            },
                        },
                    )
                    self.assertEqual(init_response.status_code, 200)
                    self.assertIn(
                        "text/event-stream",
                        init_response.headers.get("content-type", ""),
                    )
                    init_message = _parse_sse_json(init_response.text)
                    self.assertEqual(init_message["id"], 1)
                    self.assertEqual(
                        init_message["result"]["serverInfo"]["name"],
                        "support-platform-dba-mcp",
                    )

                    session_id = init_response.headers.get("mcp-session-id")
                    self.assertTrue(session_id)
                    session_headers = {**headers, "mcp-session-id": session_id}
                    initialized = await http.post(
                        url,
                        headers=session_headers,
                        json={
                            "jsonrpc": "2.0",
                            "method": "notifications/initialized",
                        },
                    )
                    self.assertIn(initialized.status_code, {200, 202, 204})

                    listed = await http.post(
                        url,
                        headers=session_headers,
                        json={
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "tools/list",
                            "params": {},
                        },
                    )
                    self.assertEqual(listed.status_code, 200)
                    listed_message = _parse_sse_json(listed.text)
                    names = {
                        tool["name"] for tool in listed_message["result"]["tools"]
                    }
                    self.assertIn("slow_log_query", names)
                    # 平台工具 search_tools/evidence_read 不走 MCP；它们是
                    # Agent 运行时依赖本地会话状态的能力，所以不应出现在这里。
                    self.assertNotIn("search_tools", names)
                    self.assertNotIn("evidence_read", names)


class ServerArgumentsTests(unittest.TestCase):
    """MCP Server 命令行参数的静态检查。"""

    def test_transport_choices_are_real(self) -> None:
        """CLI 暴露的三种传输必须和 Server/Client 支持的一致。"""
        from app.mcp.server import TRANSPORTS

        self.assertEqual(set(TRANSPORTS), {"stdio", "streamable-http", "sse"})
