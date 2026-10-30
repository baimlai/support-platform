"""真实的 MCP Client：Agent 通过 JSON-RPC 调用 MCP Server。

## 这一层解决什么问题

以前的工具是“本地 Python 函数”，Agent 循环直接调用 `ToolSpec.handler`。
现在业务工具住在独立 MCP Server 里，Agent 必须先：

1. 建立传输连接（stdio / Streamable HTTP / SSE）；
2. 发送 `initialize`，协商协议版本和能力；
3. 发送 `notifications/initialized`；
4. 用 `tools/list` 发现工具；
5. 用 `tools/call` 执行工具；
6. 把 MCP 的 `CallToolResult` 翻译成平台已有的 `ToolResult`。

对内核来说，`ToolSpec.handler` 的形状没有变；变化发生在 handler 内部：
它不再执行本地函数，而是走 MCP 协议。

## 三种传输

```text
stdio              Agent 启动 python -m app.mcp.server 子进程，用 stdin/stdout 传 JSON-RPC
streamable-http    HTTP POST /mcp；服务端可以用 SSE 推送多条消息（当前推荐）
sse                旧版 HTTP + SSE：GET /sse 建事件流，POST /messages/ 发请求
```

三者跑的是同一套 JSON-RPC 消息。学习时建议先看 `mcp_protocol.jsonl`：
里面有真实的 `initialize`、`tools/list`、`tools/call` 请求和响应。

## 对比教学：Pi、官方 SDK、本项目

同一个 MCP，至少有三种“看法”：

1. **MCP 规范**：定义 JSON-RPC 方法、消息字段、传输和生命周期。
   它不属于某一家 SDK，任何实现只要遵守规范就能互通。
2. **官方 SDK**：官方维护的参考实现，客户端、服务端、OAuth、多种传输都提供。
   优点是省事、跟随规范更新；缺点是依赖面大，内部行为不一定完全可控。
3. **Pi 的 `@earendil-works/pi-mcp`**：Pi 自己写的一份轻客户端。
   README 明确说它不依赖官方 SDK；OAuth 子模块则是从官方 TypeScript SDK
   v1.29.0 改编并保留许可证。它只做 stdio + Streamable HTTP，明确拒绝旧版 SSE，
   自己控制进程组清理、重连、`Last-Event-ID` 续传等生命周期。

Pi 为什么自己写：依赖小、协议面可控、生命周期能精确掌握、内部 API 稳定、
方便做 in-memory 测试。代价是要自己维护 JSON-RPC 状态机并持续跟规范。

本项目为什么用官方 Python SDK：这是教学项目，重点是把 MCP 协议、工具发现、
工具调用和三种传输跑通，而不是重写一套协议库。官方 SDK 仍然走真实 JSON-RPC，
我们额外用 `mcp_protocol.jsonl` 把原始消息记下来，学习时可以直接对照。

**结论：自己写 SDK 和用官方 SDK 都能是“真 MCP”；区别是实现成本和可控性，
不是协议真假。**

## Python 语法提示

- `AsyncExitStack` 可以按“后进先出”的顺序关闭多个异步上下文，
  适合管理“传输 → ClientSession”这种嵌套资源。
- `async with` 进入时调用 `__aenter__`，退出时调用 `__aexit__`；
  即使中间抛异常，退出也会执行，所以连接和子进程能被清理。
- `anyio` 是 MCP SDK 使用的异步 IO 库；`MemoryObjectReceiveStream` 是它提供的
  内存通道，MCP 的传输层和会话层通过它传递 `SessionMessage`。
- `shlex.split("uv run python -m app.mcp.server")` 会按 shell 规则切成列表，
  比直接 `str.split()` 更安全，能正确处理引号。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import sys
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import McpError
from mcp.shared.message import SessionMessage
from mcp.types import CallToolResult, Implementation
from mcp.types import TextContent as McpTextContent

from app.harness.tools import ToolExecutionError
from app.mcp import dba_tools
from app.mcp.info import McpClientInfo
from app.runtime.contracts import CancellationToken, TextContent, ToolResult, ToolSpec

logger = logging.getLogger(__name__)

# MCP Client 自己的身份。Server 在 initialize 时能看到它，
# 方便在服务端日志里区分“是谁连过来的”。
CLIENT_NAME = "support-platform-agent"
CLIENT_VERSION = "0.1.0"


class McpTransport(StrEnum):
    """MCP 支持的传输方式。

    用 `StrEnum` 而不是普通字符串，是为了避免拼写错误：
    `McpTransport.STDIO == "stdio"` 成立，同时 IDE 能提示可选值。
    """

    STDIO = "stdio"
    STREAMABLE_HTTP = "streamable-http"
    SSE = "sse"


class McpClientError(RuntimeError):
    """MCP 连接、初始化或工具调用失败。

    例子：stdio 子进程启动失败、HTTP 服务没起来、initialize 超时。
    这类错误通常意味着“工具层不可用”，不是某一条 SQL 查询失败。
    """


@dataclass(slots=True)
class McpClientConfig:
    """创建 MCP Client 所需的配置。

    - `transport`：stdio / streamable-http / sse；
    - `url`：HTTP/SSE 地址；不传时按 host/port 自动拼；
    - `command`：stdio 启动命令；不传时用当前 Python 解释器启动 `app.mcp.server`；
    - `dataset_path`：传给 MCP Server 的模拟数据文件；
    - `timeout_s`：连接、初始化和单次工具调用的超时；
    - `trace_path`：可选，把真实 JSON-RPC 消息写成 JSONL，便于学习协议。
    """

    transport: str = "stdio"
    url: str | None = None
    command: str | None = None
    host: str = "127.0.0.1"
    port: int = 8765
    dataset_path: Path | str | None = None
    timeout_s: float = 30.0
    trace_path: Path | str | None = None
    project_root: Path = field(default_factory=lambda: dba_tools.PROJECT_ROOT)

    def __post_init__(self) -> None:
        """规范化并校验配置。

        例子：环境变量里写成 `" Streamable-HTTP "`，这里统一成
        `"streamable-http"`；写错则立刻报错，避免连接阶段才出现难懂的异常。
        """
        normalized = self.transport.strip().lower()
        if normalized not in {item.value for item in McpTransport}:
            raise ValueError(
                f"不支持的 MCP transport：{self.transport!r}；"
                f"可选值：{', '.join(item.value for item in McpTransport)}"
            )
        self.transport = normalized
        self.timeout_s = max(0.1, float(self.timeout_s))

    @property
    def endpoint(self) -> str:
        """返回实际使用的 HTTP/SSE 地址；stdio 模式返回启动命令描述。"""
        if self.transport == McpTransport.STDIO:
            return self.command or f"{sys.executable} -m app.mcp.server"
        if self.url:
            return self.url
        path = "/mcp" if self.transport == McpTransport.STREAMABLE_HTTP else "/sse"
        return f"http://{self.host}:{self.port}{path}"

    def stdio_parameters(self) -> StdioServerParameters:
        """构造 stdio 子进程参数。

        `sys.executable` 是当前虚拟环境里的 Python 解释器。用它启动子进程，
        能保证子进程和 Agent 使用同一套依赖（包括 `mcp` SDK）。
        `cwd` 固定为项目根目录，这样 `python -m app.mcp.server` 一定能 import 到 `app`。
        """
        parts = shlex.split(self.command) if self.command else [sys.executable, "-m", "app.mcp.server"]
        if not parts:
            raise ValueError("MCP stdio command 不能为空")
        command, *args = parts

        # 复制当前环境，保留 PATH、代理、虚拟环境等配置。
        # 不要直接修改 os.environ，否则会影响同一个进程里的其它任务。
        env = dict(os.environ)
        env.setdefault("PYTHONUNBUFFERED", "1")
        if self.dataset_path is not None:
            env["SUPPORT_PLATFORM_MCP_DATASET"] = str(self.dataset_path)

        return StdioServerParameters(
            command=command,
            args=args,
            env=env,
            cwd=str(self.project_root),
        )


class McpProtocolTrace:
    """把真实 MCP 协议消息追加到 JSONL 文件。

    这是学习 MCP 最直接的资料：打开 `mcp_protocol.jsonl` 能看到
    `initialize`、`notifications/initialized`、`tools/list`、`tools/call`
    的请求和响应。每行结构：

    ```json
    {"ts": "...", "direction": "send", "message": {"jsonrpc": "2.0", ...}}
    ```

    为什么用 JSONL：一行一条消息，追加写不会破坏前面的内容，
    用 `tail -f` 也能实时观察协议交互。
    """

    def __init__(self, path: Path | str | None) -> None:
        self.path = Path(path) if path is not None else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, direction: str, item: Any) -> None:
        """记录一条收发消息。

        `item` 通常是 `SessionMessage`，里面包着 pydantic 的 `JSONRPCMessage`。
        如果是异常对象或其它值，就跳过；协议追踪不应该影响正常调用。
        """
        if self.path is None or not isinstance(item, SessionMessage):
            return
        try:
            message = item.message.model_dump(by_alias=True, mode="json", exclude_none=True)
            record = {
                "ts": datetime.now(UTC).isoformat(),
                "direction": direction,
                "message": message,
            }
            # 小文件追加写：打开、写一行、关闭。MCP 消息频率很低，
            # 这样写比维护长期文件句柄更简单，也不会在异常时忘记 flush。
            with self.path.open("a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            logger.exception("写入 MCP 协议追踪失败：%s", self.path)


class _TracingReceiveStream:
    """给 MCP 接收流加一层“收到什么就记什么”的代理。

    MCP SDK 的 `ClientSession` 只要求对象支持 `receive()`、`__aiter__`、
    `__aenter__`、`__aexit__`。我们包一层，不改消息内容，只多写一行追踪。
    """

    def __init__(self, stream: Any, trace: McpProtocolTrace) -> None:
        self._stream = stream
        self._trace = trace

    async def receive(self) -> Any:
        item = await self._stream.receive()
        self._trace.record("recv", item)
        return item

    async def __anext__(self) -> Any:
        try:
            return await self.receive()
        except anyio.EndOfStream:
            # `async for` 约定用 StopAsyncIteration 表示“流结束”，
            # 不能把 anyio.EndOfStream 直接漏出去。
            raise StopAsyncIteration from None

    def __aiter__(self) -> _TracingReceiveStream:
        return self

    async def __aenter__(self) -> _TracingReceiveStream:
        await self._stream.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return await self._stream.__aexit__(exc_type, exc, tb)

    async def aclose(self) -> None:
        await self._stream.aclose()


class _TracingSendStream:
    """给 MCP 发送流加一层“发什么就记什么”的代理。"""

    def __init__(self, stream: Any, trace: McpProtocolTrace) -> None:
        self._stream = stream
        self._trace = trace

    async def send(self, item: Any) -> None:
        self._trace.record("send", item)
        await self._stream.send(item)

    async def __aenter__(self) -> _TracingSendStream:
        await self._stream.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return await self._stream.__aexit__(exc_type, exc, tb)

    async def aclose(self) -> None:
        await self._stream.aclose()


class DbaMcpClient:
    """一次运行里的 MCP 连接和工具目录。

    典型用法：

    ```python
    config = McpClientConfig(transport="stdio")
    async with DbaMcpClient(config) as client:
        specs = client.tool_specs()
        result = await client.call_tool("slow_log_query", {...})
    ```

    `async with` 退出时会依次关闭 `ClientSession` 和传输层；
    stdio 模式还会等待子进程退出，避免留下孤儿进程。
    """

    def __init__(self, config: McpClientConfig | None = None) -> None:
        self.config = config or McpClientConfig()
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None
        self._tools: list[Any] = []
        self._get_session_id: Any = None
        self._trace = McpProtocolTrace(self.config.trace_path)
        self.info: McpClientInfo | None = None

    async def __aenter__(self) -> DbaMcpClient:
        """建立连接、握手、发现工具。

        任何一步失败都会清理已经打开的资源，再把异常包成 `McpClientError`。
        例子：HTTP 服务没启动时，`initialize` 会连接失败，调用方只需要处理
        一个明确的“MCP Server 连接失败”，而不是一堆 anyio/httpx 底层异常。
        """
        stack = AsyncExitStack()
        self._stack = stack
        try:
            streams = await stack.enter_async_context(self._transport_context())
            read_stream, write_stream = streams[0], streams[1]
            if len(streams) >= 3:
                # Streamable HTTP 的第三个返回值是 `get_session_id()`。
                # 它是 HTTP 会话 id，会出现在 `Mcp-Session-Id` 响应头里。
                self._get_session_id = streams[2]

            traced_read = _TracingReceiveStream(read_stream, self._trace)
            traced_write = _TracingSendStream(write_stream, self._trace)

            self._session = await stack.enter_async_context(
                ClientSession(
                    traced_read,
                    traced_write,
                    read_timeout_seconds=timedelta(seconds=self.config.timeout_s),
                    client_info=Implementation(name=CLIENT_NAME, version=CLIENT_VERSION),
                )
            )
            init_result = await self._session.initialize()
            self._tools = await self.list_tools()

            server_info = init_result.serverInfo
            self.info = McpClientInfo(
                transport=self.config.transport,
                endpoint=self.config.endpoint,
                server_name=server_info.name,
                server_version=server_info.version,
                protocol_version=str(init_result.protocolVersion),
                instructions=init_result.instructions,
                capabilities=init_result.capabilities.model_dump(
                    by_alias=True,
                    mode="json",
                    exclude_none=True,
                ),
                tools=[tool.name for tool in self._tools],
                session_id=self._get_session_id() if callable(self._get_session_id) else None,
            )
            return self
        except BaseException as exc:
            # `BaseException` 包括 CancelledError/KeyboardInterrupt。
            # 先清理，再把取消原样抛出；其它异常转成可读的 McpClientError。
            await stack.aclose()
            self._stack = None
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            raise McpClientError(
                f"连接 MCP Server 失败（transport={self.config.transport}，"
                f"endpoint={self.config.endpoint}）：{exc}"
            ) from exc

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """关闭会话和传输。

        `AsyncExitStack` 会按“后进先出”的顺序关闭：先 ClientSession，
        再 stdio/HTTP 传输。这样不会出现“会话还在读，管道已经关了”的竞态。

        对比教学：Pi 的 stdio 传输会显式执行“关 stdin → SIGTERM → SIGKILL”，
        并且杀整个进程组，目的也是避免 `npx` / `uvx` 这类包装进程留下子进程。
        我们用官方 SDK，清理细节由 SDK 负责；当前是本地模拟服务，够用。
        如果以后接真实长期运行的 MCP 服务，应该补进程组和重连测试。
        """
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None
        self._session = None

    def _transport_context(self) -> Any:
        """按配置返回一个异步上下文管理器。

        对比教学：
        - Pi 只实现 stdio 和 Streamable HTTP，并在文档里明确拒绝 legacy SSE；
        - 官方 Python SDK 三种都提供；
        - 本项目默认 stdio，生产远程推荐 Streamable HTTP，把 legacy SSE
          保留为“旧版兼容 / 观察事件流”的教学模式。

        三个客户端函数返回的元组形状略有不同：
        - stdio / sse：`(read, write)`；
        - streamable-http：`(read, write, get_session_id)`。
        调用方用下标取前两个，第三个按需保存。
        """
        if self.config.transport == McpTransport.STDIO:
            return stdio_client(self.config.stdio_parameters())
        if self.config.transport == McpTransport.STREAMABLE_HTTP:
            # 新版 SDK 推荐 `streamable_http_client`；连接超时由底层 httpx 默认值
            # 处理，单次请求的读超时由 ClientSession 的 read_timeout_seconds 管。
            return streamable_http_client(self.config.endpoint)
        if self.config.transport == McpTransport.SSE:
            return sse_client(
                self.config.endpoint,
                timeout=self.config.timeout_s,
                sse_read_timeout=max(300.0, self.config.timeout_s),
            )
        raise ValueError(f"不支持的 MCP transport：{self.config.transport}")

    async def list_tools(self) -> list[Any]:
        """调用 `tools/list`，并处理分页。

        MCP 的列表接口和很多 HTTP API 一样使用 cursor 分页：
        `result.nextCursor` 有值就带上它再请求一次，直到没有下一页。
        当前只有四个工具，一次就能拿完；保留循环是为了符合真实协议。

        对比教学：Pi 的 `McpClient.listTools()` 也是循环跟随 `nextCursor`，
        直到没有下一页。不同语言、不同 SDK，协议行为是一样的。
        """
        if self._session is None:
            raise McpClientError("MCP 会话尚未初始化")
        tools: list[Any] = []
        cursor: str | None = None
        while True:
            result = await self._session.list_tools(cursor=cursor)
            tools.extend(result.tools)
            cursor = result.nextCursor
            if not cursor:
                break
        return tools

    def tool_specs(self) -> list[ToolSpec]:
        """把 MCP `Tool` 映射成平台内核认识的 `ToolSpec`。

        映射关系：
        - `name` -> `name`；
        - `title` -> `label`（没有 title 时退回 name）；
        - `description` -> `description`；
        - `inputSchema` -> `parameters`；
        - `annotations` -> `annotations`；
        - handler：调用 `tools/call`，把结果翻译成 `ToolResult`。

        这样 Agent 循环、工具搜索、能力目录都不需要知道 MCP 的细节。

        对比教学：Pi 在 coding-agent 里把工具命名为
        `mcp__<server>__<tool>`，并做 64 字符截断和哈希去重，因为一个 Pi 会话
        可能同时接多个 MCP Server。本项目当前只有一个 DBA Server，所以保留
        MCP 原名（例如 `slow_log_query`），让 Skill 的初始工具清单更直观；
        接第二个 Server 之前，应该补上同样的命名空间和撞名处理。
        """
        specs: list[ToolSpec] = []
        for tool in self._tools:
            annotations = (
                tool.annotations.model_dump(by_alias=True, mode="json", exclude_none=True)
                if getattr(tool, "annotations", None) is not None
                else None
            )
            specs.append(
                ToolSpec(
                    name=tool.name,
                    label=tool.title or tool.name,
                    description=tool.description or "",
                    parameters=tool.inputSchema or {"type": "object", "properties": {}},
                    handler=self._make_handler(tool.name),
                    annotations=annotations,
                )
            )
        return specs

    def _make_handler(self, tool_name: str) -> Any:
        """为某个 MCP 工具创建 `ToolSpec.handler`。

        注意闭包变量：`tool_name` 通过参数传进来，不会被循环里的下一个工具覆盖。
        如果直接在 for 循环里写 `lambda` 或内部函数引用循环变量，最后可能所有
        handler 都指向同一个工具；这是 Python 闭包的经典坑。
        """

        async def handler(
            tool_call_id: str,
            arguments: dict[str, Any],
            cancellation: CancellationToken,
        ) -> ToolResult:
            del tool_call_id  # MCP 的 call id 由 JSON-RPC 自己管理，不需要传给服务端
            if cancellation.cancelled:
                raise ToolExecutionError("操作已取消")
            return await self.call_tool(tool_name, arguments)

        return handler

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        """执行 `tools/call`，把 MCP 结果翻译成平台 `ToolResult`。

        关键点：
        - `isError=True` 不抛异常，转成 `ToolResult.is_error=True`，
          让模型看到错误说明并继续；
        - 协议层错误（例如未知工具、连接断开）才抛 `ToolExecutionError`，
          由内核转成错误结果；
        - `structuredContent.details` 是平台摘要，会进入 Topic 和证据目录。

        对比教学：
        - Pi 用 `toLlmContent()` 把 text/image/resource 等 MCP 内容块转成
          自己的模型内容块，并用 `isError` 标记工具失败；
        - 本项目内核目前只支持文本内容，所以非文本块先序列化成 JSON 文本，
          保持平台消息类型简单统一。以后要支持图片/资源时，再扩展 `Content`。
        """
        if self._session is None:
            raise McpClientError("MCP 会话尚未初始化")

        started_at = time.perf_counter()
        try:
            result: CallToolResult = await self._session.call_tool(
                name,
                arguments,
                read_timeout_seconds=timedelta(seconds=self.config.timeout_s),
            )
        except McpError as exc:
            raise ToolExecutionError(f"MCP 工具 {name} 调用失败：{exc}") from exc
        except TimeoutError as exc:
            raise ToolExecutionError(
                f"MCP 工具 {name} 调用超时（超过 {self.config.timeout_s} 秒）"
            ) from exc

        duration_ms = int((time.perf_counter() - started_at) * 1000)
        details = {}
        structured = result.structuredContent or {}
        if isinstance(structured.get("details"), dict):
            details = structured["details"]

        text_blocks: list[str] = []
        for block in result.content:
            if isinstance(block, McpTextContent):
                text_blocks.append(block.text)
            else:
                # 当前 DBA 工具只返回文本；如果以后返回图片/资源，
                # 先序列化成文本，保证平台消息类型仍然统一。
                text_blocks.append(
                    json.dumps(
                        block.model_dump(by_alias=True, mode="json", exclude_none=True),
                        ensure_ascii=False,
                    )
                )

        text = "\n".join(text_blocks)
        if not text:
            # 某些 MCP Server 可能只返回 structuredContent；这里补一份文本，
            # 避免模型收到空消息。
            payload = structured.get("payload", structured)
            text = json.dumps(payload, ensure_ascii=False, indent=2)

        return ToolResult(
            content=[TextContent(text=text)],
            details=details,
            is_error=bool(result.isError),
            duration_ms=duration_ms,
        )
