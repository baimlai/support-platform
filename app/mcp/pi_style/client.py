"""Pi 风格的手写 MCP 客户端：自己管理 JSON-RPC 请求状态机。

## 这个文件为什么存在

运行时的默认客户端是 `app/mcp/client.py`，底层用官方 Python SDK。
这个文件是**教学对照实现**：把 Pi 的 `packages/mcp/src/client.ts` 的核心思路
用 Python 重写一遍。它不依赖官方 SDK 的 `ClientSession`，只依赖：

- 标准库 `asyncio` / `json`；
- HTTP 场景下的 `httpx`；
- 我们自己的两个传输实现（`stdio.py`、`streamable_http.py`）。

这样做的目的不是替换官方 SDK，而是让你能看清楚：

```text
官方 SDK 帮我们做了什么？
  → initialize、请求 id、pending 表、分页、超时、取消、结果映射

如果自己写，最少要写哪些东西？
  → 这个文件
```

## JSON-RPC 请求/响应到底怎么配对

MCP 基于 JSON-RPC 2.0。客户端发一个请求：

```json
{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"slow_log_query","arguments":{...}}}
```

服务端返回：

```json
{"jsonrpc":"2.0","id":7,"result":{"content":[...],"isError":false}}
```

关键是 `id`：客户端可能同时发多个请求，响应回来的顺序不一定和发送顺序一致，
所以要用一个字典把 `id` 映射到“等待结果的 Future”：

```text
pending = {
  7: Future(...),
  8: Future(...),
}
```

读取循环收到 `id=7` 的响应，就 `pending[7].set_result(...)`；
`request()` 里 `await future` 的地方立刻拿到结果。这就是 Pi 的
`pendingRequests` / `request()` 的核心。

## 生命周期

```text
async with PiStyleMcpClient(config) as client:
    1. 启动传输（stdio 子进程 / HTTP 客户端）
    2. 启动读取循环任务
    3. 发 initialize，保存协议版本、服务端信息
    4. 发 notifications/initialized
    5. 发 tools/list，保存工具清单
    6. 调用方使用 tool_specs() / call_tool()
退出：
    7. 取消读取循环
    8. 关闭传输（回收子进程 / DELETE 会话 / 关闭 HTTP）
    9. 所有还没完成的 Future 标记为失败，避免调用方永久等待
```

## 和 Pi 的差异（刻意保留的简化）

Pi 还实现了：

- GET SSE 长连接、`Last-Event-ID` 断线续传；
- 408/429/5xx 自动重试；
- 进度通知 `notifications/progress` 和超时续期；
- OAuth；
- 多服务器连接管理器。

本文件只实现“完成一次 DBA 调查必需”的部分：stdio、Streamable HTTP、
initialize、tools/list、tools/call、超时、取消、服务端 ping。
这些已经足够和官方 SDK 做逐条消息对比；没实现的部分在
`docs/mcp.md` 里明确标出来，不假装支持。

## Python 语法提示

- `asyncio.Future` 是“将来会有结果”的占位对象；
  `set_result()` / `set_exception()` 只能调用一次。
- `asyncio.wait_for(awaitable, timeout)` 超时会取消内部的 await；
  但网络请求未必立刻停，所以超时后我们还会发 `notifications/cancelled`。
- `dict.pop(key, None)` 在键不存在时返回 None，适合清理 pending 表。
- `itertools.count(1)` 是无限递增计数器，写请求 id 很方便；
  这里用普通整数是为了让日志里看到明确的 1、2、3。
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.harness.tools import ToolExecutionError
from app.mcp.client import McpClientConfig, McpClientError
from app.mcp.info import McpClientInfo
from app.mcp.pi_style.stdio import StdioPiTransport
from app.mcp.pi_style.streamable_http import StreamableHttpPiTransport
from app.mcp.pi_style.transport import PiTransport, PiTransportError
from app.runtime.contracts import CancellationToken, TextContent, ToolResult, ToolSpec

# 客户端身份。服务端 initialize 响应里能看到它，排查“是谁连过来的”时有用。
CLIENT_NAME = "support-platform-pi-style-agent"
CLIENT_VERSION = "0.1.0"

# 我们实现时参考的协议版本。服务端可能返回一个更低的兼容版本，
# 以服务端 initialize 响应里的 `protocolVersion` 为准。
PROTOCOL_VERSION = "2025-11-25"


class PiStyleRpcError(RuntimeError):
    """JSON-RPC 层返回了 `error` 对象。

    例子：调用了不存在的工具，服务端可能返回：

    ```json
    {"jsonrpc":"2.0","id":5,"error":{"code":-32601,"message":"Method not found"}}
    ```

    这和“工具业务失败”（`isError=true`）不同：
    - `isError=true` 是工具执行完了，但业务上失败；
    - JSON-RPC error 是协议层失败，通常意味着客户端请求不合法或服务端出错。
    """

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"JSON-RPC error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class PiStyleProtocolTrace:
    """把 JSON-RPC 收发消息写成 JSONL，格式和官方客户端保持一致。

    每行长这样：

    ```json
    {"ts":"2026-10-30T...","direction":"send","message":{"jsonrpc":"2.0",...}}
    ```

    为什么用 JSONL：一行一条，追加写不会破坏已有内容，`tail -f` 也方便。
    """

    def __init__(self, path: Path | str | None) -> None:
        self.path = Path(path) if path is not None else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, direction: str, message: dict[str, Any]) -> None:
        """追加一条记录；写文件失败不能影响协议调用。"""
        if self.path is None:
            return
        try:
            record = {
                "ts": datetime.now(UTC).isoformat(),
                "direction": direction,
                "message": message,
            }
            with self.path.open("a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            # 协议追踪是辅助功能。磁盘满、权限不足时继续跑调查，
            # 不要让“写日志失败”变成“工具调用失败”。
            return


class PiStyleMcpClient:
    """Pi 风格的手写 MCP 客户端。

    用法和官方客户端几乎一样，方便对照：

    ```python
    config = McpClientConfig(transport="stdio", trace_path=Path("pi.jsonl"))
    async with PiStyleMcpClient(config) as client:
        print(client.info.tools)
        result = await client.call_tool("slow_log_query", {...})
    ```
    """

    def __init__(self, config: McpClientConfig | None = None) -> None:
        self.config = config or McpClientConfig()
        if self.config.transport not in {"stdio", "streamable-http"}:
            # Pi 本身明确不支持旧版 HTTP+SSE；我们也不在这里实现，
            # 需要观察 legacy SSE 时请使用官方客户端的 sse 传输。
            raise ValueError(
                "PiStyleMcpClient 只实现 stdio 和 streamable-http；"
                f"当前 transport={self.config.transport!r}。"
                "旧版 SSE 请使用 DbaMcpClient（官方 SDK）。"
            )

        self._transport: PiTransport | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._next_request_id = 1
        self._closed = False
        self._tools: list[dict[str, Any]] = []
        self._protocol_version: str | None = None
        self._server_capabilities: dict[str, Any] = {}
        self._server_info: dict[str, Any] = {}
        self._instructions: str | None = None
        self._trace = PiStyleProtocolTrace(self.config.trace_path)
        self.info: McpClientInfo | None = None
        # 服务端主动发来的通知，保留下来方便教学时观察（例如 progress）。
        self.notifications: list[dict[str, Any]] = []

    async def __aenter__(self) -> PiStyleMcpClient:
        """启动传输、握手、发现工具。

        任何一步失败都要清理已经启动的子进程/HTTP 连接，再把异常包成
        `McpClientError`。例子：HTTP 端口没开时，调用方只需要处理
        “MCP 连接失败”，不用理解 httpx 的 ConnectError。
        """
        try:
            self._transport = self._build_transport()
            await self._transport.start()
            # 读取循环必须在发 initialize 之前启动，否则响应回来时没人接。
            self._read_task = asyncio.create_task(self._read_loop())
            await self._initialize()
            self._tools = await self.list_tools()
            self.info = self._build_info()
            return self
        except BaseException as exc:
            await self.close()
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            if isinstance(exc, McpClientError):
                raise
            raise McpClientError(
                f"Pi 风格 MCP 客户端连接失败（transport={self.config.transport}，"
                f"endpoint={self.config.endpoint}）：{exc}"
            ) from exc

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """退出时关闭读取循环和传输，回收子进程或 HTTP 连接。"""
        await self.close()

    def _build_transport(self) -> PiTransport:
        """按配置选择传输实现。

        这里返回的是 `PiTransport` 协议类型；Python 不要求显式继承，
        只要对象有 start/send/receive/close 就能用。
        """
        if self.config.transport == "stdio":
            return StdioPiTransport(self.config)
        return StreamableHttpPiTransport(self.config)

    async def _initialize(self) -> None:
        """发送 initialize 和 notifications/initialized。

        initialize 请求参数是 MCP 规范规定的：

        ```json
        {
          "protocolVersion": "2025-11-25",
          "capabilities": {},
          "clientInfo": {"name": "...", "version": "..."}
        }
        ```

        为什么 `capabilities` 是空对象：
        客户端当前不支持 sampling、roots 等可选能力，空对象表示“只做基本工具调用”。
        服务端不会因此拒绝；它只会知道我们不能处理那些高级请求。
        """
        result = await self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": CLIENT_NAME, "version": CLIENT_VERSION},
            },
        )

        protocol_version = result.get("protocolVersion")
        if not isinstance(protocol_version, str) or not protocol_version:
            raise McpClientError("initialize 响应缺少 protocolVersion")
        self._protocol_version = protocol_version

        server_info = result.get("serverInfo")
        self._server_info = server_info if isinstance(server_info, dict) else {}
        capabilities = result.get("capabilities")
        self._server_capabilities = capabilities if isinstance(capabilities, dict) else {}
        instructions = result.get("instructions")
        self._instructions = instructions if isinstance(instructions, str) else None

        # 告诉 HTTP 传输：后续请求要带 MCP-Protocol-Version。
        if self._transport is not None:
            self._transport.set_protocol_version(protocol_version)

        # 规范要求 initialize 成功后发这个通知，表示“客户端已经准备好了”。
        await self.notify("notifications/initialized")

    async def list_tools(self) -> list[dict[str, Any]]:
        """调用 tools/list，并跟随 nextCursor 分页。

        MCP 列表接口和很多 HTTP API 一样：

        ```json
        {"tools":[...], "nextCursor":"page-2"}
        ```

        有 `nextCursor` 就把它放进下一次请求的 `params.cursor`；
        没有就结束。当前只有四个工具，一次就能取完，但循环保证协议正确。
        """
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {}
            if cursor:
                params["cursor"] = cursor
            result = await self.request("tools/list", params)
            page = result.get("tools")
            if isinstance(page, list):
                tools.extend(item for item in page if isinstance(item, dict))
            next_cursor = result.get("nextCursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                break
            cursor = next_cursor
        return tools

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        """调用 tools/call，并把 MCP 结果翻译成平台 `ToolResult`。

        关键约定和官方客户端保持一致：

        - `isError=true` 是**业务失败**，返回 `ToolResult.is_error=True`，
          让模型看到错误说明并继续下一轮；
        - JSON-RPC `error` 是**协议失败**，抛 `ToolExecutionError`；
        - `structuredContent.details` 是平台摘要，进入 Topic 和证据。
        """
        started_at = time.perf_counter()
        try:
            result = await self.request(
                "tools/call",
                {"name": name, "arguments": arguments},
                timeout_s=self.config.timeout_s,
            )
        except PiStyleRpcError as exc:
            raise ToolExecutionError(f"MCP 工具 {name} 调用失败：{exc}") from exc
        except McpClientError as exc:
            raise ToolExecutionError(f"MCP 工具 {name} 调用失败：{exc}") from exc

        duration_ms = int((time.perf_counter() - started_at) * 1000)
        return self._to_tool_result(result, duration_ms=duration_ms, tool_name=name)

    def _to_tool_result(
        self,
        result: dict[str, Any],
        *,
        duration_ms: int,
        tool_name: str,
    ) -> ToolResult:
        """把 `CallToolResult` 字典翻译成平台结果。

        MCP 的 content 是内容块数组，例如：

        ```json
        {"type":"text","text":"{\"code\":\"ok\"}"}
        ```

        当前 DBA 工具只返回文本；未来如果返回图片/资源，这里先转成 JSON 文本，
        保证平台消息类型仍然简单统一。
        """
        structured = result.get("structuredContent")
        details: dict[str, Any] = {}
        if isinstance(structured, dict) and isinstance(structured.get("details"), dict):
            details = structured["details"]

        text_blocks: list[str] = []
        content = result.get("content")
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text")
                    text_blocks.append(text if isinstance(text, str) else str(text))
                else:
                    text_blocks.append(json.dumps(block, ensure_ascii=False))

        text = "\n".join(text_blocks)
        if not text:
            # 有些服务端只返回 structuredContent，不返回文本。
            # 这里补一份 JSON 文本，避免模型收到空消息。
            payload = structured.get("payload", structured) if isinstance(structured, dict) else structured
            text = json.dumps(payload, ensure_ascii=False, indent=2)

        return ToolResult(
            content=[TextContent(text=text)],
            details=details,
            is_error=bool(result.get("isError")),
            duration_ms=duration_ms,
        )

    def tool_specs(self) -> list[ToolSpec]:
        """把 MCP `tools/list` 的字典映射成平台 `ToolSpec`。

        映射和官方客户端完全一致：
        `name`、`title`、`description`、`inputSchema`、`annotations`。
        这样同一个 AgentRunner 可以无感替换客户端。
        """
        specs: list[ToolSpec] = []
        for tool in self._tools:
            name = tool.get("name")
            if not isinstance(name, str) or not name:
                continue
            parameters = tool.get("inputSchema")
            annotations = tool.get("annotations")
            specs.append(
                ToolSpec(
                    name=name,
                    label=str(tool.get("title") or name),
                    description=str(tool.get("description") or ""),
                    parameters=parameters if isinstance(parameters, dict) else {"type": "object"},
                    handler=self._make_handler(name),
                    annotations=annotations if isinstance(annotations, dict) else None,
                )
            )
        return specs

    def _make_handler(self, tool_name: str) -> Any:
        """为某个工具创建 handler，并在执行前检查取消令牌。

        注意闭包：`tool_name` 通过参数传入，不会在循环中被下一个工具覆盖。
        如果在 for 循环里直接写内部函数引用循环变量，Python 的闭包会捕获
        同一个变量，最后所有 handler 都可能指向最后一个工具。
        """

        async def handler(
            tool_call_id: str,
            arguments: dict[str, Any],
            cancellation: CancellationToken,
        ) -> ToolResult:
            del tool_call_id
            if cancellation.cancelled:
                raise ToolExecutionError("操作已取消")
            return await self.call_tool(tool_name, arguments)

        return handler

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, Any]:
        """发送一个 JSON-RPC 请求，等待对应 id 的响应。

        步骤：

        1. 生成唯一 id；
        2. 创建 Future，放进 `_pending`；
        3. 发送消息；
        4. `await future`，读取循环收到响应后会 `set_result()`；
        5. 超时则清理 pending，并尽力发 `notifications/cancelled`。
        """
        transport = self._transport
        if transport is None or self._closed:
            raise McpClientError("MCP 传输尚未启动或已关闭")

        request_id = self._next_request_id
        self._next_request_id += 1
        message: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            message["params"] = params

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        self._trace.record("send", message)

        try:
            await transport.send(message)
        except PiTransportError as exc:
            self._pending.pop(request_id, None)
            raise McpClientError(f"发送 MCP 请求 {method} 失败：{exc}") from exc

        effective_timeout = self.config.timeout_s if timeout_s is None else timeout_s
        try:
            return await asyncio.wait_for(future, timeout=effective_timeout)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            # 超时后服务端可能还在执行。发一个取消通知是“尽力而为”，
            # 不保证服务端一定支持，但比什么都不做好。
            await self._try_cancel(request_id, "timeout")
            raise McpClientError(
                f"MCP 请求 {method}（id={request_id}）超时"
                f"（超过 {effective_timeout} 秒）"
            ) from exc

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        """发送一条 JSON-RPC 通知（没有 id，不需要响应）。"""
        transport = self._transport
        if transport is None or self._closed:
            raise McpClientError("MCP 传输尚未启动或已关闭")
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._trace.record("send", message)
        try:
            await transport.send(message)
        except PiTransportError as exc:
            raise McpClientError(f"发送 MCP 通知 {method} 失败：{exc}") from exc

    async def _try_cancel(self, request_id: int, reason: str) -> None:
        """尽力发送取消通知，失败不影响原始超时错误。"""
        try:
            await self.notify(
                "notifications/cancelled",
                {"requestId": request_id, "reason": reason},
            )
        except McpClientError:
            return

    async def _read_loop(self) -> None:
        """持续读取传输消息，并把它分发给等待中的 Future。

        读取循环只做三件事：

        1. **响应**（有 id，且 result/error）：找到 pending Future 并完成它；
        2. **服务端请求**（有 id 和 method）：例如 ping，需要回一个响应；
        3. **通知**（有 method 没 id）：记录或忽略。

        注意 `asyncio.CancelledError` 要原样抛出，否则关闭任务时会被吞掉。
        """
        transport = self._transport
        if transport is None:
            return

        try:
            async for message in transport.receive():
                self._trace.record("recv", message)

                if "id" in message and ("result" in message or "error" in message):
                    self._resolve_response(message)
                    continue

                method = message.get("method")
                if isinstance(method, str) and "id" in message:
                    # 服务端主动发请求。MCP 当前常见的是 ping；
                    # 其它未知方法按 JSON-RPC 规范回 -32601。
                    await self._handle_server_request(message)
                    continue

                if isinstance(method, str):
                    self.notifications.append(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 读取循环要把错误传给所有等待者
            self._fail_pending(
                McpClientError(f"MCP 读取循环失败：{exc}")
            )
        else:
            # 传输正常结束（例如服务端关闭管道）。
            if not self._closed:
                self._fail_pending(McpClientError("MCP 连接已关闭"))

    def _resolve_response(self, message: dict[str, Any]) -> None:
        """根据响应里的 id 完成对应的 Future。"""
        request_id = message.get("id")
        if not isinstance(request_id, int):
            return
        future = self._pending.pop(request_id, None)
        if future is None or future.done():
            # 超时后响应才回来，或者服务端发了没人等的 id，忽略即可。
            return

        if "error" in message:
            error = message["error"]
            if isinstance(error, dict):
                code = error.get("code", -32000)
                text = error.get("message", "unknown error")
                data = error.get("data")
                future.set_exception(
                    PiStyleRpcError(
                        code if isinstance(code, int) else -32000,
                        str(text),
                        data,
                    )
                )
            else:
                future.set_exception(PiStyleRpcError(-32000, str(error)))
            return

        result = message.get("result")
        future.set_result(result if isinstance(result, dict) else {"value": result})

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        """响应服务端发来的请求。

        当前只处理 ping（返回空对象）和 roots/list（返回空 roots）。
        例子：服务端发
        `{"jsonrpc":"2.0","id":99,"method":"ping"}`，
        我们回 `{"jsonrpc":"2.0","id":99,"result":{}}`。
        """
        method = message.get("method")
        request_id = message.get("id")
        if method == "ping":
            result: dict[str, Any] = {}
        elif method == "roots/list":
            result = {"roots": []}
        else:
            await self.notify_error(
                request_id,
                -32601,
                f"Method not found: {method}",
            )
            return

        if self._transport is None:
            return
        response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        self._trace.record("send", response)
        try:
            await self._transport.send(response)
        except PiTransportError:
            pass

    async def notify_error(self, request_id: Any, code: int, message: str) -> None:
        """给服务端请求回一个 JSON-RPC error。"""
        if self._transport is None:
            return
        response = {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
        self._trace.record("send", response)
        try:
            await self._transport.send(response)
        except PiTransportError:
            pass

    def _fail_pending(self, error: Exception) -> None:
        """把所有等待中的 Future 标记为失败。

        为什么要遍历所有 pending：
        连接断了以后，不只是当前请求会失败，同时排队的其它请求也永远等不到响应。
        一起失败，调用方才能尽快退出，而不是各自等到超时。
        """
        pending = list(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(error)

    def _build_info(self) -> McpClientInfo:
        """构造和官方客户端字段一致的连接摘要。"""
        return McpClientInfo(
            transport=self.config.transport,
            endpoint=self.config.endpoint,
            server_name=str(self._server_info.get("name", "unknown")),
            server_version=str(self._server_info.get("version", "unknown")),
            protocol_version=self._protocol_version or "",
            instructions=self._instructions,
            capabilities=self._server_capabilities,
            tools=[str(tool.get("name")) for tool in self._tools if tool.get("name")],
            session_id=self._transport.session_id if self._transport is not None else None,
        )

    async def close(self) -> None:
        """关闭读取循环和传输，并让所有 pending Future 失败。"""
        if self._closed:
            return
        self._closed = True

        read_task = self._read_task
        self._read_task = None
        if read_task is not None and not read_task.done():
            read_task.cancel()
            try:
                await read_task
            except (asyncio.CancelledError, Exception):
                pass

        transport = self._transport
        self._transport = None
        if transport is not None:
            try:
                await transport.close()
            except Exception:
                # 关闭阶段的异常不应覆盖调用方正在处理的原始异常。
                pass

        self._fail_pending(McpClientError("MCP 客户端已关闭"))
