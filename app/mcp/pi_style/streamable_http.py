"""Streamable HTTP 传输：POST /mcp，响应体里用 SSE 推 JSON-RPC 消息。

## 为什么叫“Streamable”

普通的 HTTP 请求/响应只能返回一个 JSON；MCP 的 Streamable HTTP 允许
服务端在同一个响应里先推几条通知，再推最终结果。它用的就是 SSE
（Server-Sent Events）格式：

```text
HTTP/1.1 200 OK
Content-Type: text/event-stream
Mcp-Session-Id: 6d3f...

event: message
data: {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25",...}}

```

客户端 POST 一个 JSON-RPC 请求，服务端可能：

- 直接返回 `application/json`（简单实现，`json_response=True`）；
- 返回 `text/event-stream`，把一条或多条消息放在 `data:` 帧里（本项目默认）。

## SSE 帧格式（学习重点）

SSE 不是“一整段 JSON”，而是一行一行的事件：

```text
event: message      ← 事件类型，可省略
id: 42              ← 事件 id，断线重连时可用 Last-Event-ID
retry: 3000         ← 建议重连间隔（毫秒）
data: 第一行         ← 数据，可以出现多次
data: 第二行         ← 多行 data 会用换行拼起来
                     ← 空行表示一个事件结束
```

注意：`data:` 后面的 JSON 可以跨多行；解析时必须等空行再 `json.loads`。
如果只按行解析，遇到多行 JSON 就会失败。

## 会话 id 和协议版本

Streamable HTTP 是有状态的：

1. 第一次 POST `initialize`，服务端在响应头返回 `Mcp-Session-Id`；
2. 后续每个请求都要带 `Mcp-Session-Id`；
3. initialize 协商出协议版本后，后续请求通常还要带
   `MCP-Protocol-Version` 请求头。

本项目用 `httpx.AsyncClient` 发请求。`httpx` 的 headers 是大小写不敏感的，
所以 `response.headers.get("mcp-session-id")` 能读到服务端的
`Mcp-Session-Id`。

## Pi 怎么做

Pi 的 `packages/mcp/src/transports/streamable-http.ts` 处理了：

- `Mcp-Session-Id`；
- SSE `event` / `data` / `id` 帧；
- GET 事件流和 `Last-Event-ID` 断线续传；
- 408/429/5xx 重试；
- 服务端主动发来的请求（例如 ping）。

我们这一版只实现“请求-响应”必需的部分，**没有实现 GET 长连接和断线续传**。
原因是当前 DBA 工具都是短请求，服务端也不需要主动推送；把这部分留作
后续练习，比为了“看起来完整”而写一套没测过的重连逻辑更可靠。

## Python 语法提示

- `httpx.AsyncClient.stream("POST", ...)` 返回异步上下文管理器，
  可以在响应还没读完时就逐行读取，不用把整个 body 先塞进内存。
- `response.aiter_lines()` 是异步迭代器，适合逐行解析 SSE。
- `asyncio.create_task()` 让每个请求独立执行；否则一个慢工具会堵住所有请求。
- `task.add_done_callback(self._tasks.discard)` 在任务结束时自动从集合移除，
  避免集合越积越大。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.mcp.client import McpClientConfig
from app.mcp.pi_style.transport import BasePiTransport, PiTransportError


class StreamableHttpPiTransport(BasePiTransport):
    """用 HTTP POST + SSE 实现的 MCP 传输。

    典型请求头：

    ```text
    Accept: application/json, text/event-stream
    Content-Type: application/json
    Mcp-Session-Id: ...          （initialize 之后）
    MCP-Protocol-Version: ...    （initialize 之后）
    ```
    """

    def __init__(self, config: McpClientConfig) -> None:
        super().__init__()
        self.config = config
        self.url = config.endpoint
        self._client: httpx.AsyncClient | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._close_lock = asyncio.Lock()

    async def start(self) -> None:
        """创建 HTTP 客户端。

        超时策略：
        - 连接超时 10 秒，避免服务没启动时等太久；
        - 读超时至少 300 秒，因为 SSE 流可能保持打开；
        - 写超时沿用配置值。

        `httpx.Timeout(timeout, read=...)` 的第一个参数是默认值，
        后面按需覆盖某个阶段。
        """
        connect_timeout = min(10.0, self.config.timeout_s)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                self.config.timeout_s,
                connect=connect_timeout,
                read=max(300.0, self.config.timeout_s),
            ),
            follow_redirects=True,
        )

    def _headers(self, *, include_session: bool = True) -> dict[str, str]:
        """构造 MCP 请求头。

        `include_session=False` 只用于 initialize：此时还没有 session id。
        """
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if include_session and self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        if include_session and self._protocol_version:
            headers["MCP-Protocol-Version"] = self._protocol_version
        return headers

    async def send(self, message: dict[str, Any]) -> None:
        """发送一条消息。

        分两种情况：

        - **通知**（没有 `id`）：直接 `await` 发完。这样 `initialize` 之后
          的 `notifications/initialized` 一定会先于 `tools/list` 到达服务端。
        - **请求**（有 `id`）：放到后台任务里发，调用方可以立刻去等 Future。
          如果这里也 `await` 整个响应，`tools/call` 很慢时调用方就没法同时
          处理取消通知或其它请求。
        """
        if self._closed:
            raise PiTransportError("Streamable HTTP 传输已关闭")
        if "id" not in message:
            await self._send_one(message)
            return

        task = asyncio.create_task(self._send_one_and_report(message))
        # 保存 Task 引用，防止被垃圾回收；结束时自动移除。
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _send_one_and_report(self, message: dict[str, Any]) -> None:
        """后台请求任务的包装：把异常放进收件箱，让等待中的 Future 失败。"""
        try:
            await self._send_one(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 后台任务必须把错误交给上层
            self._fail(
                exc if isinstance(exc, PiTransportError) else PiTransportError(str(exc))
            )

    async def _send_one(self, message: dict[str, Any]) -> None:
        """真正执行一次 POST，并解析响应。

        响应可能是：
        - `text/event-stream`：逐帧解析；
        - `application/json`：直接解析一整个 JSON；
        - 202/204 空响应：通知的正常返回，不需要解析。
        """
        if self._client is None:
            raise PiTransportError("Streamable HTTP 客户端尚未启动")

        # 只有 initialize 请求不带 session id；判断方法是“方法名是 initialize”。
        # 注意：不能用 `self.session_id is None` 判断，因为服务端可能允许
        # 无状态模式（不返回 session id），那种情况下后续请求也不该硬塞空头。
        is_initialize = message.get("method") == "initialize"
        headers = self._headers(include_session=not is_initialize)

        try:
            async with self._client.stream(
                "POST",
                self.url,
                json=message,
                headers=headers,
            ) as response:
                # initialize 的响应头里可能带 session id，先保存再读 body。
                new_session_id = response.headers.get("mcp-session-id")
                if new_session_id:
                    self.session_id = new_session_id

                if response.status_code >= 400:
                    body = (await response.aread()).decode("utf-8", errors="replace")
                    raise PiTransportError(
                        f"MCP HTTP {response.status_code}：{body[:1000]}"
                    )

                # 通知通常返回 202/204，body 为空，读掉即可。
                if response.status_code in {202, 204}:
                    await response.aread()
                    return

                content_type = response.headers.get("content-type", "").lower()
                if "text/event-stream" in content_type:
                    async for event in _iter_sse_json(response):
                        self._push(event)
                    return

                # 有些服务端会把 json_response 设为 True，直接返回 application/json。
                body = (await response.aread()).decode("utf-8", errors="replace")
                if not body.strip():
                    return
                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError as exc:
                    raise PiTransportError(
                        f"MCP HTTP 响应不是合法 JSON：{body[:500]!r}（{exc}）"
                    ) from exc
                if isinstance(parsed, list):
                    # JSON-RPC 批次：规范允许数组，但 MCP 当前基本都是单条。
                    for item in parsed:
                        if isinstance(item, dict):
                            self._push(item)
                elif isinstance(parsed, dict):
                    self._push(parsed)
                else:
                    raise PiTransportError(f"MCP 响应必须是对象或数组：{parsed!r}")
        except httpx.HTTPError as exc:
            raise PiTransportError(
                f"连接 MCP HTTP Server 失败（{self.url}）：{exc}"
            ) from exc

    async def close(self) -> None:
        """取消后台请求、发送 DELETE 结束会话、关闭 HTTP 客户端。

        MCP 规范允许客户端用 `DELETE /mcp` 主动结束会话；服务端不支持时
        返回 405 也没关系。这里把 DELETE 放在 try/except 里，关闭流程
        不能因为服务端不支持而失败。
        """
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True

            # 先取消还在进行的请求，避免它们继续往收件箱写消息。
            for task in list(self._tasks):
                task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)

            client = self._client
            if client is not None:
                if self.session_id:
                    try:
                        await client.delete(
                            self.url,
                            headers=self._headers(),
                            timeout=2.0,
                        )
                    except Exception:  # noqa: BLE001 - 关闭清理失败不影响主流程
                        pass
                await client.aclose()
                self._client = None

            self._close_inbox()


async def _iter_sse_json(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    """逐帧解析 SSE，产出其中的 JSON 对象。

    解析状态机：

    ```text
    读到空行  -> 如果 data 缓冲非空，拼接、json.loads、产出一条消息、清空
    读到注释  -> 以 ":" 开头的行是注释，忽略
    读到 field -> 只关心 data；event/id/retry 对当前请求-响应模式不重要
    ```

    例子：

    ```text
    event: message
    data: {"jsonrpc":"2.0","id":2,"result":{"tools":[...]}}

    ```

    会产出一个字典 `{"jsonrpc": "2.0", "id": 2, "result": {...}}`。
    """
    data_lines: list[str] = []

    async for line in response.aiter_lines():
        if line == "":
            if data_lines:
                raw = "\n".join(data_lines)
                data_lines.clear()
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise PiTransportError(
                        f"MCP SSE data 不是合法 JSON：{raw[:500]!r}（{exc}）"
                    ) from exc
                if isinstance(parsed, dict):
                    yield parsed
                elif isinstance(parsed, list):
                    for item in parsed:
                        if isinstance(item, dict):
                            yield item
                continue

        # SSE 注释行用冒号开头，例如 ": keep-alive"。
        if line.startswith(":"):
            continue

        field, separator, value = line.partition(":")
        if not separator:
            # 没有冒号的行按规范当作字段名、值为空；当前用不到。
            continue
        if value.startswith(" "):
            # "data: xxx" 和 "data:xxx" 都合法，去掉可选的一个空格。
            value = value[1:]
        if field == "data":
            data_lines.append(value)

    # 流结束时可能没有最后的空行，补一次 dispatch，避免漏掉最后一条消息。
    if data_lines:
        raw = "\n".join(data_lines)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PiTransportError(
                f"MCP SSE data 不是合法 JSON：{raw[:500]!r}（{exc}）"
            ) from exc
        if isinstance(parsed, dict):
            yield parsed
        elif isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    yield item
