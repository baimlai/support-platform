"""Pi 风格手写 MCP 客户端的传输层骨架。

## 先看一张图

MCP 的“协议”和“传输”是两件不同的事：

```text
                   JSON-RPC 消息（协议层，所有传输都一样）
                              │
              ┌───────────────┴────────────────┐
              │                                │
      stdio 传输（子进程管道）        Streamable HTTP 传输（POST + SSE）
              │                                │
       stdin/stdout 各一行 JSON          HTTP body 里的 SSE data 帧
```

Pi 的 `@earendil-works/pi-mcp` 也是这么分层的：

- `packages/mcp/src/client.ts` 负责请求 id、`pending` 表、initialize、
  tools/list、tools/call、超时和取消；
- `packages/mcp/src/transports/stdio.ts` 只负责“把对象写成一行 JSON”
  和“从 stdout 读一行 JSON”；
- `packages/mcp/src/transports/streamable-http.ts` 只负责 HTTP 状态码、
  `Mcp-Session-Id`、SSE 帧解析。

这一层只有传输相关代码，不认识 `tools/list`，也不认识 DBA 工具。
好处是：换传输不用改协议状态机，测协议状态机也不用起真实进程。

## 为什么用“收件箱队列”统一两种传输

stdio 是持续流：子进程随时可能往 stdout 写消息。
Streamable HTTP 是请求/响应：每次 POST 可能返回一小段 SSE。

为了让上层的读取循环写成同一套代码，两种传输都做同一件事：

```text
底层读到一条 JSON-RPC 消息
  -> 放进 self._inbox 队列
  -> 上层 async for message in transport.receive() 取走
```

这相当于把“推送式”和“拉取式”统一成“异步队列”。
Python 的 `asyncio.Queue` 正好是协程安全的：生产者 `put()`，
消费者 `await get()`，不需要自己加锁。

## Python 语法提示

- `Protocol` 定义“鸭子类型接口”：只要对象有这些方法就能传进来，
  不要求继承。测试里可以写一个假传输，不需要真的启动进程。
- `AsyncIterator[dict[str, Any]]` 表示“可以用 `async for` 遍历的异步迭代器”。
- `asyncio.Queue()` 的 `get()` 是协程；队列空时会挂起，不占 CPU。
- `object()` 创建一个独一无二的哨兵对象，用来表示“队列关闭了”；
  不能用 `None`，因为正常消息也可能是 `None`（虽然 MCP 不会）。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Protocol


class PiTransportError(RuntimeError):
    """传输层失败：子进程退出、HTTP 连接断开、SSE 帧不是合法 JSON 等。

    例子：stdio 子进程的 stdout 突然关闭，读取循环会把这个异常放进收件箱，
    上层的 `request()` 收到后把等待中的 Future 全部标记为失败，
    调用方就不会永远卡在 `await future` 上。
    """


# 这个对象只用来做身份比较：`item is _CLOSED`。
# 用 `==` 不行，因为将来如果消息里也出现一个空对象，可能被误判成关闭。
_CLOSED = object()


class PiTransport(Protocol):
    """两种传输都要实现的最小接口。

    注意：这里没有 `request()`。请求 id、Future、超时属于协议层，
    不属于传输层；Pi 也是这么切的。
    """

    session_id: str | None

    async def start(self) -> None:
        """启动子进程或创建 HTTP 客户端。失败时抛 `PiTransportError`。"""
        ...

    async def send(self, message: dict[str, Any]) -> None:
        """发送一条 JSON-RPC 消息。

        这个方法只负责“送出去”，不负责等响应。响应会从 `receive()` 出来。
        对请求（有 `id`）来说，调用方要在发送后自己等待对应的 Future。
        """
        ...

    def receive(self) -> AsyncIterator[dict[str, Any]]:
        """返回异步迭代器，逐条产出收到的 JSON-RPC 消息。"""
        ...

    def set_protocol_version(self, version: str) -> None:
        """把 initialize 协商出的协议版本告诉传输层。

        stdio 不需要额外处理；Streamable HTTP 会把它放进
        `MCP-Protocol-Version` 请求头。默认实现为空，子类按需覆盖。
        """
        ...

    async def close(self) -> None:
        """关闭连接并回收资源。必须可重复调用，不能抛“已经关闭”的错。"""
        ...


class BasePiTransport:
    """两种传输共用的收件箱逻辑。

    子类只需要在合适的时候调用：
    - `_push(message)`：收到一条合法消息；
    - `_fail(error)`：底层出错，让读取循环抛异常；
    - `_close_inbox()`：正常关闭，让读取循环结束。

    这不属于“协议层”，只是避免 stdio 和 HTTP 各写一遍队列代码。
    """

    def __init__(self) -> None:
        # `asyncio.Queue` 不绑定事件循环，创建时可以在同步上下文；
        # 真正 `put/get` 时才会绑定当前运行中的事件循环。
        # 无界队列：MCP 消息量很小，不会因为消费慢而阻塞底层读取。
        self._inbox: asyncio.Queue[dict[str, Any] | BaseException | object] = asyncio.Queue()
        self.session_id: str | None = None
        self._protocol_version: str | None = None
        # `_closed` 表示“业务上不再接收新消息”；
        # `_inbox_closed` 表示“关闭哨兵已经放进队列”。
        # 分成两个标志，是因为子类 close() 往往先设置 `_closed=True`
        # 阻止新消息入队，然后再调用 `_close_inbox()` 唤醒读取循环。
        self._closed = False
        self._inbox_closed = False

    def _push(self, message: dict[str, Any]) -> None:
        """把一条消息放进收件箱。

        例子：stdio 读到
        `{"jsonrpc":"2.0","id":2,"result":{"tools":[...]}}`，
        就调用 `self._push(parsed)`；上层马上会从 `receive()` 拿到它。
        """
        if not self._closed:
            self._inbox.put_nowait(message)

    def _fail(self, error: BaseException) -> None:
        """把异常放进收件箱，交给上层读取循环处理。

        为什么不在底层直接 `raise`：
        读取任务（`asyncio.create_task`）里抛异常，如果没人 await 这个任务，
        Python 只会在垃圾回收时打印 "Task exception was never retrieved"，
        调用方却一直卡在等待响应。放进队列能让错误沿着正常消息路径传上去。
        """
        if not self._closed:
            self._inbox.put_nowait(error)

    def _close_inbox(self) -> None:
        """放入哨兵，告诉 `receive()` 正常结束。"""
        if not self._inbox_closed:
            self._inbox_closed = True
            self._inbox.put_nowait(_CLOSED)

    async def receive(self) -> AsyncIterator[dict[str, Any]]:
        """异步生成器：不断从队列取消息，直到遇到异常或关闭哨兵。

        `async for` 的写法：
        ```python
        async for message in transport.receive():
            print(message)
        ```
        队列空时会在 `await self._inbox.get()` 挂起，不忙等。
        """
        while True:
            item = await self._inbox.get()
            if item is _CLOSED:
                return
            if isinstance(item, BaseException):
                raise item
            # 类型检查器此时只知道 item 是 dict | BaseException | object；
            # 排除前两种后，剩下的就是 dict。`assert` 也让读者安心。
            assert isinstance(item, dict)
            yield item

    def set_protocol_version(self, version: str) -> None:
        """默认保存下来；需要请求头的 HTTP 传输会覆盖并读取它。"""
        self._protocol_version = version

    async def close(self) -> None:
        """基类只关收件箱；子类要在这里回收进程或 HTTP 连接。"""
        self._close_inbox()
