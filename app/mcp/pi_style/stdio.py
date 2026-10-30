"""stdio 传输：用子进程的 stdin/stdout 传 JSON-RPC。

## 真实线路上长什么样

MCP 的 stdio 传输规定：**一行一条 JSON 消息**，消息内部不能有裸换行。
所以客户端写出去的内容是：

```text
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}\n
{"jsonrpc":"2.0","method":"notifications/initialized"}\n
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}\n
```

服务端从 stdin 按行读，处理完把响应写到 stdout：

```text
{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25",...}}\n
```

注意：stdout 只能放协议消息；日志必须走 stderr，否则客户端会把日志当成
JSON 解析，直接报错。`app/mcp/server.py` 因此把日志级别默认设为 WARNING，
并且在 stdio 模式不打印启动地址。

## Pi 怎么做

Pi 的 `packages/mcp/src/transports/stdio.ts` 做了几件值得学的事：

1. 用 `readline` 按行切消息，而不是假设一次 `read()` 就是一条完整消息；
2. 关闭顺序是“关 stdin → 等一会儿 → SIGTERM → SIGKILL”；
3. 杀的是**整个进程组**，因为真实配置经常是 `npx some-server` 或
   `uvx some-server`，包装进程死了，真正的服务子进程可能还活着；
4. 保存 stderr 尾部，连接失败时能告诉用户服务端为什么退出。

## 我们怎么做

我们处在 Python + 本地模拟服务场景，按同样的思路实现，但代码更短：

- `asyncio.create_subprocess_exec` 启动子进程；
- 两个后台任务分别读 stdout（协议）和 stderr（日志）；
- `start_new_session=True` 让子进程成为新进程组组长，关闭时可以整组杀；
- 关闭时先礼貌关 stdin，再 SIGTERM，最后 SIGKILL 兜底。

## Python 语法提示

- `asyncio.subprocess.PIPE` 表示创建管道，父进程可以通过
  `process.stdin` / `process.stdout` 读写。
- `await process.stdout.readline()` 返回 `bytes`，因为管道是二进制流；
  要自己 `.decode("utf-8")`。
- `json.dumps(..., ensure_ascii=False)` 保留中文原样，方便教学时直接看日志；
  协议本身允许 UTF-8。
- `asyncio.create_task()` 启动的后台任务必须保存引用，否则可能被垃圾回收；
  关闭时也要 `cancel()` 并 `await`，避免留下“任务还在读已经关闭的管道”。
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections import deque
from typing import Any

from app.mcp.client import McpClientConfig
from app.mcp.pi_style.transport import BasePiTransport, PiTransportError


class StdioPiTransport(BasePiTransport):
    """用子进程管道实现的 MCP 传输。

    用法（通常由 `PiStyleMcpClient` 创建，不需要手动 new）：

    ```python
    config = McpClientConfig(transport="stdio")
    transport = StdioPiTransport(config)
    await transport.start()
    await transport.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", ...})
    async for message in transport.receive():
        print(message)
    await transport.close()
    ```
    """

    def __init__(self, config: McpClientConfig) -> None:
        super().__init__()
        self.config = config
        self._process: asyncio.subprocess.Process | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        # deque 是“有最大长度的双端队列”：超过 80 行自动丢掉最旧的。
        # 服务端日志可能很多，不能无限往内存里堆；我们只需要最后几十行排查。
        self._stderr_tail: deque[str] = deque(maxlen=80)
        self._close_lock = asyncio.Lock()

    @property
    def stderr_tail(self) -> str:
        """返回服务端 stderr 的尾部，连接失败时拼进错误信息。"""
        return "\n".join(self._stderr_tail)

    async def start(self) -> None:
        """启动 MCP Server 子进程，并开始读 stdout/stderr。

        配置里的 `stdio_parameters()` 已经处理了：
        - `command` 字符串按 shell 规则切成命令和参数；
        - 默认使用当前虚拟环境的 Python 解释器；
        - 把 `SUPPORT_PLATFORM_MCP_DATASET` 传给子进程；
        - `cwd` 固定到项目根目录，保证 `python -m app.mcp.server` 能 import。
        """
        parameters = self.config.stdio_parameters()
        command = [parameters.command, *parameters.args]
        env = dict(parameters.env or os.environ)
        cwd = parameters.cwd or str(self.config.project_root)

        try:
            self._process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=env,
                # POSIX 上让子进程成为新进程组组长。
                # 关闭时用 os.killpg 可以把它派生出来的子进程一起收掉。
                # Windows 没有进程组信号，start_new_session 会被忽略。
                start_new_session=True,
                # 单条 JSON 可能比较大（例如 limit=50 的慢日志），
                # 默认 64 KiB 的行缓冲上限不够保险，这里放大到 10 MiB。
                limit=10 * 1024 * 1024,
            )
        except OSError as exc:
            raise PiTransportError(self._format_start_error(exc)) from exc

        # 两个后台任务：一个读协议 stdout，一个读日志 stderr。
        # 必须保存 Task 引用，否则事件循环只持弱引用，任务可能被回收。
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())

    def _format_start_error(self, exc: OSError) -> str:
        """把“命令找不到/没有权限”翻译成人类能看懂的话。"""
        command = self.config.command or "python -m app.mcp.server"
        return (
            f"无法启动 MCP stdio 子进程：{command!r}（cwd={self.config.project_root}）："
            f"{exc}"
        )

    async def _read_stdout(self) -> None:
        """持续读 stdout，每读到一行就解析成 JSON 放进收件箱。

        为什么用 `while True` 而不是 `while process.stdout`：
        `readline()` 在流结束时返回 `b""`，这是最可靠的结束信号。
        收到空字节就退出循环，让上层知道连接已经断开。
        """
        process = self._process
        if process is None or process.stdout is None:
            self._fail(PiTransportError("stdio 子进程没有 stdout"))
            return

        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    # 子进程退出或关闭了 stdout。把 stderr 尾部带上，
                    # 比只报一句“连接断开”更容易定位。
                    if not self._closed:
                        message = "MCP stdio 子进程已关闭 stdout"
                        if self.stderr_tail:
                            message += f"；stderr 尾部：\n{self.stderr_tail}"
                        self._fail(PiTransportError(message))
                    return

                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    message = json.loads(text)
                except json.JSONDecodeError as exc:
                    self._fail(
                        PiTransportError(
                            f"MCP stdio 收到非 JSON 行：{text[:500]!r}（{exc}）"
                        )
                    )
                    return
                if not isinstance(message, dict):
                    self._fail(PiTransportError(f"MCP 消息必须是 JSON 对象：{message!r}"))
                    return
                self._push(message)
        except asyncio.CancelledError:
            # 关闭时我们会主动 cancel 这个任务，这是正常路径，原样抛出即可。
            raise
        except Exception as exc:  # noqa: BLE001 - 后台任务必须把任何异常交给上层
            self._fail(PiTransportError(f"读取 MCP stdio stdout 失败：{exc}"))

    async def _read_stderr(self) -> None:
        """读 stderr 并保留最后若干行。

        为什么不把 stderr 直接接到父进程：
        MCP 的 stdout 是协议通道，stderr 是日志通道。我们保留 stderr
        但不打印到父进程，避免测试输出混乱；连接失败时再拼进异常信息。
        """
        process = self._process
        if process is None or process.stderr is None:
            return

        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    return
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self._stderr_tail.append(text)
        except asyncio.CancelledError:
            raise
        except Exception:
            # stderr 只是诊断信息，读取失败不应该影响协议主流程。
            return

    async def send(self, message: dict[str, Any]) -> None:
        """把一条消息写成一行 JSON，写到子进程 stdin。

        `json.dumps` 默认会把中文转成 `\\uXXXX`；`ensure_ascii=False`
        保留中文，日志更易读。`separators=(",", ":")` 去掉多余空格，
        让一行消息更短。
        """
        if self._closed:
            raise PiTransportError("stdio 传输已关闭")
        process = self._process
        if process is None or process.stdin is None:
            raise PiTransportError("stdio 子进程尚未启动")

        line = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        try:
            process.stdin.write(line.encode("utf-8"))
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            detail = f"；stderr 尾部：\n{self.stderr_tail}" if self.stderr_tail else ""
            raise PiTransportError(f"写入 MCP stdio 子进程失败：{exc}{detail}") from exc

    async def close(self) -> None:
        """关闭 stdin、终止子进程，并等待后台读取任务退出。

        顺序和 Pi 一致，只是每一步都加了超时，避免关闭流程自己卡死：

        ```text
        1. 关闭 stdin      让服务端知道客户端不再发消息
        2. 等 0.5 秒        给服务端一个正常退出的机会
        3. SIGTERM 进程组  还不退出就礼貌杀
        4. 等 2 秒
        5. SIGKILL 进程组  最后兜底
        ```

        `asyncio.Lock` 保证 `close()` 被调用两次时不会重复杀进程。
        `async with` 退出和异常清理都可能调用 close，所以这很重要。
        """
        async with self._close_lock:
            if self._closed:
                return
            # 先标记关闭，阻止读取任务继续往队列里塞新消息。
            self._closed = True

            process = self._process
            if process is not None and process.stdin is not None:
                try:
                    process.stdin.close()
                    # `wait_closed()` 在部分 Python 版本上可能抛 BrokenPipe，
                    # 这里忽略：进程已经退出时关管道失败是正常现象。
                    await process.stdin.wait_closed()
                except (BrokenPipeError, ConnectionResetError, RuntimeError):
                    pass

            if process is not None and process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), timeout=0.5)
                except TimeoutError:
                    self._terminate_process_group(process, signal.SIGTERM)
                    try:
                        await asyncio.wait_for(process.wait(), timeout=2.0)
                    except TimeoutError:
                        self._terminate_process_group(process, signal.SIGKILL)
                        await process.wait()

            # 取消并等待后台任务，确保它们不会在事件循环关闭后还去读管道。
            for task in (self._stdout_task, self._stderr_task):
                if task is not None and not task.done():
                    task.cancel()
            for task in (self._stdout_task, self._stderr_task):
                if task is not None:
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        # 关闭阶段的读取异常没有诊断价值；主异常已经由调用方处理。
                        pass

            self._close_inbox()

    def _terminate_process_group(
        self,
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> None:
        """向整个进程组发信号；Windows 退回单进程 terminate/kill。

        例子：配置命令是 `uv run python -m app.mcp.server`。`uv` 是父进程，
        真正跑 Python 服务的是它的子进程。只杀 `uv` 可能留下 Python 进程，
        所以 POSIX 上用 `os.killpg(os.getpgid(pid), sig)` 整组杀。
        """
        if process.returncode is not None:
            return
        try:
            if os.name == "nt":
                if sig == signal.SIGKILL:
                    process.kill()
                else:
                    process.terminate()
            else:
                os.killpg(os.getpgid(process.pid), sig)
        except (ProcessLookupError, PermissionError):
            # 进程已经退出，或当前用户没有权限；都不应该在关闭流程里再抛错。
            pass
