"""Pi 风格的手写 MCP 客户端（教学对照实现）。

## 两条路线怎么选

| 场景 | 建议 |
| --- | --- |
| 正常跑 Agent、跑测试、接真实服务 | `app.mcp.client.DbaMcpClient`（官方 SDK） |
| 想看清 JSON-RPC 请求怎么配对、SSE 怎么解析 | 本包 `PiStyleMcpClient` |
| 想对照阅读 Pi 的 TypeScript 实现 | 先看 `client.py`，再看 `stdio.py` / `streamable_http.py` |
| 需要旧版 HTTP+SSE | 官方客户端的 `sse` 传输；本包不支持 |

## 文件分工

- `transport.py`：传输接口和共用的“收件箱队列”；
- `stdio.py`：子进程 stdin/stdout，一行一条 JSON；
- `streamable_http.py`：POST /mcp + SSE 帧解析 + Mcp-Session-Id；
- `client.py`：请求 id、pending Future、initialize、tools/list、tools/call。

## 和官方客户端的关系

两条路线连接的是同一个 FastMCP Server，调用的是同一组 DBA 工具，
返回的 `ToolResult` 形状也一致。测试 `tests/test_mcp_pi_style.py` 会并排调用
两边，断言文本和 `details` 相同。这样你可以确认：

> “官方 SDK 不是魔法，它只是替我们做了这些协议工作。”

更完整的协议说明、SSE 示例和 Pi 对照见 `docs/mcp.md`。
"""

from app.mcp.pi_style.client import (
    CLIENT_NAME,
    CLIENT_VERSION,
    PROTOCOL_VERSION,
    PiStyleMcpClient,
    PiStyleRpcError,
)
from app.mcp.pi_style.stdio import StdioPiTransport
from app.mcp.pi_style.streamable_http import StreamableHttpPiTransport
from app.mcp.pi_style.transport import PiTransport, PiTransportError

__all__ = [
    "CLIENT_NAME",
    "CLIENT_VERSION",
    "PROTOCOL_VERSION",
    "PiStyleMcpClient",
    "PiStyleRpcError",
    "PiTransport",
    "PiTransportError",
    "StdioPiTransport",
    "StreamableHttpPiTransport",
]
