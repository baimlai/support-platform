"""真实 MCP（Model Context Protocol）接入层。

这个包不是“把本地函数换个名字”，而是完整实现了 MCP 的客户端和服务端：

```text
Agent Loop（Pi 的 Python 移植）
  -> ToolSpec.handler
  -> DbaMcpClient                ← JSON-RPC 2.0 客户端
  -> stdio / Streamable HTTP / SSE
  -> FastMCP Server              ← 独立进程里的工具服务
  -> fixtures 模拟数据
```

## 文件分工

- `dba_tools.py`：四个 DBA 工具的业务实现和静态声明；
- `server.py`：把业务工具注册到真实 FastMCP Server，支持三种传输；
- `client.py`：官方 Python SDK 客户端，完成 initialize、tools/list、tools/call，
  并把结果翻译成平台内核认识的 `ToolResult`；
- `info.py`：两条客户端路线共用的连接摘要；
- `pi_style/`：按 Pi 思路手写的教学客户端。传输层、JSON-RPC 请求 id、
  pending Future、SSE 帧解析都可以逐行看，用来和官方 SDK 对照。

## 为什么不用“本地 import 函数”

模型看到的工具定义、调用参数、返回结果，现在都经过 MCP 协议。
Agent 和工具服务可以分别部署、分别升级，也可以用 `mcp_protocol.jsonl`
观察每一条 JSON-RPC 消息。这正是 MCP 相比普通 Function Call 的价值：
**统一发现和调用方式**，而不是把工具硬编码进 Agent 进程。

## 学习入口

1. `uv run python -m app.mcp.server --transport stdio` 手动启动服务；
2. 跑一次 CLI 调查，查看 `var/runs/<run_id>/mcp_protocol.jsonl`；
3. 阅读 `docs/mcp.md`，对照 JSON-RPC 请求、SSE 事件和三种传输的差异；
4. 跑 `uv run python -m app.cli mcp-compare --transport stdio`，
   并排观察官方 SDK 和 Pi 风格客户端的调用结果与协议追踪。
"""
