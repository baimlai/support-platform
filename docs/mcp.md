# MCP 接入说明：从 JSON-RPC 到 SSE

> 这份文档配合代码阅读，不要求先懂 MCP。
> 目标：跑通一次调查后，你能说清楚“模型发起工具调用”之后，消息到底经过了哪些层。

## 1. 先纠正一个常见误解

MCP 不是“让模型直接调用工具”的魔法。模型侧仍然是普通的 tool call：

```text
模型：我要调用 slow_log_query，参数是 {...}
```

MCP 改变的是**平台怎么发现工具、怎么调用工具**：

```text
模型 tool call
  -> Agent 内核
  -> ToolSpec.handler
  -> MCP Client
  -> JSON-RPC 2.0
  -> MCP Server
  -> 真正的工具实现
```

所以本项目里同时存在两套“名字”：

| 层 | 看到的东西 | 例子 |
| --- | --- | --- |
| 模型 | 工具声明（名称、描述、JSON Schema） | `slow_log_query` |
| Agent 内核 | `ToolSpec` + handler | `ToolSpec(name="slow_log_query", ...)` |
| MCP | `tools/list`、`tools/call` | JSON-RPC 消息 |
| 业务 | `app/mcp/dba_tools.py` | 读 fixtures 返回模拟数据 |

模型不需要知道 MCP；它只需要知道有哪些工具。MCP 是宿主（Agent 进程）
和工具服务之间的约定。

## 2. 本项目的 MCP 拓扑

```text
┌──────────────────────────────┐
│ FastAPI / CLI / 测试          │
│  AgentRunner                 │
│    -> DbaMcpClient           │  stdio 子进程，或 HTTP 连接
│       -> 官方 Python MCP SDK │
└──────────────┬───────────────┘
               │ JSON-RPC 2.0
┌──────────────▼───────────────┐
│ app.mcp.server (FastMCP)     │
│  slow_log_query              │
│  execution_plan              │
│  instance_metrics            │
│  lock_wait_query             │
│    -> app/mcp/dba_tools.py   │
│       -> fixtures/           │
└──────────────────────────────┘
```

默认 `stdio`：`AgentRunner` 启动 `python -m app.mcp.server` 子进程，
通过 stdin/stdout 通信。也可以手动启动 Streamable HTTP 服务，再让 Agent
连接 `http://127.0.0.1:8765/mcp`。

**数据仍然是模拟的，协议和进程边界是真的。**

## 3. 一次 MCP 会话的完整生命周期

### 3.1 总览

```text
Client                                      Server
  │                                            │
  │  initialize (id=1)                         │
  │ ─────────────────────────────────────────► │
  │                                            │  协商协议版本、能力
  │  result: protocolVersion, serverInfo       │
  │ ◄───────────────────────────────────────── │
  │                                            │
  │  notifications/initialized                 │
  │ ─────────────────────────────────────────► │
  │                                            │
  │  tools/list (id=2)                         │
  │ ─────────────────────────────────────────► │
  │  result: tools[]                           │
  │ ◄───────────────────────────────────────── │
  │                                            │
  │  tools/call (id=3, name=slow_log_query)    │
  │ ─────────────────────────────────────────► │
  │  result: content[], structuredContent      │
  │ ◄───────────────────────────────────────── │
  │                                            │
  │  close / DELETE session                    │
  │ ─────────────────────────────────────────► │
```

### 3.2 initialize

请求：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-11-25",
    "capabilities": {},
    "clientInfo": {"name": "support-platform-agent", "version": "0.1.0"}
  }
}
```

响应：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocolVersion": "2025-11-25",
    "capabilities": {
      "tools": {"listChanged": false}
    },
    "serverInfo": {
      "name": "support-platform-dba-mcp",
      "version": "1.30.0"
    },
    "instructions": "这是 DBA 工单调查的只读工具服务……"
  }
}
```

要读懂三个字段：

- `protocolVersion`：双方协商后的版本。客户端请求的版本服务端不一定支持，
  以响应为准；
- `capabilities`：服务端声明自己会什么。`tools.listChanged=false` 表示
  工具列表不会在会话中途变化；
- `serverInfo`：服务端身份，排查“到底连到了哪个服务”时很有用。

### 3.3 notifications/initialized

这是**通知**，没有 `id`，服务端不回复：

```json
{"jsonrpc":"2.0","method":"notifications/initialized"}
```

它表示“客户端已经准备好，可以开始正常请求了”。

### 3.4 tools/list

请求：

```json
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
```

响应（简化）：

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "tools": [
      {
        "name": "slow_log_query",
        "title": "慢日志查询",
        "description": "查询指定实例和时间范围内的慢日志摘要……",
        "inputSchema": {
          "type": "object",
          "properties": {
            "instance_id": {"type": "string"},
            "start_time": {"type": "string"},
            "end_time": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50}
          },
          "required": ["instance_id", "start_time", "end_time"]
        },
        "annotations": {
          "readOnlyHint": true,
          "destructiveHint": false,
          "idempotentHint": true,
          "openWorldHint": false
        }
      }
    ]
  }
}
```

分页：如果响应里出现 `nextCursor`，客户端要把它放进下一次请求的
`params.cursor`，直到没有下一页：

```json
{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{"cursor":"page-2"}}
```

当前只有四个工具，一次就取完；官方 SDK 和我们的 Pi 风格客户端都保留了这个循环。

### 3.5 tools/call

请求：

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "slow_log_query",
    "arguments": {
      "instance_id": "db-mysql-001",
      "start_time": "2026-10-29T10:00:00+08:00",
      "end_time": "2026-10-29T11:00:00+08:00",
      "limit": 5
    }
  }
}
```

响应：

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "result": {
    "content": [
      {"type": "text", "text": "{\"code\":\"ok\",\"data\":[...]}"}
    ],
    "structuredContent": {
      "details": {
        "top": {"sql_fingerprint": "a1b2c3d4e5f6"},
        "count": 1240
      },
      "mock": true,
      "payload": {"code": "ok", "data": []}
    },
    "isError": false
  }
}
```

三个结果字段的区别：

| 字段 | 给谁看 | 本项目用途 |
| --- | --- | --- |
| `content` | 模型 | 文本 JSON，进入下一轮模型输入 |
| `structuredContent` | 平台 | `details` 进入 Topic、证据索引和报告 |
| `isError` | 双方 | 业务失败标记；模型仍可继续调查 |

**`isError=true` 不是 JSON-RPC 错误。** 例子：监控接口超时，工具执行完了，
业务上失败；这时返回 `isError=true`，模型能看到原因并继续查锁等待。
真正的协议错误才返回 JSON-RPC `error`。

## 4. 三种传输

### 4.1 stdio

```text
Agent 子进程 ── stdin  ──► MCP Server
Agent 子进程 ◄─ stdout ──  MCP Server
                stderr ──► 日志（不是协议）
```

规则：

- 一行一条 JSON 消息；
- 消息内部不能有裸换行（JSON 字符串里的换行会转义成 `\n`）；
- stdout 只能放协议消息，日志必须走 stderr。

启动方式：

```bash
uv run python -m app.mcp.server --transport stdio
```

手动调试时，可以直接输入一行 JSON，再按回车：

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"manual","version":"0.1.0"}}}
```

适合：本地开发、单机工具、零网络配置。

### 4.2 Streamable HTTP（当前推荐）

```text
POST /mcp
Accept: application/json, text/event-stream
Content-Type: application/json
Mcp-Session-Id: ...
MCP-Protocol-Version: ...
```

服务端可能返回：

- `text/event-stream`：SSE 帧，当前 FastMCP 默认；
- `application/json`：单个 JSON（`json_response=True` 时）。

启动：

```bash
uv run python -m app.mcp.server --transport streamable-http --port 8765
# 端点：http://127.0.0.1:8765/mcp
```

会话：

1. 第一次 `initialize` 的响应头里有 `mcp-session-id`；
2. 后续请求都要带 `Mcp-Session-Id`；
3. 可选：带 `MCP-Protocol-Version`；
4. 结束时可以 `DELETE /mcp`，服务端不支持就返回 405。

### 4.3 旧版 HTTP + SSE（仅兼容/教学）

```text
GET  /sse        建立一条长连接，服务端持续推 SSE
POST /messages/  客户端发 JSON-RPC
```

启动：

```bash
uv run python -m app.mcp.server --transport sse --port 8765
```

Pi 的新版 MCP 客户端明确不支持这种方式；本项目保留它，是因为“学习 SSE”
需要看到最原始的 `event:` / `data:` 帧。它不应该用于新项目。

## 5. SSE 到底长什么样

### 5.1 帧格式

SSE 的响应体是纯文本，按行解释：

```text
event: message
id: 42
data: {"jsonrpc":"2.0","id":1,"result":{"tools":[]}}

```

规则：

- `event:` 事件类型，可省略；
- `id:` 事件 id，断线重连时可配合 `Last-Event-ID` 续传；
- `data:` 数据，可以出现多次，多行用换行拼接；
- **空行**表示一个事件结束；
- 以 `:` 开头的行是注释，例如保活 `: keep-alive`。

### 5.2 解析状态机

```text
读到 data: -> 追加到缓冲
读到空行   -> 缓冲非空？拼接 -> json.loads -> 产出一条消息 -> 清空
读到其它   -> 忽略（event/id/retry/注释）
流结束     -> 如果缓冲还有内容，补一次 dispatch
```

实现见 `app/mcp/pi_style/streamable_http.py` 的 `_iter_sse_json()`。
官方 SDK 内部也做同样的事，只是封装在 `streamable_http_client` 里。

### 5.3 用 curl 直接看

```bash
# 1. initialize，-N 关闭缓冲，-D 打印响应头
curl -N -D - http://127.0.0.1:8765/mcp \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"curl","version":"0.1.0"}}}'
```

你会看到类似：

```text
HTTP/1.1 200 OK
content-type: text/event-stream
mcp-session-id: 6d3f...

event: message
data: {"jsonrpc":"2.0","id":1,"result":{...}}
```

把返回的 session id 复制到后续请求：

```bash
curl -N http://127.0.0.1:8765/mcp \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -H 'Mcp-Session-Id: <上一步的 id>' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

## 6. 本项目的两条客户端路线

| 路线 | 代码 | 底层 | 用途 |
| --- | --- | --- | --- |
| 官方 SDK | `app/mcp/client.py` | `mcp` 包 | 运行时默认，稳定、跟随规范 |
| Pi 风格手写 | `app/mcp/pi_style/` | `asyncio` + `httpx` | 教学对照，看清协议细节 |

两条路线连接同一个 Server，返回同一个 `ToolResult`。测试会并排调用并断言结果相同：

```bash
uv run python -m unittest tests.test_mcp_pi_style -v
```

### 6.1 官方 SDK 帮我们做了什么

`DbaMcpClient` 只写了很少的协议代码：

```python
async with stdio_client(params) as (read, write):
    async with ClientSession(read, write) as session:
        init = await session.initialize()
        tools = await session.list_tools()
        result = await session.call_tool("slow_log_query", arguments)
```

SDK 内部负责：

- JSON-RPC 请求 id 和 pending Future；
- `initialize` / `notifications/initialized`；
- 分页；
- 多种传输；
- pydantic 消息类型和校验；
- 超时、取消、错误类型。

### 6.2 Pi 风格客户端手写了什么

`PiStyleMcpClient` 的核心只有四块：

1. `request()`：生成 id、建 Future、发送、等待；
2. `_read_loop()`：读消息，按 id 完成 Future；
3. `_initialize()`：握手并发 initialized 通知；
4. `list_tools()` / `call_tool()`：业务方法。

传输层只做两件事：

- `StdioPiTransport`：一行 JSON 写入 stdin，按行读 stdout；
- `StreamableHttpPiTransport`：POST，解析 SSE `data:` 帧，保存 session id。

### 6.3 并排跑一遍

```bash
# 两条路线都走 stdio，各自启动一个 MCP 子进程
uv run python -m app.cli mcp-compare --transport stdio

# 先启动 HTTP Server，再对比 HTTP 路线
uv run python -m app.cli mcp-serve --transport streamable-http --port 8765
uv run python -m app.cli mcp-compare \
  --transport streamable-http \
  --url http://127.0.0.1:8765/mcp
```

输出会并排打印：

```text
官方 Python SDK 客户端（运行时默认路线）
support-platform-dba-mcp v1.30.0 / 协议 2025-11-25 / 工具 ...
slow_log_query：1530 字符，top=a1b2c3d4e5f6
instance_metrics：is_error=True，实例监控接口超时（模拟）……

Pi 风格手写客户端（教学对照路线）
...同样内容...

对比结论
slow_log_query 文本一致：True
slow_log_query details 一致：True
instance_metrics 错误结果一致：True
```

协议追踪默认写到 `var/mcp-compare/official_protocol.jsonl` 和
`var/mcp-compare/pi_style_protocol.jsonl`。两份文件可以直接用 diff 工具对照。

## 7. Pi 的做法与取舍

Pi 的 `@earendil-works/pi-mcp` 是一个独立 TypeScript MCP 客户端，不依赖官方 SDK。
它自己实现了：

- stdio 和 Streamable HTTP；
- 请求 id、pending 表、超时、取消；
- 分页 `tools/list`；
- `Last-Event-ID` 断线续传、GET 事件流；
- 408/429/5xx 重试；
- 进程组清理；
- OAuth（从官方 TypeScript SDK 改编）。

Pi 明确不支持旧版 HTTP+SSE。它的 coding-agent 还把 MCP 工具命名成
`mcp__<server>__<tool>`，支持 `codemode` / `deferred` / `direct` / `hidden`
四种暴露模式。

本项目没有照搬这些产品级能力，原因：

1. 当前是单 Server、短请求的 DBA 调查，不需要多服务器命名空间；
2. 常驻连接、重连、OAuth、resources 不是任务书要求；
3. 教学重点是“协议长什么样”，不是“重写一套生产级 MCP SDK”。

但我们保留了 Pi 最有价值的设计对照：**传输层和协议层分开**、
**请求 id + pending Future**、**stdio 进程组清理**、**SSE 帧解析**。
这些都在 `app/mcp/pi_style/` 里逐行注释。

## 8. 怎么调试

### 8.1 查看协议追踪

每次 `AgentRunner` 运行都会写：

```text
var/runs/<run_id>/mcp_protocol.jsonl
```

每行：

```json
{"ts":"...","direction":"send","message":{"jsonrpc":"2.0","id":1,"method":"initialize",...}}
{"ts":"...","direction":"recv","message":{"jsonrpc":"2.0","id":1,"result":{...}}}
```

常用命令：

```bash
# 只看方法名
jq -r 'select(.direction=="send") | .message.method // "(response)"' \
  var/runs/<run_id>/mcp_protocol.jsonl

# 看某次 tools/call 的完整请求和响应
jq -c 'select(.message.id==4)' var/runs/<run_id>/mcp_protocol.jsonl
```

### 8.2 打开 Server 日志

```bash
SUPPORT_PLATFORM_MCP_LOG_LEVEL=DEBUG \
  uv run python -m app.mcp.server --transport stdio
```

stdio 模式下日志必须走 stderr，否则会污染协议通道。我们的客户端会保留
stderr 尾部，连接失败时把它拼进异常信息。

### 8.3 常见错误

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `MCP stdio 收到非 JSON 行` | 服务端把日志写到了 stdout | 日志改 stderr，或调低日志级别 |
| `MCP HTTP 404` | 路径不是 `/mcp` | Streamable HTTP 用 `/mcp`，旧版 SSE 用 `/sse` |
| `Mcp-Session-Id` 缺失 | 服务端是无状态模式，或 initialize 失败 | 先看 initialize 响应和 Server 日志 |
| `tools/list` 为空 | Server 没注册工具，或连错服务 | 对比 `serverInfo.name` |
| `isError=true` | 工具业务失败 | 看 `content` 文本和 `details`，不是协议坏了 |
| 客户端超时 | 工具太慢或 Server 卡住 | 调大 `--mcp-timeout`，看 `notifications/cancelled` |

## 9. 代码阅读顺序

建议按这个顺序看：

1. `app/mcp/dba_tools.py`：四个工具的业务实现，和 MCP 无关；
2. `app/mcp/server.py`：FastMCP 怎么注册工具、怎么返回 `CallToolResult`；
3. `app/mcp/client.py`：官方 SDK 客户端怎么把 MCP 结果翻译成 `ToolResult`；
4. `app/mcp/pi_style/transport.py`：传输接口和收件箱；
5. `app/mcp/pi_style/stdio.py`：一行一条 JSON 和子进程清理；
6. `app/mcp/pi_style/streamable_http.py`：POST、SSE 帧、session id；
7. `app/mcp/pi_style/client.py`：请求 id、Future、initialize、tools/call；
8. `tests/test_mcp.py`、`tests/test_mcp_pi_style.py`：真实进程和真实 HTTP 的验证。

## 10. 当前边界

- 工具结果和数据仍然是 `fixtures/` 模拟数据，不连接生产库；
- 只实现 tools，不实现 prompts、resources、sampling；
- 不支持 OAuth；
- Pi 风格客户端不支持旧版 SSE；
- Pi 风格客户端没有实现 GET 长连接、`Last-Event-ID` 续传、自动重连；
- 每次调查新建连接，调查结束关闭，不做常驻连接池。

这些边界是刻意保留的：先把协议主路径走通、测透，再按需要增加产品级能力。
