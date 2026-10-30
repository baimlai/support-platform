# Pi 的 MCP 接入 vs 本项目当前实现

> 结论先行：**采用双轨。** 运行时默认继续用**官方 Python SDK**，保证协议正确和稳定；同时新增一份**按 Pi 思路手写的 Python 教学客户端**（stdio + Streamable HTTP），逐行对照 JSON-RPC 请求 id、pending Future、子进程管道和 SSE 帧解析。不照搬 Pi 的 codemode / OAuth / 资源系统，也不把官方 SDK 换掉。
>
> 该方案已在 2026-10-30 确认并落地，代码见 `app/mcp/pi_style/`，对比命令见 `uv run python -m app.cli mcp-compare`，协议说明见 `docs/mcp.md`。

## 1. 先说清楚：Pi 的 MCP 到底在哪一层

Pi 的 Agent 内核本身仍然只认“模型发 tool call → 执行工具 → 把结果回给模型”。
MCP 不改变这个循环，它改变的是**工具从哪里发现、怎么调用**。

Pi 的做法分两层：

```text
Pi Agent Loop（pi-agent-core）
  -> AgentTool（普通工具接口）
  -> Pi 的 MCP 适配层
  -> @earendil-works/pi-mcp（自己实现的 MCP Client）
  -> stdio / Streamable HTTP
  -> 外部 MCP Server
```

关键点：

1. **Pi 没有把 MCP 写进 Agent 内核。** MCP 是 coding-agent 的内置扩展
   （`builtin:mcp`），通过扩展机制注册服务器、连接服务器、把 MCP 工具转成
   `AgentTool`。
2. **Pi 没有使用官方 MCP TypeScript SDK。** `@earendil-works/pi-mcp`
   是独立实现的客户端，只保留自己需要的协议面；官方 SDK 只作为 OAuth 部分的
   参考来源。
3. **Pi 的 MCP 工具最终仍然变成普通的 `AgentTool`。** 所以模型侧看到的还是
   tool call，不是某种“MCP 专用消息”。

这和本项目当前的设计是同一个大方向：

```text
Pi 风格 Agent Loop
  -> ToolSpec.handler
  -> 我们的 DbaMcpClient
  -> 官方 Python MCP SDK
  -> FastMCP Server
  -> fixtures
```

## 2. Pi 的实现要点（基于 `@earendil-works/pi-mcp` 1.4.0 和 coding-agent 源码）

### 2.1 传输

- 支持 `stdio`；
- 支持 `Streamable HTTP`；
- 有内存测试传输；
- **明确不支持旧版 HTTP+SSE**（文档原文：legacy HTTP+SSE 不在初始核心范围内）；
- 协议版本 `2025-11-25`，兼容 `2025-06-18`、`2025-03-26`、`2024-11-05`。

### 2.2 协议能力

Pi 自己实现了：

- `initialize` / `notifications/initialized`；
- 分页 `tools/list`；
- `tools/call`；
- 进度通知、超时续期；
- 请求取消；
- Streamable HTTP 会话、GET 事件流重连、`Last-Event-ID` 续传；
- stdio 关闭顺序：关 stdin → SIGTERM → SIGKILL，并且杀整个进程组；
- 服务端 `ping`、`roots/list`；
- OAuth（PKCE、动态客户端注册、刷新令牌等）。

### 2.3 coding-agent 里的工具暴露

Pi 把 MCP 工具命名成：

```text
mcp__<server>__<tool>
```

非法字符替换成 `_`，超过 64 字符加哈希后缀，避免不同工具名撞车。

Pi 的暴露模式：

| 模式 | 含义 |
| --- | --- |
| `codemode`（默认） | 不直接声明给模型，模型写 JavaScript 在 QuickJS 沙箱里调用；脚本通过 `searchTools()` 发现工具 |
| `deferred` | 不直接声明，等 `tool_search` 命中后才在下一轮声明给模型 |
| `direct` | 像普通工具一样直接声明给模型 |
| `hidden` | 注册但不可达 |

Pi 还支持按服务器、按工具配置暴露方式，支持通配符。

### 2.4 连接生命周期

- 会话启动时后台连接所有启用的 MCP Server；
- 第一条提示最多等 10 秒，而且只等“有 direct 工具”的服务器；
- 其它服务器按需等待；
- HTTP 的 408/429/5xx 最多重试两次；
- 连接断开后标记 disconnected，下次调用再重连；
- 工具列表变化时，增加新工具、移除已撤销工具；
- stdio 服务失败时，`/mcp` 能看到服务端 stderr 的尾部；
- 日志写到 `~/.pi/agent/mcp.log`，5 MB 轮转。

### 2.5 配置与认证

- `~/.pi/agent/mcp.json`（用户级）和 `.pi/mcp.json`（项目级）；
- 项目级配置要等项目被信任后才加载；
- `command` 是一个可执行文件，不是 shell 字符串；
- 环境变量和 HTTP header 支持 `${VAR}` 展开和 `!command` 执行；
- OAuth 凭据存在 `~/.pi/agent/mcp-auth.json`。

### 2.6 资源

Pi 额外暴露三个资源工具：

```text
list_mcp_resources
list_mcp_resource_templates
read_mcp_resource
```

这说明 Pi 不只接 MCP 的 tools，也接 resources。

## 3. 本项目当前实现

### 3.1 已经完成的部分

- 真实 FastMCP Server：`app/mcp/server.py`；
- 真实 MCP Client：`app/mcp/client.py`；
- 支持三种传输：`stdio`、`streamable-http`、`sse`；
- 完整的 `initialize` → `notifications/initialized` → `tools/list` → `tools/call`；
- 分页 `tools/list`；
- `CallToolResult` 的 `content`、`structuredContent`、`isError` 映射；
- 工具注解 `readOnlyHint` 等原样保留；
- 原始 JSON-RPC 追踪：`mcp_protocol.jsonl`；
- AgentRunner 每次运行真实连接 MCP Server；
- `/api/capabilities` 展示 MCP 连接信息；
- CLI `mcp-serve` 可独立启动 Server；
- 专项测试覆盖 stdio、Streamable HTTP、旧版 SSE、原始 HTTP+SSE 帧；
- 当前全量测试 57 个通过；其中 `tests/test_mcp_pi_style.py` 专门对比两条客户端路线。

### 3.2 和 Pi 的主要差异

| 维度 | Pi | 本项目当前 | 评价 |
| --- | --- | --- | --- |
| MCP 客户端 | 自己实现，不依赖官方 SDK | 官方 Python SDK `mcp` 1.30.0 | Python 生态下官方 SDK 更稳，协议细节由 SDK 维护 |
| 传输 | stdio + Streamable HTTP | stdio + Streamable HTTP + 旧版 SSE | 旧版 SSE 是额外学习项，不是默认 |
| 工具命名 | `mcp__<server>__<tool>` | 原名，如 `slow_log_query` | 单服务器够用；多服务器会有撞名风险 |
| 暴露模式 | codemode / deferred / direct / hidden | Skill 初始工具 + `search_tools` 按需加载 | 行为上接近 deferred + direct，但没有显式元数据 |
| 发现方式 | `tools/list` + tool search | `tools/list` + 本地 `search_tools` | 方向一致，搜索实现不同 |
| 资源 | 支持 resources 工具 | 暂不支持 | 当前 DBA 工具没有 resources |
| OAuth | 完整支持 | 不支持 | 当前是本地模拟服务，不需要 |
| 连接生命周期 | 后台连接、重试、重连、工具列表变化 | 每次运行同步连接，运行结束关闭 | 一次性调查场景够用；常驻服务需要补 |
| stdio 清理 | 关 stdin → SIGTERM → SIGKILL，杀进程组 | 交给官方 SDK 管理 | SDK 已能清理子进程，但进程组细节未单独验证 |
| 配置 | `mcp.json`，支持多服务器 | 环境变量 / CLI 参数，单服务器 | 当前够用，多服务器前需要升级 |
| 工具命名空间 | 显式 server 命名空间 | 无 | 多服务器前必须补 |

## 4. 为什么不建议照搬 Pi 的实现

### 4.1 Pi 手写客户端是“产品取舍”，不是“协议要求”

Pi 不依赖官方 SDK，是为了控制协议面、减少依赖、实现自己的重连和进程组管理。
这不代表官方 SDK 不真实。对本项目：

- 用官方 Python SDK 仍然是真实 MCP；
- 协议消息、JSON-RPC、SSE 帧都能在 `mcp_protocol.jsonl` 和测试里看到；
- 少维护一套 JSON-RPC 状态机，更适合学习“协议长什么样”，而不是“怎么手写协议栈”。

### 4.2 codemode 不适合当前 DBA 工单场景

Pi 的 codemode 需要 QuickJS 沙箱，让模型写 JavaScript 批量调用工具。
本项目的工具调用是 DBA 调查流程，模型直接发 tool call 更清楚，也更容易做
证据落盘、Topic 更新和审计。强行上 codemode 会增加沙箱、超时、结果过滤等
大量与 DBA 无关的复杂度。

### 4.3 旧版 SSE 值得保留为“学习模式”

Pi 已经拒绝 legacy SSE，这是对的：新项目应该用 Streamable HTTP。
但你的目标包含“学习 SSE”，所以保留 `sse` 传输、并在测试和文档里明确它是
**旧版兼容/教学用途**，比直接删掉更符合本项目目标。

### 4.4 Pi 的完整能力是“产品级”，不是本轮任务书要求

OAuth、resources、断线重连、日志轮转、配置继承、项目信任，都是 coding-agent
产品需要的能力。当前任务是 DBA 工单调查小平台，优先保证：

1. MCP 协议是真的；
2. 工具调用链能跑通；
3. 结果和失败能进入证据/Topic；
4. 代码能读懂、能测试。

## 5. 建议方案

### 方案 A+（推荐）：官方 SDK + 关键设计对齐 Pi

保留现在能跑的 MCP 链路，只补三项“低成本、高价值”的对齐：

1. **工具命名预留命名空间**
   - 当前单服务器继续用原名，避免破坏 Skill 和模型剧本；
   - 在 `ToolSpec` / 能力目录里增加 `server` 元数据；
   - 接第二个 MCP Server 前，统一切换到 `mcp__<server>__<tool>`，
     并做 64 字符截断和哈希去重。

2. **显式暴露模式**
   - 把当前行为写成元数据：
     - `slow_log_query`：`direct`（Skill 初始直接声明）；
     - 其余业务工具：`deferred`（`search_tools` 命中后下一轮声明）；
   - 不实现 codemode，只保留 Pi 的 `direct` / `deferred` 两个概念。

3. **传输定位**
   - 默认 `stdio`；
   - 远程推荐 `streamable-http`；
   - `sse` 保留为旧版兼容/教学模式，文档明确标注 deprecated；
   - 不为 legacy SSE 做重连和续传。

### 方案 B：严格对齐 Pi

- 工具名改成 `mcp__support_platform_dba__<tool>`；
- 删除 legacy SSE，只留 stdio + Streamable HTTP；
- 增加后台连接、重试、断线重连、工具列表变化；
- 增加 resources 工具；
- 暂不做 codemode/OAuth。

代价：要改 Skill 初始工具名、mock 模型剧本、测试断言和前端展示；
当前 57 个测试里会有一批需要重写。

### 方案 C：完整移植 Pi 的 MCP 能力

包括 codemode、OAuth、resources、配置继承、进程组管理、日志轮转。
不建议本轮做：工作量大，且大部分与 DBA 工单调查无关。

## 6. 已确认的实现方案：双轨

2026-10-30 确认的最终方案：

1. **运行时默认仍是官方 Python SDK**：`app/mcp/client.py`，保证现有
   AgentRunner、FastAPI、CLI、测试和真实 DeepSeek 调查不受影响。
2. **新增 Pi 风格手写客户端**：`app/mcp/pi_style/`，只做标准库 + `httpx`：
   - `transport.py`：传输接口 + 收件箱队列；
   - `stdio.py`：子进程管道、逐行 JSON、进程组清理；
   - `streamable_http.py`：POST /mcp、SSE 帧解析、`Mcp-Session-Id`；
   - `client.py`：请求 id、pending Future、initialize、tools/list、tools/call。
3. **教学对照**：新增 `mcp-compare` 命令，并排调用两条路线，打印结果差异
   和两份协议追踪；`tests/test_mcp_pi_style.py` 断言同一工具调用的文本和
   `details` 完全一致。
4. **工具命名**：单服务器继续用原名，不破坏 Skill 和模型剧本；多服务器时
   再引入 Pi 的 `mcp__<server>__<tool>` 命名空间。
5. **旧版 SSE**：官方客户端保留为旧版兼容/教学模式；Pi 风格客户端明确拒绝，
   和 Pi 的取舍一致。
6. **不实现**：codemode、OAuth、resources、GET 长连接、`Last-Event-ID` 续传、
   自动重连、常驻连接池。这些留作后续练习，当前文档明确标注边界。

## 7. 参考来源

- Pi MCP 包：`@earendil-works/pi-mcp` 1.4.0（npm / `packages/mcp`）
  - README：传输、协议面、`toLlmContent`、`AgentTool` 映射
  - `packages/mcp/src/client.ts`：请求关联、初始化、超时、取消
  - `packages/mcp/src/transports/stdio.ts`、`streamable-http.ts`
- Pi coding-agent：
  - `packages/coding-agent/docs/mcp.md`
  - `packages/coding-agent/src/extensions/mcp/tools.ts`
  - `packages/coding-agent/src/extensions/mcp/runtime.ts`
  - `packages/coding-agent/src/core/mcp-servers.ts`
- Pi 工具暴露示例：
  - `packages/agent/examples/mcp-codemode/tools.ts`
  - `packages/agent/examples/mcp-codemode/main.ts`
- Pi 仓库：<https://github.com/earendil-works/pi>
