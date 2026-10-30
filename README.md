# DBA 工单 Agent 小平台（Python 后端）

一个可本地运行的 DBA 工单调查小平台：按任务书要求，从 Pi Agent 选择性移植一个最小
Python 内核，配上**模拟的只读 DBA 工具**，把一条 DBA 工单从输入跑到调查报告。

当前进度：**第 3 步“HTTP API + 前端联调”已完成，并扩展到 10 个多数据库 Mock 场景**。
除了 MySQL 慢 SQL，还覆盖 MySQL 行锁、PostgreSQL 长事务/DDL 锁、Redis 热 Key/内存淘汰、
MongoDB COLLSCAN/复制延迟、Elasticsearch 磁盘水位和 ClickHouse Parts 积压。
FastAPI 提供任务创建/查询/取消、事件轮询/SSE、轮次上下文、证据分页和报告接口；
前端工作台可以按任务选择模型来源。模型后端是可选的：默认使用本地模拟模型，
也可以选择 DeepSeek（需要服务端配置 Key）。

> **重要：当前没有连接任何真实数据库或监控系统。**
> 所有工单、慢日志、执行计划、监控和锁等待数据都来自 `fixtures/`，并且明确标记了
> `mock=true`。报告内容只用于验证 Agent 流程，不代表真实客户、实例或业务事实。

## 快速开始

环境要求：Python 3.11+（推荐 3.12）、[uv](https://docs.astral.sh/uv/)。

```bash
cd /Users/baiml/PycharmProjects/support-platform

# 安装依赖并创建 .venv
uv sync --python 3.12

# 查看 Skill 和工具
uv run python -m app.cli list-skills
uv run python -m app.cli list-tools

# 跑一张模拟慢 SQL 工单
uv run python -m app.cli run --ticket fixtures/tickets/slow_sql_001.json

# 跑多数据库场景（Redis 热 Key、ClickHouse Parts 等）
uv run python -m app.cli run --ticket fixtures/tickets/redis_hot_key_001.json
uv run python -m app.cli run --ticket fixtures/tickets/clickhouse_too_many_parts_001.json

# 启动 HTTP 服务（另开一个终端），前端默认连 127.0.0.1:8000
uv run python -m app.cli serve --host 127.0.0.1 --port 8000

# 可选：让页面/接口能选择 DeepSeek。Key 只放环境变量，不要写进代码。
export DEEPSEEK_API_KEY=sk-...
uv run python -m app.cli serve --host 127.0.0.1 --port 8000

# 也可以用 CLI 直接跑一张真实模型调查
uv run python -m app.cli run \
  --ticket fixtures/tickets/slow_sql_001.json \
  --model-provider deepseek --model deepseek-flash

# 跑测试和静态检查
uv run python -m unittest discover -s tests -v
uv run ruff check .
```

服务起来后：

```bash
curl -sS http://127.0.0.1:8000/api/health
curl -sS http://127.0.0.1:8000/api/samples
curl -sS http://127.0.0.1:8000/api/models

# 创建任务 → 立刻返回 202 和 task_id
curl -sS -X POST http://127.0.0.1:8000/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"sample_id": "slow-sql-001"}'

# 可选：按任务选择 DeepSeek（服务端必须配置 DEEPSEEK_API_KEY）
curl -sS -X POST http://127.0.0.1:8000/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"sample_id": "slow-sql-001", "model_provider": "deepseek", "model_name": "deepseek-flash"}'

# 轮询任务、读事件、取消
curl -sS http://127.0.0.1:8000/api/tasks/<task_id>
curl -sS 'http://127.0.0.1:8000/api/tasks/<task_id>/events?after_seq=0&limit=200'
curl -sS -X POST http://127.0.0.1:8000/api/tasks/<task_id>/cancel
```

OpenAPI 文档：`http://127.0.0.1:8000/docs`。也可以 `uv run python -m app.api`
或 `uv run uvicorn app.api.main:app --port 8000` 启动。

CLI 常用参数（`run` 子命令）：

| 参数 | 默认值 | 作用 |
|---|---|---|
| `--ticket` | `fixtures/tickets/slow_sql_001.json` | 工单 JSON 路径 |
| `--skill` | 工单 `skill`，再退回 `slow-sql` | 加载哪个 Skill；场景工单自动用 `dba-triage` |
| `--dataset` | 工单 `dataset` 或默认慢 SQL 数据 | 手工覆盖 MCP 模拟数据 |
| `--max-turns` | `10` | 最大轮数，防止无限循环 |
| `--tool-timeout` | `5.0` | 单个工具的默认超时秒数 |
| `--run-root` | `var/runs` | 运行记录根目录 |
| `--model-delay` | `0.005` | 模拟模型每段文本的停顿秒数，设 0 最快 |
| `--max-inline-chars` | `4000` | 单条工具结果放进上下文的最大字符数，超过就保存为证据 |
| `--context-budget` | `12000` | 整份模型输入的字符预算（字符估算，不是 token） |
| `--context-messages` | `10` | 最多保留多少条最近消息 |
| `--model-provider` | `mock` | 模型来源：`mock` 或 `deepseek` |
| `--model` | provider 默认值 | 模型名，例如 `deepseek-flash` |
| `--model-base` | `https://api.deepseek.com` | OpenAI 兼容接口根地址 |
| `--model-timeout` | `120` | 模型读超时秒数 |

`serve` 子命令参数：`--host`（默认 `127.0.0.1`）、`--port`（默认 `8000`）、
`--reload`（开发时自动重载，默认关闭）。服务配置用 `SUPPORT_PLATFORM_*`
环境变量，完整列表见 `outputs/进度与联调.md`。

## 真实 MCP 接入（不是本地函数调用）

业务工具现在住在独立的 MCP Server 里。一次调查的真实链路是：

```text
AgentRunner
  -> MCP Client（JSON-RPC 2.0）
  -> stdio 子进程 / Streamable HTTP
  -> FastMCP Server
  -> app/mcp/dba_tools.py
  -> fixtures 模拟数据
```

工具结果仍然是 `fixtures/` 模拟数据，但 **MCP Server、客户端、协议消息、
传输和错误映射都是真的**。运行时默认使用官方 Python SDK
（`app/mcp/client.py`）；另外提供了一份按 Pi 思路手写的教学客户端
（`app/mcp/pi_style/`），用于对照 JSON-RPC 请求 id、pending Future、
stdio 管道和 SSE 帧解析。

```bash
# 默认 stdio：Agent 自动启动 MCP 子进程
uv run python -m app.cli run --ticket fixtures/tickets/slow_sql_001.json

# 手动启动 Streamable HTTP MCP Server（另开终端）
uv run python -m app.cli mcp-serve --transport streamable-http --port 8765

# 让 Agent 连接这个 HTTP Server
uv run python -m app.cli run \
  --ticket fixtures/tickets/slow_sql_001.json \
  --mcp-transport streamable-http \
  --mcp-url http://127.0.0.1:8765/mcp

# 并排对比官方 SDK 和 Pi 风格客户端
uv run python -m app.cli mcp-compare --transport stdio

# 看协议原文
cat var/runs/<run_id>/mcp_protocol.jsonl
```

协议、SSE 帧、三种传输的差异和调试方法见 `docs/mcp.md`。

## 已完成的能力

### 第 1 步：后端最小执行链

- 最小 Agent 循环：模型请求 → 助手回复 → 工具调用 → 工具结果 → 下一轮 → 停止。
- 消息、`tool_call_id`、工具结果三者按 id 关联。
- 每轮模型请求前的 `transform_context`、`convert_to_llm` 钩子位置。
- 工具执行前后的 `before_tool_call`、`after_tool_call` 钩子位置。
- 最大轮数、工具超时、协作式取消令牌。
- 4 个模拟只读工具：
  - `slow_log_query`：正常返回慢日志；
  - `execution_plan`：按 SQL 指纹返回执行计划；
  - `instance_metrics`：**故意超时失败**，用于验证失败不会被说成“没有异常”；
  - `lock_wait_query`：返回 `no_data`，用于验证“无数据不等于没有锁等待”。
- 模拟模型根据**实际工具结果**生成报告，不写死最终文案。

### 第 2 步：DBA 调查能力

- **慢 SQL Skill**：`skills/slow-sql/SKILL.md` 说明适用场景、要收集的证据、排查分支、
  结束条件和输出要求；`skill.json` 声明初始只开放 `slow_log_query`。
- **关键词工具搜索**：模型调用平台工具 `search_tools`，命中后把工具加入**后续轮次**。
  搜索不授予权限，未加载的工具即使被模型猜中名字也无法执行。
- **原始证据落盘**：工具结果超过 `max_inline_chars` 时保存为 `evidence/ev-xxxxxx.json`，
  模型只收到截断预览和证据编号；需要全文时用 `evidence_read(evidence_id, offset, limit)`
  分段读取，单次最多 8000 字符。
- **Topic**：把当前问题、范围、事实及证据引用、候选原因、未决事项、下一步、
  已尝试工具、证据读取和搜索记录持久化到 `topic.json`。事实只记录工具实际返回的数据，
  模型猜测单独放在候选原因里，并标注 `candidate` / `supported` / `unverified`。
- **有界上下文**：同时限制单条工具结果和整份模型输入。超预算时按“系统提示词 + 工单 +
  Topic + 最近消息”裁剪，并保证工具调用与结果成对保留；被裁掉的工具结果会用引用占位。
  预算单位是**字符估算**，不是精确 token 数。
- **运行记录**：新增 `topic.json`、`evidence/`，模型请求里记录每轮的上下文预算结果。
- 平台工具 `search_tools` 和 `evidence_read` 始终可见，不参与业务工具搜索。

一次完整的模拟调查（6 轮）：

```text
slow_log_query（大结果落盘 ev-000001）
  → evidence_read（分页读取被截断的证据）
  → search_tools（加载执行计划、实例监控、锁等待）
  → execution_plan（ev-000002）
  → instance_metrics（超时失败，ev-000003）
  → lock_wait_query（no_data，ev-000004）
  → 输出带证据引用、未验证项和限制说明的报告
```

### 第 3 步：HTTP API 与前端联调

- **任务接口**：创建（`202` 受理，不阻塞）、查询快照、协作式取消（运行中 `202`，
  已终态 `200` 幂等）。
- **过程接口**：事件分页（`after_seq` 游标）和 SSE（先补历史、保活、终态
  `stream_end` 关闭）；每轮真正发给模型的消息、工具和预算；证据列表与按字符分页读取；
  最终 Markdown 报告。
- **任务持久化**：SQLite `tasks` 表保存状态、轮次、证据数、能力快照；
  运行记录仍在 `var/runs/<task_id>/`，目录名直接复用任务 id。
- **重启恢复**：启动时把上个进程遗留的 `queued/running` 任务标成 `interrupted`，
  不会假装还在运行；关闭时先协作取消再硬取消。
- **配置**：运行目录、数据库、CORS、模型停顿、超时、预算都可用
  `SUPPORT_PLATFORM_*` 环境变量覆盖，默认零配置。
- **可选模型**：`GET /api/models` 返回 `mock` 和 `deepseek` 的可用性；
  `POST /api/tasks` 接受可选的 `model_provider` / `model_name`，
  没配 DeepSeek Key 时该选项显示不可用，显式选择会返回 `422`，不会悄悄退回模拟模型。
  任务快照会记录实际使用的 `model_provider` 和 `model_name`。
  已用 `deepseek-flash` 实机跑通一张 5 轮、4 条证据的完整任务。
  前端工作台的“模型来源 / 模型”下拉框已联调：页面分别创建过 `mock` 和
  `deepseek` 任务，终态快照都记录了实际模型。
- **测试**：65 个用例，覆盖完整流程、自由文本、取消幂等、SSE、错误码、重启恢复、
  模型目录/选择、SQLite 迁移、DeepSeek 适配器的 SSE/工具调用/错误翻译、
  官方 SDK 和 Pi 风格客户端的 MCP 协议/结果对比，以及十个 Mock 场景的
  数据契约、路径安全和端到端报告断言。

### 第 4 步：十场景多数据库 Mock

- **10 个场景**：1 个 MySQL 慢 SQL（保留旧六轮流程）+ 9 个新场景，覆盖
  MySQL、PostgreSQL、Redis、MongoDB、Elasticsearch、ClickHouse。
- **数据驱动剧本**：新增 `app/adapters/scenario_mock.py`，从
  `fixtures/mock_dba/*.json` 读取 `scenario.steps`、`scenario.diagnosis`
  和 `tool_results`，统一执行“搜索工具 → 调用工具 → 读 Topic → 生成报告”。
- **16 个场景工具**：`mysql_innodb_status`、`mysql_processlist`、`pg_activity`、
  `pg_table_stats`、`pg_locks`、`redis_info`、`redis_slowlog`、`redis_key_stats`、
  `mongo_profiler`、`mongo_explain`、`mongo_rs_status`、`mongo_server_status`、
  `es_cluster_health`、`es_shard_allocation`、`clickhouse_query_log`、
  `clickhouse_parts`；MCP Server 只注册当前数据集用到的工具。
- **通用 DBA Skill**：`skills/dba-triage` 初始业务工具为空，强制模型先
  `search_tools`，演示真实的“工具发现 → 按需加载 → 调用”流程。
- **按工单选 Skill/数据**：请求 `skill` > 工单 `skill` > 默认 `slow-sql`；
  场景数据集路径必须在 `fixtures/mock_dba/` 内且 `mock=true`，创建任务时即校验。
- **报告契约不变**：证据充分时只给一个主根因；多个修复候选按首选/备选/临时缓解分组；
  “已排除/待验证”折叠展示；所有关键事实带 `ev-xxxxxx` 证据编号。
- **前端**：样例下拉框显示数据库类型，预览显示数据库、场景标题和 Skill 标签；
  后端按工单自动选择 Skill，前端不需要猜。
- 场景设计、研究来源和验证命令见 `outputs/多场景Mock设计.md`。

接口字段、状态码约定和真实响应样例见 `outputs/进度与联调.md` 与
`outputs/api-samples/`。

## 运行记录

每次运行在 `var/runs/<run_id>/` 下生成（HTTP 任务的 `run_id` 就是 `task_id`）：

```text
run.json              运行状态、轮数、停止原因、Skill/工具能力快照、Topic
events.jsonl          运行事件，一行一个，带自增 seq
model_requests.jsonl  模型真正收到的消息、工具声明和上下文预算
model_responses.jsonl 模型流式返回的每一帧
messages.json         运行结束时的完整消息历史
report.md             最终 Markdown 报告
topic.json            最后一次持久化的 Topic
evidence/
  index.jsonl         证据索引：编号、工具、大小、是否截断、错误状态、预览
  ev-000001.json      工具原始结果（按当前任务隔离）
```

`var/` 已加入 `.gitignore`，运行记录不会提交到仓库。
HTTP 服务的任务元信息另存于 `var/tasks.db`（SQLite，WAL 模式），同样不提交。

## 项目结构

```text
app/
  runtime/
    contracts.py          消息、工具、事件、钩子的类型
    pi_reference/         从 Pi 选择性移植的循环 + 许可证说明
  harness/
    runner.py             串起 Skill、工具会话、证据、Topic、上下文和内核
    skills.py             Skill 加载与声明
    tool_search.py        关键词搜索、按需加载、平台工具
    evidence.py           原始结果落盘、索引、分页读取
    topic.py              Topic 的事实/候选/证据引用持久化
    context.py            系统提示词、工单消息、有界上下文
    tools.py              工具目录
  adapters/
    mock_model.py         按剧本执行的模拟模型（旧慢 SQL）
    scenario_mock.py      多数据库场景剧本引擎和统一报告生成器
    deepseek_model.py     DeepSeek OpenAI 兼容接口适配器（SSE -> 平台事件）
    models.py             模型工厂、provider 校验、默认模型名
    run_recorder.py       事件与运行结果落盘
  mcp/
    dba_tools.py          20 个只读 DBA 工具的业务实现（4 个旧工具 + 16 个场景工具）
    scenario_registry.py  按数据集动态注册场景工具
    server.py             真实 FastMCP Server（stdio / Streamable HTTP / SSE）
    client.py             官方 Python SDK 客户端（运行时默认）
    info.py               两条客户端路线共用的连接摘要
    pi_style/             Pi 风格手写客户端（stdio + Streamable HTTP，教学对照）
  api/
    main.py               FastAPI 路由、请求校验、异常→状态码、CORS、lifespan
    service.py            任务生命周期、后台执行、取消、事件/SSE、证据/报告读取
    store.py              SQLite tasks 表（任务元信息、重启恢复）
    settings.py           SUPPORT_PLATFORM_* 环境变量配置
    __main__.py           python -m app.api 启动服务
  cli.py                  命令行入口（run / list-* / mcp-serve / mcp-compare / serve）
skills/
  slow-sql/               慢 SQL 调查 Skill
  dba-triage/             通用 DBA 分诊 Skill（多数据库场景）
tests/
  test_agent_loop.py      循环、工具可见性、完整流程和记录测试
  test_harness.py         证据、Skill、搜索、Topic、上下文预算测试
  test_api.py             HTTP 任务、事件、SSE、取消、错误码、模型选择、十场景端到端
  test_scenarios.py       场景数据契约、路径安全和 mock 标记校验
  test_deepseek_model.py  DeepSeek SSE/工具调用/错误翻译（MockTransport，不联网）
  test_mcp.py             官方 SDK 客户端：stdio / Streamable HTTP / 旧版 SSE
  test_mcp_pi_style.py    Pi 风格手写客户端与官方客户端结果对比
  test_store.py           SQLite 任务表与老库迁移测试
fixtures/
  tickets/                10 张模拟工单
  mock_dba/               各场景的模拟工具结果（慢日志、锁、监控、复制状态等）
docs/
  任务书.md               原始任务书
  pi-mapping.md           Pi 源码到 Python 的映射和差异
  mcp.md                  MCP 协议、JSON-RPC、SSE、两条客户端路线和调试方法
outputs/
  进度与联调.md           开发进度、接口约定、前后端同步记录
  多场景Mock设计.md       10 个场景的设计、研究来源和验证方式
  api-samples/            第 3 步真实 HTTP 响应样例与抓取命令
```

## Pi 来源

- 源项目：https://github.com/earendil-works/pi
- 参考提交：`babe30a7e6c185346c85859da30b58183e5964f0`（2026-10-29）
- 许可证：MIT，Copyright (c) 2025 Mario Zechner
- 详见：`app/runtime/pi_reference/NOTICE.md`、`docs/pi-mapping.md`

本项目是 Python 重写，不是 Pi 官方 Python SDK；字段和事件做了简化，差异都记录在
`docs/pi-mapping.md`。

## 下一步

1. 任务列表/历史分页与筛选，方便回看已经运行过的任务。
2. 联调中发现字段不一致，先更新 `outputs/进度与联调.md` 和样例，再改代码。
3. 任务书之外的后续项：鉴权、真实只读数据源接入。
