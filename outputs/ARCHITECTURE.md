# 一条用户 query 的完整调用链与代码导读

## 1. Executive Summary

本项目把 DBA 工单交给单个 Agent，经过多轮模型请求和只读工具取证，输出带证据引用的报告。
Python 后端使用 FastAPI，工具通过真实 MCP 协议连接，当前业务数据均为 fixture 模拟数据。

**按你的阅读目标，先读第 7 节，再打开 [多轮时序图](ARCHITECTURE_DIAGRAMS.md)，
最后沿 [函数索引](QUERY_CALL_CHAIN.md) 的源码链接逐段查看。**索引含函数所在文件、最新行号、职责与来源。

本次已把 Agent 包装层、输入队列、准备 Hook、搜索排名、重试、预览截断与可选摘要接入共用执行链。
完整回归 87 个测试通过。真实模型的语义能力和新摘要策略的实际效果，本次未使用付费接口验证。

## 2. Repository Purpose

用户 query：“订单查询从 10 点开始变慢，请调查原因，说明证据和验证步骤。”
系统确认范围，查询慢日志，找 SQL 指纹，查询执行计划，检查监控和锁等待，再给出结论。
实例和时间范围当前沿用模拟样例；不会根据自然语言直接连接客户数据库。

这是一个后端服务加独立 MCP 工具进程。api、harness、runtime 是模块划分，
不是三个独立部署的微服务；不应为了学习微服务就把每个 Python 类拆成网络服务。

## 3. Audience Guide

- 学 Python：重点看函数作为参数、闭包、dataclass、async/await、异步生成器、try/finally。
- 学业务：重点看 Skill、工单范围、工具结果、证据、Topic 的事实与候选、最终报告。
- 学服务开发：重点看薄路由、后台任务生命周期、配置注入、适配器边界、取消与资源清理。
- 维护代码：先读 AGENTS.md、任务书、Pi 映射，再更新调用链清单和相关行为测试。

## 4. System Context

独立前端通过 HTTP 提交和查询任务；FastAPI 在本进程用 asyncio 后台任务运行调查。
MCP 默认采用 stdio 子进程，连接与回收由异步上下文管理器负责。
模型可选择本地 mock 或 DeepSeek；Key 留在后端环境变量中。
SQLite 存任务状态，运行目录存事件、模型输入输出、证据、Topic、报告及可选摘要。

## 5. Main Entry Points

| 入口 | 做什么 | 直接证据 | 置信度 |
|---|---|---|---|
| POST /api/tasks | 接收 sample_id 或 ticket_text | app/api/main.py::create_task | 高 |
| python -m app.cli run | 从 JSON 工单运行共用 Runner | app/cli.py::_run_ticket | 高 |
| python -m app.walkthrough | query、运行中补充、结束后追问的完整演示 | app/walkthrough.py::run_walkthrough | 高 |
| python -m app.api / cli serve | 启动 FastAPI 服务 | app/api/__main__.py、app/cli.py | 高 |
| python -m app.mcp.server | 独立运行工具服务 | app/mcp/server.py::main | 高 |

## 6. Major Components

| 文件/模块 | 主要职责 | 为什么放在这一层 |
|---|---|---|
| app/api/main.py | HTTP 请求/响应、依赖注入、错误码 | 不把调查逻辑写进路由 |
| app/api/service.py | 创建后台任务、取消、状态和材料查询 | asyncio.Task 与数据库状态在这里协调 |
| app/api/store.py | SQLite 任务记录 | 数据库操作集中，避免散落 SQL |
| app/harness/runner.py | 装配模型、Skill、工具会话、证据、Topic、策略和 Agent | CLI/API 共用；业务装配与循环执行分开 |
| app/runtime/contracts.py | 消息、工具、Hook、事件的类型 | 各层依赖同一个接口契约 |
| app/runtime/pi_reference/agent.py | 状态、订阅、取消、输入队列、prompt/continue | 包装底层 Loop，管理一次活动运行 |
| app/runtime/pi_reference/loop.py | 多轮模型/工具循环和执行控制 | 不理解慢 SQL 诊断规则 |
| app/runtime/pi_reference/retry.py | 模型临时错误分类与退避 | 重试请求，不重新执行整张工单 |
| app/runtime/pi_reference/compaction.py | 安全切分、摘要验证、检查点提交 | 完整历史与模型视图分开 |
| app/runtime/pi_reference/search.py | 分字段 BM25 排名 | 纯函数，不碰权限或任务状态 |
| app/runtime/pi_reference/truncate.py | 行数/UTF-8 字节截断 | 纯函数，不删除原始证据 |
| app/harness/context.py | 系统规则、工单、Topic、摘要视图与预算 | DBA 调查上下文策略 |
| app/harness/tool_search.py | 工具发现、DBA 别名、按需加载、证据读取工具 | 平台能力目录和本次可见集合 |
| app/harness/evidence.py | 保存原始结果，提供有界预览和分页 | 模型不需要吞下全部大日志 |
| app/harness/topic.py | 事实/候选/失败/待验证/下一步 | 业务调查状态，避免压缩后失忆 |
| app/adapters/context_summary.py | mock 摘录或真实模型摘要 | 内核不绑定某个供应商或模拟剧本 |
| app/adapters/run_recorder.py | 请求、响应、事件、报告落盘 | 可追踪每次尝试和实际模型输入 |
| app/mcp/client.py、server.py、dba_tools.py | 协议连接、服务端路由、模拟业务函数 | 跨进程调用和业务查询分开 |

## 7. Data and Control Flow

### 7.1 query 如何进入后台调查

```text
用户输入 query
  main.py::TaskCreateRequest 校验请求字段
  main.py::create_task
    service.py::TaskService.create_task
      _build_ticket：把 ticket_text 写入工单描述，保留模拟实例/时间范围
      _resolve_skill / _resolve_provider / _resolve_model_name：确定能力和模型
      store.py::TaskStore.create：先写 queued 任务
      asyncio.create_task(_run_task(...))：登记后台协程，HTTP 返回 202

事件循环稍后执行后台协程：
  TaskService._run_task
    任务改为 running，构造 RunOptions
    AgentRunner.run
      build_model / load_skill / RunRecorder
      async with DbaMcpClient：连接、initialize、tools/list
      AgentRunner._run_with_mcp
        ToolRegistry → EvidenceStore → TopicManager → ToolSession
        创建 emit、模型记录/重试包装、摘要器、上下文闭包
        Agent(context, config, stream_fn).subscribe(emit)
        await Agent.prompt([build_user_message(ticket)])
```

这里不是“HTTP 路由一路同步调用到最终报告”。HTTP 提交与后台执行在 create_task 处分开，
页面通过后续查询读进度；刷新页面不会取消后台协程。

### 7.2 每一轮如何请求模型

```text
Agent.prompt → Agent._run → run_agent_loop → _run_turns 的 while
  有上一轮且确定继续：prepare_next_turn(last_turn)   [可选]
  安全点追加 steering / 已到时机的 follow-up
  prepare_request(RequestContext)                   [可选，首轮也执行]
  _stream_assistant_response
    config.transform_context(messages, token)
      context.py::make_transform_context 返回的 transform 闭包
        ContextCompactor.transform                  [开启摘要才会生成新摘要]
          检查视图字符量、工具配对和安全切分点
          summarize(...)：mock 摘录 / 独立真实模型摘要
          校验结束状态、长度、证据引用；先写盘再提交检查点
        TopicManager.topic_dict()：取此刻的调查状态
        build_model_context：固定头与近期完整工具块的预算裁剪
    config.convert_to_llm(messages)
    _resolve_tools → ToolSession.visible_specs：取最新已加载工具
    ModelRequest(messages, tools, model, cancellation, metadata)
    stream_fn(request)
      retry.py::with_model_retry 返回的 stream
        RunRecorder.wrap_stream_fn 返回的 wrapped
          MockDbaModel.stream / DeepSeekModel.stream
    async for：start / delta / tool_call / done / error
    把 partial 替换为最终 AssistantMessage，并返回上层
```

Hook 不会自动触发。Runner 创建函数 → 存进 config → Loop 显式调用字段里的函数。
prepare_request 和 prepare_next_turn 目前是可供业务注入的能力；普通 Runner 默认不安装它们，
对应分支跳过。实际的上下文处理通过 transform_context 安装，测试另行验证准备 Hook 的顺序。

Retry 在 Recorder 外面：每次尝试都经过 Recorder，原始失败尝试不会消失。
重试时重置当前助手预览，终态才写入那一条助手消息，不重复执行已完成工具。
额度、认证失败和上下文过长不按临时错误盲重试；当前未实现溢出后自动压缩并重新发请求。

### 7.3 模型提出工具调用后走哪里

```text
_run_turns
  从 AssistantMessage.content 提取 ToolCall
  _execute_tool_calls
    ToolExecutionStart → emit
    _prepare_and_execute_tool
      _resolve_tools：执行阶段也只能找到已加载工具
      _validate_tool_arguments：JSON Schema 校验、深复制
      before_tool_call                            [可选]
      asyncio.wait_for(tool.handler(...), timeout)

      业务工具 handler：
        DbaMcpClient._make_handler 返回的 handler 闭包
        DbaMcpClient.call_tool
        MCP SDK ClientSession.call_tool
        ===== JSON-RPC tools/call，经 stdio 或 HTTP 跨进程 =====
        FastMCP 路由 → server.py::create_server.slow_log_query
        dba_tools.py::slow_log_query → fixtures 模拟结果

      平台工具 handler：
        search_tools → ToolSession.search → search_tool_specs → rank_documents
        evidence_read → EvidenceStore.read
        平台工具直接在本进程运行，不经过 MCP

    after_tool_call → EvidenceProcessor.after_tool_call
      EvidenceStore.save：先完整保存结果
      _bound_details / _bound_content → truncate_text：生成有界模型内容
    ToolExecutionEnd → Agent._receive → Runner.emit
      RunRecorder.on_event：先保存事件
      调用方回调：API 更新轮次，或示例加入用户输入
      TopicManager.update_from_event → _persist → topic_updated
    _create_tool_result_message：同一个 tool_call_id 写回结果
  current.messages.extend(本轮工具结果)
  finish_turn → TurnEnd → 判断继续或结束
```

工具超时也会变成错误结果；锁等待 no_data 是“没有取到数据”，不能直接当成没有锁异常。
after_tool_call 仅对真正执行过的工具触发，找不到工具或参数非法的调用不会伪造业务证据。

### 7.4 实际跑出的七轮：工具多轮与用户多轮同时存在

执行命令：

```bash
uv run python -m app.walkthrough --output outputs/query-walkthrough
```

| 轮次 | 输入/模型动作 | 工具和状态 | 下一轮为什么发生 |
|---|---|---|---|
| 1 | 用户原 query，模型先查慢日志 | slow_log_query，大结果保存 ev-000001 | 已有工具结果；示例也把补充和追问入队 |
| 2 | 先追加 steering：“工具失败和无数据要单独说明，保留证据编号” | evidence_read 读取慢日志分页 | 模型继续取证 |
| 3 | 根据已有材料搜索后续能力 | search_tools，加载执行计划/监控/锁工具 | 工具集合变化，下轮声明新工具 |
| 4 | 查询执行计划 | execution_plan，ev-000002 | 继续验证其他分支 |
| 5 | 验证监控和锁情况，一轮两个工具 | instance_metrics 失败 ev-000003；lock_wait_query 无数据 ev-000004 | 模型收到失败/无数据结果 |
| 6 | 原调查输出报告，无工具调用 | 报告文本 | 此时才取 follow-up，开启下一轮 |
| 7 | 用户追问：“请再给我一份验证步骤” | 模拟模型再次输出报告文本 | 无工具、无排队输入，结束 |

第 7 轮证明追问进入了真实上下文；mock 仍按剧本回答，不证明它能理解任意自然语言要求。
真实模型才会根据补充消息重新选择工具/组织回答。

本次记录位于 [七轮执行记录](query-walkthrough/20261103-163007-TCK-20261029-001-a761/walkthrough.json)。
同目录的 model_requests.jsonl 可看每轮实际消息，messages.json 可看未裁剪的完整历史，
topic.json 可看调查状态，compaction.json 可看最后摘要边界，evidence 可看原始工具结果。
模型输入前的摘要实际在第 3、4、5、6、7 轮触发。

**HTTP 层目前仍没有追加消息路由。**上述用户多轮通过内核 Agent.steer/follow_up 演示，
并在 Runner 的 MCP 生命周期内消费。Runner 返回后连接已关闭，不应再拿它的工具 handler 续跑。
单独使用 Agent 时，可以在资源仍可用的前提下多次 prompt；continue_run 不重复添加原 query，
从现有用户/工具结果继续，或消费排队的新输入。它们都不等于进程重启自动恢复。

### 7.5 最后怎样回到用户

```text
_run_turns 返回 reason / error / turns
  run_agent_loop → AgentEnd → LoopResult
  Agent._run：清理 running/streaming/pending 状态，归还尚未生效的队列消息
  RunRecorder.finish：messages.json、report.md、run.json
  AgentRunner.run 退出 async with：关闭 MCP 会话/回收子进程
  TaskService._run_task：更新 SQLite 终态并清理内存后台任务

页面查询：
  GET task → TaskService.get_task
  GET events 或 SSE → read_events / stream_events
  GET turn context → read_turn_context（按 metadata.turn，跳过 purpose=compaction）
  GET evidence → EvidenceStore.read（有界分页）
  GET report → TaskService.get_report
```

## 8. Runtime, Configuration, and Deployment

```bash
uv sync --python 3.12
uv run python -m app.cli serve --host 127.0.0.1 --port 8000
uv run python -m app.cli run --compact --compact-trigger 4000 --compact-keep 2
uv run python -m app.walkthrough --output outputs/query-walkthrough
```

默认模型 mock，无密钥也能运行。DeepSeek Key 从后端环境变量读取，不放文档或日志。
摘要默认关闭，API 环境变量 SUPPORT_PLATFORM_COMPACTION_ENABLED=1 可开启。
临时模型错误默认最多重试 2 次；SUPPORT_PLATFORM_MODEL_RETRIES=0 可关闭。
摘要采用字符估算，不是精确 token 计数；固定系统规则、工单、Topic 和最新块可能超预算，
记录中的 over_budget 会如实标记。

当前没有为本次功能创建容器集群或多服务部署；MCP stdio 子进程已能展示协议边界与资源管理。

## 9. Testing and Verification

```bash
uv run python -m unittest discover -s tests -q
uv run ruff check .
uv run python scripts/check_call_chain.py
```

本次完整回归 87 个测试通过，含真实本地 MCP HTTP/SSE；沙箱需允许绑定本地端口。
扩展测试集中在 tests/test_runtime_extensions.py：Hook 顺序、队列/继续/取消、摘要边界和失败、
重试不重复工具、逐次记录与生成器清理、中文检索和字节截断、轮次查询和七轮端到端。
现有十场景、API、模型适配器、存储和 MCP 测试继续覆盖旧行为。

维护 docs/call-chain.json 后运行 scripts/check_call_chain.py：检查函数/调用点仍存在，
刷新函数索引行号。静态检查无法证明动态绑定对象完全正确，因此还要跑行为测试。

## 10. How to Navigate the Codebase

1. 本文第 7 节和时序图：先定位自己正在看哪一段。
2. app/harness/runner.py：看所有部件在哪里装配，尤其是函数对象怎么传入 config。
3. app/runtime/pi_reference/agent.py、loop.py：看运行控制和多轮推进。
4. app/runtime/contracts.py：遇到看不懂的参数/返回值时回查类型。
5. app/harness/context.py、tool_search.py、evidence.py、topic.py：看 DBA 调查如何保留进度。
6. app/mcp/client.py、server.py、dba_tools.py：看跨进程边界和工具业务实现。
7. tests/test_runtime_extensions.py 与运行目录：用真实输入/输出反查分支。

## 11. Safe Change Guide for Humans and AI Agents

先读 AGENTS.md 和 docs/任务书.md；个人扩展文件存在时按约定读取。
新增 Pi 模块注明固定来源、MIT 许可、行为差异和教学注释，不声称官方 Python SDK。
app/runtime 不导入 DBA Topic 或数据库表；业务装配在 Harness，网络/模型协议在适配器。
新字段/事件同步更新 contracts、Pi 映射和联调说明；不要把工具调用与结果拆开。
outputs 下的上游源码是参考下载，不是我们维护的业务实现；运行日志也不是源代码。
常见改动先跑相关测试，再跑完整回归、ruff 和调用链检查。

## 12. Assumptions and Items Needing Human Validation

| 项目 | 当前证据和边界 | 置信度 |
|---|---|---|
| 新模块运行链路 | 本地单元/API/MCP 测试和七轮实际记录 | 高 |
| 真实摘要语义质量 | 本次未请求付费模型；只验证协议、失败保护和注入方式 | 尚待真实评估 |
| 中文工具搜索 | 用片段和 DBA 别名适配，非 Intl.Segmenter 一比一实现 | 高，召回质量仍依赖目录 |
| 多轮 HTTP 对话 | 未新增追加消息接口；只有内核队列和演示 API | 高 |
| 进程重启 | 未完任务标 interrupted；不自动恢复 | 高 |

### Validation notes

已修正“请求流水号等于调查轮次”的旧假设：摘要和重试会额外占号。
图中 MCP 是跨进程协议边界，未画成本地函数直接跳转。
prepare Hook 是可选注入，普通 Runner 未配置时不执行；摘要只有开启并满足预算条件才执行。
业务数据仍是模拟；图示和摘要不能用来宣称真实数据库诊断已经验证。

## 13. Suggested Architecture Improvements

| Category | Recommendation | Evidence | Senior developer rationale | Manager rationale | Effort | Risk | Priority |
|---|---|---|---|---|---|---|---|
| Documentation/context | 改函数时同步调用链清单并重新生成索引 | docs/call-chain.json、检查脚本 | 避免重命名后导读断链 | 降低上手成本 | S | Low | P1 |
| Verification/guardrail | 后续接真实模型时评估摘要是否保留关键判断 | 当前只验证引用和协议 | 引用存在不等于语义正确 | 避免错误结论进入业务 | M | Medium | P1 |
| Contract/schema | 有前端续聊需求后，再设计任务会话和追加输入接口 | 当前仅 Agent API | 明确连接、任务、对话三个生命周期 | 控制扩展范围 | M | Medium | P2 |
| Documentation/context | 若加入多个 MCP Server，补跨服务路由的关系证据 | 当前单 Server 与测试 | 静态索引无法证明远端动态路由 | 便于故障归属 | S | Low | P2 |

## 14. Glossary

| 名词 | 意思 |
|---|---|
| query | 用户提出的调查问题 |
| turn | 一次模型回复加它提出的工具执行；可含多个工具 |
| request_seq | 所有模型请求的流水号；重试与摘要也占号 |
| steering | 安全点介入的补充或纠正 |
| follow-up | 原调查自然停下后处理的追问 |
| Hook | 由外部注入、在明确位置显式调用的函数 |
| Topic | 调查范围、事实、候选、未决事项与下一步 |
| evidence_id | 原始工具结果引用，例如 ev-000001 |
| checkpoint | 已验证的摘要和原历史保留边界 |

## 15. Appendix: Evidence Map

| 结论 | 证据 | 置信度 |
|---|---|---|
| HTTP 立刻受理、调查后台运行 | `app/api/service.py` 的 create_task/_run_task；`tests/test_api.py` | 高 |
| Hook 是函数对象传递 | `app/harness/runner.py` 的 config 装配；`app/runtime/contracts.py`；准备 Hook 顺序测试 | 高 |
| 运行中补充与结束后追问时机不同 | `app/runtime/pi_reference/agent.py`、`app/runtime/pi_reference/loop.py`；队列时序测试 | 高 |
| 重试不重复工具且完整记录尝试 | `app/runtime/pi_reference/retry.py`、`app/adapters/run_recorder.py`；RetryTests | 高 |
| 摘要不覆盖完整历史、失败不提交 | `app/runtime/pi_reference/compaction.py`、CompactionTests、运行检查点 | 高 |
| 真实跨进程工具调用 | `app/mcp/client.py`、`app/mcp/server.py`；`tests/test_mcp.py` | 高 |
| 七轮与四条证据 | 本文链接的 walkthrough.json 及同目录运行记录 | 高 |

文档结构校验使用 repo-architecture-walkthrough 技能附带 verifier，并针对 outputs 目录指定文档路径；
源码关系另由项目内调用链脚本校验。图中的动态调用语义以测试和实际运行记录为依据。
