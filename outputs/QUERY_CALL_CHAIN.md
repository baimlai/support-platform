# Query 函数调用链索引

由 scripts/check_call_chain.py 从 docs/call-chain.json 和 Python AST 生成。

先读 [完整调用链导读](ARCHITECTURE.md)，再看 [时序图](ARCHITECTURE_DIAGRAMS.md)。

## 函数、文件与职责

| 标识 | 函数（点击打开源文件） | 主要做什么 | 来源 |
|---|---|---|---|
| http | [create_task](/Users/baiml/PycharmProjects/support-platform/app/api/main.py:167) | HTTP 请求校验后交给任务服务 | 平台 |
| create | [TaskService.create_task](/Users/baiml/PycharmProjects/support-platform/app/api/service.py:204) | 校验模型/Skill/数据、入库、启动后台任务 | 平台 |
| ticket | [TaskService._build_ticket](/Users/baiml/PycharmProjects/support-platform/app/api/service.py:326) | 把用户 query 放进模拟工单范围 | 平台 |
| background | [TaskService._run_task](/Users/baiml/PycharmProjects/support-platform/app/api/service.py:373) | 组装 RunOptions、等待 Runner、更新任务终态 | 平台 |
| store | [TaskStore.create](/Users/baiml/PycharmProjects/support-platform/app/api/store.py:206) | 在 SQLite 保存任务元信息 | 平台 |
| runner | [AgentRunner.run](/Users/baiml/PycharmProjects/support-platform/app/harness/runner.py:174) | 创建模型、Skill、记录器并管理 MCP 连接生命周期 | 平台 |
| assemble | [AgentRunner._run_with_mcp](/Users/baiml/PycharmProjects/support-platform/app/harness/runner.py:237) | 装配工具会话、Topic、上下文、重试和 Agent | 平台 |
| emit | [AgentRunner._run_with_mcp.emit](/Users/baiml/PycharmProjects/support-platform/app/harness/runner.py:290) | 先落事件，再通知调用方，再更新 Topic | 平台 |
| skill | [load_skill](/Users/baiml/PycharmProjects/support-platform/app/harness/skills.py:124) | 读取调查方法和初始工具声明 | 平台 |
| model | [build_model](/Users/baiml/PycharmProjects/support-platform/app/adapters/models.py:136) | 选择 mock 或 DeepSeek 适配器 | 平台 |
| mcp_connect | [DbaMcpClient.__aenter__](/Users/baiml/PycharmProjects/support-platform/app/mcp/client.py:325) | 启动/连接 MCP、初始化并获取工具目录 | 平台适配 |
| mcp_specs | [DbaMcpClient.tool_specs](/Users/baiml/PycharmProjects/support-platform/app/mcp/client.py:451) | 把 MCP 声明和 handler 转成 ToolSpec | 平台适配 |
| mcp_handler | [DbaMcpClient._make_handler.handler](/Users/baiml/PycharmProjects/support-platform/app/mcp/client.py:497) | 闭包记住工具名，通过 MCP 执行 | 平台适配 |
| mcp_call | [DbaMcpClient.call_tool](/Users/baiml/PycharmProjects/support-platform/app/mcp/client.py:509) | SDK tools/call、错误和结果转换 | 平台适配 |
| server_tool | [create_server.slow_log_query](/Users/baiml/PycharmProjects/support-platform/app/mcp/server.py:201) | 服务端 MCP 方法，接收参数并调用业务函数 | 平台模拟 |
| business | [slow_log_query](/Users/baiml/PycharmProjects/support-platform/app/mcp/dba_tools.py:184) | 从 fixture 筛选实例与时间范围内的慢日志 | 平台模拟 |
| prompt | [Agent.prompt](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/agent.py:121) | 追加用户输入，启动一轮完整运行 | Pi 移植 |
| agent_run | [Agent._run](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/agent.py:155) | 并发保护、安装队列函数、调用底层 Loop | Pi 移植/适配 |
| receive | [Agent._receive](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/agent.py:133) | 更新流式状态并顺序通知订阅者 | Pi 移植 |
| steer | [Agent.steer](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/agent.py:104) | 运行中的补充/纠正先入队 | Pi 移植 |
| followup | [Agent.follow_up](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/agent.py:108) | 自然结束后的追问先入队 | Pi 移植 |
| continue | [Agent.continue_run](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/agent.py:129) | 不重复追加原 query，从当前上下文继续 | Pi 移植 |
| loop | [run_agent_loop](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:172) | 初始化运行状态、错误边界和最终结果 | Pi 移植/平台保护 |
| turns | [_run_turns](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:277) | 每轮准备、调用模型、执行工具、继续/停止 | Pi 移植 |
| stream | [_stream_assistant_response](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:444) | 整理请求、消费模型流、生成助手消息 | Pi 移植 |
| make_transform | [make_transform_context](/Users/baiml/PycharmProjects/support-platform/app/harness/context.py:190) | 创建并返回记住工单/Topic/预算的闭包 | 平台 |
| transform | [make_transform_context.transform](/Users/baiml/PycharmProjects/support-platform/app/harness/context.py:209) | 先摘要视图，再拼 Topic 和有界上下文 | 平台 |
| context | [build_model_context](/Users/baiml/PycharmProjects/support-platform/app/harness/context.py:237) | 固定头和近期完整工具块的预算裁剪 | 平台 |
| compact | [ContextCompactor.transform](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/compaction.py:154) | 选安全前缀、摘要校验、原子提交检查点 | Pi 参考/适配 |
| summary | [make_model_summarizer.summarize](/Users/baiml/PycharmProjects/support-platform/app/adapters/context_summary.py:54) | 用无工具权限的独立模型请求生成摘要 | 平台适配 |
| mock_summary | [extractive_mock_summary](/Users/baiml/PycharmProjects/support-platform/app/adapters/context_summary.py:26) | 明确标注的模拟摘录，不冒充语义摘要 | 平台模拟 |
| retry | [with_model_retry.stream](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/retry.py:102) | 分类临时错误、可取消退避、逐次尝试 | Pi 参考/流适配 |
| record_stream | [RunRecorder.wrap_stream_fn.wrapped](/Users/baiml/PycharmProjects/support-platform/app/adapters/run_recorder.py:173) | 每次尝试保存真实请求与模型流事件 | 平台 |
| mock_stream | [MockDbaModel._stream](/Users/baiml/PycharmProjects/support-platform/app/adapters/mock_model.py:136) | 从 Topic 和实际结果确定下一步模拟动作 | 平台模拟 |
| mock_action | [MockDbaModel._next_action](/Users/baiml/PycharmProjects/support-platform/app/adapters/mock_model.py:163) | 慢 SQL 六轮剧本或多场景引擎 | 平台模拟 |
| real_stream | [DeepSeekModel._stream](/Users/baiml/PycharmProjects/support-platform/app/adapters/deepseek_model.py:191) | 构造 HTTP 请求、读取 SSE、合并工具参数 | 平台适配 |
| execute | [_execute_tool_calls](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:643) | 顺序执行本轮工具、后处理、发送结果事件 | Pi 移植 |
| prepare_tool | [_prepare_and_execute_tool](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:758) | 查找可见工具、校验、前置 Hook、超时执行 | Pi 移植/超时扩展 |
| validate | [_validate_tool_arguments](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:871) | 用 JSON Schema 校验参数并深复制 | Python 适配 |
| search_tool | [build_platform_tools.search_tools](/Users/baiml/PycharmProjects/support-platform/app/harness/tool_search.py:345) | 平台工具入口，搜索并加载后续能力 | 平台 |
| search_session | [ToolSession.search](/Users/baiml/PycharmProjects/support-platform/app/harness/tool_search.py:276) | 维护可见工具集合和搜索记录 | 平台 |
| search_specs | [search_tool_specs](/Users/baiml/PycharmProjects/support-platform/app/harness/tool_search.py:182) | 将 DBA 别名和工具 Schema 送入排名器 | 平台适配 |
| rank | [rank_documents](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/search.py:73) | 精确名称、覆盖数、分字段 BM25 排名 | Pi 移植/中文适配 |
| read_tool | [build_platform_tools.evidence_read](/Users/baiml/PycharmProjects/support-platform/app/harness/tool_search.py:369) | 按引用读取当前任务的证据分页 | 平台 |
| process | [EvidenceProcessor.after_tool_call](/Users/baiml/PycharmProjects/support-platform/app/harness/evidence.py:327) | 完整结果保存后，再生成有界模型内容 | 平台 |
| save_evidence | [EvidenceStore.save](/Users/baiml/PycharmProjects/support-platform/app/harness/evidence.py:144) | 保存原始证据和索引 | 平台 |
| bound | [_bound_content](/Users/baiml/PycharmProjects/support-platform/app/harness/evidence.py:401) | 生成引用、摘要和截断预览的 JSON 信封 | 平台 |
| truncate | [truncate_text](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/truncate.py:29) | 按完整行与 UTF-8 字节限制预览 | Pi 移植 |
| read_evidence | [EvidenceStore.read](/Users/baiml/PycharmProjects/support-platform/app/harness/evidence.py:238) | 校验引用并按字符读取证据片段 | 平台 |
| topic | [TopicManager.update_from_event](/Users/baiml/PycharmProjects/support-platform/app/harness/topic.py:198) | 根据工具结果更新事实/候选/未验证项 | 平台 |
| persist_topic | [TopicManager._persist](/Users/baiml/PycharmProjects/support-platform/app/harness/topic.py:638) | 持久化当前 Topic | 平台 |
| finish | [RunRecorder.finish](/Users/baiml/PycharmProjects/support-platform/app/adapters/run_recorder.py:223) | 保存完整历史、报告和能力快照 | 平台 |
| turn_context | [TaskService.read_turn_context](/Users/baiml/PycharmProjects/support-platform/app/api/service.py:710) | 按真实轮次读最终模型尝试，跳过摘要请求 | 平台 |
| report | [TaskService.get_report](/Users/baiml/PycharmProjects/support-platform/app/api/service.py:797) | 任务终态后读取最终报告 | 平台 |
| walk | [run_walkthrough](/Users/baiml/PycharmProjects/support-platform/app/walkthrough.py:29) | 执行含补充和追问的可重现样例 | 教学入口 |
| continue_loop | [run_agent_loop_continue](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:258) | 校验已有历史后从不追加新prompt的入口继续 | Pi 移植 |
| resolve_tools | [_resolve_tools](/Users/baiml/PycharmProjects/support-platform/app/runtime/pi_reference/loop.py:855) | 每次请求/执行前获取当前可见工具 | 平台扩展 |
| visible_tools | [ToolSession.visible_specs](/Users/baiml/PycharmProjects/support-platform/app/harness/tool_search.py:320) | 返回本次会话当前已加载工具的新列表 | 平台 |

## 调用关系

Hook/回调边是动态绑定，不是文件间的直接 import。

- http → create：直接调用，调用点 `create_task(...)`。
- create → ticket：直接调用，调用点 `_build_ticket(...)`。
- create → store：直接调用，调用点 `create(...)`。
- create → background：create_task 启动后台协程，调用点 `_run_task(...)`。
- background → runner：直接调用，调用点 `run(...)`。
- runner → model：直接调用，调用点 `build_model(...)`。
- runner → skill：直接调用，调用点 `load_skill(...)`。
- runner → mcp_connect：async with 初始化，调用点 `DbaMcpClient(...)`。
- runner → assemble：直接调用，调用点 `_run_with_mcp(...)`。
- assemble → mcp_specs：直接调用，调用点 `tool_specs(...)`。
- assemble → make_transform：创建闭包，调用点 `make_transform_context(...)`。
- assemble → prompt：直接调用，调用点 `prompt(...)`。
- assemble → finish：直接调用，调用点 `finish(...)`。
- prompt → agent_run：直接调用，调用点 `_run(...)`。
- continue → agent_run：直接调用，调用点 `_run(...)`。
- agent_run → loop：直接调用，调用点 `run_agent_loop(...)`。
- loop → turns：直接调用，调用点 `_run_turns(...)`。
- turns → stream：逐轮调用，调用点 `_stream_assistant_response(...)`。
- stream → transform：config 注入的 Hook，调用点 `transform_context(...)`。
- transform → compact：可选摘要策略，调用点 `transform(...)`。
- transform → context：直接调用，调用点 `build_model_context(...)`。
- compact → summary：注入真实模型摘要器（deepseek），调用点 `summarize(...)`。
- compact → mock_summary：注入模拟摘录器（mock），调用点 `summarize(...)`。
- stream → retry：注入模型包装函数，调用点 `stream_fn(...)`。
- retry → record_stream：注入记录包装函数，调用点 `stream_fn(...)`。
- record_stream → mock_stream：mock 分支，调用点 `stream_fn(...)`。
- record_stream → real_stream：deepseek 分支，调用点 `stream_fn(...)`。
- mock_stream → mock_action：直接调用，调用点 `_next_action(...)`。
- turns → execute：本轮含工具调用时，调用点 `_execute_tool_calls(...)`。
- execute → prepare_tool：逐个工具调用，调用点 `_prepare_and_execute_tool(...)`。
- prepare_tool → validate：执行前校验，调用点 `_validate_tool_arguments(...)`。
- prepare_tool → mcp_handler：ToolSpec.handler 动态绑定（业务工具），调用点 `handler(...)`。
- mcp_handler → mcp_call：直接调用，调用点 `call_tool(...)`。
- server_tool → business：MCP 服务端本地调用，调用点 `slow_log_query(...)`。
- prepare_tool → search_tool：ToolSpec.handler 动态绑定（搜索工具），调用点 `handler(...)`。
- search_tool → search_session：直接调用，调用点 `search(...)`。
- search_session → search_specs：直接调用，调用点 `search_tool_specs(...)`。
- search_specs → rank：直接调用，调用点 `rank_documents(...)`。
- prepare_tool → read_tool：ToolSpec.handler 动态绑定（证据工具），调用点 `handler(...)`。
- read_tool → read_evidence：直接调用，调用点 `read(...)`。
- execute → process：config 注入的后置 Hook，调用点 `after_tool_call(...)`。
- process → save_evidence：直接调用，调用点 `save(...)`。
- process → bound：直接调用，调用点 `_bound_content(...)`。
- bound → truncate：直接调用，调用点 `truncate_text(...)`。
- execute → receive：Agent._receive 作为事件出口，调用点 `emit(...)`。
- receive → emit：Runner 用 subscribe 绑定 emit，调用点 `listener(...)`。
- emit → topic：工具结束事件触发，调用点 `update_from_event(...)`。
- topic → persist_topic：状态有变化时写盘，调用点 `_persist(...)`。
- walk → steer：模拟运行中用户补充，调用点 `steer(...)`。
- walk → followup：模拟用户追问，调用点 `follow_up(...)`。
- mcp_call → server_tool：JSON-RPC tools/call 跨进程路由，非本地直接跳转，调用点 `call_tool(...)`。
- agent_run → continue_loop：continue_run 无新消息分支，调用点 `run_agent_loop_continue(...)`。
- continue_loop → loop：共享循环入口，调用点 `run_agent_loop(...)`。
- stream → resolve_tools：每轮请求前刷新工具声明，调用点 `_resolve_tools(...)`。
- resolve_tools → visible_tools：config 注入的当前工具提供器，调用点 `tools_provider(...)`。
- stream → receive：消息流事件出口，调用点 `emit(...)`。
