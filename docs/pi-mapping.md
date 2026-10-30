# Pi 内核移植映射

本文件记录：哪些代码参考了 Pi、对应到本项目的哪个文件、哪些行为改了、哪些暂未移植。
目的不是追求 API 完全一致，而是让“为什么这样写”可追溯、可审查。

## 1. 参考版本

- 源项目：https://github.com/earendil-works/pi
- 参考提交：`babe30a7e6c185346c85859da30b58183e5964f0`（2026-10-29）
- 许可证：MIT，Copyright (c) 2025 Mario Zechner
- 许可证与出处：`app/runtime/pi_reference/NOTICE.md`

阅读过的源码：

| Pi 文件 | 关注内容 |
|---|---|
| `packages/agent/src/agent-loop.ts` | `agentLoop`、`runAgentLoop`、`runLoop`、流式响应、工具执行、事件发出顺序 |
| `packages/agent/src/types.ts` | 消息、工具、钩子、循环配置、运行事件 |
| `packages/agent/src/agent.ts` | Agent 包装类、状态、取消、订阅、steering/follow-up 队列 |
| `packages/ai/src/types.ts` | 系统/用户/助手/工具结果消息、工具声明、模型流式事件 |
| `packages/ai/src/utils/transcript.ts` | 上下文规范化、工具声明转换、工具状态变化 |
| `packages/ai/src/utils/event-stream.ts` | 事件流封装 |
| `packages/ai/src/utils/validation.ts` | 工具参数校验 |

## 2. 模块映射

| Pi（TypeScript） | 本项目（Python） | 说明 |
|---|---|---|
| `packages/agent/src/agent-loop.ts` `agentLoop` / `runAgentLoop` | `app/runtime/pi_reference/loop.py` `run_agent_loop` | 一次运行的入口 |
| `runLoop` | `loop.py` `_run_turns` | 多轮“模型 → 工具 → 模型”循环 |
| `streamAssistantResponse` | `loop.py` `_stream_assistant_response` | 调模型并把流式事件转成运行事件 |
| `executeToolCalls` | `loop.py` `_execute_tool_calls` | 顺序执行一批工具调用 |
| `prepareToolCall` | `loop.py` `_prepare_and_execute_tool` | 查找、校验、前置钩子、执行、超时 |
| `finalizeExecutedToolCall` | `_execute_tool_calls` 内的 after 钩子分支 | 只在工具真的执行后调用 |
| `validateToolArguments` | `loop.py` `_validate_tool_arguments` | 用 JSON Schema 校验，不抛给调用方 |
| `packages/agent/src/types.ts` 消息/工具/事件类型 | `app/runtime/contracts.py` | 简化后的 Python dataclass |
| `Agent` 包装类（状态、abort、订阅、队列） | 未移植；由 `run_agent_loop` 的 `emit`、`CancellationToken`、记录器替代 | 本轮单 Agent、本地运行 |
| `EventStream` | 未移植；用异步生成器 + `EventSink` 替代 | 事件顺序保持一致 |
| `transcript.ts` 的上下文规范化 | `app/harness/context.py` `build_initial_context` | 系统提示词和工具由 Harness 装配 |
| `validation.ts` 的参数校验 | `_validate_tool_arguments` | 只用 `jsonschema`，不做 TypeBox 式类型转换 |
| 循环取出工具声明的位置 | `loop.py` `_resolve_tools` + `AgentLoopConfig.tools_provider` | 每轮重新取“当前可见工具”，支持第 2 步的按需加载 |
| 工具结果写回 transcript | `ToolResultMessage.meta` + `EvidenceProcessor` | 原始结果落盘，模型只收到有界内容和证据引用 |

## 3. 事件映射

| Pi 事件 | 本项目事件 | 说明 |
|---|---|---|
| `agent_start` | `AgentStart` | 运行开始 |
| `turn_start` | `TurnStart` | 轮次从 1 开始 |
| `message_start` | `MessageStart` | 新消息进入上下文 |
| `message_update` | `MessageUpdate` | 助手流式更新，内嵌 `AssistantStreamEvent` |
| `message_end` | `MessageEnd` | 消息完成 |
| `tool_execution_start` | `ToolExecutionStart` | 参数校验之前就发出 |
| `tool_execution_update` | `ToolExecutionUpdate` | 本轮模拟工具不产生，接口保留 |
| `tool_execution_end` | `ToolExecutionEnd` | 成功/失败都会发 |
| `turn_end` | `TurnEnd` | 带本轮助手消息和工具结果 |
| `agent_end` | `AgentEnd` | 增加 `reason` 字段 |
| （无） | `TopicUpdated` | **本项目新增**：Harness 更新 Topic 后补发，Pi 内核不知道 Topic |

`ToolResultMeta` 也是本项目新增的平台元信息，挂在工具结果消息和
`tool_execution_end.data.result.meta` 上，记录证据编号、是否截断、原始大小和内联上限。
Pi 的工具结果只有工具自己的 `details`，没有统一的证据引用。

模型流式事件也做了简化：

| Pi 助手事件 | 本项目事件 |
|---|---|
| `start` | `AssistantStart` |
| `text_delta` | `AssistantTextDelta` |
| `toolcall_end` | `AssistantToolCallEvent` |
| `done` | `AssistantDone` |
| `error` | `AssistantError` |

未移植 `text_start`、`text_end`、`thinking_*`、`toolcall_start/delta` 等更细粒度事件。
原因：本轮前端只需要文本增量和“完整工具调用”，更细的事件会增加适配器和测试成本。

## 4. 钩子映射

| Pi 配置项 | 本项目配置项 | 位置 |
|---|---|---|
| `transformContext` | `AgentLoopConfig.transform_context` | 每轮模型请求前 |
| `convertToLlm` | `AgentLoopConfig.convert_to_llm` | 上下文转换后、模型请求前 |
| `beforeToolCall` | `AgentLoopConfig.before_tool_call` | 参数校验后、工具执行前 |
| `afterToolCall` | `AgentLoopConfig.after_tool_call` | 工具执行后，仅对真正执行过的调用 |
| `finishTurn` | `AgentLoopConfig.finish_turn` | `turn_end` 前 |

## 5. 本项目新增或改变的行为

1. **最大轮数 `max_turns`**
   - Pi 低层 `agentLoop` 没有轮数上限，`maxTurns` 在 durable harness 里。
   - 本项目在循环配置中增加 `max_turns`，达到上限返回 `reason="max_turns"`。
   - 目的：本地运行和以后 HTTP 后台任务都必须防止无限循环。

2. **工具超时 `tool_timeout_s`**
   - Pi 低层循环依赖取消信号，没有统一的工具超时。
   - 本项目用 `asyncio.wait_for` 包住工具执行，超时转成 `is_error=True` 的工具结果。
   - 注意：只能中断可取消的异步等待，纯 CPU 死循环仍需要工具自己检查取消令牌。

3. **`AgentEnd.reason`**
   - Pi 的 `agent_end` 只带 messages。
   - 本项目增加 `completed`、`max_turns`、`cancelled`、`error`，方便记录和前端展示。

4. **工具顺序执行**
   - Pi 支持 parallel/sequential 两种模式。
   - 本轮只做 sequential：模拟工具少，顺序执行更容易看懂时间线和排查。

5. **硬取消处理**
   - `asyncio.CancelledError` 继续向上抛，只补一个 `AgentEnd(reason="cancelled")` 再抛。
   - 协作式取消（`CancellationToken`）会为剩余工具调用生成错误结果，保证调用/结果配对完整。

6. **系统提示词和工具声明的放置**
   - Pi 低层循环把系统提示词和工具声明放在 transcript 的系统消息里。
   - 本项目由 `app/harness/context.py` 拼系统消息，工具单独挂在 `ModelRequest.tools` 上。
   - 这样 Harness 可以独立控制上下文，不需要改动内核。

7. **参数校验**
   - Pi 使用 TypeBox/JSON Schema 并进行类型转换。
   - 本项目只用 `jsonschema.validate` 做基础校验，不做类型转换，返回深拷贝。

8. **`tools_provider` 动态工具集**
   - Pi 低层循环从上下文拿固定工具列表；工具增删通过事件通知外部。
   - 本项目在 `AgentLoopConfig` 增加 `tools_provider`，每次模型请求前重新取一份
     “当前可见工具”。`ToolSession.search()` 加载工具后，下一轮请求立即生效。
   - 只改工具声明，不改内核循环结构；不配置 `tools_provider` 时行为与第 1 步一致。

9. **工具结果证据化**
   - Pi 的工具结果直接进 transcript。
   - 本项目在 `after_tool_call` 位置用 `EvidenceProcessor` 处理：每条业务工具结果
     落盘为 `ev-xxxxxx`，超过 `max_inline_chars` 时只把有界预览和引用放进上下文。
   - 失败结果同样落盘，错误本身也是证据；平台工具（搜索、证据读取）不产生新证据。

10. **`TopicUpdated` 平台事件**
    - Topic 是 DBA 调查层的数据结构，Pi 没有这个概念。
    - Harness 在工具结束后更新事实、候选原因、未决事项和下一步，只有内容真的变化
      才发 `topic_updated`，避免重复事件。

11. **字符预算与裁剪**
    - Pi 把上下文管理交给调用方；本项目提供 `ContextBudget` 作为默认策略：
      系统提示词和工单必留，Topic 超限时保留结构化摘要，近期消息按块裁剪。
    - 预算单位明确标为字符估算，不是 token；工具调用和结果按块成对保留，
      被裁掉的工具结果用带证据编号的占位文本替代。

## 6. 暂不移植的内容

| 内容 | 原因 |
|---|---|
| steering / follow-up 队列 | 本轮单 Agent、单次调查，不需要运行中插入新指令 |
| `prepareNextTurn` | 没有动态切换模型/工具的需求 |
| 工具声明增删事件（`toolsAdded/toolsRemoved`） | 用 `tools_provider` 每次返回当前集合，不单独发增删事件；需要时下次请求自然带上 |
| 并行工具执行 | 模拟工具无依赖，顺序执行足够；并行会放大事件排序复杂度 |
| 图片内容、思考内容 | DBA 工单场景暂不需要 |
| provider 身份 | 已补：`AssistantMessage.provider/model`、任务快照和每轮模型请求都记录实际模型；usage/cost 统计仍未接 |
| 重试、跨进程续跑 | 第 3 步只做任务持久化和重启标记 `interrupted`，不自动续跑 |
| Agent 包装类的订阅机制 | 当前用 `emit` 回调；HTTP 层由 `TaskService` + SQLite 管理任务状态，不复刻 Pi 的订阅队列 |
| `exec`/bash 等 Pi 内置工具 | 与 DBA 只读调查无关，且本环境不执行任意命令 |

## 7. 验证

自动化测试共 57 个用例，分七个文件。HTTP 接口层（`app/api/`）和 MCP 接入层
（`app/mcp/`）属于平台自有代码，不来自 Pi；Pi 映射只覆盖内核、Harness 和事件契约。

`tests/test_agent_loop.py`（10 个）：

| 用例 | 验证点 |
|---|---|
| `test_full_mock_run_completes_and_writes_records` | 6 轮完整流程、6 个工具、id 配对、4 条证据、Topic 事实、报告落盘、事件 seq、预算裁剪 |
| `test_max_turns_stops_without_fake_report` | 轮数上限、只发一次 `turn_start`、不伪造最终报告 |
| `test_unloaded_tool_is_not_executable` | 未通过搜索加载的业务工具不能执行 |
| `test_search_loads_tool_for_next_request` | 搜索后下一轮模型请求能看到新工具 |
| `test_tool_exception_becomes_error_result` | 工具异常转错误结果，循环继续 |
| `test_tool_timeout_becomes_error_result` | `asyncio.wait_for` 超时转错误结果 |
| `test_unknown_tool_returns_error_result` | 未知工具返回 `not found` |
| `test_invalid_arguments_are_rejected_before_execution` | 参数校验失败时不执行工具 |
| `test_cooperative_cancellation_stops_after_tool` | 取消后停止、调用/结果仍配对 |
| `test_stream_without_done_is_reported_as_error` | 模型流缺少 `done/error` 时按错误处理 |

`tests/test_harness.py`（12 个）：

| 用例 | 验证点 |
|---|---|
| `test_save_read_and_restore_index` | 证据保存、分页读取、截断标记、索引恢复 |
| `test_invalid_evidence_id_is_rejected` | 路径穿越等非法证据编号被拒绝 |
| `test_large_result_becomes_bounded_envelope` | 大结果落盘，模型只收到有界信封 |
| `test_small_result_and_platform_tool_are_not_changed` | 小结果不被截断，平台工具不产生证据 |
| `test_load_default_skill` / `test_invalid_skill_name_is_rejected` | 默认 Skill 加载；非法 Skill 名拒绝 |
| `test_search_matches_business_tools` / `test_session_loads_tools_on_demand` | 关键词命中；按需加载 |
| `test_invalid_initial_tools_are_rejected` | Skill 声明不存在的初始工具时报错 |
| `test_topic_tracks_facts_failures_and_platform_calls` | 事实、失败、证据读取、搜索、未决事项 |
| `test_drops_old_blocks_but_keeps_pairs` | 旧消息裁剪后调用/结果仍成对 |
| `test_compressed_topic_keeps_structured_facts` | Topic 超限压缩后仍保留结构化事实 |

`tests/test_api.py`（11 个）：

| 用例 | 验证点 |
|---|---|
| `test_samples_and_capabilities` | 样例目录、Skill/工具能力目录、`kind` 区分 |
| `test_sample_task_full_flow` | 创建 `202`、完整任务、事件分页、Topic、证据分页、报告 |
| `test_model_catalog_defaults_and_selection` | `/api/models`、health 默认模型、任务快照模型字段 |
| `test_deepseek_selection_is_persisted_without_network` | 显式选 DeepSeek 时 provider/model 传到后台参数（替换 `_run_task`，不联网） |
| `test_free_text_ticket` | 自由文本沿用样例范围、保留原文、`source=user_text` |
| `test_cancel_is_cooperative_and_idempotent` | 取消返回 `202`、终态 `cancelled`、重复取消 `200` |
| `test_sse_stream_ends_after_terminal` | SSE 补历史、发送 `stream_end` 后关闭 |
| `test_unknown_ids_and_bad_requests` | 未知 id `404`、请求体冲突/未知 Skill/未知字段 `422` |
| `test_report_is_404_before_terminal_and_turn_context_404` | 未终态报告、未知轮次 |
| `test_unknown_model_provider_and_missing_key_return_422` | 未知 provider、缺 Key 都在创建前拒绝 |
| `test_restart_marks_unfinished_tasks_interrupted` | 启动时遗留任务标为 `interrupted` |

`tests/test_deepseek_model.py`（8 个）：

| 用例 | 验证点 |
|---|---|
| `test_text_stream_becomes_platform_events` | SSE 文本分片拼成完整回复，provider/model/stop_reason 正确 |
| `test_tool_call_fragments_are_merged` | 同一工具调用的分片按 `index` 合并，参数 JSON 完整 |
| `test_http_error_becomes_assistant_error` | 401 转 `AssistantError`，带排查提示 |
| `test_cancel_before_request_does_not_call_network` | 请求前取消不联网，返回 `aborted` |
| `test_malformed_tool_arguments_become_error` | 坏 JSON 参数不交给执行层 |
| `test_messages_and_tools_are_converted` | 四种消息和工具声明转 OpenAI 格式 |
| `test_missing_api_key_has_clear_error` | 缺 Key 在构造阶段报错 |
| `test_sse_parser_handles_comments_and_data` | 跳过 SSE 注释和空行 |

`tests/test_store.py`（2 个）：

| 用例 | 验证点 |
|---|---|
| `test_initialize_adds_model_columns_without_breaking_old_rows` | 老库自动补 `model_provider/model_name`，历史任务默认 mock |
| `test_create_defaults_to_mock` | 新任务不传模型字段时默认 mock |

运行命令：

```bash
uv run python -m unittest discover -s tests -v
uv run ruff check .
```

## 8. 维护约定

- 升级 Pi 参考版本时，先更新本文件和 `NOTICE.md` 中的提交号，再改代码。
- 如果新增/删除事件、钩子或消息字段，必须同步更新：
  - `app/runtime/contracts.py`
  - `docs/pi-mapping.md`
  - `outputs/进度与联调.md` 中的接口约定（涉及前端时）
- 涉及“与 Pi 行为不同”的改动，在 `loop.py` 顶部和本文件第 5 节同时记录原因。
