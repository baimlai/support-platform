# 第 3 步 HTTP 响应样例

这些文件不是手写的示例，而是从**本地运行的 FastAPI 服务**直接抓取的响应，供前端对照字段形状。
抓取时间：2026-10-30；主要 mock 任务 id：`6700ec91-a402-4419-a7a1-6d6d9effe175`；
真实 DeepSeek 任务 id：`7f786872-21bb-4f2f-aac1-1122c48601eb`。
模型目录、`task-created.json` 和 422 样例是在加入“可选模型”后重新抓取的。
`report.json` 随后按“结论优先”的章节格式重新抓取，来源任务
`44f3afe1-1ca8-4ea3-9e1d-0b6f49ab3736`（`markdown` 2479 字符）。
十场景上线后，`samples.json` 已按最新接口重新抓取（10 个样例、6 种数据库），
其余文件仍来自旧的慢 SQL 示例任务，字段结构没有变化。

> `models.json` 是在配置了 `DEEPSEEK_API_KEY` 后抓取的（`available=true`）。
> `task-deepseek-no-key.json` 是未配置 Key 时抓取的 422 样例，用于前端展示错误提示。

> 所有内容均为模拟数据（`mock=true`），不代表真实客户、实例或数据库事实。

## 1. 怎么复现

终端 A 启动服务：

```bash
cd /Users/baiml/PycharmProjects/support-platform
uv run python -m app.cli serve --host 127.0.0.1 --port 8000
```

终端 B 依次执行：

```bash
BASE=http://127.0.0.1:8000

# 1) 目录与模型
curl -sS "$BASE/api/health"         > health.json
curl -sS "$BASE/api/samples"        > samples.json
curl -sS "$BASE/api/capabilities"   > capabilities.json
curl -sS "$BASE/api/models"         > models.json

# 1.1) 选 DeepSeek 但没有配置 Key（422）
curl -sS -X POST "$BASE/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"sample_id":"slow-sql-001","model_provider":"deepseek","model_name":"deepseek-flash"}' \
  > task-deepseek-no-key.json

# 2) 创建任务（202）
TASK=$(curl -sS -X POST "$BASE/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"sample_id": "slow-sql-001", "model_provider": "mock", "model_name": "mock-dba"}' \
  | tee task-created.json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["task_id"])')
echo "$TASK"

# 3) 轮询直到 status 进入终态
curl -sS "$BASE/api/tasks/$TASK" > task.json

# 4) 事件（第一页）与增量
curl -sS "$BASE/api/tasks/$TASK/events?after_seq=0&limit=1000"     > events-page-1.json
curl -sS "$BASE/api/tasks/$TASK/events?after_seq=241&limit=200"    > events-end.json

# 5) 单条事件：从 events-page-1.json 里按 type 过滤
python3 - <<'PY'
import json
events = json.load(open("events-page-1.json"))["events"]
for kind, name in (("topic_updated", "event-topic-updated.json"),
                   ("tool_execution_end", "event-tool-execution-end.json")):
    first = next(e for e in events if e["type"] == kind)
    json.dump(first, open(name, "w"), ensure_ascii=False, indent=2)
PY

# 6) 轮次上下文：第 1 轮和第 6 轮
curl -sS "$BASE/api/tasks/$TASK/turns/1/context" > turn-1-context.json
curl -sS "$BASE/api/tasks/$TASK/turns/6/context" > turn-6-context.json

# 7) 证据列表与分段读取
curl -sS "$BASE/api/tasks/$TASK/evidence" > evidence-list.json
curl -sS "$BASE/api/tasks/$TASK/evidence/ev-000001?offset=0&limit=4000" > evidence-read-first-page.json
curl -sS "$BASE/api/tasks/$TASK/evidence/ev-000001?offset=4000&limit=800" > evidence-read-offset-4000.json
curl -sS "$BASE/api/tasks/$TASK/evidence/ev-000001?offset=4000&limit=4000" > evidence-read-page-2.json

# 8) 报告
curl -sS "$BASE/api/tasks/$TASK/report" > report.json

# 9) SSE（任务已终态时，会补历史并立即发送 stream_end）
curl -sS -N "$BASE/api/tasks/$TASK/events/stream?after_seq=241"
```

真实 DeepSeek 任务（会消耗一次真实模型调用，可选）：

```bash
DEEPSEEK_TASK=$(curl -sS -X POST "$BASE/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"sample_id":"slow-sql-001","model_provider":"deepseek","model_name":"deepseek-flash","max_turns":8}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["task_id"])')
curl -sS "$BASE/api/tasks/$DEEPSEEK_TASK" > task-completed-deepseek.json
curl -sS "$BASE/api/tasks/$DEEPSEEK_TASK/turns/1/context" > turn-1-context-deepseek.json
```

注意：`task-created.json` 里的 `task_id` 是抓取时那一次的任务，重新跑会得到新的 UUID。

## 2. 文件说明

| 文件 | 对应接口 | 看点 |
|---|---|---|
| `health.json` | `GET /api/health` | `model` 是服务端默认模型，不代表具体任务 |
| `models.json` | `GET /api/models` | 当前配置了 Key：`mock` 和 `deepseek` 都 `available=true`，DeepSeek 候选模型为 flash/v4-pro |
| `samples.json` | `GET /api/samples` | 10 个样例，覆盖 MySQL/PostgreSQL/Redis/MongoDB/Elasticsearch/ClickHouse；新增 `database_type`、`scenario_id`、`scenario_label`、`scenario_order`、`skill`、`symptom` |
| `capabilities.json` | `GET /api/capabilities` | `skills[].content` 是 Skill Markdown；`tools[].kind` 区分 `business` / `platform`；`models` 与 `/api/models` 同形 |
| `task-created.json` | `POST /api/tasks`（202） | 创建瞬间：`status=queued`、`model_provider=mock`、`model_name=mock-dba`、`topic=null`、`report=null` |
| `task-deepseek-no-key.json` | `POST /api/tasks`（422） | 选 DeepSeek 但未配置 Key：`detail` 提示设置 `DEEPSEEK_API_KEY`（未配置 Key 的机器上抓取） |
| `task-completed.json` | `GET /api/tasks/{id}` | mock 终态快照：`status=completed`、`turns=6`、`evidence_count=4`、`topic`、`report`、`capabilities`、`model_provider/model_name` |
| `task-completed-deepseek.json` | `GET /api/tasks/{id}` | 真实 DeepSeek 终态快照：`model_provider=deepseek`、`model_name=deepseek-flash`、5 轮、4 条证据 |
| `turn-1-context-deepseek.json` | `GET /api/tasks/{id}/turns/1/context` | `model=deepseek-flash`；第 1 轮 3 个工具（1 业务 + 2 平台） |
| `events-page-1.json` | `GET /api/tasks/{id}/events?after_seq=0&limit=1000` | 243 条事件；`next_seq=243`、`has_more=false`。文件约 1 MB，用 `jq`/脚本看，不要整页展开 |
| `events-end.json` | `GET /api/tasks/{id}/events?after_seq=241&limit=200` | 增量读取只剩 `turn_end`、`agent_end` 两条 |
| `event-topic-updated.json` | 事件子集 | `data.topic` 的完整结构：facts/candidates/next_steps/attempted_tools 等 |
| `event-tool-execution-end.json` | 事件子集 | `data.evidence_id` 在顶层；`data.result.meta` 带 `truncated`、`raw_size_chars` |
| `turn-1-context.json` | `GET /api/tasks/{id}/turns/1/context` | 第 1 轮只有 3 个工具（1 业务 + 2 平台） |
| `turn-6-context.json` | `GET /api/tasks/{id}/turns/6/context` | 6 个工具；`budget.dropped_messages=4`、`used_chars=11799/12000` |
| `evidence-list.json` | `GET /api/tasks/{id}/evidence` | 4 条证据；`ev-000003.is_error=true` |
| `evidence-read-first-page.json` | `GET /api/tasks/{id}/evidence/ev-000001?offset=0&limit=4000` | 第一页：`offset=0`、`limit=4000`、`total_chars=16658`、`truncated=true` |
| `evidence-read-offset-4000.json` | 同接口，`offset=4000&limit=800` | 从第 4001 个字符读 800 字；前端常用的分页大小 |
| `evidence-read-page-2.json` | 同接口，`offset=4000&limit=4000` | 第二页 4000 字符，证明可以按偏移连续翻页 |
| `report.json` | `GET /api/tasks/{id}/report` | `markdown` 是完整报告；`structured` 固定为 `null`；章节为 `## 结论`、`## 修复建议`、`## 证据链`、`## 已排除与待验证`、`## 调查过程`、`## 验证方法`、`## 当前限制` |

## 3. 前端重点对照

1. **工具卡片用 `tool_execution_start/end`**。随后还会出现 `role=toolResult` 的
   `message_start/message_end`，那是写回模型上下文的工具消息，不要再渲染成一张卡片。
2. **证据编号在 `tool_execution_end.data.evidence_id`**，不在 `result` 内部；
   `result.meta.evidence_id` 是同一份信息的另一种挂载位置，二选一即可。
3. **`message_update` 是流式增量**，只用于“正在输出”的临时展示；
   最终文本以 `message_end.data.message` 为准。
4. **任务进终态后停止轮询**。`events.has_more=false` 只表示当前没有更多事件，
   不代表任务结束；是否继续看 `GET /api/tasks/{id}.status`。
5. **报告只在终态可取**，提前请求返回 `404 {"detail": "报告尚未生成"}`。
6. **取消是协作式的**，`POST /cancel` 返回 `202` 时任务可能仍是 `running`，
   继续轮询到 `cancelled` 或 `interrupted`。
7. **SSE 帧格式**：`id: <seq>`、`event: <type>`、`data: <事件 JSON>`；
   每 15 秒一条 `: keep-alive` 注释；终态后发 `event: stream_end` 并关闭连接。
   断线重连时把最后收到的 `seq` 作为 `after_seq`，不会重复消费。
8. **模型下拉框**用 `models.json`；`available=false` 的 provider 要置灰并显示 `reason`。
   创建任务后以响应里的 `model_provider/model_name` 为准展示“本次实际使用”，
   不要用页面当前下拉框的值回填历史任务。
9. **报告页结论优先**：按 `##` 标题取章节，把 `## 结论` 里的
   `**最可能根因：**`、`**置信度：**`、`**直接证据：**`、`**结论边界：**` 放在第一屏；
   `## 修复建议` 紧随其后；`## 已排除与待验证`、`## 证据链`、`## 调查过程` 等折叠。
   没有 `## 结论` 的旧报告要明确提示并原样展示候选，不要把候选原因写成已确认根因。
   完整约定见 `outputs/进度与联调.md` 5.9。
