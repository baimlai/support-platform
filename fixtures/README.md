# 模拟数据说明

本目录下所有数据均为**模拟数据**，仅用于验证 Agent 的执行流程，不代表任何真实客户、实例、SQL 或业务事实。

## 十个场景

| 工单 | 数据集 | 数据库 | 场景 |
|---|---|---|---|
| `tickets/slow_sql_001.json` | `mock_dba/slow_sql_001.json` | MySQL | 慢查询缺联合索引（旧六轮流程） |
| `tickets/mysql_row_lock_001.json` | `mock_dba/mysql_row_lock_001.json` | MySQL | 批量更新引发行锁等待 |
| `tickets/pg_vacuum_bloat_001.json` | `mock_dba/pg_vacuum_bloat_001.json` | PostgreSQL | 长事务阻塞 autovacuum，表膨胀 |
| `tickets/pg_ddl_lock_001.json` | `mock_dba/pg_ddl_lock_001.json` | PostgreSQL | DDL 被空闲事务阻塞，查询排队 |
| `tickets/redis_hot_key_001.json` | `mock_dba/redis_hot_key_001.json` | Redis | 单分片热 Key |
| `tickets/redis_memory_eviction_001.json` | `mock_dba/redis_memory_eviction_001.json` | Redis | maxmemory 触发持续淘汰 |
| `tickets/mongo_collscan_001.json` | `mock_dba/mongo_collscan_001.json` | MongoDB | 缺复合索引导致 COLLSCAN |
| `tickets/mongo_replica_lag_001.json` | `mock_dba/mongo_replica_lag_001.json` | MongoDB | 从节点磁盘饱和导致复制延迟 |
| `tickets/es_disk_watermark_001.json` | `mock_dba/es_disk_watermark_001.json` | Elasticsearch | 磁盘高水位导致副本未分配 |
| `tickets/clickhouse_too_many_parts_001.json` | `mock_dba/clickhouse_too_many_parts_001.json` | ClickHouse | 小批量写入导致 Parts 积压 |

九个新场景工单都带：

- `scenario_mode: true`：告诉 API/CLI 走 `ScenarioEngine`，不要走旧慢 SQL 剧本；
- `skill: "dba-triage"`：通用 DBA 分诊 Skill；
- `dataset`：对应数据集路径，必须在 `fixtures/mock_dba/` 内；
- `scenario_id` / `scenario_label` / `scenario_order`：给前端展示和排序；
- `investigation_plan`：初始调查步骤，会进入 Topic 的 `next_steps`。

场景数据集里的 `scenario.steps` 决定调用哪些工具，`tool_results` 保存每个工具的
模拟返回。所有 payload 都带 `mock=true`。新增场景的结构和字段说明见
`outputs/多场景Mock设计.md`。

## 慢 SQL 样例

`tickets/slow_sql_001.json` 是一张旧流程工单：没有 `scenario_mode`，继续走
`MockDbaModel` 的六步剧本。它保留数据库类型和排序元数据，便于前端和十场景
列表一起展示，但执行路径完全不变。

## 慢日志样例

`slow_log_query` 默认返回 50 条不同指纹的 SQL，完整 JSON 约 16658 字符。
这故意超过了单条结果 4000 字符的内联上限，用来验证：

1. 原始结果保存为 `evidence/ev-000001.json`；
2. 模型只看到带 `truncated=true`、`evidence_id` 的预览；
3. 模型可以用 `evidence_read` 按 `offset/limit` 分段读取原文。

最慢的一条是：

- 指纹 `a1b2c3d4e5f6`；
- 样例 SQL：`SELECT ... FROM orders WHERE user_id = ? AND status IN (?, ?) ORDER BY created_at DESC LIMIT 20`；
- 平均耗时 2380 ms，最大 8120 ms，出现 1240 次；
- 平均扫描 185000 行，只返回 20 行；
- 慢日志采样覆盖率约 0.92，报告里必须保留“不代表全量请求”的限制。

## 其它工具的行为

- `execution_plan`：按指纹返回执行计划。最慢 SQL 的访问类型是 `ALL`（全表扫描），
  `key=null`，Extra 含 `Using filesort`。
- `instance_metrics`：**故意抛超时错误**，用来验证“工具失败不会被解释成没有异常”。
- `lock_wait_query`：返回 `status=no_data`，用来验证“无数据不等于没有锁等待”。

后续接入真实工具时，`fixtures/` 不应作为生产数据源。
