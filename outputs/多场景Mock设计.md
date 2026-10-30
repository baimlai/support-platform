# 多数据库 DBA Mock 场景设计（10 个）

> 目标：让 Agent 不只会在 MySQL 慢日志里打转，而是能按“先取证、再收敛”的方法处理
> 云数据库里更常见的故障：锁等待、膨胀、复制延迟、内存淘汰、分片水位、写入积压等。
>
> 重要边界：**场景数据全部是模拟数据**（`mock=true`），不连接任何真实数据库；
> MCP 协议、工具注册、调用链和报告生成是真实代码。结论只用于演示排查方法。

## 1. 为什么选这十个

真实的云数据库工单大致可以分成五类：

1. **单条 SQL 慢**：缺索引、执行计划退化；
2. **锁与事务冲突**：行锁排队、DDL 被长事务阻塞、vacuum 被旧快照拖住；
3. **容量与内存**：Redis 达到 maxmemory、Elasticsearch 磁盘水位、ClickHouse parts 积压；
4. **复制与高可用**：MongoDB 从节点延迟、副本集/分片节点资源不一致；
5. **访问模式问题**：Redis 热 Key、MongoDB 全集合扫描。

十个场景覆盖了 MySQL、PostgreSQL、Redis、MongoDB、Elasticsearch、ClickHouse
六种数据库，每种至少一个“资源类”问题和一个“访问/结构类”问题。这样既能演示
不同数据库的专用工具，也能反复练习同一条报告契约：**一个主根因 + 多组修复候选 +
可展开的已排除/待验证**。

## 2. 十个场景总表

| # | 工单 | 数据库 | 场景 | 主根因（一句话） | 关键工具 |
|---|---|---|---|---|---|
| 1 | `slow_sql_001` | MySQL | 慢查询缺联合索引 | 订单列表查询走全表扫描 + filesort | `slow_log_query`、`execution_plan`、`instance_metrics`、`lock_wait_query` |
| 2 | `mysql_row_lock_001` | MySQL | 批量更新引发行锁等待 | 财务单事务更新 1.8 万行，支付会话排队 | `mysql_innodb_status`、`mysql_processlist` |
| 3 | `pg_vacuum_bloat_001` | PostgreSQL | 长事务阻塞 autovacuum | 报表连接 `idle in transaction` 31 分钟，订单表膨胀 38.4% | `pg_activity`、`pg_table_stats` |
| 4 | `pg_ddl_lock_001` | PostgreSQL | DDL 被空闲事务阻塞 | 报表事务持有 AccessShareLock，DDL 等 AccessExclusiveLock 754 秒 | `pg_locks`、`pg_activity` |
| 5 | `redis_hot_key_001` | Redis | 单分片热 Key | 活动配置 Key 约 8.2 万 QPS，分片 3 CPU 94% | `redis_key_stats`、`redis_info`、`redis_slowlog` |
| 6 | `redis_memory_eviction_001` | Redis | 内存淘汰 | 642MB 无 TTL 大 Key + 18 万无 TTL 商品缓存，触发 `allkeys-lru` | `redis_info`、`redis_key_stats`、`redis_slowlog` |
| 7 | `mongo_collscan_001` | MongoDB | COLLSCAN | `orders` 缺 `{status, createdAt}` 复合索引，扫 284 万文档 | `mongo_profiler`、`mongo_explain`、`mongo_server_status` |
| 8 | `mongo_replica_lag_001` | MongoDB | 从节点复制延迟 | 回填放大 oplog，低 IOPS 从节点磁盘 99% 饱和 | `mongo_rs_status`、`mongo_server_status` |
| 9 | `es_disk_watermark_001` | Elasticsearch | 磁盘高水位 | ILM 未生效，三节点磁盘 90.8%~92.4%，48 个副本未分配 | `es_cluster_health`、`es_shard_allocation` |
| 10 | `clickhouse_too_many_parts_001` | ClickHouse | Parts 积压 | 每秒 6 批、每批 42 行，当前分区 1842 个 part，超过 300 阈值 | `clickhouse_query_log`、`clickhouse_parts` |

## 3. 逐个场景说明

### 3.1 MySQL 慢查询（保留的旧流程）

- **现象**：订单列表从 10:00 开始变慢，部分请求超时。
- **取证**：慢日志 → 分页读证据 → 搜索工具 → 执行计划 → 实例监控（故意超时）→ 锁等待（no_data）。
- **根因**：`orders` 上的查询缺少能同时覆盖过滤和排序的联合索引，执行计划为 `ALL`，扫描 18.5 万行只返回 20 行，并触发 filesort。
- **价值**：演示“工具失败不等于没有异常”“慢日志覆盖率只有 0.92”这两个报告边界。
- **注意**：这张工单没有 `scenario_mode`，继续走旧的六轮剧本，保证历史行为不变。

### 3.2 MySQL 行锁等待（`mysql_row_lock_001`）

- **现象**：支付接口大量超时，数据库连接数升高。
- **取证**：InnoDB 状态看到 37 个事务等待行锁、最长 42.6 秒；当前会话定位到 `finance_batch` 的批量 UPDATE。
- **根因**：财务对账单事务更新同一商户 1.8 万行，持锁 68 秒；支付更新落在同一批行上，形成等待链。
- **修复方向**：分批提交（500~1000 行/批）、评估 `(merchant_id, status)` 联合索引、设置合理的锁等待超时和快速失败。
- **为什么值得练**：这是“不是 SQL 慢，而是事务太大”的典型；报告必须把“支付 SQL 缺索引”与“批量任务持锁”分开，不能把结果放大因素当根因。

### 3.3 PostgreSQL 表膨胀（`pg_vacuum_bloat_001`）

- **现象**：订单查询越来越慢，表空间持续增长。
- **取证**：`pg_activity` 发现报表连接 `idle in transaction` 1860 秒、持有旧 xmin；`pg_table_stats` 显示 842 万死元组、膨胀率 38.4%。
- **根因**：报表服务查询后没有提交/回滚，旧快照阻止 autovacuum 回收死元组。
- **修复方向**：修复事务边界、设置 `idle_in_transaction_session_timeout`、常规 `VACUUM (ANALYZE)`；严重时评估 `pg_repack`，不要直接 `VACUUM FULL`。
- **边界**：当前是时间相关性证据，没有抓到 autovacuum 等待 xmin 的实时锁视图，报告必须写“待验证”。

### 3.4 PostgreSQL DDL 锁队列（`pg_ddl_lock_001`）

- **现象**：发版执行 `ALTER TABLE invoices` 后，发票查询大面积超时。
- **取证**：`pg_locks` 看到 DDL 等待 AccessExclusiveLock 754 秒，被报表事务的 AccessShareLock 阻塞，后面排了 46 个查询；`pg_activity` 确认阻塞者是 `idle in transaction`。
- **根因**：报表事务不结束，DDL 拿不到排他锁；后续查询又排在 DDL 后面，影响从发版扩大到在线业务。
- **修复方向**：先处理阻塞事务，再让 DDL 带 `lock_timeout`；能并发执行的索引变更优先用 `CREATE INDEX CONCURRENTLY`。
- **为什么值得练**：锁等待链要按“谁持有、谁等待、谁排在后面”三个层次读，不能只看等待时长。

### 3.5 Redis 热 Key（`redis_hot_key_001`）

- **现象**：商品详情 p99 升高，Redis 集群单分片 CPU 打满。
- **取证**：Key 访问排行发现 `promo:2026:home:banner` 约 82000 QPS，是第二名的 19 倍；INFO 显示分片 3 CPU 94%、其他分片 24%/27%；SLOWLOG 显示同一 Key 的 GET 11~13ms。
- **根因**：同一个活动配置 Key 的所有读请求落到单个分片，CPU 排队导致延迟。
- **修复方向**：本地缓存、Key 拆分（随机后缀副本）、只读副本/专用缓存层。
- **排除项**：内存不足（6.2GB/8GB 且无淘汰）、网络故障（三分片均能响应）。

### 3.6 Redis 内存淘汰（`redis_memory_eviction_001`）

- **现象**：商品缓存命中率从 94% 降到 61%，`evicted_keys` 持续增长。
- **取证**：INFO 显示内存 7.94GB/8GB（99.2%）、`allkeys-lru`、1 小时淘汰 124 万 Key；Key 统计发现 642MB 的 `product:snapshot:all` 无 TTL、近 24 小时几乎不读，另有约 18 万商品缓存无 TTL；SLOWLOG 显示三个分片都出现商品 GET 慢命令。
- **根因**：无 TTL 大 Key + 大量无 TTL 缓存把内存推到上限，LRU 把仍在使用的商品缓存淘汰掉。
- **修复方向**：快照迁出 Redis 或设 TTL、商品缓存统一 TTL、评估 `volatile-lru` 前先统计 TTL 覆盖率、按业务拆实例。
- **为什么和热 Key 成对**：两个场景都可能表现为“Redis 慢”，但一个是单点流量，一个是全局内存压力；工具调用顺序不同，结论也必须不同。

### 3.7 MongoDB 全集合扫描（`mongo_collscan_001`）

- **现象**：运营后台订单筛选平均 1.8 秒，主节点 CPU 升高。
- **取证**：profiler 发现 `status + createdAt` 查询 `planSummary=COLLSCAN`，10 分钟 3841 次，扫描 284 万文档返回 50 条；explain 确认只有 `_id` 和 `orderNo` 索引；serverStatus 显示 CPU 87%，但 cache 68%、磁盘 41%，排除内存和磁盘瓶颈。
- **根因**：缺少 `{status: 1, createdAt: -1}` 复合索引。
- **修复方向**：低峰期建索引、强制分页、上线后再次 explain 验证 `IXSCAN`。
- **边界**：还没执行索引变更，构建期间的 IO 和磁盘空间需要评估。

### 3.8 MongoDB 复制延迟（`mongo_replica_lag_001`）

- **现象**：报表从只读节点读到十几分钟前的数据。
- **取证**：`replSetGetStatus` 显示一个从节点落后 842 秒，另一个只落后 2 秒；`serverStatus` 显示落后节点磁盘 99%、await 48ms、写队列 37，主节点磁盘仅 35%。
- **根因**：历史数据回填产生大量 oplog，落后从节点的低 IOPS 云盘回放不过来。
- **修复方向**：限速/暂停回填、提升从节点磁盘 IOPS 或规格、报表敏感查询改读主节点、必要时重建从节点。
- **排除项**：复制网络故障（心跳 2ms、无同步错误）、主节点瓶颈（CPU 52%、磁盘 35%）、从节点宕机（health=1）。

### 3.9 Elasticsearch 磁盘水位（`es_disk_watermark_001`）

- **现象**：日志集群从 green 变 yellow，48 个副本未分配。
- **取证**：cluster health 显示主分片全部正常、只有副本未分配；allocation explain 明确返回三台数据节点磁盘 90.8%~92.4%，超过 90% high watermark；ILM 策略 `logs-30d` 未绑定到近期索引，最老索引还是 7 月 18 日。
- **根因**：ILM 删除阶段没有生效，磁盘持续增长并越过高水位，Elasticsearch 拒绝分配副本。
- **修复方向**：修复索引模板/ILM 绑定、按保留策略删除或归档旧索引、扩容磁盘或节点；临时把副本调 0 只能救急。
- **边界**：删除前必须确认日志保留合规要求；调高 watermark 只是掩盖风险。

### 3.10 ClickHouse Parts 积压（`clickhouse_too_many_parts_001`）

- **现象**：埋点 INSERT 大量失败，异常码 252 `Too many parts`。
- **取证**：query_log 显示 10 分钟 384 次失败、约 6.1 批/秒、每批平均 42 行；parts 统计显示当前分区 1842 个活跃 part、中位 42 行/16KB，整表 96 个分区共 5820 个 part；merge 吞吐 3.4 part/s，追不上 6.1 part/s 的写入。
- **根因**：小批量高频 INSERT 每次生成一个 part，超过 `parts_to_throw_insert=300`。
- **修复方向**：客户端合并批次（1 万~10 万行或 1~5 秒）、开启 `async_insert`、用 Kafka/Buffer 表缓冲、评估按月分区。
- **排除项**：磁盘不足（61%）、内存不足（54%）、SQL 语法错误（已进入写入阶段）。

## 4. 场景数据契约

每个场景数据集（`fixtures/mock_dba/*.json`）遵循同一份结构：

```json
{
  "mock": true,
  "dataset_id": "redis-hot-key-001",
  "database_type": "redis",
  "instance_id": "redis-cluster-005",
  "database": "0",
  "scenario": {
    "label": "Redis 单分片热 Key 导致延迟升高",
    "search_query": "Redis 热key 访问次数 分片 CPU slowlog 内存",
    "steps": [
      {
        "tool": "redis_key_stats",
        "label": "访问次数最高的 Key",
        "args": {"instance_id": "$instance_id", "limit": 10},
        "reason": "为什么先查这个工具"
      }
    ],
    "diagnosis": {
      "root_cause": "一个主根因",
      "confidence": "高",
      "evidence_keys": ["redis.hot_key"],
      "boundary": "结论边界",
      "fix": {"primary": [], "alternate": [], "mitigation": []},
      "excluded": [],
      "pending": [],
      "verification": [],
      "limitations": []
    }
  },
  "tool_results": {
    "redis_key_stats": {
      "payload": {"mock": true, "...": "给模型看的完整结果"},
      "details": {
        "summary": "给报告用的一句话",
        "topic": {
          "facts": [
            {"key": "redis.hot_key", "statement": "可引用事实", "data": {}}
          ],
          "candidates": [
            {"key": "hot_key", "statement": "...", "status": "supported"}
          ],
          "open_questions": [],
          "next_steps": []
        }
      },
      "is_error": false,
      "message": ""
    }
  }
}
```

关键约定：

- `scenario.steps` 的 tool 集合必须和 `tool_results` 的 key 集合完全一致；
- `diagnosis.evidence_keys` 引用的每个 key 必须真的出现在某个 `details.topic.facts` 里；
- `details.topic` 是 Topic 和报告的唯一事实来源，模型文本不能凭空造事实；
- 路径必须落在 `fixtures/mock_dba/` 内，且顶层 `mock=true`，防止误读真实数据。

## 5. 研究来源

以下是设计场景时参考的公开文档。场景数值是模拟的，但故障机理和排查工具与真实环境一致：

- MySQL InnoDB 锁：<https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html>
- MySQL 元数据锁：<https://dev.mysql.com/doc/refman/8.0/en/metadata-locking.html>
- PostgreSQL 监控 `pg_stat_activity`：<https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW>
- PostgreSQL 日常 vacuum：<https://www.postgresql.org/docs/current/routine-vacuuming.html>
- PostgreSQL 显式锁：<https://www.postgresql.org/docs/current/explicit-locking.html>
- Redis 延迟排查：<https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency/>
- Redis 淘汰策略：<https://redis.io/docs/latest/develop/reference/eviction/>
- MongoDB profiler：<https://www.mongodb.com/docs/manual/reference/database-profiler/>
- MongoDB explain 结果：<https://www.mongodb.com/docs/manual/reference/explain-results/>
- MongoDB 副本集排障：<https://www.mongodb.com/docs/manual/tutorial/troubleshoot-replica-sets/>
- Elasticsearch cluster health：<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-health>
- Elasticsearch 分片分配与磁盘水位：<https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings>
- ClickHouse Too many parts：<https://clickhouse.com/docs/knowledgebase/too-many-parts>
- ClickHouse 异步写入：<https://clickhouse.com/docs/guides/inserting-data>
- ClickHouse `system.parts`：<https://clickhouse.com/docs/operations/system-tables/parts>

## 6. 验证方式

```bash
cd /Users/baiml/PycharmProjects/support-platform

# 数据契约 + 路径安全 + 十个场景端到端
uv run python -m unittest tests.test_scenarios -v
uv run python -m unittest tests.test_api.ScenarioApiTests -v

# 逐个手工跑
uv run python -m app.cli run --ticket fixtures/tickets/redis_hot_key_001.json --model-delay 0
uv run python -m app.cli run --ticket fixtures/tickets/clickhouse_too_many_parts_001.json --model-delay 0
```

端到端断言包括：任务完成、Skill 自动选择、报告七个章节各出现一次、
只有一个“最可能根因”、至少一个证据编号、场景规划的工具都实际调用过。
