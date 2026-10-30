"""DBA 工具的“业务实现”和工具目录。

## 这个文件为什么存在

这里放的是**工具本身真正做什么**：读取模拟数据、校验实例、拼装结果。
MCP Server（`app/mcp/server.py`）只负责把下面这些函数暴露成 MCP 工具；
Agent 侧（`app/mcp/client.py`）只负责通过 MCP 协议调用它们。

把业务实现单独放一层，有三个好处：

1. MCP Server 和测试可以复用同一份逻辑，不会出现“服务端一套、测试一套”；
2. 以后换成真实数据库客户端时，只改这里的查询函数，MCP 协议层不用动；
3. 能力目录（`/api/capabilities`）可以只读工具声明，不需要启动 MCP 进程。

## 当前数据来源

数据来自 `fixtures/mock_dba/slow_sql_001.json`，文件里必须有 `"mock": true`。
这里**不连接任何生产数据库**，也不会执行写操作。工具结果可以用于联调，
但报告里必须继续标明“模拟数据”。

## Python 语法提示

- `@dataclass(slots=True)`：自动生成 `__init__`、`__repr__`、`__eq__`，
  `slots=True` 只允许使用声明过的字段，写错属性名会立刻报错。
- `Path(...).resolve()`：把相对路径转成绝对路径，避免子进程工作目录不同导致读错文件。
- `with path.open(...) as file:`：上下文管理器，代码块结束时自动关闭文件。
- `dict.get(key, default)`：键不存在时返回默认值，不会抛 `KeyError`。
- `sorted(..., key=..., reverse=True)`：返回新列表，不改原数据。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.harness.tools import ToolExecutionError
from app.runtime.contracts import CancellationToken, ToolResult, ToolSpec

# 项目根目录：app/mcp/dba_tools.py -> app/mcp -> app -> 项目根。
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 默认模拟数据文件。测试可以用参数换成临时 fixture，不污染仓库。
DEFAULT_DATASET_PATH = PROJECT_ROOT / "fixtures" / "mock_dba" / "slow_sql_001.json"

# MCP 工具注解：告诉客户端这些工具是只读的。
# 字段名使用 MCP 规范的 camelCase，序列化到协议里就是 readOnlyHint 等。
READ_ONLY_ANNOTATIONS: dict[str, bool] = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}


@dataclass(slots=True)
class ToolOutcome:
    """一次工具业务执行的结果。

    - `text`：给模型看的文本。成功时是缩进 JSON，失败时是一句人能读懂的错误；
    - `payload`：成功时的结构化载荷，会放进 MCP 的 `structuredContent.payload`；
    - `details`：平台内部摘要（例如最慢 SQL 指纹、是否全表扫描），
      不直接塞进模型正文，但会用于 Topic、证据目录和前端；
    - `is_error`：业务失败标记。MCP 层会把它映射成 `CallToolResult.isError`。

    例子：慢日志查询成功时，`payload` 是完整的 50 行 JSON，`details` 只保留
    “最慢指纹 / 平均耗时 / 扫描行数”这几个平台真正要用的字段。
    """

    text: str
    details: dict[str, Any]
    payload: dict[str, Any] | None = None
    is_error: bool = False


@dataclass(frozen=True, slots=True)
class BusinessToolDefinition:
    """一个业务工具的静态声明。

    `parameters` 是给模型和能力目录看的 JSON Schema。
    MCP Server 自己会根据函数签名生成一份 Schema；这份声明用于不启动 MCP 进程时
    展示能力目录和做工具搜索。两边的字段名、必填项必须保持一致，测试会检查。
    """

    name: str
    label: str
    description: str
    parameters: dict[str, Any]


def _json_text(value: Any) -> str:
    """把结果转成模型容易阅读的缩进 JSON。

    `ensure_ascii=False` 让中文保持可读，排查时不需要再解码成 `\\uXXXX`。
    """
    return json.dumps(value, ensure_ascii=False, indent=2)


def _success(payload: dict[str, Any], details: dict[str, Any]) -> ToolOutcome:
    """构造一个成功结果。

    统一从这里构造，保证“模型正文”和“平台摘要”不会一边有一边没有。
    """
    return ToolOutcome(
        text=_json_text(payload),
        details=details,
        payload=payload,
        is_error=False,
    )


def _failure(message: str, details: dict[str, Any] | None = None) -> ToolOutcome:
    """构造一个业务失败结果。

    注意：业务失败不是“ MCP 协议错误”。协议错误会让整次调用抛异常；
    这里返回 `isError=True`，模型仍然能看到错误说明，并继续调查其它分支。
    例子：监控接口超时、实例不在模拟数据范围、SQL 指纹查不到执行计划。
    """
    return ToolOutcome(
        text=message,
        details=details or {},
        payload=None,
        is_error=True,
    )


def load_dataset(path: Path | str | None = None) -> dict[str, Any]:
    """读取模拟数据，并检查 `mock=true`。

    如果文件不存在或结构不对，直接抛错，而不是悄悄返回空数据。
    DBA 场景里“没有数据”和“数据源坏了”必须区分，不能把读取失败伪装成正常结果。
    """
    dataset_path = Path(path) if path is not None else DEFAULT_DATASET_PATH
    if not dataset_path.exists():
        raise FileNotFoundError(f"模拟数据文件不存在：{dataset_path}")

    with dataset_path.open("r", encoding="utf-8") as file:
        dataset = json.load(file)

    if not dataset.get("mock"):
        # 保护措施：万一有人把真实数据文件放到这里，拒绝把它当模拟数据使用。
        raise ValueError(f"数据文件未标记 mock=true，拒绝当作模拟数据使用：{dataset_path}")
    return dataset


def _check_instance(requested: str, expected: str) -> None:
    """确认请求的实例确实在模拟数据范围内。

    例子：数据里只有 `db-mysql-001`，模型却查 `db-mysql-999`。
    这时返回明确错误，而不是一份看起来正常的空结果。
    """
    if requested != expected:
        raise ToolExecutionError(
            f"实例 {requested} 不在模拟数据范围内（当前仅支持 {expected}）。"
            "这不代表真实实例不存在，只是模拟工具无法查询。"
        )


def _require_block(dataset: dict[str, Any], key: str) -> dict[str, Any]:
    """读取数据集里的一个固定数据块；缺失时给出可读错误。

    为什么要有这个函数：
    以前服务端只加载慢 SQL 数据，所以 `dataset["slow_log"]` 几乎不可能缺失。
    现在同一套 MCP Server 会加载 MySQL、PostgreSQL、Redis 等不同场景的数据；
    如果模型在 PostgreSQL 场景里误调了 `slow_log_query`，直接 `dataset["slow_log"]`
    会抛 `KeyError`，最后变成一条难懂的协议错误。这里改成明确的业务失败，
    告诉模型“当前数据集没有这个数据块”，模型可以换工具继续调查。

    例子：
        `_require_block(dataset, "slow_log")`
        成功 -> 返回慢日志数据块；
        失败 -> 抛 `ToolExecutionError("当前模拟数据不包含 slow_log ...")`。
    """
    block = dataset.get(key)
    if not isinstance(block, dict):
        raise ToolExecutionError(
            f"当前模拟数据不包含 `{key}` 数据块，无法执行这个工具。"
            "这不代表真实实例没有数据，请根据当前场景选择其它工具。"
        )
    return block


def slow_log_query(
    dataset: dict[str, Any],
    *,
    instance_id: str,
    start_time: str,
    end_time: str,
    limit: int = 5,
) -> ToolOutcome:
    """查询慢日志摘要，按平均耗时从高到低返回。

    例子参数：
        instance_id="db-mysql-001"
        start_time="2026-10-29T10:00:00+08:00"
        end_time="2026-10-29T11:00:00+08:00"
        limit=5

    返回的 `payload` 和以前本地工具完全一致，这样模型行为不会因为换成 MCP
    而发生变化；变化只在“工具怎么被发现、怎么被调用”这一层。
    """
    expected_instance = str(dataset["instance_id"])
    _check_instance(instance_id, expected_instance)

    slow_log_data = _require_block(dataset, "slow_log")
    rows = sorted(
        slow_log_data["rows"],
        key=lambda row: row["avg_latency_ms"],
        reverse=True,
    )[: max(1, int(limit))]

    payload = {
        "mock": True,
        "source": slow_log_data["source"],
        "instance_id": expected_instance,
        "requested_range": {"start": start_time, "end": end_time},
        "coverage": slow_log_data["coverage"],
        "row_count": len(rows),
        "rows": rows,
        "note": "结果来自模拟慢日志，只用于验证流程。",
    }

    top_row = rows[0] if rows else None
    stats = {
        "returned_rows": len(rows),
        "fingerprint_count": len(slow_log_data["rows"]),
        "max_avg_latency_ms": max(
            (row.get("avg_latency_ms", 0) for row in slow_log_data["rows"]),
            default=0,
        ),
        "min_avg_latency_ms": min(
            (row.get("avg_latency_ms", 0) for row in slow_log_data["rows"]),
            default=0,
        ),
        "total_count": sum(row.get("count", 0) for row in slow_log_data["rows"]),
    }
    details = {
        "mock": True,
        "source": slow_log_data["source"],
        "row_count": len(rows),
        "coverage_ratio": slow_log_data["coverage"].get("coverage_ratio"),
        "coverage_note": slow_log_data["coverage"].get("note"),
        "top": (
            {
                "sql_fingerprint": top_row.get("sql_fingerprint"),
                "sample_sql": top_row.get("sample_sql"),
                "count": top_row.get("count"),
                "avg_latency_ms": top_row.get("avg_latency_ms"),
                "max_latency_ms": top_row.get("max_latency_ms"),
                "avg_rows_examined": top_row.get("avg_rows_examined"),
                "avg_rows_sent": top_row.get("avg_rows_sent"),
            }
            if top_row is not None
            else None
        ),
        "stats": stats,
    }
    return _success(payload, details)


def execution_plan(
    dataset: dict[str, Any],
    *,
    instance_id: str,
    sql_fingerprint: str,
) -> ToolOutcome:
    """按 SQL 指纹查询执行计划。

    例子参数：
        instance_id="db-mysql-001"
        sql_fingerprint="a1b2c3d4e5f6"

    关键平台摘要：
    - `full_scan=True`：出现 `type=ALL`；
    - `key=None`：没有命中索引；
    - `extra`：是否出现 `Using filesort` / `Using temporary`。
    """
    expected_instance = str(dataset["instance_id"])
    _check_instance(instance_id, expected_instance)

    plans = _require_block(dataset, "execution_plans")
    plan = plans.get(sql_fingerprint)
    if plan is None:
        raise ToolExecutionError(
            f"模拟数据中没有指纹 {sql_fingerprint} 的执行计划。"
            "这不代表真实实例没有执行计划，请检查指纹或重新采集。"
        )

    payload = {
        "mock": True,
        **plan,
        "note": "执行计划来自模拟 EXPLAIN，只用于验证流程，不代表真实执行计划。",
    }
    plan_rows = plan.get("plan", [])
    first_row = plan_rows[0] if plan_rows else {}
    details = {
        "mock": True,
        "source": plan.get("source"),
        "sql_fingerprint": sql_fingerprint,
        "full_scan": any(row.get("type") == "ALL" for row in plan_rows),
        "access_type": first_row.get("type"),
        # key 为 None 表示没命中索引；保留 None，让 Topic 能区分“没走索引”和“空字符串”。
        "key": first_row.get("key"),
        "estimated_rows": plan.get("estimated_rows"),
        "actual_rows_sent": plan.get("actual_rows_sent"),
        "extra": plan.get("extra", []),
    }
    return _success(payload, details)


def instance_metrics(
    dataset: dict[str, Any],
    *,
    instance_id: str,
    start_time: str,
    end_time: str,
) -> ToolOutcome:
    """查询实例监控。

    当前模拟环境**故意返回超时错误**，用于验证：
    - MCP 的 `isError=true` 能被客户端正确映射；
    - 模型必须把失败写成“未验证”，不能写成“监控正常”；
    - 失败结果仍然会进入证据目录，方便事后排查。

    真实实现里，这个函数会换成监控系统的 HTTP 查询；接口形状不变。
    """
    del start_time, end_time  # 模拟数据不按时间过滤，但参数保留，保持真实接口形状
    expected_instance = str(dataset["instance_id"])
    _check_instance(instance_id, expected_instance)
    return _failure(
        "实例监控接口超时（模拟）：等待 5 秒仍未返回，"
        "CPU、IO 等监控数据未获取到，不能据此判断实例负载是否正常。"
    )


def lock_wait_query(
    dataset: dict[str, Any],
    *,
    instance_id: str,
    start_time: str,
    end_time: str,
) -> ToolOutcome:
    """查询锁等待样本。

    返回 `status=no_data` 表示采样窗口内没有样本，**不等于**实例一定没有锁等待。
    报告里必须保留这个限制。
    """
    expected_instance = str(dataset["instance_id"])
    _check_instance(instance_id, expected_instance)
    lock_wait_data = _require_block(dataset, "lock_wait")
    payload = {
        "mock": True,
        "instance_id": expected_instance,
        "requested_range": {"start": start_time, "end": end_time},
        "status": "no_data",
        "samples": [],
        "source": lock_wait_data["source"],
        "coverage": lock_wait_data["coverage"],
        "note": lock_wait_data["note"],
    }
    details = {
        "mock": True,
        "source": lock_wait_data["source"],
        "status": "no_data",
        "sample_count": 0,
    }
    return _success(payload, details)


# ---------------------------------------------------------------------------
# 多数据库场景工具：一套“数据驱动”的只读工具
# ---------------------------------------------------------------------------
#
# 慢 SQL 的四个工具是“一个工具一段专门逻辑”的写法，适合讲清楚业务计算。
# 但十个场景如果每个工具都复制一遍读取 JSON、校验实例、构造结果的代码，
# 文件会变得很难维护。这里换一种更通用的做法：
#
#   dataset["tool_results"]["pg_table_stats"] = {
#       "payload": {...给模型看的完整结果...},
#       "details": {
#           "summary": "...给报告调查过程用的一句话...",
#           "topic": {"facts": [...], "candidates": [...]}
#       }
#   }
#
# 每个工具仍然有独立的 MCP 名称、标题、说明和 JSON Schema，模型看到的是
# “PostgreSQL 表统计”“Redis 热 Key 统计”这样的真实工具；只有执行层共用
# 同一个 `scenario_tool()`。这样既保留工具接口的真实性，也避免复制粘贴。
#
# 数据仍然是假的，但工具发现、参数校验、JSON-RPC 调用、错误映射和证据落盘
# 全部走真实链路，和慢 SQL 工具完全一样。


def _scenario_tool_definition(
    name: str,
    label: str,
    description: str,
    *,
    with_time_range: bool = False,
    properties: dict[str, Any] | None = None,
    required: tuple[str, ...] = (),
) -> BusinessToolDefinition:
    """创建一条多场景工具声明。

    参数说明：
    - `with_time_range=True`：自动加入 `start_time` / `end_time` 两个必填参数；
    - `properties`：该工具特有的参数，例如 Redis 的 `limit`、MongoDB 的 `collection`；
    - `required`：特有参数里哪些是必填。

    例子：
        `_scenario_tool_definition("redis_slowlog", ..., properties={"limit": ...})`
        最终 Schema 会同时包含 `instance_id` 和 `limit`。

    为什么统一加 `additionalProperties=False`：
    模型多传一个拼错的参数时，FastMCP 会直接拒绝，而不是悄悄忽略。
    排查“为什么参数没生效”时，这个约束能省很多时间。
    """
    all_properties: dict[str, Any] = {
        "instance_id": {"type": "string", "description": "实例 ID"},
    }
    required_names = ["instance_id"]
    if with_time_range:
        all_properties["start_time"] = {
            "type": "string",
            "description": "开始时间，ISO 8601 格式",
        }
        all_properties["end_time"] = {
            "type": "string",
            "description": "结束时间，ISO 8601 格式",
        }
        required_names.extend(["start_time", "end_time"])
    if properties:
        all_properties.update(properties)
    required_names.extend(required)

    return BusinessToolDefinition(
        name=name,
        label=label,
        description=description,
        parameters={
            "type": "object",
            "properties": all_properties,
            "required": required_names,
            "additionalProperties": False,
        },
    )


# 16 个多数据库只读工具。名称尽量贴近真实 DBA 会用的接口：
# - MySQL：InnoDB 状态、进程列表；
# - PostgreSQL：活动会话、表统计、锁；
# - Redis：INFO、SLOWLOG、Key 统计；
# - MongoDB：profiler、explain、副本集状态、serverStatus；
# - Elasticsearch：集群健康、分片分配解释；
# - ClickHouse：query_log、system.parts。
SCENARIO_TOOL_DEFINITIONS: tuple[BusinessToolDefinition, ...] = (
    _scenario_tool_definition(
        "mysql_innodb_status",
        "InnoDB 状态",
        (
            "查询 MySQL InnoDB 引擎状态快照，包含活跃事务、行锁等待、阻塞事务、"
            "等待时长和 history list 长度。用于排查行锁等待链、长事务和事务积压。只读。"
        ),
        with_time_range=True,
    ),
    _scenario_tool_definition(
        "mysql_processlist",
        "MySQL 会话列表",
        (
            "查询 MySQL 当前会话，包含用户、来源地址、执行时长、状态和正在执行的 SQL，"
            "可按执行时长过滤。用于确认阻塞事务来自哪个应用以及排队会话数量。只读。"
        ),
        properties={
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 20,
                "description": "最多返回多少个会话，默认 20",
            }
        },
    ),
    _scenario_tool_definition(
        "pg_activity",
        "PostgreSQL 活动会话",
        (
            "查询 PostgreSQL pg_stat_activity 摘要，包含长事务、idle in transaction、"
            "事务年龄 xmin age、当前 SQL 和等待事件。用于排查长事务阻塞 autovacuum "
            "或 DDL 锁等待。只读。"
        ),
        with_time_range=True,
        properties={
            "min_duration_s": {
                "type": "number",
                "minimum": 0,
                "default": 1,
                "description": "只返回持续至少多少秒的会话，默认 1 秒",
            }
        },
    ),
    _scenario_tool_definition(
        "pg_table_stats",
        "PostgreSQL 表统计",
        (
            "查询 PostgreSQL 表的活元组、死元组、膨胀比例、最近 VACUUM/autovacuum "
            "时间和索引膨胀信息。用于判断表膨胀和 autovacuum 是否被阻塞。只读。"
        ),
        properties={
            "schema": {
                "type": "string",
                "default": "public",
                "description": "schema 名称，默认 public",
            },
            "table": {
                "type": "string",
                "default": "",
                "description": "表名；不传时返回该 schema 下死元组最多的表",
            },
        },
    ),
    _scenario_tool_definition(
        "pg_locks",
        "PostgreSQL 锁等待",
        (
            "查询 PostgreSQL pg_locks 和 pg_blocking_pids，返回锁类型、是否已授予、"
            "阻塞会话和等待队列。用于排查 DDL 被长事务阻塞、以及后续查询排队。只读。"
        ),
        with_time_range=True,
    ),
    _scenario_tool_definition(
        "redis_info",
        "Redis INFO",
        (
            "查询 Redis INFO 摘要，包含内存使用、maxmemory、淘汰策略、已淘汰 key 数、"
            "命中率、CPU、连接数、op/s 和慢日志计数。用于排查内存淘汰和单分片热点。只读。"
        ),
        properties={
            "section": {
                "type": "string",
                "default": "all",
                "description": "INFO 分区，例如 memory、stats、cpu；默认 all",
            }
        },
    ),
    _scenario_tool_definition(
        "redis_slowlog",
        "Redis SLOWLOG",
        (
            "查询 Redis SLOWLOG，返回慢命令、耗时、客户端地址和执行时间。"
            "用于定位热 Key、慢命令和单分片延迟。只读。"
        ),
        properties={
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "default": 10,
                "description": "最多返回多少条慢日志，默认 10",
            }
        },
    ),
    _scenario_tool_definition(
        "redis_key_stats",
        "Redis Key 统计",
        (
            "按访问次数或内存占用统计 Redis Key，返回 Key 名称、QPS、value 大小、"
            "TTL 和所在分片。用于识别热 Key、大 Key 和无过期时间的 Key，只读，不遍历全库。"
        ),
        properties={
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "default": 10,
                "description": "最多返回多少个 Key，默认 10",
            }
        },
    ),
    _scenario_tool_definition(
        "mongo_profiler",
        "MongoDB Profiler",
        (
            "查询 MongoDB profiler 里的慢操作，包含命名空间、查询条件、planSummary、"
            "扫描文档数、返回文档数、执行次数和耗时。用于发现 COLLSCAN 和高频慢查询。只读。"
        ),
        with_time_range=True,
        properties={
            "min_duration_ms": {
                "type": "integer",
                "minimum": 0,
                "default": 100,
                "description": "只返回耗时至少多少毫秒的操作，默认 100",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "default": 10,
                "description": "最多返回多少条慢操作，默认 10",
            },
        },
    ),
    _scenario_tool_definition(
        "mongo_explain",
        "MongoDB 执行计划",
        (
            "对 MongoDB 查询执行 explain，返回 winningPlan、COLLSCAN/IXSCAN、"
            "totalDocsExamined、nReturned 和可用索引。用于确认缺少索引或索引失效。只读。"
        ),
        properties={
            "database": {"type": "string", "description": "数据库名，例如 commerce"},
            "collection": {"type": "string", "description": "集合名，例如 orders"},
            "filter_json": {
                "type": "string",
                "default": "{}",
                "description": "查询过滤条件 JSON 字符串，例如 {\"status\":\"PAID\"}",
            },
        },
        required=("database", "collection"),
    ),
    _scenario_tool_definition(
        "mongo_rs_status",
        "MongoDB 副本集状态",
        (
            "查询 MongoDB replSetGetStatus，返回节点角色、健康状态、oplog 时间差、"
            "复制延迟和主节点写入位置。用于排查副本集复制延迟和只读节点数据陈旧。只读。"
        ),
    ),
    _scenario_tool_definition(
        "mongo_server_status",
        "MongoDB Server Status",
        (
            "查询 MongoDB serverStatus，包含 CPU、连接数、WiredTiger cache、磁盘 IO、"
            "读写队列和网络统计。用于判断资源瓶颈是否与慢查询或复制延迟相关。只读。"
        ),
    ),
    _scenario_tool_definition(
        "es_cluster_health",
        "Elasticsearch 集群健康",
        (
            "查询 Elasticsearch cluster health 和索引状态，返回 green/yellow/red、"
            "活跃主分片、副本分片和未分配分片数量。用于排查集群 yellow/red。只读。"
        ),
        with_time_range=True,
    ),
    _scenario_tool_definition(
        "es_shard_allocation",
        "Elasticsearch 分片分配",
        (
            "调用 Elasticsearch cluster allocation explain，返回分片未分配原因、"
            "节点磁盘使用率、watermark 和分配决策。用于确认磁盘水位或节点离线。只读。"
        ),
    ),
    _scenario_tool_definition(
        "clickhouse_query_log",
        "ClickHouse 查询日志",
        (
            "查询 ClickHouse system.query_log，返回失败 INSERT/SELECT、异常码、"
            "单批写入行数、执行次数和耗时。用于排查 Too many parts 和小批量写入。只读。"
        ),
        with_time_range=True,
        properties={
            "min_duration_ms": {
                "type": "integer",
                "minimum": 0,
                "default": 1000,
                "description": "只返回耗时至少多少毫秒的查询，默认 1000",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "default": 10,
                "description": "最多返回多少条记录，默认 10",
            },
        },
    ),
    _scenario_tool_definition(
        "clickhouse_parts",
        "ClickHouse Parts 统计",
        (
            "查询 ClickHouse system.parts 和 system.merges，返回活跃 part 数、"
            "分区分布、merge 吞吐和积压。用于判断是否因小批量写入触发 Too many parts。只读。"
        ),
        properties={
            "database": {
                "type": "string",
                "default": "",
                "description": "数据库名；不传时返回当前数据集中指定的库",
            },
            "table": {
                "type": "string",
                "default": "",
                "description": "表名；不传时返回当前数据集中指定的表",
            },
        },
    ),
)


def scenario_tool(
    dataset: dict[str, Any],
    tool_name: str,
    arguments: dict[str, Any],
) -> ToolOutcome:
    """执行一个数据驱动的场景工具。

    执行顺序：
    1. 校验实例 ID，防止模型把 A 实例的查询发到 B 实例；
    2. 在 `dataset["tool_results"]` 里找这个工具的结果；
    3. 如果数据集明确标记 `is_error=True`，返回业务失败；
    4. 否则把 `payload` 给模型，把 `details` 留给平台（Topic、证据、前端）。

    例子：
        PostgreSQL 场景的 `pg_table_stats` 结果里可能有：
        `dead_ratio=27.5%`、`last_autovacuum=null`。
        这些字段放在 `details.topic.facts` 里，报告生成器不需要解析长文本，
        直接按 key 就能引用。

    为什么找不到结果时返回 `is_error=True` 而不是抛 `KeyError`：
    模型可能搜索到了别的数据库工具并尝试调用。让它看到“当前数据集不支持这个工具”，
    比整次 MCP 调用变成协议错误更容易恢复。
    """
    expected_instance = str(dataset.get("instance_id") or "")
    requested_instance = str(arguments.get("instance_id") or "")
    if expected_instance:
        _check_instance(requested_instance, expected_instance)

    tool_results = dataset.get("tool_results")
    if not isinstance(tool_results, dict):
        return _failure(
            f"当前模拟数据没有 `tool_results`，无法执行 `{tool_name}`。"
            "这不代表真实实例没有数据，请确认当前场景的数据集。"
        )

    result = tool_results.get(tool_name)
    if not isinstance(result, dict):
        available = "、".join(sorted(tool_results)) or "无"
        return _failure(
            f"当前模拟数据集不包含工具 `{tool_name}`。可用工具：{available}。"
            "请根据当前数据库类型重新搜索并选择工具。"
        )

    details = result.get("details") if isinstance(result.get("details"), dict) else {}
    if result.get("is_error"):
        message = str(result.get("message") or f"{tool_name} 执行失败（模拟）")
        return _failure(message, dict(details))

    payload = result.get("payload")
    if not isinstance(payload, dict):
        return _failure(
            f"工具 `{tool_name}` 的模拟结果缺少 payload 对象，无法展示结构化结果。",
            dict(details),
        )
    return _success(dict(payload), dict(details))


# 四个慢 SQL 工具保留原来的专门实现；多数据库场景工具共用 `scenario_tool()`。
# 两类合并后，能力目录和工具搜索能一次看到全部业务工具。
LEGACY_BUSINESS_TOOL_DEFINITIONS: tuple[BusinessToolDefinition, ...] = (
    BusinessToolDefinition(
        name="slow_log_query",
        label="慢日志查询",
        description=(
            "查询指定实例和时间范围内的慢日志摘要，按平均耗时从高到低返回。"
            "返回 SQL 指纹、样例 SQL、执行次数、平均/最大耗时、扫描行数和返回行数。"
            "只读，不会修改实例。"
        ),
        parameters={
            "type": "object",
            "properties": {
                "instance_id": {"type": "string", "description": "实例 ID"},
                "start_time": {
                    "type": "string",
                    "description": "开始时间，ISO 8601 格式，例如 2026-10-29T10:00:00+08:00",
                },
                "end_time": {
                    "type": "string",
                    "description": "结束时间，ISO 8601 格式，例如 2026-10-29T11:00:00+08:00",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 50,
                    "description": "最多返回多少条慢日志，默认 5",
                },
            },
            "required": ["instance_id", "start_time", "end_time"],
            "additionalProperties": False,
        },
    ),
    BusinessToolDefinition(
        name="execution_plan",
        label="执行计划查询",
        description=(
            "按 SQL 指纹查询模拟执行计划，返回访问类型、命中索引、估算扫描行数、"
            "过滤比例和 Extra 信息。用于判断是否全表扫描或额外排序。只读。"
        ),
        parameters={
            "type": "object",
            "properties": {
                "instance_id": {"type": "string", "description": "实例 ID"},
                "sql_fingerprint": {
                    "type": "string",
                    "description": "慢日志里返回的 SQL 指纹，例如 a1b2c3d4e5f6",
                },
            },
            "required": ["instance_id", "sql_fingerprint"],
            "additionalProperties": False,
        },
    ),
    BusinessToolDefinition(
        name="instance_metrics",
        label="实例监控查询",
        description=(
            "查询实例在指定时间范围内的 CPU、IO 等监控指标。"
            "注意：本轮模拟环境故意让这个接口超时，用于验证失败场景。只读。"
        ),
        parameters={
            "type": "object",
            "properties": {
                "instance_id": {"type": "string", "description": "实例 ID"},
                "start_time": {"type": "string", "description": "开始时间，ISO 8601"},
                "end_time": {"type": "string", "description": "结束时间，ISO 8601"},
            },
            "required": ["instance_id", "start_time", "end_time"],
            "additionalProperties": False,
        },
    ),
    BusinessToolDefinition(
        name="lock_wait_query",
        label="锁等待查询",
        description=(
            "查询指定时间范围内的锁等待样本。返回 no_data 表示采样窗口内没有样本，"
            "不等于实例一定没有锁等待。只读。"
        ),
        parameters={
            "type": "object",
            "properties": {
                "instance_id": {"type": "string", "description": "实例 ID"},
                "start_time": {"type": "string", "description": "开始时间，ISO 8601"},
                "end_time": {"type": "string", "description": "结束时间，ISO 8601"},
            },
            "required": ["instance_id", "start_time", "end_time"],
            "additionalProperties": False,
        },
    ),
)

# 对外统一的业务工具目录 = 慢 SQL 专用工具 + 多数据库场景工具。
# 能力目录会展示全部 20 个工具；真正运行时 MCP Server 只注册当前数据集
# 能回答的工具，所以模型不会在 PostgreSQL 工单里看到 MySQL 慢日志工具。
BUSINESS_TOOL_DEFINITIONS: tuple[BusinessToolDefinition, ...] = (
    *LEGACY_BUSINESS_TOOL_DEFINITIONS,
    *SCENARIO_TOOL_DEFINITIONS,
)

BUSINESS_TOOL_NAMES = frozenset(item.name for item in BUSINESS_TOOL_DEFINITIONS)


def business_tool_declarations() -> list[dict[str, Any]]:
    """返回能力目录里展示的业务工具声明。

    这里只读常量，不启动 MCP 进程，也不读数据文件。
    例子：`/api/capabilities` 和 CLI `list-tools` 都调用这个函数。
    """
    return [
        {
            "name": item.name,
            "label": item.label,
            "description": item.description,
            "parameters": item.parameters,
            "annotations": dict(READ_ONLY_ANNOTATIONS),
        }
        for item in BUSINESS_TOOL_DEFINITIONS
    ]


async def _not_executable(
    tool_call_id: str,
    arguments: dict[str, Any],
    cancellation: CancellationToken,
) -> ToolResult:
    """静态声明的占位处理器。

    `ToolSession.create` 需要 `ToolSpec.handler`，但能力目录和工具搜索只关心
    声明，不应该真的执行工具。如果哪天有人误用了静态声明去执行，这里会明确报错，
    而不是返回一份看似正常的数据。
    """
    del tool_call_id, arguments, cancellation
    raise ToolExecutionError(
        "这是静态工具声明，不能直接执行；请通过 MCP 客户端调用真实 MCP Server。"
    )


def static_tool_specs() -> list[ToolSpec]:
    """把静态声明转成 `ToolSpec`，供工具搜索和可见性测试使用。

    真正的 Agent 运行不会用这份列表：`AgentRunner` 会连接 MCP Server，
    用 `tools/list` 返回的真实工具定义来构造 `ToolSpec`。
    """
    return [
        ToolSpec(
            name=item.name,
            label=item.label,
            description=item.description,
            parameters=item.parameters,
            handler=_not_executable,
            annotations=dict(READ_ONLY_ANNOTATIONS),
        )
        for item in BUSINESS_TOOL_DEFINITIONS
    ]
