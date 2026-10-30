r"""关键词工具搜索与按需加载。

## 为什么不让模型一开始就看到所有工具

工具声明要占用上下文，而且工具越多，模型越容易“挑错工具”。
这一层先只开放 Skill 声明的一两个业务工具，模型需要时调用平台工具
`search_tools`，命中后把新工具加入后续轮次的请求。

## 搜索规则

这是**简单关键词搜索**，不是向量检索：
- 工具名、标签、描述、参数名都参与匹配；
- 中文查询通过一张别名表映射到英文关键词，例如“执行计划”会匹配
  `execution_plan` 的描述里的 `execution`、`plan`、`explain`；
- 命中分数高者优先，同分按工具名排序；
- 搜索只改变“后续请求能看到哪些工具”，**不授予任何权限**。
  没被加载的工具，执行阶段也查不到，模型猜名字也没用。

## 实现说明

- `re.split(r"[\s,，、;；]+", text)`：按空格和中英文标点切词。
  例子：`"执行计划, 锁等待"` 会切成 `["执行计划", "锁等待"]`。
- `frozenset({...})` 是“不可变集合”，适合放常量；成员判断是 O(1)。
- `sorted(..., key=lambda item: (-item[1], item[0].name))`：
  先按分数从高到低（所以取负号），分数相同再按名字升序。
- `set` 的 `|` 是并集，`update` 是原地并入。
- `@classmethod` 的第一个参数是类本身（习惯写 `cls`），用来提供“另一种创建对象的方式”。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from app.harness.evidence import DEFAULT_READ_LIMIT, EvidenceNotFound, EvidenceStore
from app.harness.tools import ToolExecutionError
from app.runtime.contracts import CancellationToken, TextContent, ToolResult, ToolSpec

# 平台工具：所有 Skill 都自动可用，不参与业务工具搜索，也不保存证据。
# 为什么用 frozenset：它是不可变集合，适合做常量，避免被调用方误改。
PLATFORM_TOOL_NAMES = frozenset({"search_tools", "evidence_read"})

# 中文别名表：键是模型/用户可能说的中文，值是对应的英文关键词。
# 例子：搜“执行计划”时，会把 execution、plan、explain 当作加分词，
# 这样即使工具名是英文 execution_plan，也能被中文查询命中。
_ALIASES: dict[str, tuple[str, ...]] = {
    "慢日志": ("slow", "log"),
    "慢查询": ("slow", "log"),
    "执行计划": ("execution", "plan", "explain"),
    "访问路径": ("plan", "index", "key"),
    "索引": ("index", "key", "plan"),
    "监控": ("metrics", "monitor", "cpu", "io"),
    "指标": ("metrics", "monitor"),
    "资源": ("metrics", "cpu", "io"),
    "锁等待": ("lock", "wait"),
    "锁": ("lock", "wait"),
    "等待事件": ("lock", "wait", "event"),
    "证据": ("evidence", "read"),
    "读取": ("evidence", "read"),
    "搜索": ("search", "tools"),
    # 多数据库场景的关键词。模型或用户说中文时，能命中英文工具名/描述。
    # 例子：搜“表膨胀”→ 展开 vacuum/table/dead，命中 pg_table_stats。
    "mysql": ("mysql", "innodb", "processlist"),
    "innodb": ("mysql", "innodb", "transaction", "lock"),
    "事务": ("transaction", "trx", "innodb"),
    "会话": ("processlist", "session", "activity"),
    "postgres": ("postgres", "pg", "activity", "table"),
    "postgresql": ("postgres", "pg", "activity", "table"),
    "膨胀": ("bloat", "vacuum", "dead", "table"),
    "死元组": ("dead", "tuple", "vacuum"),
    "表统计": ("table", "stats", "vacuum"),
    "活动会话": ("activity", "session", "transaction"),
    "ddl": ("ddl", "lock", "table"),
    "表锁": ("lock", "table", "ddl"),
    "redis": ("redis", "key", "memory", "slowlog"),
    "热key": ("redis", "hot", "key"),
    "热 key": ("redis", "hot", "key"),
    "大key": ("redis", "big", "key", "memory"),
    "大 key": ("redis", "big", "key", "memory"),
    "内存": ("memory", "evict", "redis"),
    "淘汰": ("evict", "memory", "policy"),
    "命中率": ("hit", "rate", "stats"),
    "mongodb": ("mongo", "profiler", "explain", "replication"),
    "mongo": ("mongo", "profiler", "explain", "replication"),
    "集合": ("collection", "mongo", "explain"),
    "全表扫描": ("collscan", "scan", "explain"),
    "副本集": ("replica", "replication", "rs", "lag"),
    "复制延迟": ("replication", "lag", "oplog"),
    "elasticsearch": ("elasticsearch", "es", "cluster", "shard"),
    "集群": ("cluster", "health", "shard"),
    "分片": ("shard", "allocation", "cluster"),
    "磁盘": ("disk", "watermark", "allocation"),
    "水位": ("watermark", "disk", "allocation"),
    "clickhouse": ("clickhouse", "parts", "query"),
    "part": ("parts", "merge", "clickhouse"),
    "parts": ("parts", "merge", "clickhouse"),
    "小批量": ("insert", "parts", "batch"),
    "合并": ("merge", "parts", "clickhouse"),
}

# 平台工具的 JSON Schema 单独放常量，CLI 的 `list-tools` 和能力目录可以直接复用，
# 不需要为了“展示”而创建一个证据目录。
SEARCH_TOOLS_PARAMETERS: dict[str, Any] = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": "关键词，例如：执行计划、实例监控、锁等待",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 10,
            "description": "最多返回多少个匹配工具，默认 5",
        },
    },
    "required": ["query"],
    "additionalProperties": False,
}

EVIDENCE_READ_PARAMETERS: dict[str, Any] = {
    "type": "object",
    "properties": {
        "evidence_id": {
            "type": "string",
            "pattern": r"^ev-\d{6}$",
            "description": "证据编号，例如 ev-000001",
        },
        "offset": {
            "type": "integer",
            "minimum": 0,
            "description": "从第几个字符开始读，默认 0",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 8000,
            "description": "最多读多少字符，默认 4000",
        },
    },
    "required": ["evidence_id"],
    "additionalProperties": False,
}

PLATFORM_TOOL_DECLARATIONS: list[dict[str, Any]] = [
    {
        "name": "search_tools",
        "label": "工具搜索",
        "description": (
            "按关键词搜索当前 Skill 可用的业务工具。命中后工具会加入后续轮次，"
            "搜索本身不授予权限；没有加载的工具无法执行。"
        ),
        "parameters": SEARCH_TOOLS_PARAMETERS,
    },
    {
        "name": "evidence_read",
        "label": "证据读取",
        "description": (
            "按证据编号分段读取工具原始结果。工具结果被截断时，"
            "用 offset/limit 读取完整内容；每次最多 8000 字符。"
        ),
        "parameters": EVIDENCE_READ_PARAMETERS,
    },
]


def _tokenize(query: str) -> list[str]:
    """把查询切成关键词，并统一转小写。

    例子：
        `"执行计划, 锁等待"` -> `["执行计划", "锁等待"]`
        `"Slow  Log"` -> `["slow", "log"]`
    """
    return [part for part in re.split(r"[\s,，、;；]+", query.lower()) if part]


def _alias_terms(query: str) -> set[str]:
    """找出查询里出现的中文别名，展开成对应的英文关键词。

    例子：查询包含“执行计划”，返回 `{"execution", "plan", "explain"}`。
    为什么先看“整个查询是否包含别名”，而不是只按切词匹配：
    “查一下执行计划”这类自然语言不会被空格切开，直接做子串判断更稳。
    """
    lowered = query.lower()
    terms: set[str] = set()
    for alias, expansions in _ALIASES.items():
        if alias in lowered:
            terms.update(expansions)
    return terms


def _tool_fields(tool: ToolSpec) -> tuple[str, str, str, str]:
    """取出参与搜索的四个文本字段，并统一转小写。

    返回顺序：名称、标签、描述、参数名。
    分开保留是为了按字段给不同权重：命中工具名比命中描述更有说服力。
    """
    name = tool.name.lower()
    label = tool.label.lower()
    description = tool.description.lower()
    parameter_names = " ".join(tool.parameters.get("properties", {}).keys()).lower()
    return name, label, description, parameter_names


def search_tool_specs(
    tools: list[ToolSpec],
    query: str,
    limit: int = 5,
) -> list[tuple[ToolSpec, int]]:
    """按关键词给工具打分，返回 `(工具, 分数)` 列表。

    打分规则（分数只是相对值，不需要精确）：
    - 查询整体等于工具名/标签：+20；
    - 查询里包含工具名/标签：+10；
    - 每个查询词命中名称 +6、标签 +4、描述 +2、参数名 +1；
    - 每个中文别名展开词按同样规则加分。

    例子：查询“执行计划”，`execution_plan` 的描述里有 `execution plan`，
    会拿到较高分；`slow_log_query` 的描述里没有这些词，分数为 0，不返回。
    """
    query_text = query.strip()
    if not query_text:
        return []

    query_lower = query_text.lower()
    tokens = _tokenize(query_lower)
    alias_terms = _alias_terms(query_lower)
    # 两个集合合并后去重；如果查询既没有词也没有别名，直接返回空。
    candidate_terms = set(tokens) | alias_terms
    if not candidate_terms:
        return []

    scored: list[tuple[ToolSpec, int]] = []
    for tool in tools:
        name, label, description, parameter_names = _tool_fields(tool)
        score = 0

        if query_lower in (name, label):
            score += 20
        elif name in query_lower or label in query_lower:
            score += 10

        for term in candidate_terms:
            if term in name:
                score += 6
            if term in label:
                score += 4
            if term in description:
                score += 2
            if term in parameter_names:
                score += 1

        if score > 0:
            scored.append((tool, score))

    # 先按分数降序，再按名字升序，保证同样的查询每次返回顺序一致。
    scored.sort(key=lambda item: (-item[1], item[0].name))
    return scored[: max(1, int(limit))]


@dataclass(slots=True)
class ToolSession:
    """一次运行里的工具可见状态。

    分成三个字典是为了把“有什么”和“现在能用什么”分开：
    - `business_tools`：本次运行注册的全部业务工具；
    - `platform_tools`：搜索、证据读取这类平台工具，始终可见；
    - `visible`：当前真正发给模型的工具，初始只有 Skill 声明的业务工具 + 平台工具。

    `searches` 记录每次搜索，写进 `run.json`，方便事后回答
    “模型为什么突然用起了 execution_plan”。
    """

    business_tools: dict[str, ToolSpec]
    platform_tools: dict[str, ToolSpec]
    visible: dict[str, ToolSpec]
    initial_names: list[str] = field(default_factory=list)
    loaded_by_search: list[str] = field(default_factory=list)
    searches: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def create(
        cls,
        business_tools: list[ToolSpec],
        initial_tool_names: list[str],
    ) -> ToolSession:
        """按 Skill 的初始工具清单创建会话。

        例子：Skill 声明 `initial_tools=["slow_log_query"]`，
        模型第一轮只能看到 slow_log_query；执行计划、监控、锁等待要等搜索后加载。
        """
        business: dict[str, ToolSpec] = {}
        for tool in business_tools:
            if tool.name in business:
                raise ValueError(f"工具名重复：{tool.name}")
            business[tool.name] = tool

        unknown = [name for name in initial_tool_names if name not in business]
        if unknown:
            raise ValueError(f"Skill 声明的初始工具不存在：{unknown}")

        visible = {name: business[name] for name in initial_tool_names}
        return cls(
            business_tools=business,
            platform_tools={},
            visible=visible,
            initial_names=list(initial_tool_names),
        )

    def add_platform_tools(self, tools: list[ToolSpec]) -> None:
        """挂上平台工具，并让它们始终可见。

        为什么平台工具不通过搜索加载：
        `search_tools` 如果自己也需要搜索才能用，模型就卡死了；
        `evidence_read` 在第一条大结果出现时就可能要用，也必须一开始就可见。
        """
        for tool in tools:
            self.platform_tools[tool.name] = tool
            self.visible[tool.name] = tool

    def search(self, query: str, limit: int = 5) -> dict[str, Any]:
        """搜索业务工具，并把命中项加入可见集合。

        返回给模型的内容刻意保持小：只给名称、标签、描述和是否已加载，
        不重复发完整参数 Schema。下一轮 `tools_provider` 会把完整声明放进请求。
        """
        matches = search_tool_specs(list(self.business_tools.values()), query, limit)
        match_payload: list[dict[str, Any]] = []
        newly_loaded: list[str] = []

        for tool, score in matches:
            already_loaded = tool.name in self.visible
            if not already_loaded:
                self.visible[tool.name] = tool
                self.loaded_by_search.append(tool.name)
                newly_loaded.append(tool.name)
            match_payload.append(
                {
                    "name": tool.name,
                    "label": tool.label,
                    "description": tool.description,
                    "score": score,
                    "already_loaded": already_loaded,
                }
            )

        self.searches.append(
            {
                "query": query,
                "matches": [item["name"] for item in match_payload],
                "loaded": list(newly_loaded),
            }
        )
        return {
            "query": query,
            "matches": match_payload,
            "loaded": newly_loaded,
            "visible_tools": self.visible_names(),
            "note": (
                "搜索只影响后续轮次的工具声明，不授予权限；"
                "没有加载的工具无法执行。"
            ),
        }

    def visible_specs(self) -> list[ToolSpec]:
        """返回当前应发给模型的工具列表。

        这个方法会作为 `AgentLoopConfig.tools_provider` 传入内核。
        每次模型请求前都会重新调用，所以搜索结果能在下一轮立刻生效。
        """
        return list(self.visible.values())

    def visible_names(self) -> list[str]:
        """返回当前可见的工具名，写运行记录和 CLI 输出时用。"""
        return list(self.visible.keys())

    def business_visible_names(self) -> list[str]:
        """只返回当前可见的业务工具名，不包含平台工具。"""
        return [name for name in self.visible if name in self.business_tools]


def build_platform_tools(session: ToolSession, store: EvidenceStore) -> list[ToolSpec]:
    """创建 `search_tools` 和 `evidence_read` 两个平台工具。

    参数：
    - `session`：工具可见状态，`search_tools` 会修改它；
    - `store`：证据存储，`evidence_read` 按编号和偏移读取原始结果。
    """

    async def search_tools(
        tool_call_id: str,
        arguments: dict[str, Any],
        cancellation: CancellationToken,
    ) -> ToolResult:
        """搜索业务工具并加载命中项。"""
        del tool_call_id  # 平台工具不需要调用 id，但处理器接口要求保留
        if cancellation.cancelled:
            raise ToolExecutionError("操作已取消")

        query = str(arguments.get("query", "")).strip()
        if not query:
            raise ToolExecutionError("search_tools 的 query 不能为空")
        limit = int(arguments.get("limit", 5))
        payload = session.search(query, limit)
        return ToolResult(
            content=[TextContent(text=json.dumps(payload, ensure_ascii=False, indent=2))],
            details={
                "query": query,
                "match_count": len(payload["matches"]),
                "loaded": payload["loaded"],
            },
        )

    async def evidence_read(
        tool_call_id: str,
        arguments: dict[str, Any],
        cancellation: CancellationToken,
    ) -> ToolResult:
        """按编号和字符偏移读取证据片段。"""
        del tool_call_id
        if cancellation.cancelled:
            raise ToolExecutionError("操作已取消")

        evidence_id = str(arguments.get("evidence_id", ""))
        offset = int(arguments.get("offset", 0))
        limit = int(arguments.get("limit", DEFAULT_READ_LIMIT))
        try:
            payload = store.read(evidence_id, offset=offset, limit=limit)
        except EvidenceNotFound as exc:
            # 证据不存在是工具执行失败的一种，转成内核认识的错误结果，
            # 让模型看到明确原因，而不是让任务直接崩掉。
            raise ToolExecutionError(str(exc)) from exc

        return ToolResult(
            content=[TextContent(text=json.dumps(payload, ensure_ascii=False, indent=2))],
            details={
                "evidence_id": evidence_id,
                "offset": payload["offset"],
                "limit": payload["limit"],
                "total_chars": payload["total_chars"],
                "truncated": payload["truncated"],
            },
        )

    return [
        ToolSpec(
            name="search_tools",
            label="工具搜索",
            description=PLATFORM_TOOL_DECLARATIONS[0]["description"],
            parameters=SEARCH_TOOLS_PARAMETERS,
            handler=search_tools,
        ),
        ToolSpec(
            name="evidence_read",
            label="证据读取",
            description=PLATFORM_TOOL_DECLARATIONS[1]["description"],
            parameters=EVIDENCE_READ_PARAMETERS,
            handler=evidence_read,
        ),
    ]
