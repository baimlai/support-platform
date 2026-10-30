"""真实的 MCP Server：把只读 DBA 工具暴露给 Agent。

工具分两组：

- 旧慢 SQL 数据：4 个固定工具，保持原有流程不变；
- 多数据库场景数据：根据数据集里的 `tool_results` 动态注册
  MySQL、PostgreSQL、Redis、MongoDB、Elasticsearch、ClickHouse 工具。

## 为什么是“真实 MCP”，不是本地函数调用

启动这个文件后，它就是一个独立的 MCP 服务进程。Agent 不再直接 import
`slow_log_query(...)` 然后执行，而是：

```text
Agent -> MCP Client -> JSON-RPC 2.0 -> MCP Server -> dba_tools.py -> fixtures
```

协议层使用的是官方 Python SDK（`mcp` / FastMCP），所以下面这些真实约定都由
SDK 负责处理，不需要我们自己拼 JSON：

- `initialize` 握手和协议版本协商；
- `notifications/initialized` 通知；
- `tools/list` 工具发现；
- `tools/call` 工具调用；
- `CallToolResult` 的 `content`、`structuredContent`、`isError`；
- `stdio`、`streamable-http`（HTTP + SSE）、`sse`（旧版 HTTP + SSE）三种传输。

## 工具结果怎么设计

每个工具返回 `CallToolResult`：

- `content` 放给模型看的文本。慢日志等成功结果是一段 JSON；
- `structuredContent` 放平台要用的结构化摘要：
  `{"payload": ..., "details": ..., "mock": true}`；
- 业务失败（监控超时、实例不存在、指纹查不到）返回 `isError=True`，
  而不是让整个 MCP 请求变成协议错误。这样模型还能看到失败原因，继续查其它分支。

## 运行方式

```bash
# stdio：由 Agent 子进程启动，也可以手动调试
uv run python -m app.mcp.server --transport stdio

# Streamable HTTP：POST /mcp，服务端用 SSE 推送事件
uv run python -m app.mcp.server --transport streamable-http --port 8765

# 旧版 HTTP + SSE：GET /sse 建立事件流，POST /messages/ 发消息
uv run python -m app.mcp.server --transport sse --port 8765
```

## 对比教学：Pi 只做客户端，我们为什么还要写 Server

Pi 的 `@earendil-works/pi-mcp` 是一个**客户端**：它连接别人已经写好的 MCP Server，
再把工具转成 Pi 的 `AgentTool`。Pi 不负责提供 DBA 工具服务。

本项目要演示完整的两端，所以：

- `server.py` 用官方 Python SDK 的 FastMCP 写真实 MCP Server；
- `client.py` 用官方 Python SDK 的 ClientSession 写真实 MCP Client；
- 两边通过 JSON-RPC 互通，工具仍然只读 fixtures 模拟数据。

这不矛盾：Pi 的定位是“宿主/客户端”，我们这里既扮演宿主，也提供一个本地工具服务。
如果以后 DBA 工具由别的团队提供，我们完全可以删掉自己的 Server，只保留 Client
去连他们的 Streamable HTTP 地址。

## Python 语法提示

- `Annotated[str, Field(description="...")]`：给参数加上说明和校验条件，
  FastMCP 会据此生成真实的 JSON Schema。
- `async def`：MCP 工具处理函数可以写成协程，适合以后接异步数据库/HTTP 客户端。
- `argparse`：标准库命令行解析器；`choices` 会在启动前挡住非法传输类型。
- `raise SystemExit(main())`：把返回值当进程退出码，方便脚本和 CI 判断。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field

from app.mcp import dba_tools
from app.mcp.scenario_registry import register_scenario_tools

# MCP Server 的身份信息。Client 在 `initialize` 响应里能看到它，
# 排查“到底连到了哪个服务”时很有用。
SERVER_NAME = "support-platform-dba-mcp"
SERVER_VERSION = "0.1.0"
SERVER_INSTRUCTIONS = (
    "这是 DBA 工单调查的只读工具服务。所有数据均来自 fixtures 模拟数据；"
    "工具不会连接生产实例，也不会执行任何写操作。"
)

# CLI 和环境变量共用的传输类型。顺序也是帮助信息里的展示顺序。
TRANSPORTS = ("stdio", "streamable-http", "sse")


def _annotations() -> ToolAnnotations:
    """创建 MCP 工具注解。

    `ToolAnnotations` 是 MCP 规范里的元信息，客户端可以据此决定是否弹确认框、
    是否允许自动执行。这里四个工具都是只读查询，所以：
    - `readOnlyHint=True`：不修改数据；
    - `destructiveHint=False`：不会破坏数据；
    - `idempotentHint=True`：同样参数重复调用结果一致；
    - `openWorldHint=False`：只访问本地模拟数据，不访问外部开放世界。
    """
    return ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )


def _to_call_tool_result(outcome: dba_tools.ToolOutcome) -> CallToolResult:
    """把业务结果转成 MCP 标准结果。

    成功时：
    - `content` 是模型可读的 JSON 文本；
    - `structuredContent.payload` 是同一份结构化数据；
    - `structuredContent.details` 是平台摘要。

    失败时：
    - `content` 是一句错误说明；
    - `isError=True`，客户端会把它映射成 `ToolResult.is_error`，
      模型仍然能继续下一轮，而不是整个任务崩掉。
    """
    structured: dict[str, Any] = {"details": outcome.details, "mock": True}
    if outcome.payload is not None:
        structured["payload"] = outcome.payload
    else:
        structured["error"] = outcome.text

    return CallToolResult(
        content=[TextContent(type="text", text=outcome.text)],
        structuredContent=structured,
        isError=outcome.is_error,
    )


def _business_error(exc: Exception) -> CallToolResult:
    """把可预期的业务异常转成 `isError=true` 结果。

    例子：模型查了一个模拟数据里不存在的实例。这个错误应该让模型看到并改正，
    而不是变成 JSON-RPC 层面的“服务器内部错误”。
    """
    return _to_call_tool_result(dba_tools._failure(str(exc)))


def create_server(
    dataset_path: Path | str | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> FastMCP:
    """创建 MCP Server 实例。

    参数：
    - `dataset_path`：模拟数据文件；不传时读环境变量或默认 fixture；
    - `host` / `port`：只在 HTTP/SSE 传输时生效，stdio 模式忽略。

    为什么把创建逻辑放在函数里，而不是模块级全局变量：
    测试可以创建多个独立 Server，各自使用临时数据文件，不会互相串数据。
    """
    env_dataset = os.getenv("SUPPORT_PLATFORM_MCP_DATASET", "").strip()
    resolved_dataset = Path(dataset_path or env_dataset) if (dataset_path or env_dataset) else None
    dataset = dba_tools.load_dataset(resolved_dataset)

    server = FastMCP(
        name=SERVER_NAME,
        instructions=SERVER_INSTRUCTIONS,
        host=host,
        port=port,
        # `json_response=False` 表示 Streamable HTTP 优先走 SSE 事件流；
        # 设为 True 时普通请求会直接返回 application/json。
        # 这里保留 SSE，方便观察服务端推送和事件格式。
        json_response=False,
        # 让 stdio 模式只把 MCP 协议消息写到 stdout；日志统一走 stderr。
        # 默认 WARNING：正常调查时不会每个请求都刷一行 "Processing request"；
        # 排查协议问题时可以设置 SUPPORT_PLATFORM_MCP_LOG_LEVEL=DEBUG 打开详细日志。
        log_level=os.getenv("SUPPORT_PLATFORM_MCP_LOG_LEVEL", "WARNING").upper(),
    )

    @server.tool(
        name="slow_log_query",
        title="慢日志查询",
        description=(
            "查询指定实例和时间范围内的慢日志摘要，按平均耗时从高到低返回。"
            "返回 SQL 指纹、样例 SQL、执行次数、平均/最大耗时、扫描行数和返回行数。"
            "只读，不会修改实例。"
        ),
        annotations=_annotations(),
    )
    async def slow_log_query(
        instance_id: Annotated[str, Field(description="实例 ID")],
        start_time: Annotated[
            str,
            Field(description="开始时间，ISO 8601 格式，例如 2026-10-29T10:00:00+08:00"),
        ],
        end_time: Annotated[
            str,
            Field(description="结束时间，ISO 8601 格式，例如 2026-10-29T11:00:00+08:00"),
        ],
        limit: Annotated[
            int,
            Field(ge=1, le=50, description="最多返回多少条慢日志，默认 5"),
        ] = 5,
    ) -> CallToolResult:
        """MCP 工具：查询慢日志。

        FastMCP 会根据上面的函数签名生成 `tools/list` 里的 `inputSchema`，
        并在 `tools/call` 时先校验参数，再调用这个函数。
        """
        try:
            outcome = dba_tools.slow_log_query(
                dataset,
                instance_id=instance_id,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        except dba_tools.ToolExecutionError as exc:
            return _business_error(exc)
        return _to_call_tool_result(outcome)

    @server.tool(
        name="execution_plan",
        title="执行计划查询",
        description=(
            "按 SQL 指纹查询模拟执行计划，返回访问类型、命中索引、估算扫描行数、"
            "过滤比例和 Extra 信息。用于判断是否全表扫描或额外排序。只读。"
        ),
        annotations=_annotations(),
    )
    async def execution_plan(
        instance_id: Annotated[str, Field(description="实例 ID")],
        sql_fingerprint: Annotated[
            str,
            Field(description="慢日志里返回的 SQL 指纹，例如 a1b2c3d4e5f6"),
        ],
    ) -> CallToolResult:
        """MCP 工具：按指纹查询执行计划。"""
        try:
            outcome = dba_tools.execution_plan(
                dataset,
                instance_id=instance_id,
                sql_fingerprint=sql_fingerprint,
            )
        except dba_tools.ToolExecutionError as exc:
            return _business_error(exc)
        return _to_call_tool_result(outcome)

    @server.tool(
        name="instance_metrics",
        title="实例监控查询",
        description=(
            "查询实例在指定时间范围内的 CPU、IO 等监控指标。"
            "注意：本轮模拟环境故意让这个接口超时，用于验证失败场景。只读。"
        ),
        annotations=_annotations(),
    )
    async def instance_metrics(
        instance_id: Annotated[str, Field(description="实例 ID")],
        start_time: Annotated[str, Field(description="开始时间，ISO 8601")],
        end_time: Annotated[str, Field(description="结束时间，ISO 8601")],
    ) -> CallToolResult:
        """MCP 工具：查询实例监控。

        先 `await asyncio.sleep(0.05)` 模拟一次真实 IO 等待，再返回业务错误。
        这个等待也让时间线里能看到真实的工具耗时，而不是瞬间返回。
        """
        await asyncio.sleep(0.05)
        try:
            outcome = dba_tools.instance_metrics(
                dataset,
                instance_id=instance_id,
                start_time=start_time,
                end_time=end_time,
            )
        except dba_tools.ToolExecutionError as exc:
            return _business_error(exc)
        return _to_call_tool_result(outcome)

    @server.tool(
        name="lock_wait_query",
        title="锁等待查询",
        description=(
            "查询指定时间范围内的锁等待样本。返回 no_data 表示采样窗口内没有样本，"
            "不等于实例一定没有锁等待。只读。"
        ),
        annotations=_annotations(),
    )
    async def lock_wait_query(
        instance_id: Annotated[str, Field(description="实例 ID")],
        start_time: Annotated[str, Field(description="开始时间，ISO 8601")],
        end_time: Annotated[str, Field(description="结束时间，ISO 8601")],
    ) -> CallToolResult:
        """MCP 工具：查询锁等待样本。"""
        try:
            outcome = dba_tools.lock_wait_query(
                dataset,
                instance_id=instance_id,
                start_time=start_time,
                end_time=end_time,
            )
        except dba_tools.ToolExecutionError as exc:
            return _business_error(exc)
        return _to_call_tool_result(outcome)

    # ------------------------------------------------------------------
    # 多数据库场景工具
    # ------------------------------------------------------------------
    # 上面的四个慢 SQL 工具是“手写实现”。多数据库场景工具由
    # `scenario_registry.py` 根据当前数据集的 `tool_results` 动态注册。
    #
    # 关键的一步：如果当前数据集不是慢 SQL 数据（没有 slow_log / execution_plans /
    # lock_wait 这些块），就把四个慢 SQL 工具从 tools/list 里移除。
    # 这样 PostgreSQL 工单里，模型不会看到“慢日志查询”这种当前数据回答不了的
    # 工具，工具列表和真实排查范围保持一致。
    #
    # `FastMCP.remove_tool()` 只影响之后的 tools/list，已经定义的处理函数不会执行。
    supports_legacy = any(
        key in dataset for key in ("slow_log", "execution_plans", "lock_wait")
    )
    if not supports_legacy:
        for legacy_name in (
            "slow_log_query",
            "execution_plan",
            "instance_metrics",
            "lock_wait_query",
        ):
            server.remove_tool(legacy_name)

    register_scenario_tools(server, dataset)

    return server


def serve(
    *,
    transport: str = "stdio",
    host: str = "127.0.0.1",
    port: int = 8765,
    dataset_path: Path | str | None = None,
) -> int:
    """启动 MCP Server，阻塞直到进程退出。

    这个方法必须在同步上下文调用：FastMCP 内部会自己创建事件循环，
    如果已经在一个 `asyncio.run()` 里面再调用，会报“事件循环已在运行”。
    CLI 的 `mcp-serve` 子命令和 `python -m app.mcp.server` 都走这里。
    """
    if transport not in TRANSPORTS:
        raise SystemExit(f"不支持的 MCP transport：{transport}；可选：{', '.join(TRANSPORTS)}")

    server = create_server(dataset_path, host=host, port=port)
    if transport != "stdio":
        # 只有 HTTP/SSE 模式才打印启动地址；stdio 模式的 stdout 必须留给协议消息。
        print(
            f"MCP Server 已启动：transport={transport}，"
            f"http://{host}:{port}{'/mcp' if transport == 'streamable-http' else '/sse'}",
            file=sys.stderr,
        )

    try:
        server.run(transport=transport)  # type: ignore[arg-type]
    except KeyboardInterrupt:
        return 130
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """构造 MCP Server 的命令行参数。"""
    parser = argparse.ArgumentParser(
        prog="support-platform-mcp",
        description="DBA 只读工具 MCP Server（stdio / Streamable HTTP / SSE）",
    )
    parser.add_argument(
        "--transport",
        choices=TRANSPORTS,
        default="stdio",
        help="传输方式，默认 stdio",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP/SSE 监听地址")
    parser.add_argument("--port", type=int, default=8765, help="HTTP/SSE 监听端口")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="模拟数据文件；不传时读 SUPPORT_PLATFORM_MCP_DATASET 或默认 fixture",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """模块入口：`python -m app.mcp.server`。"""
    args = _build_parser().parse_args(argv)
    return serve(
        transport=args.transport,
        host=args.host,
        port=args.port,
        dataset_path=args.dataset,
    )


if __name__ == "__main__":
    raise SystemExit(main())
