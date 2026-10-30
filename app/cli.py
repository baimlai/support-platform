"""第 2 步的命令行入口，第 3 步增加 HTTP 服务启动命令。

用法：
    uv run python -m app.cli list-skills
    uv run python -m app.cli list-tools
    uv run python -m app.cli run --ticket fixtures/tickets/slow_sql_001.json
    uv run python -m app.cli run --ticket fixtures/tickets/slow_sql_001.json --max-turns 3
    uv run python -m app.cli run --ticket fixtures/tickets/slow_sql_001.json --mcp-transport sse --mcp-url http://127.0.0.1:8765/sse
    uv run python -m app.cli mcp-serve --transport streamable-http --port 8765
    uv run python -m app.cli mcp-compare --transport stdio
    uv run python -m app.cli serve --port 8000

第 2 步和 step 1 的最大区别：
CLI 不再自己拼“工具目录 + 模型 + 循环”，而是把装配工作交给 `AgentRunner`。
这样 CLI、测试和以后的 FastAPI 后台任务共用同一条执行链，不会出现
“命令行能跑、接口行为不一样”的分叉。

## 实现说明

- `argparse` 是标准库的命令行解析工具：定义一次参数，自动生成 `--help`，
  还能在参数缺失/类型不对时给出友好提示。
- `argparse.Namespace` 是一个普通对象，`args.ticket` 就是命令行传进来的值。
- `asyncio.run(main())` 是启动异步程序的推荐方式：它会创建事件循环、运行协程，
  最后关闭循环。不要在异步函数里面再调用 `asyncio.run`。
- `raise SystemExit("消息")` 会打印消息并以非 0 状态退出，适合命令行参数错误。
- `if __name__ == "__main__":` 表示“这个文件被直接运行时才执行”，
  被其它模块 import 时不会自动跑主流程。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path
from typing import Any

from app.adapters.models import SUPPORTED_MODEL_PROVIDERS, ModelConfigurationError
from app.adapters.scenario_mock import resolve_ticket_dataset
from app.harness.context import ContextBudget
from app.harness.runner import AgentRunner, RunOptions
from app.harness.skills import DEFAULT_SKILL_NAME, list_skills, load_skill
from app.harness.tool_search import PLATFORM_TOOL_DECLARATIONS
from app.mcp.client import DbaMcpClient, McpClientConfig, McpClientError
from app.mcp.dba_tools import business_tool_declarations
from app.mcp.pi_style import PiStyleMcpClient
from app.mcp.server import TRANSPORTS
from app.mcp.server import serve as serve_mcp
from app.runtime.contracts import (
    AgentEnd,
    CancellationToken,
    ToolExecutionEnd,
    ToolExecutionStart,
    TopicUpdated,
    TurnEnd,
    TurnStart,
)


def _load_ticket(path: Path) -> dict[str, Any]:
    """读取并检查工单文件。

    检查的是“跑模拟流程必须有的字段”，不是完整工单校验：
    实例用于查数据，时间范围用于工具参数，工单号用于运行 id。
    """
    if not path.exists():
        raise SystemExit(f"工单文件不存在：{path}")
    try:
        ticket = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        # `raise ... from exc` 会保留原始异常作为“原因”，
        # 排查时能看到完整调用链；不加 from 也能跑，但会丢掉原始错误上下文。
        raise SystemExit(f"工单不是合法 JSON：{path}（{exc}）") from exc
    if not isinstance(ticket, dict):
        raise SystemExit(f"工单顶层必须是对象：{path}")

    missing = [key for key in ("ticket_id", "instance_id", "time_range") if key not in ticket]
    if missing:
        raise SystemExit(f"工单缺少字段 {missing}：{path}")
    if not ticket.get("mock"):
        # 不直接拒绝，是为了以后接真实工单时还能复用同一入口；
        # 但一定要提醒：当前工具返回的仍然是模拟数据。
        print("警告：工单未标记 mock=true；当前工具仍使用模拟数据。", file=sys.stderr)
    return ticket


def _format_arguments(arguments: dict[str, Any]) -> str:
    """把工具参数压成一行，CLI 时间线里不展开大段 JSON。"""
    return json.dumps(arguments, ensure_ascii=False, separators=(",", ":"))


def _print_event(event: Any) -> None:
    """打印适合人看的关键事件。

    `message_update` 每个字符都会触发一次，打印会刷屏，所以这里只保留轮次、
    工具节点和 Topic 更新。完整事件仍在 `events.jsonl` 里。
    """
    if isinstance(event, TurnStart):
        print(f"\n--- 第 {event.turn} 轮 ---")
    elif isinstance(event, ToolExecutionStart):
        print(f"  -> 调用 {event.tool_name} {_format_arguments(event.arguments)}")
    elif isinstance(event, ToolExecutionEnd):
        status = "失败" if event.is_error else "成功"
        duration = f"，{event.duration_ms} ms" if event.duration_ms is not None else ""
        evidence = event.result.meta.evidence_id if event.result.meta is not None else None
        evidence_text = f"，证据 {evidence}" if evidence else ""
        print(f"  <- {event.tool_name} {status}{duration}{evidence_text}")
        if event.is_error:
            # 失败原因直接打出来，演示时不用再翻文件。
            text = "".join(block.text for block in event.result.content)
            print(f"     错误：{text}")
    elif isinstance(event, TopicUpdated):
        topic = event.topic
        facts = topic.get("facts") or []
        candidates = topic.get("candidates") or []
        next_steps = topic.get("next_steps") or []
        next_text = "；".join(str(item) for item in next_steps[:2])
        print(
            f"  [Topic] 事实 {len(facts)} 条，候选原因 {len(candidates)} 条"
            f"，下一步：{next_text or '无'}"
        )
    elif isinstance(event, TurnEnd):
        text = "".join(
            block.text for block in event.message.content if getattr(block, "type", "") == "text"
        )
        if text.strip():
            preview = text.strip().splitlines()[0]
            print(f"  [助手] {preview[:100]}")
    elif isinstance(event, AgentEnd):
        print(f"\n运行结束：reason={event.reason}")


def _print_tool(
    name: str,
    label: str,
    description: str,
    parameters: dict[str, Any],
    timeout_s: float | None = None,
) -> None:
    """打印一个工具的名称、参数和说明。"""
    fields = ", ".join(parameters.get("properties", {}).keys())
    timeout = "默认超时" if timeout_s is None else f"{timeout_s} 秒"
    print(f"{name}（{label}，{timeout}）")
    print(f"  参数：{fields or '无'}")
    print(f"  {description}")


def _build_parser() -> argparse.ArgumentParser:
    # `add_subparsers` 让一个命令拥有多个子命令，
    # 用法类似 `git commit` / `git status`。`required=True` 表示必须选一个子命令。
    parser = argparse.ArgumentParser(
        prog="support-platform",
        description="DBA 工单 Agent 小平台（第 2 步：Skill + 工具搜索 + 证据 + Topic）",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="运行一张模拟工单")
    run_parser.add_argument(
        "--ticket",
        type=Path,
        default=Path("fixtures/tickets/slow_sql_001.json"),
        help="工单 JSON 路径，默认 fixtures/tickets/slow_sql_001.json",
    )
    run_parser.add_argument(
        "--skill",
        default=None,
        help="加载哪个 Skill；不传时用工单里的 skill，再退回 slow-sql",
    )
    run_parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="手工覆盖 MCP 模拟数据；场景工单默认使用 ticket.dataset",
    )
    run_parser.add_argument("--max-turns", type=int, default=10, help="最大轮数，默认 10")
    run_parser.add_argument(
        "--tool-timeout",
        type=float,
        default=5.0,
        help="单个工具的默认超时秒数，默认 5 秒",
    )
    run_parser.add_argument(
        "--run-root",
        type=Path,
        default=Path("var/runs"),
        help="运行记录根目录，默认 var/runs",
    )
    run_parser.add_argument(
        "--model-delay",
        type=float,
        default=0.005,
        help="模拟模型每段文本的停顿秒数，默认 0.005；设成 0 可以最快跑完",
    )
    run_parser.add_argument(
        "--max-inline-chars",
        type=int,
        default=4000,
        help="单条工具结果放进上下文的最大字符数，默认 4000；超过就保存为证据",
    )
    run_parser.add_argument(
        "--context-budget",
        type=int,
        default=12000,
        help="整份模型输入的字符预算，默认 12000（字符估算，不是 token）",
    )
    run_parser.add_argument(
        "--context-messages",
        type=int,
        default=10,
        help="最多保留多少条最近消息，默认 10",
    )
    run_parser.add_argument(
        "--model-provider",
        choices=sorted(SUPPORTED_MODEL_PROVIDERS),
        default="mock",
        help="模型来源：mock（默认，本地剧本）或 deepseek（真实接口）",
    )
    run_parser.add_argument(
        "--model",
        default=None,
        help="模型名，例如 deepseek-flash；不传时读 DEEPSEEK_MODEL 或使用默认值",
    )
    run_parser.add_argument(
        "--model-base",
        default=None,
        help="OpenAI 兼容接口根地址，默认 https://api.deepseek.com",
    )
    run_parser.add_argument(
        "--model-timeout",
        type=float,
        default=120.0,
        help="模型读超时秒数，默认 120",
    )
    run_parser.add_argument(
        "--mcp-transport",
        choices=sorted(TRANSPORTS),
        default="stdio",
        help="MCP 传输方式，默认 stdio（Agent 自动启动 MCP 子进程）",
    )
    run_parser.add_argument(
        "--mcp-url",
        default=None,
        help="MCP HTTP/SSE 地址，例如 http://127.0.0.1:8765/mcp",
    )
    run_parser.add_argument(
        "--mcp-command",
        default=None,
        help="stdio 启动命令；默认使用当前 Python 解释器启动 app.mcp.server",
    )
    run_parser.add_argument(
        "--mcp-timeout",
        type=float,
        default=30.0,
        help="MCP 连接/初始化/单次调用超时秒数，默认 30",
    )

    subparsers.add_parser("list-tools", help="列出业务工具和平台工具")
    subparsers.add_parser("list-skills", help="列出当前可加载的 Skill")

    serve_parser = subparsers.add_parser("serve", help="启动 FastAPI 服务，供前端联调")
    serve_parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000, help="监听端口，默认 8000")
    serve_parser.add_argument(
        "--reload",
        action="store_true",
        help="改代码后自动重启，开发时可用；生产/演示建议关闭",
    )

    mcp_parser = subparsers.add_parser(
        "mcp-serve",
        help="启动真实 MCP Server（DBA 只读工具）",
    )
    mcp_parser.add_argument(
        "--transport",
        choices=sorted(TRANSPORTS),
        default="stdio",
        help="MCP 传输方式，默认 stdio",
    )
    mcp_parser.add_argument("--host", default="127.0.0.1", help="HTTP/SSE 监听地址")
    mcp_parser.add_argument("--port", type=int, default=8765, help="HTTP/SSE 监听端口")
    mcp_parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="模拟数据文件；不传时读 SUPPORT_PLATFORM_MCP_DATASET 或默认 fixture",
    )

    compare_parser = subparsers.add_parser(
        "mcp-compare",
        help="并排运行官方 SDK 和 Pi 风格客户端，对比协议行为",
    )
    compare_parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
        help="对比使用的传输；Pi 风格客户端不支持旧版 sse，默认 stdio",
    )
    compare_parser.add_argument(
        "--url",
        default=None,
        help="Streamable HTTP 地址，例如 http://127.0.0.1:8765/mcp",
    )
    compare_parser.add_argument(
        "--command",
        dest="mcp_command",
        default=None,
        help="stdio 启动命令；默认使用当前 Python 解释器启动 app.mcp.server",
    )
    compare_parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="连接和单次调用超时秒数，默认 30",
    )
    compare_parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="模拟数据文件；不传时使用默认 fixture",
    )
    compare_parser.add_argument(
        "--ticket",
        type=Path,
        default=Path("fixtures/tickets/slow_sql_001.json"),
        help="从哪张工单取实例和时间范围",
    )
    compare_parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="慢日志查询返回条数，默认 3",
    )
    compare_parser.add_argument(
        "--trace-dir",
        type=Path,
        default=Path("var/mcp-compare"),
        help="两份协议追踪的目录，默认 var/mcp-compare",
    )
    return parser


def _serve(args: argparse.Namespace) -> int:
    """启动 uvicorn。

    为什么不在 `_main_async` 里调用：
    `uvicorn.run` 内部会自己创建事件循环，而 `_main_async` 已经在一个事件循环里。
    在运行中的事件循环里再调用 `asyncio.run` 会直接报错，所以服务启动放在
    `main()` 的同步分支里。
    """
    import uvicorn  # 延迟导入：只跑 CLI 调查时不必加载 Web 依赖

    uvicorn.run(
        "app.api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    return 0


async def _run_ticket(args: argparse.Namespace) -> Any:
    """装配并运行一次调查，落盘后返回 RunOutcome。"""
    ticket = _load_ticket(args.ticket)

    # 与 HTTP 服务保持同一套优先级：命令行显式指定 > 工单自带 > 默认 slow-sql。
    skill_name = (
        (args.skill or "").strip()
        or str(ticket.get("skill") or "").strip()
        or DEFAULT_SKILL_NAME
    )

    # 先加载一次 Skill，只为了在开始运行前打印给用户看。
    # AgentRunner 内部还会再加载一次；Skill 文件很小，这点重复可以接受，
    # 换来的是 CLI 能在开跑前就发现“Skill 名字写错了”。
    try:
        skill = load_skill(skill_name)
    except (LookupError, ValueError) as exc:
        raise SystemExit(f"Skill 加载失败：{exc}") from exc

    # 场景工单自带数据集；`--dataset` 只用于手工覆盖或调试。
    # 旧工单没有 scenario_mode，这里会返回 None，让 MCP Client 使用服务端默认数据。
    try:
        dataset_path = resolve_ticket_dataset(ticket, fallback=args.dataset)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"场景数据不可用：{exc}") from exc

    options = RunOptions(
        ticket=ticket,
        skill_name=skill_name,
        max_turns=args.max_turns,
        tool_timeout_s=args.tool_timeout,
        run_root=args.run_root,
        model_delay_s=args.model_delay,
        max_inline_chars=args.max_inline_chars,
        context_budget=ContextBudget(
            max_chars=args.context_budget,
            max_recent_messages=args.context_messages,
        ),
        model_provider=args.model_provider,
        model_name=args.model,
        model_base_url=args.model_base,
        model_timeout_s=args.model_timeout,
        dataset_path=dataset_path,
        mcp_transport=args.mcp_transport,
        mcp_url=args.mcp_url,
        mcp_command=args.mcp_command,
        mcp_timeout_s=args.mcp_timeout,
    )
    cancellation = CancellationToken()

    # 第一次 Ctrl-C 不硬杀进程，而是触发协作式取消：
    # 这样已经产生的消息和事件还能完整落盘，报告里也会标“调查未完成”。
    # `asyncio.get_running_loop()` 只能在协程内部调用。
    # `loop.add_signal_handler` 把 Ctrl-C 交给取消令牌，而不是直接抛 KeyboardInterrupt。
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, cancellation.cancel)
        signal_installed = True
    except (NotImplementedError, RuntimeError):
        # Windows 的默认事件循环不支持 add_signal_handler；CLI 仍可正常运行。
        signal_installed = False

    print("模拟模式：所有工单和工具数据均为模拟，不连接真实实例。")
    print(f"工单：{ticket.get('ticket_id')} / 实例：{ticket.get('instance_id')}")
    print(
        f"Skill：{skill.label}（{skill.name} v{skill.version}），"
        f"初始业务工具：{', '.join(skill.initial_tools) or '无'}"
    )

    try:
        outcome = await AgentRunner(options).run(
            cancellation=cancellation,
            on_event=_print_event,
        )
    except ModelConfigurationError as exc:
        raise SystemExit(f"模型配置错误：{exc}") from exc
    except McpClientError as exc:
        raise SystemExit(f"MCP 连接失败：{exc}") from exc
    finally:
        if signal_installed:
            loop.remove_signal_handler(signal.SIGINT)

    return outcome


def _print_outcome(outcome: Any) -> None:
    """运行结束后打印报告、证据目录和运行记录路径。"""
    recorder = outcome.recorder
    print("\n最终报告：")
    print("=" * 72)
    print(recorder.report_path.read_text(encoding="utf-8").rstrip())
    print("=" * 72)

    print(f"运行记录目录：{recorder.run_dir}")
    print(f"事件：{recorder.events_path}")
    print(f"完整消息：{recorder.messages_path}")
    print(f"Topic：{recorder.run_dir / 'topic.json'}")
    print(f"证据目录：{outcome.evidence.evidence_dir}")
    print(
        "MCP："
        f"{outcome.mcp.transport} / {outcome.mcp.endpoint} / "
        f"{outcome.mcp.server_name} v{outcome.mcp.server_version} / "
        f"协议 {outcome.mcp.protocol_version}"
    )
    print(f"MCP 协议追踪：{recorder.run_dir / 'mcp_protocol.jsonl'}")

    items = outcome.evidence.list()
    if items:
        print("证据列表：")
        for item in items:
            flags = []
            if item.get("is_error"):
                flags.append("失败")
            if item.get("truncated"):
                flags.append("原始结果超过内联上限")
            flag_text = f"（{'，'.join(flags)}）" if flags else ""
            print(
                f"  {item['evidence_id']} {item['tool_name']} "
                f"{item['size_chars']} 字符{flag_text}"
            )
    else:
        print("证据列表：无")

    print(f"最终可见工具：{', '.join(outcome.session.visible_names())}")
    loaded = outcome.session.loaded_by_search
    print(f"通过搜索加载：{', '.join(loaded) if loaded else '无'}")


async def _compare_mcp(args: argparse.Namespace) -> int:
    """并排运行官方 SDK 和 Pi 风格客户端，打印可对照的结果。

    这不是“跑两次 Agent”，而是只做 MCP 层对比：

    ```text
    官方 SDK:   initialize -> tools/list -> tools/call
    Pi 风格:    initialize -> tools/list -> tools/call（自己拼 JSON-RPC）
    ```

    两边使用同一份工单参数、同一个 Server、同一组 fixtures。输出重点看：

    - 服务端名字、协议版本、工具清单是否一致；
    - 慢日志结果文本和 details 是否完全一致；
    - 监控工具的业务错误是否都映射成 `is_error=True`；
    - 两份 `*_protocol.jsonl` 里的请求 id、方法名和 SSE 帧。
    """
    ticket = _load_ticket(args.ticket)
    time_range = ticket.get("time_range") or {}
    instance_id = str(ticket.get("instance_id"))
    start_time = str(time_range.get("start"))
    end_time = str(time_range.get("end"))
    if not instance_id or not start_time or not end_time:
        raise SystemExit("工单缺少 instance_id 或 time_range.start/end，无法对比 MCP 工具")

    args.trace_dir.mkdir(parents=True, exist_ok=True)
    official_config = McpClientConfig(
        transport=args.transport,
        url=args.url,
        command=args.mcp_command,
        dataset_path=args.dataset,
        timeout_s=args.timeout,
        trace_path=args.trace_dir / "official_protocol.jsonl",
    )
    pi_config = McpClientConfig(
        transport=args.transport,
        url=args.url,
        command=args.mcp_command,
        dataset_path=args.dataset,
        timeout_s=args.timeout,
        trace_path=args.trace_dir / "pi_style_protocol.jsonl",
    )

    slow_args = {
        "instance_id": instance_id,
        "start_time": start_time,
        "end_time": end_time,
        "limit": args.limit,
    }
    metrics_args = {
        "instance_id": instance_id,
        "start_time": start_time,
        "end_time": end_time,
    }

    print("=" * 72)
    print("官方 Python SDK 客户端（运行时默认路线）")
    print("=" * 72)
    async with DbaMcpClient(official_config) as official:
        official_info = official.info
        assert official_info is not None
        print(
            f"{official_info.server_name} v{official_info.server_version} / "
            f"协议 {official_info.protocol_version} / 工具 {', '.join(official_info.tools)}"
        )
        official_slow = await official.call_tool("slow_log_query", slow_args)
        official_metrics = await official.call_tool("instance_metrics", metrics_args)
        print(
            f"slow_log_query：{len(official_slow.content[0].text)} 字符，"
            f"top={official_slow.details.get('top', {}).get('sql_fingerprint')}"
        )
        print(
            f"instance_metrics：is_error={official_metrics.is_error}，"
            f"{official_metrics.content[0].text.splitlines()[0]}"
        )

    print()
    print("=" * 72)
    print("Pi 风格手写客户端（教学对照路线）")
    print("=" * 72)
    async with PiStyleMcpClient(pi_config) as pi_style:
        pi_info = pi_style.info
        assert pi_info is not None
        print(
            f"{pi_info.server_name} v{pi_info.server_version} / "
            f"协议 {pi_info.protocol_version} / 工具 {', '.join(pi_info.tools)}"
        )
        pi_slow = await pi_style.call_tool("slow_log_query", slow_args)
        pi_metrics = await pi_style.call_tool("instance_metrics", metrics_args)
        print(
            f"slow_log_query：{len(pi_slow.content[0].text)} 字符，"
            f"top={pi_slow.details.get('top', {}).get('sql_fingerprint')}"
        )
        print(
            f"instance_metrics：is_error={pi_metrics.is_error}，"
            f"{pi_metrics.content[0].text.splitlines()[0]}"
        )

    print()
    print("=" * 72)
    print("对比结论")
    print("=" * 72)
    same_text = official_slow.content[0].text == pi_slow.content[0].text
    same_details = official_slow.details == pi_slow.details
    same_metrics = (
        official_metrics.is_error == pi_metrics.is_error
        and official_metrics.content[0].text == pi_metrics.content[0].text
    )
    print(f"slow_log_query 文本一致：{same_text}")
    print(f"slow_log_query details 一致：{same_details}")
    print(f"instance_metrics 错误结果一致：{same_metrics}")
    print(f"协议追踪：{official_config.trace_path}（官方）")
    print(f"协议追踪：{pi_config.trace_path}（Pi 风格）")
    print(
        "提示：打开两份 jsonl，对照同一 id 的 initialize / tools/list / tools/call；"
        "官方 SDK 的追踪由 SessionMessage 序列化，Pi 风格的追踪由我们直接记录字典。"
    )
    return 0 if (same_text and same_details and same_metrics) else 1


async def _main_async(args: argparse.Namespace) -> int:
    if args.command == "list-tools":
        print("业务工具（通过真实 MCP Server 提供）：")
        for declaration in business_tool_declarations():
            _print_tool(
                declaration["name"],
                declaration["label"],
                declaration["description"],
                declaration["parameters"],
            )
        print("\n平台工具（始终可见，不参与业务搜索）：")
        for declaration in PLATFORM_TOOL_DECLARATIONS:
            _print_tool(
                declaration["name"],
                declaration["label"],
                declaration["description"],
                declaration["parameters"],
            )
        return 0

    if args.command == "list-skills":
        skills = list_skills()
        if not skills:
            print("没有找到 Skill。")
            return 0
        for skill in skills:
            print(f"{skill.name}（{skill.label} v{skill.version}）")
            print(f"  初始业务工具：{', '.join(skill.initial_tools) or '无'}")
            print(f"  {skill.description}")
        return 0

    if args.command == "mcp-compare":
        return await _compare_mcp(args)

    outcome = await _run_ticket(args)
    _print_outcome(outcome)
    return 0 if outcome.result.reason == "completed" else 1


def main(argv: list[str] | None = None) -> int:
    """同步入口，方便 `python -m app.cli` 和控制台脚本调用。

    说明：`argv=None` 时 argparse 自动读取 `sys.argv[1:]`；
    测试里可以显式传 `["run", "--ticket", ...]`，不依赖真实命令行。
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        return _serve(args)
    if args.command == "mcp-serve":
        # MCP Server 自己管理事件循环，必须放在同步分支启动。
        return serve_mcp(
            transport=args.transport,
            host=args.host,
            port=args.port,
            dataset_path=args.dataset,
        )
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        print("\n已强制中断，运行记录可能不完整。", file=sys.stderr)
        return 130


# `__name__` 在“直接运行”时等于 "__main__"，被 import 时等于模块名。
# 这样写可以保证 `python -m app.cli` 能跑，同时测试 import 本文件时不会自动执行。
# `raise SystemExit(main())` 把 main() 的返回值（0 或 1）作为进程退出码。
if __name__ == "__main__":
    raise SystemExit(main())
