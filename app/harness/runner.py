"""把一次完整的 DBA 调查运行装配起来。

## 为什么单独有 Runner

第 1 步的装配逻辑散在 CLI 里：读工单、建工具、建模型、跑循环、落盘。
第 2 步多出了 Skill、工具会话、证据存储、Topic 和有界上下文；第 3 步还要
把这套东西接到 FastAPI 后台任务里。如果继续放在 CLI，HTTP 层要么复制一遍，
要么 import CLI，两种都不合适。

`AgentRunner` 只负责“按配置装配 + 跑一次”，不负责打印、不负责读命令行参数。
这样 CLI、测试和以后的 HTTP 任务可以共用同一条执行链，行为不会分叉。

## 一次运行的装配顺序

```text
Skill（方法说明 + 初始工具）
  -> 运行记录器（run.json / events.jsonl / report.md）
  -> MCP Client（真实 JSON-RPC：initialize / tools/list）
  -> 业务工具目录（来自 MCP tools/list，不是本地函数）
  -> 证据存储（evidence/）
  -> Topic（topic.json）
  -> 工具会话（可见工具 + 平台工具 search_tools/evidence_read）
  -> 上下文与预算钩子
  -> AgentLoopConfig（tools_provider / transform_context / after_tool_call）
  -> run_agent_loop
```

顺序不是随便排的：记录器要最先创建，因为 MCP 协议追踪、证据和 Topic
都要写进同一个运行目录；MCP 的 `tools/list` 必须在构造工具会话之前完成，
否则不知道该给 Skill 提供哪些真实工具；平台工具又需要工具会话和证据存储，
所以最后挂上去。

## MCP 生命周期

`AgentRunner.run()` 用 `async with DbaMcpClient(...)` 包住整次调查：

```text
进入：启动/连接 MCP Server -> initialize -> tools/list
运行：模型发起 tool_calls -> MCP tools/call -> 证据/Topic
退出：关闭 ClientSession -> 关闭传输 -> 回收 stdio 子进程
```

这样即使模型调用中途抛异常，连接和子进程也会被清理，不会留下孤儿进程。

对比教学：Pi 在 coding-agent 启动时后台连接所有 MCP Server，第一条提示最多
等 10 秒，连接断开后下次调用再重连；它还要处理常驻会话、工具列表变化和 OAuth。
本项目的调查是“一次任务、一次连接、结束关闭”，所以同步连接更简单，也更容易
把每条协议消息写进本次运行目录。以后要做常驻服务或多 Server，再引入 Pi 那种
连接管理器和重试策略。

## 实现说明

- `dataclass(slots=True)` 用来装“运行参数”和“运行结果”，字段固定，读写直观。
- `asyncio` 的 `await` 会按顺序执行；本文件里没有并发，因为一次调查本来就是顺序的。
- `emit` 里递归调用自己是安全的：`topic_updated` 不是 `ToolExecutionEnd`，
  不会再进入更新分支。用递归是为了让 Topic 事件走和普通事件完全相同的落盘/打印路径。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.adapters.models import build_model
from app.adapters.run_recorder import RunRecorder
from app.harness.context import (
    ContextBudget,
    build_initial_context,
    build_user_message,
    make_transform_context,
)
from app.harness.evidence import DEFAULT_MAX_INLINE_CHARS, EvidenceProcessor, EvidenceStore
from app.harness.skills import DEFAULT_SKILL_NAME, Skill, load_skill
from app.harness.tool_search import PLATFORM_TOOL_NAMES, ToolSession, build_platform_tools
from app.harness.tools import ToolRegistry
from app.harness.topic import TopicManager
from app.mcp.client import DbaMcpClient, McpClientConfig
from app.mcp.info import McpClientInfo
from app.runtime.contracts import (
    AgentContext,
    AgentEvent,
    AgentLoopConfig,
    CancellationToken,
    ToolExecutionEnd,
)
from app.runtime.pi_reference.loop import LoopResult, run_agent_loop


@dataclass(slots=True)
class RunOptions:
    """一次调查的运行参数。

    字段说明：
    - `ticket`：工单字典，CLI 从 JSON 读，以后 HTTP 接口从请求体读；
    - `skill_name`：加载哪个 Skill，默认慢 SQL；
    - `max_turns`：轮数上限，模拟流程需要 6 轮，默认 10 留一点余量；
    - `run_root` / `run_id`：运行记录写到哪里；测试会传临时目录；
    - `max_inline_chars`：单条工具结果放进上下文的上限，超过就落证据；
    - `context_budget`：整份模型输入的字符预算；
    - `dataset_path`：模拟数据路径，测试可以换成自己的 fixture。

    MCP 相关字段：
    - `mcp_transport`：stdio / streamable-http / sse；
    - `mcp_url`：HTTP/SSE 地址；stdio 模式留空，由 Client 启动子进程；
    - `mcp_command`：stdio 启动命令；留空时使用当前 Python 解释器；
    - `mcp_timeout_s`：MCP 连接、初始化和单次调用的超时；
    - `mcp_trace_path`：协议追踪文件；留空时写到运行目录下的
      `mcp_protocol.jsonl`。
    """

    ticket: dict[str, Any]
    skill_name: str = DEFAULT_SKILL_NAME
    max_turns: int = 10
    tool_timeout_s: float = 5.0
    run_root: Path | str = Path("var/runs")
    run_id: str | None = None
    model_delay_s: float = 0.005
    max_inline_chars: int = DEFAULT_MAX_INLINE_CHARS
    context_budget: ContextBudget = field(default_factory=ContextBudget)
    dataset_path: Path | str | None = None
    # MCP 连接配置。默认 stdio：由 Agent 启动一个 MCP 子进程，本地零配置。
    mcp_transport: str = "stdio"
    mcp_url: str | None = None
    mcp_command: str | None = None
    mcp_timeout_s: float = 30.0
    mcp_trace_path: Path | str | None = None
    # 模型选择：默认 mock，保证测试和演示零配置；传 deepseek 才走真实接口。
    model_provider: str = "mock"
    model_name: str | None = None
    model_base_url: str | None = None
    model_api_key: str | None = None
    model_timeout_s: float = 120.0
    model_max_tokens: int = 4096
    model_temperature: float = 0.2


@dataclass(slots=True)
class RunOutcome:
    """一次运行结束后，调用方通常要用的东西。

    例子：CLI 用 `recorder.report_path` 打印报告，测试用 `session.searches`
    断言工具搜索确实发生过，第 3 步的 HTTP 任务用 `result.reason` 映射任务状态。
    """

    result: LoopResult
    recorder: RunRecorder
    skill: Skill
    session: ToolSession
    topic: TopicManager
    evidence: EvidenceStore
    context: AgentContext
    # 本次连接到的 MCP Server 摘要：传输方式、协议版本、工具清单等。
    # CLI 会打印它，run.json 也会保存一份，方便确认“确实走了 MCP”。
    mcp: McpClientInfo


class AgentRunner:
    """按 `RunOptions` 装配并执行一次调查。"""

    def __init__(self, options: RunOptions) -> None:
        self.options = options

    async def run(
        self,
        cancellation: CancellationToken | None = None,
        on_event: Callable[[AgentEvent], None] | None = None,
    ) -> RunOutcome:
        """跑完一次调查并返回结果。

        `on_event` 是给 CLI 打印用的同步回调；事件已经先写进记录器，
        所以即使回调写得慢，也不会影响 `events.jsonl` 的完整性。
        """
        options = self.options
        ticket = options.ticket

        # 先装配模型，配置错误（例如缺少 API Key）会在创建运行目录之前就失败，
        # 不会留下一个只有空目录的“运行记录”。
        # 返回的 model_id 会写进每轮请求记录和事件，方便排查“这次到底用了哪个模型”。
        model, model_id = build_model(
            options.model_provider,
            model_delay_s=options.model_delay_s,
            model_name=options.model_name,
            base_url=options.model_base_url,
            api_key=options.model_api_key,
            timeout_s=options.model_timeout_s,
            max_tokens=options.model_max_tokens,
            temperature=options.model_temperature,
        )

        # 1) Skill：决定系统提示词和初始可见的业务工具。
        skill = load_skill(options.skill_name)

        # 2) 记录器先创建，因为 MCP 协议追踪、证据和 Topic 都要用它的运行目录。
        recorder = RunRecorder(
            run_root=options.run_root,
            ticket=ticket,
            run_id=options.run_id,
        )

        # 3) 真实 MCP 连接。`async with` 会在整次调查结束后关闭会话和传输；
        #    stdio 模式还会回收 MCP 子进程。
        #    trace_path 默认写进运行目录，打开就能看到原始 JSON-RPC 收发。
        mcp_config = McpClientConfig(
            transport=options.mcp_transport,
            url=options.mcp_url,
            command=options.mcp_command,
            dataset_path=options.dataset_path,
            timeout_s=options.mcp_timeout_s,
            trace_path=options.mcp_trace_path or recorder.run_dir / "mcp_protocol.jsonl",
        )
        async with DbaMcpClient(mcp_config) as mcp:
            return await self._run_with_mcp(
                options=options,
                ticket=ticket,
                skill=skill,
                recorder=recorder,
                model=model,
                model_id=model_id,
                cancellation=cancellation,
                on_event=on_event,
                mcp=mcp,
            )

    async def _run_with_mcp(
        self,
        *,
        options: RunOptions,
        ticket: dict[str, Any],
        skill: Skill,
        recorder: RunRecorder,
        model: Any,
        model_id: str,
        cancellation: CancellationToken | None,
        on_event: Callable[[AgentEvent], None] | None,
        mcp: DbaMcpClient,
    ) -> RunOutcome:
        """在 MCP 连接已建立的前提下装配并执行调查。

        为什么拆成单独的方法：
        MCP 的生命周期必须用 `async with` 包住，如果把几十行装配代码全缩进
        到 `with` 里面，可读性会很差。拆出来后，`run()` 只负责“连接和释放”，
        这个方法只负责“使用连接”。
        """
        mcp_info = mcp.info
        if mcp_info is None:
            # 正常不会发生：进入 DbaMcpClient.__aenter__ 时已经完成 initialize。
            # 防御性判断可以避免后面写 run.json 时才出现 None 序列化错误。
            raise RuntimeError("MCP Client 已连接，但没有初始化信息")

        # 4) 业务工具目录来自 MCP 的 `tools/list`。
        #    注意：这里没有 import 任何 DBA 工具函数；工具可以住在另一个进程里。
        registry = ToolRegistry(mcp.tool_specs())

        # 5) 证据存储：业务工具结果落盘，平台工具结果跳过。
        evidence = EvidenceStore(
            run_dir=recorder.run_dir,
            run_id=recorder.run_id,
            max_inline_chars=options.max_inline_chars,
        )

        # 6) Topic：初始状态就写一份 topic.json，前端一开始就有东西可展示。
        topic = TopicManager(ticket, path=recorder.run_dir / "topic.json")

        # 7) 工具会话：先按 Skill 挑初始业务工具，再挂平台工具。
        session = ToolSession.create(registry.all(), skill.initial_tools)
        platform_tools = build_platform_tools(session, evidence)
        session.add_platform_tools(platform_tools)

        # 8) 上下文：系统提示词 + Skill；每轮请求前再由 transform 钩子裁剪。
        context = build_initial_context(ticket, skill=skill)
        transform = make_transform_context(
            context=context,
            ticket=ticket,
            topic_provider=topic.topic_dict,
            budget=options.context_budget,
        )

        processor = EvidenceProcessor(
            store=evidence,
            skip_tool_names=set(PLATFORM_TOOL_NAMES),
        )

        config = AgentLoopConfig(
            model=model_id,
            max_turns=options.max_turns,
            tool_timeout_s=options.tool_timeout_s,
            # 每轮模型请求前重新取一次可见工具，搜索加载的新工具下一轮生效。
            tools_provider=session.visible_specs,
            transform_context=transform,
            after_tool_call=processor.after_tool_call,
        )

        async def emit(event: AgentEvent) -> None:
            """事件出口：先落盘，再交给调用方，最后尝试更新 Topic。

            顺序很重要：Topic 更新要用到 `tool_execution_end` 里的证据编号，
            所以必须等 after_tool_call 已经把结果处理完（事件里已经带 meta）。
            """
            await recorder.on_event(event)
            if on_event is not None:
                on_event(event)

            if isinstance(event, ToolExecutionEnd):
                topic_event = topic.update_from_event(event)
                if topic_event is not None:
                    # 递归调用自己，让 topic_updated 也写进 events.jsonl 并打印。
                    # 它不会再次进入这个分支，所以不会无限递归。
                    await emit(topic_event)

        result = await run_agent_loop(
            prompts=[build_user_message(ticket)],
            context=context,
            config=config,
            emit=emit,
            stream_fn=recorder.wrap_stream_fn(model.stream),
            cancellation=(
                cancellation if cancellation is not None else CancellationToken()
            ),
        )

        # 9) 运行结束：把 MCP 连接信息、能力使用情况、最终 Topic、证据数量
        #    一并写进 run.json。这样事后能确认这次调查到底连了哪个 MCP Server、
        #    用了哪种传输和协议版本。
        recorder.finish(
            result,
            capabilities={
                "mcp": mcp_info.to_dict(),
                "skill": skill.declaration(),
                "initial_visible_tools": [
                    *session.initial_names,
                    *sorted(PLATFORM_TOOL_NAMES),
                ],
                "final_visible_tools": session.visible_names(),
                "loaded_by_search": list(session.loaded_by_search),
                "searches": list(session.searches),
            },
            topic=topic.topic_dict(),
            evidence_count=evidence.count,
        )

        return RunOutcome(
            result=result,
            recorder=recorder,
            skill=skill,
            session=session,
            topic=topic,
            evidence=evidence,
            context=context,
            mcp=mcp_info,
        )
