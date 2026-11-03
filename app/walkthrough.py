"""可运行的 query 调用链示例：一张工单、运行中纠正、结束后的追问。

运行：python -m app.walkthrough --output outputs/query-walkthrough
工具经过真实 MCP，数据与模型使用 mock。它展示消息怎样进入每轮上下文，
不声称模拟模型能理解任意补充要求。HTTP 追加对话接口尚未提供，这里用 Agent API。
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from app.harness.runner import AgentRunner, RunOptions
from app.runtime.contracts import (
    AgentEvent,
    MessageEnd,
    ToolExecutionEnd,
    UserMessage,
    to_jsonable,
)
from app.runtime.pi_reference.agent import Agent
from app.runtime.pi_reference.compaction import CompactionSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


async def run_walkthrough(output: Path, query: str) -> Path:
    """把执行产物保留在指定目录；摘要/Topic/证据和真实模型输入都可以逐轮查看。"""
    ticket = json.loads((PROJECT_ROOT / "fixtures/tickets/slow_sql_001.json").read_text(encoding="utf-8"))
    # 用用户 query 替换描述，实例/时间范围沿用明确标注 mock 的样例。
    ticket["description"] = query
    ticket["source"] = "walkthrough_query"
    active: Agent | None = None
    queued = False
    timeline = []

    def attach(agent: Agent) -> None:
        """Runner 装配好 Agent 后回调；nonlocal 允许给外层函数的变量赋值。"""
        nonlocal active
        active = agent

    def observe(event: AgentEvent) -> None:
        """事件回调模拟两次用户输入；先排队，模型请求前才消费。"""
        nonlocal queued
        if isinstance(event, MessageEnd) and isinstance(event.message, UserMessage):
            timeline.append({"kind": "user_message", "turn": event.turn, "text": event.message.content})
        elif isinstance(event, ToolExecutionEnd):
            timeline.append(
                {
                    "kind": "tool_result",
                    "turn": event.turn,
                    "tool": event.tool_name,
                    "is_error": event.is_error,
                    "evidence": event.result.meta.evidence_id if event.result.meta else None,
                }
            )
            if not queued and active is not None:
                active.steer(UserMessage(content="补充：工具失败和无数据要单独说明，保留证据编号。"))
                active.follow_up(UserMessage(content="追问：请再给我一份验证步骤。"))
                queued = True
        elif event.type in ("context_compacted", "model_retry"):
            timeline.append(to_jsonable(event))

    outcome = await AgentRunner(
        RunOptions(
            ticket=ticket,
            run_root=output,
            model_delay_s=0,
            compaction=CompactionSettings(enabled=True, trigger_chars=4000, keep_recent_messages=2),
        )
    ).run(on_event=observe, on_agent=attach)
    requests = [
        json.loads(line)
        for line in outcome.recorder.model_requests_path.read_text(encoding="utf-8").splitlines()
    ]
    walkthrough = {
        "mock": True,
        "query": query,
        "status": outcome.result.reason,
        "turns": outcome.result.turns,
        "run_dir": str(outcome.recorder.run_dir),
        "timeline": timeline,
        "model_requests": [
            {
                "request_seq": row["request_seq"],
                "turn": row["metadata"]["turn"],
                "purpose": row["metadata"]["purpose"],
                "tools": [tool["name"] for tool in row["tools"]],
                "budget": row["metadata"].get("context_budget"),
            }
            for row in requests
        ],
        "note": "同一次调用中的多轮取证，加 steering 和 follow-up；不是 HTTP 跨任务续聊或重启恢复。",
    }
    path = outcome.recorder.run_dir / "walkthrough.json"
    path.write_text(json.dumps(walkthrough, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def main() -> None:
    """命令行只解析输入和启动事件循环；业务装配仍走共用 Runner。"""
    parser = argparse.ArgumentParser(description="运行带补充输入和追问的完整 query 调用链")
    parser.add_argument("--output", type=Path, default=Path("outputs/query-walkthrough"))
    parser.add_argument("--query", default="订单查询从 10 点开始变慢，请调查原因，说明证据和验证步骤。")
    args = parser.parse_args()
    path = asyncio.run(run_walkthrough(args.output, args.query))
    print(f"调用链样例已生成：{path}")


if __name__ == "__main__":
    # 被 import 时不运行；只有 python -m app.walkthrough 才启动示例。
    main()
