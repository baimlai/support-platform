"""把一次 Agent 运行完整写到磁盘。

第 1 步先落本地文件，原因：
- 失败场景需要事后看清楚“模型收到了什么、工具返回了什么、为什么这样总结”；
- 第 3 步的 HTTP 接口需要事件序列和报告，这里先把数据形状固定下来；
- 现在不引入数据库，减少运行依赖，目录可以直接用文本工具查看。

一个运行目录的样子：
    var/runs/20261029-183000-a1b2/
        run.json             运行元信息，开始和结束时都会更新
        events.jsonl         运行事件，每行一个，带自增 seq
        model_requests.jsonl 每次真正发给模型的输入
        model_responses.jsonl 模型返回的每个流式事件
        messages.json        运行结束时的完整消息历史
        report.md            最后一条助手文本（没有就写失败说明）

注意：这里记录的是“模型实际收到的消息”，不是完整上下文。第 2 步做有界上下文后，
两者会不一样，正好可以用这两个文件对照排查。

## 实现说明

- `Path(...) / "名字"` 用斜杠拼路径，跨平台安全，比 `os.path.join` 更短。
- `mkdir(parents=True, exist_ok=True)`：父目录不存在就一起建；目录已存在也不报错。
- `with path.open("a", encoding="utf-8") as file:` 是追加模式（append）。
  每次写一行，进程中途崩溃也不会丢掉已经写好的记录。
- `json.dumps(..., ensure_ascii=False) + "\n"` 就是 JSONL：一行一个 JSON 对象。
  它比“一个大 JSON 数组”更容易追加，也更容易用 `tail` 查看。
- `datetime.now(UTC).isoformat()` 生成带时区的时间，跨机器排查不会混淆本地时间。
- `secrets.token_hex(2)` 生成随机后缀，比 `random` 更适合需要唯一性的场景。
- `@staticmethod` 表示这个方法不依赖实例数据，用 `类名.方法名()` 调用即可。
"""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.runtime.contracts import (
    AgentEvent,
    AssistantStreamEvent,
    ModelRequest,
    StreamFn,
    ToolExecutionEnd,
    assistant_text,
    now_ms,
    to_jsonable,
)
from app.runtime.pi_reference.loop import LoopResult


def _utc_now() -> str:
    """返回带时区的 ISO 时间，例子：`2026-10-29T10:30:00.123456+00:00`。"""
    return datetime.now(UTC).isoformat()


def _new_run_id(ticket: dict[str, Any] | None) -> str:
    """生成运行 id：时间 + 工单号片段 + 随机后缀。

    例子：`20261029-183000-TCK-20261029-001-3f2a`。
    加随机后缀是为了防止同一秒内重复运行导致目录冲突。
    """
    stamp = time.strftime("%Y%m%d-%H%M%S")
    ticket_id = str((ticket or {}).get("ticket_id", "no-ticket"))
    # 这是“生成器表达式”，和列表推导式很像，但不会先建一个完整列表，
    # 而是边算边交给 `"".join(...)`，更省内存。`char.isalnum()` 判断字母或数字。
    # 只保留字母、数字和连字符，避免目录名里出现 `/` 或空格。
    safe_ticket = "".join(
        char for char in ticket_id if char.isalnum() or char in ("-", "_")
    )[:40]
    return f"{stamp}-{safe_ticket}-{secrets.token_hex(2)}"


class RunRecorder:
    """负责目录创建、事件追加和运行结果落盘。"""

    def __init__(
        self,
        run_root: Path | str = Path("var/runs"),
        ticket: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> None:
        # `run_id or _new_run_id(ticket)` 利用“空值短路”：
        # 传了非空 run_id 就用它；传 None/空字符串才生成新 id。
        self.run_id = run_id or _new_run_id(ticket)
        self.run_dir = Path(run_root) / self.run_id
        # parents=True 会连父目录一起创建；exist_ok=True 表示目录已存在时不报错。
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.events_path = self.run_dir / "events.jsonl"
        self.model_requests_path = self.run_dir / "model_requests.jsonl"
        self.model_responses_path = self.run_dir / "model_responses.jsonl"
        self.messages_path = self.run_dir / "messages.json"
        self.report_path = self.run_dir / "report.md"
        self.run_path = self.run_dir / "run.json"

        self._seq = 0
        self._request_seq = 0
        self._started_at = _utc_now()
        self._meta: dict[str, Any] = {
            "run_id": self.run_id,
            "created_at": self._started_at,
            "updated_at": self._started_at,
            "status": "running",
            "stop_reason": None,
            "turns": 0,
            "ticket": ticket,
            "error": None,
            "paths": {
                "events": self.events_path.name,
                "model_requests": self.model_requests_path.name,
                "model_responses": self.model_responses_path.name,
                "messages": self.messages_path.name,
                "report": self.report_path.name,
            },
        }
        self._write_meta()

    # ---------- 事件 ----------

    async def on_event(self, event: AgentEvent) -> None:
        """事件接收器，直接传给 `run_agent_loop(emit=...)`。

        `seq` 从 1 开始递增，前端轮询时可以用 `after_seq` 只取新增事件。
        """
        self._seq += 1
        data = self._event_data(event)
        # 第 2 步：把证据编号提到 data 顶层，前端不用再翻 result.meta。
        # 例子：`tool_execution_end.data.evidence_id == "ev-000001"`。
        if isinstance(event, ToolExecutionEnd):
            meta = event.result.meta
            data["evidence_id"] = meta.evidence_id if meta is not None else None
        record = {
            "seq": self._seq,
            "ts": now_ms(),
            "type": event.type,
            "turn": getattr(event, "turn", None),
            "data": data,
        }
        self._append_jsonl(self.events_path, record)

    @staticmethod
    def _event_data(event: AgentEvent) -> dict[str, Any]:
        """把事件转成 JSON 对象。

        `type` 已经放在记录顶层，所以从 `data` 里去掉，避免重复；
        `turn` 保留在 `data` 里，因为前端按事件类型读取时可以直接拿到，
        同时记录顶层也有一份，便于用 seq/turn 快速筛选。
        """
        data = to_jsonable(event)
        if not isinstance(data, dict):  # 理论上不会发生，防御一下
            return {"value": data}
        data.pop("type", None)
        return data

    # ---------- 模型输入输出 ----------

    def wrap_stream_fn(self, stream_fn: StreamFn) -> StreamFn:
        """给模型适配器包一层记录。

        为什么要包在这里而不是在模型里：
        `ModelRequest` 已经过 `transform_context` 和 `convert_to_llm`，记录它才是
        “模型真正看到的输入”；在模型内部记录容易漏掉平台层的裁剪。
        """

        # `wrapped` 是嵌套的异步生成器，它“包住”原来的 stream_fn。
        # 这种写法叫装饰器模式（这里用函数闭包实现）：不修改模型代码，
        # 就能在每次请求前后插入记录逻辑。
        async def wrapped(request: ModelRequest) -> AsyncIterator[AssistantStreamEvent]:
            self._request_seq += 1
            request_seq = self._request_seq
            self._append_jsonl(
                self.model_requests_path,
                {
                    "request_seq": request_seq,
                    "ts": now_ms(),
                    "model": request.model,
                    "metadata": to_jsonable(request.metadata),
                    "messages": to_jsonable(request.messages),
                    "tools": [tool.declaration() for tool in request.tools],
                },
            )

            try:
                async for event in stream_fn(request):
                    self._append_jsonl(
                        self.model_responses_path,
                        {
                            "request_seq": request_seq,
                            "ts": now_ms(),
                            "event": to_jsonable(event),
                        },
                    )
                    yield event
            except Exception as exc:
                # 模型流中途出错时先记一行，再把异常继续抛给内核处理。
                self._append_jsonl(
                    self.model_responses_path,
                    {
                        "request_seq": request_seq,
                        "ts": now_ms(),
                        "error": str(exc) or exc.__class__.__name__,
                    },
                )
                raise

        return wrapped

    # ---------- 结束 ----------

    def finish(
        self,
        result: LoopResult,
        *,
        capabilities: dict[str, Any] | None = None,
        topic: dict[str, Any] | None = None,
        evidence_count: int | None = None,
    ) -> None:
        """写入完整消息、最终报告和运行元信息。

        即使运行失败也要调用，保证目录里有一套可排查的记录。

        第 2 步新增的三个可选参数，用来回答“这次用了哪些能力”：
        - `capabilities`：加载的 Skill、初始/最终可见工具、搜索历史；
        - `topic`：运行结束时的调查 Topic；
        - `evidence_count`：一共保存了多少条证据。
        """
        messages = to_jsonable(result.context.messages)
        self.messages_path.write_text(
            json.dumps(messages, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        report = self._extract_report(result)
        self.report_path.write_text(report, encoding="utf-8")

        # 这里用字典做“映射表”，比写一长串 if/elif 更清楚。
        # `.get(key, default)` 的第二个参数表示“映射表里没有这个 key 时用什么值”。
        status = {
            "completed": "completed",
            "cancelled": "cancelled",
            "error": "failed",
            # 达到轮数上限属于非正常结束：没有拿到最终报告，按失败记录，
            # 但 stop_reason 仍然保留 max_turns，方便区分“报错”和“轮数用尽”。
            "max_turns": "failed",
        }.get(result.reason, "failed")

        self._meta.update(
            {
                "updated_at": _utc_now(),
                "status": status,
                "stop_reason": result.reason,
                "turns": result.turns,
                "error": result.error,
                "finished_at": _utc_now(),
            }
        )
        if capabilities is not None:
            self._meta["capabilities"] = capabilities
        if topic is not None:
            self._meta["topic"] = topic
            # TopicManager 固定把 topic.json 写在运行目录下，这里登记相对路径，
            # 第 3 步的 HTTP 接口可以直接按 run.json 找文件。
            self._meta["paths"]["topic"] = "topic.json"
        if evidence_count is not None:
            self._meta["evidence_count"] = evidence_count
            self._meta["paths"]["evidence"] = "evidence"
        self._write_meta()

    def _extract_report(self, result: LoopResult) -> str:
        """取最后一条有文本的助手消息作为报告。

        失败或达到轮数上限时在前面加一句说明，避免把半截过程当成最终结论。
        """
        # `reversed(...)` 从后往前遍历，不会复制列表。
        # 我们想找“最后一条有文本的助手消息”，所以倒着找最快。
        final_text = ""
        for message in reversed(result.context.messages):
            if getattr(message, "role", None) != "assistant":
                continue
            text = assistant_text(message)
            if text.strip():
                final_text = text
                break

        if result.reason == "completed" and final_text:
            return final_text
        if result.reason == "completed":
            return "# 运行结束\n\n本次运行正常结束，但模型没有产生文本报告。"

        reason_text = {
            "max_turns": "达到最大轮数，调查未完成",
            "cancelled": "运行被取消，调查未完成",
            "error": "运行出错，调查未完成",
        }.get(result.reason, f"运行结束（{result.reason}），调查未完成")
        header = f"> {reason_text}。"
        if result.error:
            header += f"\n>\n> 错误信息：{result.error}"
        return f"{header}\n\n{final_text}".strip() + "\n"

    # ---------- 文件工具 ----------

    def _write_meta(self) -> None:
        """覆盖写入 run.json。文件很小，直接覆盖比维护增量更新简单。"""
        self.run_path.write_text(
            json.dumps(self._meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
        """追加一行 JSON。

        说明：`@staticmethod` 说明这个方法不需要 `self`，只依赖传入的参数。
        用追加模式而不是把全部记录留在内存里，长运行也不会因为中途崩溃丢掉前面的记录。
        """
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
