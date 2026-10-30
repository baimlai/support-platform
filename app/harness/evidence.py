r"""工具原始结果的落盘与有界读取。

## 为什么要有这一层

真实 DBA 工具经常返回很大的 JSON：慢日志可能几百行，监控可能几千个点。
如果把这些内容原样塞进模型上下文，会有三个问题：
1. 上下文很快被撑爆，后面的关键消息反而被挤掉；
2. 模型注意力被大量重复数据稀释，容易漏掉真正重要的行；
3. 出错时无法回看“工具到底返回了什么”，因为记录里只剩被裁剪过的版本。

因此平台把工具结果分成两份：
- **原始结果**：完整写进 `var/runs/<run_id>/evidence/ev-000001.json`；
- **有界内容**：只把前一段预览和工具摘要放进模型上下文，并附带 `evidence_id`。

模型需要更多内容时，调用平台工具 `evidence_read` 按 `offset/limit` 分段读取。
这和前端“证据列表 + 分段预览”用的是同一份数据。

## 目录结构

```text
var/runs/<run_id>/
  evidence/
    index.jsonl        一行一条证据元信息，列表页只读它，不读全文
    ev-000001.json     第一条证据的完整内容
    ev-000002.json     ...
```

## 实现说明

- `re.compile(r"^ev-\d{6}$")`：证据编号只允许 `ev-` 加 6 位数字。
  这样既统一了格式，也挡住了 `../` 这类路径穿越写法；读取前还会再检查文件是否存在。
- `Path.write_text(..., encoding="utf-8")`：一次写完整个文件。证据文件写完就不会再改，
  所以不需要追加模式。
- `json.dumps(..., ensure_ascii=False)`：中文保持原样，方便直接打开文件排查。
- `dataclass(slots=True)`：证据元信息只是一个轻量数据盒子，用 slots 避免多出用不到的属性。
- 字符数一律用 `len(text)` 计算的是“字符数”，不是字节数，也不是精确 token 数。
  报告和接口里会明确写“字符”，不假装成 token 计数。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.runtime.contracts import (
    AfterToolCallContext,
    AfterToolCallResult,
    TextContent,
    ToolResultMeta,
    to_jsonable,
)

# 默认放给模型的最大字符数。超过这个值就保存原文、只给预览。
DEFAULT_MAX_INLINE_CHARS = 4000

# `evidence_read` 默认一次读多少字符；上限再大也不超过 8000，避免一次读回太多。
DEFAULT_READ_LIMIT = 4000
MAX_READ_LIMIT = 8000

# 证据列表里展示的预览长度。列表页只用于“认得出是哪条证据”，不需要全文。
_LIST_PREVIEW_CHARS = 300

# 证据编号的合法格式：ev-000001、ev-000123 ...
_EVIDENCE_ID_RE = re.compile(r"^ev-\d{6}$")


class EvidenceNotFound(KeyError):
    """证据不存在，或者编号格式不合法。

    继承 `KeyError` 是因为它本质上就是“按 key 查不到”。
    例子：前端传了 `evidence_id=../../etc/passwd`，这里会直接拒绝，
    而不是拿这个字符串去拼文件路径。
    """


@dataclass(slots=True)
class EvidenceMeta:
    """证据的“目录卡片”，不包含全文。

    列表接口、前端证据列表、报告里的引用都只需要这些字段。
    全文放在单独的 JSON 文件里，按需读取。
    """

    evidence_id: str
    tool_call_id: str
    tool_name: str
    created_at: str
    size_chars: int
    is_error: bool
    truncated: bool
    preview: str

    def to_dict(self) -> dict[str, Any]:
        """转成可以直接 JSON 序列化的字典。"""
        return {
            "evidence_id": self.evidence_id,
            "tool_call_id": self.tool_call_id,
            "tool_name": self.tool_name,
            "created_at": self.created_at,
            "size_chars": self.size_chars,
            "is_error": self.is_error,
            "truncated": self.truncated,
            "preview": self.preview,
        }


def _utc_now() -> str:
    """返回带时区的 ISO 时间，例：`2026-10-30T02:30:00.123456+00:00`。"""
    return datetime.now(UTC).isoformat()


class EvidenceStore:
    """管理一次运行里的证据文件。

    一个运行一个目录，证据编号从 `ev-000001` 开始递增。
    编号只在本运行内有效，不同任务之间不会串。
    """

    def __init__(
        self,
        run_dir: Path | str,
        run_id: str,
        max_inline_chars: int = DEFAULT_MAX_INLINE_CHARS,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.run_id = run_id
        # 下限设成 1200：再小的话，连“被截断了，请读证据”的说明都放不下，
        # 模型会看到一段没法理解的残片。上限不设死，调用方可以按上下文预算调整。
        self.max_inline_chars = max(1200, int(max_inline_chars))
        self.evidence_dir = self.run_dir / "evidence"
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.evidence_dir / "index.jsonl"

        self._items: list[EvidenceMeta] = []
        self._seq = 0
        self._load_index()

    # ---------- 写入 ----------

    def save(
        self,
        tool_call_id: str,
        tool_name: str,
        content: str,
        details: Any,
        is_error: bool,
        duration_ms: int | None = None,
    ) -> EvidenceMeta:
        """保存一条工具原始结果，返回证据元信息。

        即使结果很短也会保存，原因是：
        - 前端证据列表需要统一展示“每个工具返回了什么”；
        - 报告里的每一条事实都应该能追到原始引用；
        - 失败结果同样值得保留，排查时不能只看到一句“失败了”。
        """
        self._seq += 1
        evidence_id = f"ev-{self._seq:06d}"
        size_chars = len(content)
        truncated = size_chars > self.max_inline_chars
        created_at = _utc_now()

        record = {
            "evidence_id": evidence_id,
            "run_id": self.run_id,
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "created_at": created_at,
            "size_chars": size_chars,
            "is_error": is_error,
            "truncated": truncated,
            "duration_ms": duration_ms,
            # `content` 是工具返回的原始文本；`details` 是工具自己的业务摘要。
            "content": content,
            "details": to_jsonable(details),
        }
        path = self.evidence_dir / f"{evidence_id}.json"
        path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        meta = EvidenceMeta(
            evidence_id=evidence_id,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            created_at=created_at,
            size_chars=size_chars,
            is_error=is_error,
            truncated=truncated,
            preview=content[:_LIST_PREVIEW_CHARS],
        )
        self._items.append(meta)
        self._append_index(meta)
        return meta

    def _append_index(self, meta: EvidenceMeta) -> None:
        """把目录卡片追加到 `index.jsonl`。

        为什么不每次列表都把所有证据文件读一遍：
        证据文件可能很大，列表页只关心编号、工具名、大小和预览；
        单独维护一份小索引，读起来更快，也不会把全文加载进内存。
        """
        with self.index_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(meta.to_dict(), ensure_ascii=False) + "\n")

    def _load_index(self) -> None:
        """进程重启后恢复已有证据编号，避免新证据覆盖旧文件。

        例子：同一个运行目录被重新打开，索引里已经有 `ev-000003`，
        下一条新证据必须从 `ev-000004` 开始。
        """
        if not self.index_path.exists():
            return
        for line in self.index_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            data = json.loads(line)
            meta = EvidenceMeta(
                evidence_id=str(data["evidence_id"]),
                tool_call_id=str(data.get("tool_call_id", "")),
                tool_name=str(data.get("tool_name", "")),
                created_at=str(data.get("created_at", "")),
                size_chars=int(data.get("size_chars", 0)),
                is_error=bool(data.get("is_error", False)),
                truncated=bool(data.get("truncated", False)),
                preview=str(data.get("preview", "")),
            )
            self._items.append(meta)
            # `ev-000007` 按 `-` 切开取后半段再转整数，得到 7。
            self._seq = max(self._seq, int(meta.evidence_id.split("-")[1]))

    # ---------- 读取 ----------

    def read(
        self,
        evidence_id: str,
        offset: int = 0,
        limit: int = DEFAULT_READ_LIMIT,
    ) -> dict[str, Any]:
        """按字符偏移读取一段证据内容。

        例子：证据总长 15000 字符，`offset=4000, limit=4000` 返回第 4001–8000 个字符，
        并把 `truncated=True`，告诉调用方后面还有内容。
        """
        self._validate_id(evidence_id)
        path = self.evidence_dir / f"{evidence_id}.json"
        if not path.exists():
            raise EvidenceNotFound(f"证据不存在：{evidence_id}")

        record = json.loads(path.read_text(encoding="utf-8"))
        content = str(record.get("content", ""))
        total_chars = len(content)

        # 参数做一次兜底：负数偏移按 0 处理，limit 限制在 1–8000。
        offset = max(0, int(offset))
        limit = max(1, min(MAX_READ_LIMIT, int(limit)))
        chunk = content[offset : offset + limit]

        return {
            "evidence_id": evidence_id,
            "tool_name": record.get("tool_name"),
            "created_at": record.get("created_at"),
            "offset": offset,
            "limit": limit,
            "total_chars": total_chars,
            "content": chunk,
            # 注意：这里的 truncated 表示“后面还有没读完的内容”，
            # 和证据列表里的 truncated（“当初放进上下文的预览被截断了”）不是同一个含义。
            "truncated": offset + limit < total_chars,
        }

    def list(self) -> list[dict[str, Any]]:
        """返回证据目录卡片列表，不读全文。"""
        return [meta.to_dict() for meta in self._items]

    def describe(self, evidence_id: str) -> dict[str, Any]:
        """返回单条证据的目录卡片。"""
        self._validate_id(evidence_id)
        for meta in self._items:
            if meta.evidence_id == evidence_id:
                return meta.to_dict()
        raise EvidenceNotFound(f"证据不存在：{evidence_id}")

    @property
    def count(self) -> int:
        """当前已保存多少条证据。"""
        return len(self._items)

    @staticmethod
    def _validate_id(evidence_id: str) -> None:
        """检查证据编号格式，挡住路径穿越。

        例子：`ev-000001` 合法；`../secret`、`ev-1/../x` 都不合法。
        只靠文件是否存在来判断不够，因为攻击者可以用 `..` 拼出别的路径。
        """
        if not _EVIDENCE_ID_RE.match(evidence_id):
            raise EvidenceNotFound(f"证据编号格式不合法：{evidence_id}")


class EvidenceProcessor:
    """把工具结果接进证据层的 `after_tool_call` 钩子。

    它做三件事：
    1. 保存完整原始文本；
    2. 把过大的内容换成“预览 + 摘要 + evidence_id”；
    3. 把平台元信息写进 `ToolResultMeta`，让记录器和前端知道证据编号。

    为什么用 `after_tool_call` 而不是包在每个工具里：
    工具只负责“查数据”，不应该关心上下文预算和证据文件；
    平台统一处理所有工具，失败结果也能一视同仁地保存。
    """

    def __init__(
        self,
        store: EvidenceStore,
        skip_tool_names: set[str] | None = None,
    ) -> None:
        self.store = store
        # 平台工具（search_tools、evidence_read）不产生业务证据，跳过。
        # 否则 `evidence_read` 读一次证据又生成一条新证据，会无限套娃。
        self.skip_tool_names = set(skip_tool_names or ())

    async def after_tool_call(
        self,
        context: AfterToolCallContext,
    ) -> AfterToolCallResult | None:
        """工具执行完后的统一处理。

        返回 `None` 表示“不改工具结果”；返回 `AfterToolCallResult` 表示
        按字段覆盖。这里会同时覆盖 content、details 和 meta。
        """
        if context.tool_call.name in self.skip_tool_names:
            return None

        raw_text = "".join(block.text for block in context.result.content)
        meta = self.store.save(
            tool_call_id=context.tool_call.id,
            tool_name=context.tool_call.name,
            content=raw_text,
            details=context.result.details,
            is_error=context.result.is_error,
            duration_ms=context.result.duration_ms,
        )

        bounded_details = _bound_details(
            context.result.details,
            max_chars=self.store.max_inline_chars,
            evidence_id=meta.evidence_id,
        )
        visible_text = _bound_content(
            raw_text=raw_text,
            details=bounded_details,
            meta=meta,
            max_inline_chars=self.store.max_inline_chars,
        )
        return AfterToolCallResult(
            content=[TextContent(text=visible_text)],
            details=bounded_details,
            meta=ToolResultMeta(
                evidence_id=meta.evidence_id,
                truncated=meta.truncated,
                raw_size_chars=meta.size_chars,
                max_inline_chars=self.store.max_inline_chars,
            ),
        )


def _bound_details(details: Any, max_chars: int, evidence_id: str) -> Any:
    """限制 `details` 的大小。

    `details` 是工具自己的业务摘要，通常很小；但平台不能假设工具一定守规矩。
    如果某个工具把整张表塞进 details，这里也会把它截断，并指向证据文件。

    例子：details 序列化后有 50000 字符，超过上限 2000，
    就返回一个小字典：`{"truncated": True, "size_chars": 50000, "preview": "...", ...}`。
    """
    if details is None:
        return None
    try:
        text = json.dumps(to_jsonable(details), ensure_ascii=False)
    except (TypeError, ValueError):
        # 极端情况：工具塞了一个无法 JSON 序列化的对象。
        # 不在这里报错，转成字符串后继续走有界处理，避免整个任务因为摘要而失败。
        text = str(details)
    limit = max(400, max_chars // 2)
    if len(text) <= limit:
        return details
    return {
        "truncated": True,
        "size_chars": len(text),
        "preview": text[: limit // 2],
        "evidence_id": evidence_id,
        "note": "details 过大，完整内容在证据文件里。",
    }


def _bound_content(
    raw_text: str,
    details: Any,
    meta: EvidenceMeta,
    max_inline_chars: int,
) -> str:
    """决定模型到底能看到什么。

    两种情况：
    - 原始内容没超限：原样返回，模型可以继续按原来的方式解析 JSON；
    - 原始内容超限：返回一个“说明信封”，里面有证据编号、原文大小、预览和摘要。
      模型看不到全文，但知道去哪里读。

    为什么不直接 `raw_text[:4000]`：
    直接切字符串会把 JSON 切成半截，模型可能误以为数据不完整或尝试补全。
    用结构化的信封明确告诉它“这是截断预览，完整内容在 ev-xxx”，更不容易误判。
    """
    if not meta.truncated:
        return raw_text

    # 预览给到上限的三分之一左右，剩下的空间留给摘要和说明。
    preview_chars = max(200, max_inline_chars // 3)
    envelope = {
        "truncated": True,
        "evidence_id": meta.evidence_id,
        "tool_name": meta.tool_name,
        "size_chars": meta.size_chars,
        "max_inline_chars": max_inline_chars,
        "preview": raw_text[:preview_chars],
        "summary": details,
        "next": (
            "完整原始结果已保存。需要更多内容时调用 "
            f'evidence_read(evidence_id="{meta.evidence_id}", offset=0, limit={DEFAULT_READ_LIMIT})。'
        ),
    }
    text = json.dumps(envelope, ensure_ascii=False, indent=2)
    if len(text) <= max_inline_chars:
        return text

    # 最后一道保险：如果摘要本身还是太大，就只保留最小说明。
    # 原则是“宁可少给，也不能把超大内容重新塞回上下文”。
    minimal = {
        "truncated": True,
        "evidence_id": meta.evidence_id,
        "size_chars": meta.size_chars,
        "max_inline_chars": max_inline_chars,
        "next": (
            f"完整内容见证据 {meta.evidence_id}，请调用 "
            f'evidence_read(evidence_id="{meta.evidence_id}", offset=0, '
            f"limit={DEFAULT_READ_LIMIT})。"
        ),
    }
    return json.dumps(minimal, ensure_ascii=False, indent=2)
