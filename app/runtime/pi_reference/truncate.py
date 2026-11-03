"""移植 Pi v1.5.0 core/tools/truncate.ts 的行数/UTF-8 字节双上限。

来源提交 f9a26365568127461c47a07a37dcb42c140a3ac5，MIT 许可见 NOTICE.md。
保留头部用于文件预览，保留尾部用于日志。字节上限与模型的字符预算不是同一单位。
差异：不移植终端显示宽度，只保留后端文本需要的字段。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class TruncationResult:
    """文本和截断原因一起返回，调用方就不会把片段误当成全文。"""

    content: str
    truncated: bool
    truncated_by: Literal["lines", "bytes"] | None
    total_lines: int
    total_bytes: int
    output_lines: int
    output_bytes: int
    last_line_partial: bool = False
    first_line_exceeds_limit: bool = False


def truncate_text(
    text: str,
    *,
    max_lines: int = 2000,
    max_bytes: int = 50 * 1024,
    tail: bool = False,
) -> TruncationResult:
    """取完整行；只有尾部的单行太长时，允许返回不破坏 UTF-8 的最后一段。

    例如中文“锁”等于 3 个 UTF-8 字节。encode 后按字节截断，再用 ignore
    丢掉边界上不足一个字符的字节，可以保证返回文本里没有半个汉字。
    """
    if max_lines < 1 or max_bytes < 1:
        raise ValueError("行数和字节上限必须大于 0")
    raw = text.encode("utf-8")
    lines = text.split("\n") if text else []
    if text.endswith("\n"):
        lines.pop()
    if len(lines) <= max_lines and len(raw) <= max_bytes:
        return TruncationResult(text, False, None, len(lines), len(raw), len(lines), len(raw))
    chosen: list[str] = []
    used = 0
    by: Literal["lines", "bytes"] = "lines"
    partial = False
    first_too_large = bool(lines) and len(lines[0].encode("utf-8")) > max_bytes
    for line in reversed(lines) if tail else lines:
        if len(chosen) >= max_lines:
            break
        encoded = line.encode("utf-8")
        size = len(encoded) + bool(chosen)
        if used + size > max_bytes:
            by = "bytes"
            if tail and not chosen:
                chosen.append(encoded[-max_bytes:].decode("utf-8", errors="ignore"))
                partial = True
            break
        chosen.append(line)
        used += size
    if tail:
        chosen.reverse()
    content = "\n".join(chosen)
    return TruncationResult(
        content,
        True,
        by,
        len(lines),
        len(raw),
        len(chosen),
        len(content.encode("utf-8")),
        last_line_partial=partial,
        first_line_exceeds_limit=first_too_large,
    )
