"""Skill 的加载与声明。

## Skill 是什么

Skill 不是“另一套代码”，而是一份给模型看的调查方法说明 + 初始工具清单。
例子：`slow-sql` Skill 告诉模型“先查慢日志，再按指纹查执行计划，最后补监控和锁等待”，
并声明一开始只开放 `slow_log_query` 一个业务工具；其它工具通过 `search_tools` 按需加载。

这样做的好处：
- 系统提示词里只放“当前任务真正需要的方法”，不用把所有 DBA 知识都塞进去；
- 工具可以按 Skill 装配，不同工单类型以后可以挂不同 Skill；
- Skill 是普通文件，改调查流程不需要改 Python 代码。

## 文件格式

```text
skills/slow-sql/
  skill.json   名称、描述、初始工具、正文文件名
  SKILL.md     给模型看的调查方法正文
```

`skill.json` 例子：

```json
{
  "name": "slow-sql",
  "label": "慢 SQL 调查",
  "version": "1",
  "description": "面向 MySQL 慢 SQL 工单的只读调查流程。",
  "initial_tools": ["slow_log_query"],
  "content_file": "SKILL.md"
}
```

## 实现说明

- `Path(__file__).resolve().parents[2]`：从 `app/harness/skills.py` 往上数到项目根目录，
  再拼 `skills`。这样不管从哪个工作目录启动，都能找到 Skill 文件。
- 正则 `^[a-z0-9][a-z0-9-]*$` 限制 Skill 名称只能用小写字母、数字和连字符。
  例子：`slow-sql` 合法；`../secret`、`慢sql` 都不合法，直接拒绝，避免路径穿越。
- `dataclass(slots=True)` 只装数据，不装行为；`declaration()` 负责转成可 JSON 的字典。
- `field(default_factory=list)` 用来给 `initial_tools` 一个独立空列表。
  如果写成 `= []`，所有 Skill 实例会共用同一个列表，改一个会影响另一个。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# 项目根目录：app/harness/skills.py -> app/harness -> app -> 项目根。
SKILLS_ROOT = Path(__file__).resolve().parents[2] / "skills"

# 默认加载的 Skill。第 2 步的演示工单是慢 SQL，所以默认用它。
DEFAULT_SKILL_NAME = "slow-sql"

# Skill 名称只允许小写字母、数字、连字符，且必须以字母或数字开头。
_SKILL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


class SkillNotFound(LookupError):
    """Skill 不存在，或者名称不合法。

    继承 `LookupError` 表示“按名字查不到”。例子：CLI 传了 `--skill nope`，
    这里会给出明确错误，而不是悄悄加载一个空 Skill 继续跑。
    """


@dataclass(slots=True)
class Skill:
    """一个已加载的 Skill。

    字段说明：
    - `content`：给模型看的 Markdown 正文；
    - `initial_tools`：一开始就开放的业务工具名；
    - `version`：Skill 自己的版本，方便以后对比行为变化。
    """

    name: str
    label: str
    description: str
    content: str
    initial_tools: list[str] = field(default_factory=list)
    version: str = "1"

    def declaration(self, include_content: bool = True) -> dict[str, Any]:
        """转成可以发给前端或写进运行记录的字典。

        `include_content=True` 时带上正文，前端的能力目录页面可以直接展示；
        如果只想展示标题和描述，传 `False` 可以减少体积。
        """
        data: dict[str, Any] = {
            "name": self.name,
            "label": self.label,
            "version": self.version,
            "description": self.description,
            "initial_tools": list(self.initial_tools),
        }
        if include_content:
            data["content"] = self.content
        return data

    def system_section(self) -> str:
        """生成拼进系统提示词的片段。

        为什么把 Skill 放在系统提示词而不是用户消息：
        调查方法是平台给的稳定规则，不是用户临时输入；放在系统消息里，
        模型更容易把它当成“必须遵守的流程”，也不会和工单正文混在一起。
        """
        tools = "、".join(self.initial_tools) or "无"
        return (
            f"## 当前 Skill：{self.label}（{self.name} v{self.version}）\n\n"
            f"{self.description}\n\n"
            f"初始可用业务工具：{tools}。需要其它工具时，先调用 `search_tools` 搜索；"
            "搜索命中并加载后，工具会在下一轮可用。搜索本身不授予权限，"
            "没有加载的工具无法执行。\n\n"
            f"{self.content.strip()}"
        )


def load_skill(name: str = DEFAULT_SKILL_NAME, root: Path | str = SKILLS_ROOT) -> Skill:
    """按名称加载一个 Skill。

    例子：`load_skill("slow-sql")` 会读取 `skills/slow-sql/skill.json` 和对应的
    `SKILL.md`，返回一个 `Skill` 对象。文件缺失或格式不对时直接抛错，
    避免模型拿着一份空方法说明去“自由发挥”。
    """
    if not _SKILL_NAME_RE.match(name):
        raise SkillNotFound(f"Skill 名称不合法：{name}")

    skill_dir = Path(root) / name
    declaration_path = skill_dir / "skill.json"
    if not declaration_path.exists():
        raise SkillNotFound(f"Skill 不存在：{name}（缺少 {declaration_path}）")

    try:
        data = json.loads(declaration_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Skill 声明不是合法 JSON：{declaration_path}（{exc}）") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Skill 声明顶层必须是对象：{declaration_path}")

    # 正文文件名只允许“文件名”，不允许带目录。
    # 例子：`SKILL.md` 合法；`../secret.md` 会被拒绝，避免读到 Skill 目录之外。
    content_file = str(data.get("content_file", "SKILL.md"))
    if Path(content_file).name != content_file:
        raise ValueError(f"Skill 正文文件名不合法：{content_file}")
    content_path = skill_dir / content_file
    if not content_path.exists():
        raise SkillNotFound(f"Skill 正文不存在：{content_path}")

    initial_tools = data.get("initial_tools", [])
    if not isinstance(initial_tools, list) or not all(
        isinstance(item, str) for item in initial_tools
    ):
        raise ValueError(f"Skill initial_tools 必须是字符串列表：{declaration_path}")

    return Skill(
        name=str(data.get("name") or name),
        label=str(data.get("label") or name),
        description=str(data.get("description", "")),
        content=content_path.read_text(encoding="utf-8"),
        initial_tools=list(initial_tools),
        version=str(data.get("version", "1")),
    )


def list_skills(root: Path | str = SKILLS_ROOT) -> list[Skill]:
    """列出目录下所有带 `skill.json` 的 Skill，按名称排序。

    例子：`skills/` 下同时有 `slow-sql` 和 `lock-wait` 时，
    返回两个 Skill，前端能力目录可以一次展示出来。
    """
    root_path = Path(root)
    if not root_path.exists():
        return []

    skills: list[Skill] = []
    for child in sorted(root_path.iterdir()):
        if child.is_dir() and (child / "skill.json").exists():
            skills.append(load_skill(child.name, root=root_path))
    return skills
