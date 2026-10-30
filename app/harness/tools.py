"""工具目录：本轮只做“按名字查找”和“导出声明”。

第 2 步会在这里加关键词搜索、适用场景和按需加载。现在保持简单：
- 工具定义用 `ToolSpec`；
- 目录只负责保存和查找，不负责执行；
- 执行由内核调用 `ToolSpec.handler`，权限和结果处理以后在工具网关里加。

## 实现说明

- 类里以 `_` 开头的属性（如 `self._tools`）是“内部实现”，外部不应直接依赖。
- `dict[str, ToolSpec]` 是带类型的字典注解：键是字符串，值是 ToolSpec。
  它不影响运行，但 IDE 和类型检查工具能据此提示错误。
- `list(...)`、`[... for ... in ...]` 会创建新列表。返回副本可以防止调用方
  无意中改动目录内部的数据。
- `raise ValueError(...)` 用于“调用方给错了参数”，比返回 None 更容易在启动时发现问题。
"""

from __future__ import annotations

from app.runtime.contracts import ToolSpec


class ToolExecutionError(Exception):
    """工具执行失败的通用异常。

    工具可以抛这个异常，也可以抛普通异常。内核都会转成 `is_error=True` 的工具结果，
    模型看到的是错误说明，而不是整个任务崩掉。
    例子：模拟监控接口超时时抛
    `ToolExecutionError("实例监控接口超时（模拟）")`。
    """


class ToolRegistry:
    """保存本次可用的工具。

    为什么不让内核直接拿一个 list：
    以后要加搜索、权限过滤、按 Skill 装配工具时，目录是合适的扩展点；
    内核只通过 `AgentContext.tools` 拿到最终列表，不需要知道这些规则。
    """

    def __init__(self, tools: list[ToolSpec]) -> None:
        # `self` 指向正在创建的这个实例。给 `self._tools` 赋值后，
        # 同一个实例的其它方法都能通过 `self._tools` 访问它。
        # 用字典保存，按名字查找是 O(1)，也顺便防止同名工具被悄悄注册两次。
        self._tools: dict[str, ToolSpec] = {}
        for tool in tools:
            if tool.name in self._tools:
                raise ValueError(f"工具名重复：{tool.name}")
            self._tools[tool.name] = tool

    def all(self) -> list[ToolSpec]:
        """返回全部工具，顺序按注册顺序。"""
        return list(self._tools.values())

    def get(self, name: str) -> ToolSpec | None:
        """按名字取工具；不存在返回 None。

        说明：`dict.get(key)` 在键不存在时返回 None，不会像 `dict[key]` 那样抛 KeyError。
        适合“允许查不到”的场景；如果查不到属于错误，就应该用 `dict[key]` 让问题尽早暴露。
        """
        return self._tools.get(name)

    def names(self) -> list[str]:
        """返回工具名列表，CLI 展示和调试时用。

        `list(self._tools.keys())` 会复制一份键列表。直接返回 `self._tools.keys()` 的话，
        调用方拿到的是“实时视图”，目录一变它也跟着变，容易产生意外。
        """
        return list(self._tools.keys())

    def declarations(self) -> list[dict]:
        """返回可以发给模型或写入执行记录的工具声明。"""
        return [tool.declaration() for tool in self._tools.values()]
