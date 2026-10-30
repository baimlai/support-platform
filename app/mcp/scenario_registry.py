"""把多数据库场景工具注册到真实 MCP Server。

## 这个文件为什么单独存在

`server.py` 里的慢 SQL 工具是四个手写的 `async def`，每个工具都有自己的业务逻辑。
多数据库场景有 16 个工具，如果也一个个手写，重复代码会非常多：

```text
校验实例 -> 读取 dataset["tool_results"][name] -> 包装 CallToolResult
```

这 16 个工具真正的差异只有“名称、标题、说明、参数 Schema”，执行逻辑完全一样。
所以这里做两件事：

1. 根据 `dba_tools.SCENARIO_TOOL_DEFINITIONS` 动态生成 MCP 工具函数；
2. 在 `tools/list` 里仍然暴露真实的工具名、标题、说明和参数 JSON Schema。

## 动态函数为什么能生成正确 Schema

FastMCP 注册工具时会看 `inspect.signature(fn)`，再用 Pydantic 生成 JSON Schema。
普通函数的签名是写死的；这里给 `**kwargs` 函数手动设置 `__signature__`，
FastMCP 就会把 `instance_id`、`limit` 等参数当成真实参数来校验和展示。

这是 Python 里一个很实用的进阶写法：
- `inspect.Parameter` 描述“一个参数叫什么、什么类型、有没有默认值”；
- `inspect.Signature` 把多个参数按顺序组合起来；
- 把 Signature 赋给函数的 `__signature__`，其它库用 `inspect.signature()` 看时
  就会得到我们构造的签名。

对比教学：
- 手写 16 个函数最直观，但复制粘贴多、容易漏改；
- 完全动态注册代码少，但调试时要理解 `inspect`；
- 本项目选择动态注册，是因为 Schema 和工具说明已经集中放在静态声明里，
  测试会逐个比对 MCP `tools/list` 和静态目录，能及时发现不一致。

## 为什么工具函数仍然返回 CallToolResult

业务函数 `dba_tools.scenario_tool()` 返回平台自己的 `ToolOutcome`。
MCP 层必须把它翻译成协议标准的 `CallToolResult`：
- `content`：模型可读文本；
- `structuredContent.details`：Topic、报告和前端使用的摘要；
- `isError`：业务失败标记，不是 JSON-RPC 协议错误。
"""

from __future__ import annotations

import inspect
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field

from app.mcp import dba_tools


def _annotations() -> ToolAnnotations:
    """所有场景工具都是只读查询，使用同一组 MCP 注解。"""
    return ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )


def _to_call_tool_result(outcome: dba_tools.ToolOutcome) -> CallToolResult:
    """把业务结果翻译成 MCP 标准结果。

    这段逻辑与慢 SQL 工具使用的转换完全一致。单独保留一份，是为了让
    `scenario_registry.py` 不依赖 `server.py`（否则会形成循环导入）。
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


def _python_type(schema_type: str) -> type[Any]:
    """把 JSON Schema 类型转成 Python 类型。

    只支持当前工具用到的三种：
    - `integer` -> `int`；
    - `number` -> `float`；
    - 其它（主要是 `string`）-> `str`。

    例子：`limit` 是 integer，模型传 `"10"` 时 FastMCP/Pydantic 会校验并转成 10；
    传 `"abc"` 会得到明确的参数错误，而不是工具执行到一半才报错。
    """
    if schema_type == "integer":
        return int
    if schema_type == "number":
        return float
    return str


def _annotation_for(property_schema: dict[str, Any]) -> Any:
    """根据属性 Schema 创建 `Annotated[类型, Field(...)]`。

    `Field` 里的 `description` 会出现在 MCP `tools/list` 的 `inputSchema` 中；
    `ge` / `le` 对应 JSON Schema 的 `minimum` / `maximum`，由 Pydantic 真正执行。

    例子：
        {"type": "integer", "minimum": 1, "maximum": 50, "description": "条数"}
        -> Annotated[int, Field(ge=1, le=50, description="条数")]
    """
    python_type = _python_type(str(property_schema.get("type", "string")))
    field_kwargs: dict[str, Any] = {}
    description = property_schema.get("description")
    if description:
        field_kwargs["description"] = description
    minimum = property_schema.get("minimum")
    maximum = property_schema.get("maximum")
    if minimum is not None:
        field_kwargs["ge"] = minimum
    if maximum is not None:
        field_kwargs["le"] = maximum
    return Annotated[python_type, Field(**field_kwargs)]


def _build_signature(definition: dba_tools.BusinessToolDefinition) -> inspect.Signature:
    """根据静态 JSON Schema 反向构造 Python 函数签名。

    为什么参数都写成 `KEYWORD_ONLY`：
    FastMCP 调用工具时使用关键字参数，例如
    `fn(instance_id="...", limit=10)`。关键字参数不依赖顺序，工具增减参数时
    不会悄悄把第二个参数传到错误的位置。

    必填参数没有默认值；可选参数使用 Schema 里的 `default`。
    例子：
        `limit` 的 Schema 是 `{"type":"integer","default":10}`，
        生成的签名就是 `*, limit: int = 10`。
    """
    schema = definition.parameters
    properties = schema.get("properties") or {}
    required = set(schema.get("required") or [])

    parameters: list[inspect.Parameter] = []
    for name, property_schema in properties.items():
        if not isinstance(property_schema, dict):
            continue
        parameter = inspect.Parameter(
            name=name,
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=_annotation_for(property_schema),
            default=(
                inspect.Parameter.empty
                if name in required
                else property_schema.get("default")
            ),
        )
        parameters.append(parameter)
    return inspect.Signature(parameters=parameters)


def _make_tool_function(
    definition: dba_tools.BusinessToolDefinition,
    dataset: dict[str, Any],
) -> Any:
    """为一个工具声明创建真正的异步执行函数。

    返回的函数会被 FastMCP 注册。它本身只接收 `**kwargs`，但 `__signature__`
    让 FastMCP 看到完整参数列表；调用时 Pydantic 已经完成校验和类型转换。
    """

    async def scenario_tool_handler(**kwargs: Any) -> CallToolResult:
        try:
            outcome = dba_tools.scenario_tool(dataset, definition.name, kwargs)
        except dba_tools.ToolExecutionError as exc:
            # 业务异常（例如实例 ID 不在当前数据集）转成 isError=true，
            # 让模型看到原因后可以改正，而不是让整个 MCP 请求失败。
            outcome = dba_tools._failure(str(exc))
        return _to_call_tool_result(outcome)

    # `__name__` 是 Python 调试和日志里显示的函数名。
    # 动态函数默认都叫 `scenario_tool_handler`，这里改成真实工具名，
    # 堆栈和测试输出更容易读。
    scenario_tool_handler.__name__ = definition.name
    scenario_tool_handler.__doc__ = definition.description
    scenario_tool_handler.__signature__ = _build_signature(definition)  # type: ignore[attr-defined]
    return scenario_tool_handler


def register_scenario_tools(
    server: FastMCP,
    dataset: dict[str, Any],
) -> list[str]:
    """注册当前数据集支持的多数据库工具，返回注册的工具名。

    数据集格式：

    ```json
    {
      "tool_results": {
        "pg_activity": {...},
        "pg_table_stats": {...}
      }
    }
    ```

    注册规则：
    - 只注册 `tool_results` 里真实存在结果的工具；
    - 工具名必须在静态目录里，防止数据文件写错名字却悄悄生成一个假工具；
    - 慢 SQL 的四个工具不在 `tool_results` 里，由 `server.py` 单独注册；
    - 返回注册列表，测试和调试时可以确认“这次到底暴露了哪些工具”。
    """
    tool_results = dataset.get("tool_results")
    if not isinstance(tool_results, dict) or not tool_results:
        return []

    definitions = {item.name: item for item in dba_tools.SCENARIO_TOOL_DEFINITIONS}
    legacy_names = {item.name for item in dba_tools.LEGACY_BUSINESS_TOOL_DEFINITIONS}
    registered: list[str] = []

    for name in tool_results:
        if name in legacy_names:
            raise ValueError(
                f"数据集 tool_results 里出现了慢 SQL 专用工具 {name}；"
                "请使用它在 server.py 里的专门实现，不要走通用场景执行器。"
            )
        definition = definitions.get(name)
        if definition is None:
            raise ValueError(
                f"数据集包含未知场景工具 {name}；"
                f"可用工具：{', '.join(sorted(definitions))}"
            )

        server.add_tool(
            _make_tool_function(definition, dataset),
            name=definition.name,
            title=definition.label,
            description=definition.description,
            annotations=_annotations(),
        )
        registered.append(name)

    return registered
