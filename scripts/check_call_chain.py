"""校验教学调用链的函数仍存在，并生成带最新行号的函数索引。

维护方式：改函数名/文件后更新 docs/call-chain.json，再运行本脚本。
AST 检查只能证明函数与调用点存在，无法证明动态 Hook 的真实绑定对象；
后者由 tests/test_runtime_extensions.py 的顺序和端到端测试验证。
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def definitions(path: Path) -> dict[str, ast.AST]:
    """遍历类和嵌套函数，保留全名，例如 AgentRunner._run_with_mcp.emit。"""
    result = {}

    def visit(body: list[ast.stmt], prefix: str = "") -> None:
        for item in body:
            if isinstance(item, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                name = prefix + item.name
                result[name] = item
                visit(item.body, name + ".")

    visit(ast.parse(path.read_text(encoding="utf-8")).body)
    return result


def validate() -> tuple[dict, list[str]]:
    """返回已校验的清单和错误列表；作为函数也便于以后接入 CI。"""
    spec = json.loads((ROOT / "docs/call-chain.json").read_text(encoding="utf-8"))
    indexed = {}
    errors = []
    for key, node in spec["nodes"].items():
        path = ROOT / node["path"]
        if not path.is_file():
            errors.append(f"{key}: 文件不存在 {node['path']}")
            continue
        defined = definitions(path)
        if node["function"] not in defined:
            errors.append(f"{key}: 函数不存在 {node['function']}")
            continue
        function = defined[node["function"]]
        node["line"] = function.lineno
        indexed[key] = function
    for edge in spec["edges"]:
        source, target = edge["from"], edge["to"]
        if source not in indexed or target not in indexed:
            errors.append(f"调用边引用未知函数：{source} -> {target}")
            continue
        site = edge["site"]
        calls = {
            item.func.id if isinstance(item.func, ast.Name) else item.func.attr
            for item in ast.walk(indexed[source])
            if isinstance(item, ast.Call) and isinstance(item.func, (ast.Name, ast.Attribute))
        }
        if site not in calls:
            errors.append(f"{source} 中找不到调用点 {site}，请复查到 {target} 的关系")
    return spec, errors


def main() -> None:
    """生成索引时自动取真实行号，避免代码一增行文档链接就过期。"""
    spec, errors = validate()
    if errors:
        raise SystemExit("\n".join(errors))
    lines = [
        "# Query 函数调用链索引",
        "",
        "由 scripts/check_call_chain.py 从 docs/call-chain.json 和 Python AST 生成。",
        "",
        "先读 [完整调用链导读](ARCHITECTURE.md)，再看 [时序图](ARCHITECTURE_DIAGRAMS.md)。",
        "",
        "## 函数、文件与职责",
        "",
        "| 标识 | 函数（点击打开源文件） | 主要做什么 | 来源 |",
        "|---|---|---|---|",
    ]
    for key, node in spec["nodes"].items():
        target = f"{ROOT / node['path']}:{node['line']}"
        lines.append(f"| {key} | [{node['function']}]({target}) | {node['role']} | {node['origin']} |")
    lines.extend(["", "## 调用关系", "", "Hook/回调边是动态绑定，不是文件间的直接 import。", ""])
    for edge in spec["edges"]:
        lines.append(f"- {edge['from']} → {edge['to']}：{edge['kind']}，调用点 `{edge['site']}(...)`。")
    path = ROOT / "outputs/QUERY_CALL_CHAIN.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"调用链通过：{len(spec['nodes'])} 个函数、{len(spec['edges'])} 条关系；索引 {path}")


if __name__ == "__main__":
    main()
