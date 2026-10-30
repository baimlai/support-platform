"""模型、执行记录等具体实现。

## 适配器层的职责

适配器负责把外部世界翻译成内核认识的形状：
- `mock_model` 把“固定剧本”翻译成助手流式事件；
- `deepseek_model` 把 DeepSeek 的 OpenAI 兼容流翻译成助手事件；
- `run_recorder` 把运行事件翻译成磁盘上的 JSONL/JSON/Markdown。

DBA 工具不在这一层：它们是独立 MCP Server 里的业务实现，见 `app/mcp/dba_tools.py`
和 `app/mcp/server.py`。适配层只负责模型、记录等外部依赖，工具发现和调用走 MCP
客户端（`app/mcp/client.py` 或 `app/mcp/pi_style/`）。

它们都依赖 `app.runtime.contracts` 里的协议，但内核不 import 它们。
这就是依赖倒置：内核定义“我需要什么形状”，适配器来满足，而不是内核去认识每个外部系统。

## 导入方向

如果哪天在 `app/runtime/` 里看到 `import app.adapters...`，通常说明分层被破坏了。
可以用一个简单的 grep 检查：

    grep -R "app.adapters" app/runtime

第 2 步接真实数据库/监控时，也建议先写适配器，再把适配器注入 harness，
不要在业务逻辑里直接写 `pymysql.connect(...)` 或 `requests.get(...)`。
"""
