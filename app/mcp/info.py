"""MCP 连接摘要的公共数据结构。

## 为什么要单独放一个文件

本项目现在有两条 MCP 客户端路线：

1. `app/mcp/client.py`：运行时默认使用，底层是官方 Python SDK；
2. `app/mcp/pi_style/`：教学对照使用，底层是我们按 Pi 的思路手写的
   JSON-RPC 状态机。

两条路线实现不同，但“连接成功以后要告诉外面什么信息”应该完全一样：
传输方式、服务端名字和版本、协议版本、工具清单、HTTP 会话 id。
如果各自定义一个类，`AgentRunner`、CLI、测试和前端就会出现两套字段名，
对比时很容易看花眼。

所以把 `McpClientInfo` 放在这里，两边共用。它只装数据，不依赖官方 SDK，
也不依赖任何一个传输实现：

```python
info = McpClientInfo(
    transport="stdio",
    endpoint="/path/to/python -m app.mcp.server",
    server_name="support-platform-dba-mcp",
    server_version="1.30.0",
    protocol_version="2025-11-25",
    instructions="...",
    capabilities={"tools": {"listChanged": False}},
    tools=["slow_log_query", ...],
)
```

## Python 语法提示

- `@dataclass(slots=True)` 会自动生成 `__init__`、`__repr__`、`__eq__`；
  `slots=True` 则限制实例只能有这里声明的属性，写错字段名会立即报错。
- `str | None` 是 Python 3.10 的联合类型写法，表示“字符串或空值”。
- `dict[str, Any]` 表示键是字符串、值暂时可以是任意 JSON 类型的字典。
- `to_dict()` 返回普通字典，因为 `json.dumps()` 不认识 dataclass 对象；
  写 `run.json` 之前必须转换一次。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class McpClientInfo:
    """一次 MCP 连接的可序列化摘要。

    字段含义：
    - `transport`：`stdio`、`streamable-http` 或 `sse`；
    - `endpoint`：实际连接地址，或 stdio 的启动命令；
    - `server_name` / `server_version`：`initialize` 响应里的 `serverInfo`；
    - `protocol_version`：协商后的 MCP 协议版本，例如 `2025-11-25`；
    - `instructions`：服务端给出的使用说明，可能为空；
    - `capabilities`：服务端声明的能力，例如是否支持工具列表变化；
    - `tools`：`tools/list` 返回的工具名；
    - `session_id`：Streamable HTTP 的 `Mcp-Session-Id`，stdio 和 SSE 为 None。
    """

    transport: str
    endpoint: str
    server_name: str
    server_version: str
    protocol_version: str
    instructions: str | None
    capabilities: dict[str, Any]
    tools: list[str]
    session_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """转成 JSON 可序列化字典。

        例子：写进 `run.json` 后长这样：

        ```json
        {
          "transport": "stdio",
          "server": {"name": "support-platform-dba-mcp", "version": "1.30.0"},
          "protocol_version": "2025-11-25",
          "tools": ["slow_log_query", "execution_plan"],
          "protocol": "JSON-RPC 2.0"
        }
        ```

        `list(self.tools)` 复制一份列表，避免调用方拿到内部列表后随手修改，
        导致同一份连接信息在不同地方显示不一致。
        """
        return {
            "transport": self.transport,
            "endpoint": self.endpoint,
            "server": {"name": self.server_name, "version": self.server_version},
            "protocol_version": self.protocol_version,
            "instructions": self.instructions,
            "capabilities": self.capabilities,
            "tools": list(self.tools),
            "session_id": self.session_id,
            "protocol": "JSON-RPC 2.0",
        }
