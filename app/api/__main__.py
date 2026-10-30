"""用 `python -m app.api` 启动本地开发服务。

等价于：

```bash
uv run uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

默认只监听本机，避免把模拟服务意外暴露到局域网。
需要其它机器访问时，显式传 `--host 0.0.0.0` 或直接用 uvicorn 命令。
"""

from __future__ import annotations

import uvicorn


def main() -> None:
    """启动 uvicorn。"""
    uvicorn.run("app.api.main:app", host="127.0.0.1", port=8000, reload=False)


if __name__ == "__main__":
    main()
