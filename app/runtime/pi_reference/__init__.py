"""从 Pi 选择性移植的 Python 内核。

这里不是 Pi 官方原生 SDK。具体参考提交、对应源码和改动见 `docs/pi-mapping.md`。

## 为什么目录叫 `pi_reference`

“参考实现”意味着两件事：
1. 循环结构、事件顺序、工具调用生命周期尽量贴近 Pi，方便对照原项目学习；
2. 但它是本项目的 Python 重写，不是 Pi 的官方 Python SDK，也没有承诺 API 完全一致。

命名上保留 `pi_reference`，是为了以后有人问“这段逻辑从哪来的”时，能一眼找到出处，
而不是把移植代码伪装成完全自研。开源许可证和出处说明放在同目录的 `NOTICE.md`。

## `__all__` 与导入副作用

`from app.runtime.pi_reference.loop import ...` 会执行 `loop.py` 的顶层代码。
本项目顶层只做 import 和类型定义，没有连接数据库或启动后台任务，
所以 import 很快，也不会产生难以清理的状态。写微服务时也要保持这个习惯。
"""

from app.runtime.pi_reference.loop import LoopResult, ToolArgumentError, run_agent_loop

__all__ = ["LoopResult", "ToolArgumentError", "run_agent_loop"]
