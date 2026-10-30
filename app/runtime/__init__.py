"""平台运行接口和内核。

业务代码从这里取 `contracts` 里的类型，以及 `run_agent_loop`。
不要让业务层直接 import `pi_reference.loop` 内部函数，这样以后替换内核时改动范围更小。

## 包的“公共出口”

`__all__` 是一个字符串列表，用来声明“这个包希望外部使用哪些名字”。
它主要影响 `from app.runtime import *`，也是给读代码的人看的契约：
看到这里只有 5 个名字，就知道外部不该依赖包里的其它内部实现。

注意：`__all__` 不会阻止别人导入其它名字，它只是约定和提示。
真正的强隔离要靠代码审查、目录结构和 lint 规则。
"""

from app.runtime.contracts import AgentContext, AgentLoopConfig, CancellationToken
from app.runtime.pi_reference.loop import LoopResult, run_agent_loop

__all__ = [
    "AgentContext",
    "AgentLoopConfig",
    "CancellationToken",
    "LoopResult",
    "run_agent_loop",
]
