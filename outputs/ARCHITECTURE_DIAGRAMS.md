# Architecture Diagrams

这些图对应 [完整导读](ARCHITECTURE.md) 第 7 节。函数源码位置见 [可维护索引](QUERY_CALL_CHAIN.md)。
置信度均为高：入口来自路由/服务代码，动态 Hook 和多轮语义由行为测试及实际运行记录支持。

## 1. query 从提交到报告

```mermaid
sequenceDiagram
    participant U as 用户或前端
    participant H as main.create_task
    participant S as TaskService
    participant D as SQLite
    participant R as AgentRunner
    participant A as Agent与Loop
    participant M as MCP工具进程
    participant F as 运行目录
    U->>H: POST /api/tasks，ticket_text
    H->>S: create_task
    S->>S: _build_ticket，校验Skill和模型
    S->>D: TaskStore.create，queued
    S->>S: asyncio.create_task运行_run_task
    H-->>U: 202，task_id
    S->>R: await run
    R->>M: async with，initialize，tools/list
    R->>A: 装配后await Agent.prompt
    loop 多轮模型与工具取证
        A->>M: JSON-RPC tools/call
        M-->>A: 工具结果
        A->>F: 证据、事件、Topic、实际模型输入
    end
    A-->>R: LoopResult
    R->>F: RunRecorder.finish，完整历史与报告
    R->>M: 关闭会话与子进程
    R-->>S: RunOutcome
    S->>D: 更新终态
    U->>S: GET状态、事件、轮次、证据、报告
    S-->>U: 保存的运行结果
```

依据：main.py::create_task → service.py::create_task/_run_task → runner.py::run。
图中后台协程与 HTTP 响应分开；平台搜索和证据分页在本进程执行，不属于 MCP 跨进程箭头。
没有假设前端具体组件实现；当前核对范围是后端仓库。

## 2. 每一轮内部的函数调用

```mermaid
flowchart TD
    T["_run_turns：本轮开始"] --> P["prepare_request，可选"]
    P --> S["_stream_assistant_response"]
    S --> X["config.transform_context"]
    X --> C["ContextCompactor.transform，可选摘要"]
    C --> B["build_model_context：Topic与预算"]
    B --> V["convert_to_llm和_resolve_tools"]
    V --> R["with_model_retry.stream"]
    R --> D["RunRecorder.wrapped"]
    D --> M["模型适配器stream"]
    M --> A["最终AssistantMessage"]
    A --> Q{"含ToolCall？"}
    Q -->|有| E["_execute_tool_calls"]
    E --> K["_prepare_and_execute_tool"]
    K --> H["tool.handler：业务MCP或本地平台工具"]
    H --> O["after_tool_call：存证据与有界结果"]
    O --> I["ToolExecutionEnd：记录与更新Topic"]
    I --> J["ToolResultMessage加入完整历史"]
    J --> F["finish_turn和TurnEnd"]
    Q -->|无| F
    F --> N{"继续条件、队列、取消、轮数"}
    N -->|继续| Z["prepare_next_turn，可选"]
    Z --> T
    N -->|结束| END["AgentEnd和LoopResult"]
```

依据：loop.py::_run_turns/_stream_assistant_response/_execute_tool_calls。
摘要、准备 Hook 和重试都存在条件；不是每轮一定生成摘要或发生重试。
失败尝试经记录器保存，但最终只形成这一轮的助手消息；没有把整个工单重新执行。

## 3. 用户多轮和工具多轮的区别

```mermaid
sequenceDiagram
    participant U as 用户输入演示回调
    participant A as Agent队列
    participant L as _run_turns
    participant M as 模型
    participant T as 工具执行
    U->>A: prompt，原query
    A->>L: run_agent_loop
    L->>M: 第1轮
    M-->>L: 调slow_log_query
    L->>T: 执行并保存ev-000001
    U->>A: steer，补充失败与证据要求
    U->>A: follow_up，再给验证步骤
    A-->>L: 第2轮安全点取steering
    L->>M: 第2轮，包含补充用户消息
    M-->>L: 调evidence_read
    L->>T: 分页读取
    loop 第3到5轮，follow-up仍排队
        L->>M: 最新Topic与有界上下文
        M-->>L: 搜工具、计划、监控、锁
        L->>T: 执行工具与记录结果
    end
    L->>M: 第6轮
    M-->>L: 报告，无工具调用
    A-->>L: 自然停下后取follow-up
    L->>M: 第7轮，包含用户追问
    M-->>L: 模拟报告文本
    L-->>A: completed
```

依据：app/walkthrough.py 的事件回调和实际七轮记录；队列时机有专门测试。
mock 只证明消息进入上下文，不证明任意补充的语义都被正确执行。
该图使用内核 Agent API；现有 HTTP 接口尚未提供运行中追加对话路由。

## 4. Hook 为什么会调用到另一文件

```mermaid
flowchart LR
    R["Runner调用make_transform_context"] --> F["context.py创建async transform闭包"]
    F --> V["return transform：返回函数对象"]
    V --> C["AgentLoopConfig.transform_context保存对象"]
    C --> L["Loop每轮await config.transform_context"]
    L --> X["执行闭包：摘要视图、取最新Topic、预算裁剪"]
    X --> M["返回本轮消息列表给模型请求"]
```

闭包提前记住 context、ticket、topic_provider、budget 和 compactor；每轮另接收最新 messages 和 token。
依据：runner.py 的 config 构造、context.py::make_transform_context、loop.py 的显式调用。
before/after Hook、模型包装和订阅回调也使用函数作为值，但触发时机分别由各自调用点决定。

## 5. 模块依赖与进程边界

```mermaid
flowchart LR
    API["API/CLI"] --> H["Harness装配与DBA状态"]
    H --> A["模型/记录适配器"]
    H --> R["runtime公共契约与Pi移植"]
    A --> R
    H --> MC["MCP Client"]
    MC --> R
    MC -. "stdio或HTTP，JSON-RPC" .-> MS["FastMCP Server"]
    MS --> B["模拟DBA业务函数与fixtures"]
```

runtime 通过配置使用模型与工具函数，不 import DBA Topic、任务数据库或具体网络适配器。
MCP SDK 负责协议传输，平台适配负责结果归一化；api/harness/runtime 仍在同一个服务进程。
依据为实际 imports、Runner 的 async with 及 MCP 传输集成测试。
