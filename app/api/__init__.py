"""HTTP 接口层：FastAPI 应用、任务服务、SQLite 存储和配置。

模块划分：
- `settings.py`：环境变量和默认配置；
- `store.py`：任务元信息持久化；
- `service.py`：任务生命周期、后台执行、事件/证据/报告查询；
- `main.py`：路由、依赖注入、异常到 HTTP 状态码的映射。
"""
