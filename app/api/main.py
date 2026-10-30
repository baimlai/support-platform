"""FastAPI 接口层（第 3 步）。

## 接口分三类

1. **任务**：创建、查询、取消；
2. **过程材料**：事件分页/SSE、每轮实际模型输入、证据列表与分段读取；
3. **目录**：样例工单、Skill/工具/模型能力、最终报告。

## 为什么接口要“薄”

所有业务逻辑都在 `TaskService` 和 `AgentRunner` 里。接口层只做三件事：
- 校验请求参数（Pydantic）；
- 调用 service；
- 把异常转成合适的 HTTP 状态码。

这样以后换 Web 框架，或者加一个命令行/消息队列入口，不需要重写业务逻辑。

## 状态码约定

- `POST /api/tasks` → `202`：任务已受理，不代表已经跑完；
- `POST /api/tasks/{id}/cancel` → `202`：取消请求已收到，状态可能还是 running；
  已经终态时返回 `200`，按幂等处理；
- 未知任务、未知证据、未知轮次 → `404`；
- 请求体不合法（两个来源都没给、给了两个、字段类型不对）→ `422`；
- 选了 DeepSeek 但服务端没配 Key → `422`，`detail` 里说明要设置哪个环境变量。

## 实现说明

- `APIRouter(prefix="/api")` 把路由前缀集中在一处，下面写路径时不用重复 `/api`。
- `Depends(get_service)` 是 FastAPI 的依赖注入：每个请求自动调用 `get_service`，
  从 `request.app.state.service` 取同一个 TaskService。
- `StreamingResponse` 接收异步生成器，边生成边发给浏览器，适合 SSE。
- `JSONResponse` 用于需要动态状态码的场景（取消接口 200/202 二选一）。
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Query, Request
from fastapi import Path as ApiPath
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.api.service import (
    TERMINAL_STATUSES,
    InvalidTaskRequest,
    ReportNotReady,
    SampleNotFound,
    TaskNotFound,
    TaskService,
    TurnContextNotFound,
)
from app.api.settings import ApiSettings
from app.api.store import utc_now
from app.harness.evidence import EvidenceNotFound

router = APIRouter(prefix="/api")


class TaskCreateRequest(BaseModel):
    """创建任务的请求体。

    两种用法二选一：
    - `{"sample_id": "slow-sql-001"}`：用内置样例；
    - `{"ticket_text": "订单查询变慢...", "tenant_id": "tenant-demo"}`：自由文本。

    `extra="forbid"` 会拒绝拼错的字段，例如把 `ticket_text` 写成 `ticketText`。
    这样做的好处是联调时能立刻发现字段名不对，而不是任务跑起来才发现内容是空的。
    """

    model_config = ConfigDict(extra="forbid")

    sample_id: str | None = Field(default=None, max_length=120)
    ticket_text: str | None = Field(default=None, max_length=4000)
    tenant_id: str | None = Field(default=None, max_length=64)
    # 不传时使用样例工单自带的 `skill`，再退回服务端默认 `slow-sql`。
    # 为什么允许为空：多数据库样例由数据自己声明用 `dba-triage`，前端不需要
    # 为每个样例硬编码 Skill 名。
    skill: str | None = Field(default=None, min_length=1, max_length=64)
    # `None` 表示“用服务端默认值”，而不是强行写死 mock。
    # 这样部署时可以通过环境变量改默认模型，页面仍然可以按任务覆盖。
    model_provider: str | None = Field(
        default=None,
        max_length=32,
        description="模型来源：mock 或 deepseek；不传用服务端默认值",
    )
    model_name: str | None = Field(
        default=None,
        max_length=120,
        description="模型名，例如 deepseek-flash；不传用 provider 默认值",
    )
    max_turns: int = Field(default=10, ge=1, le=30)

    @model_validator(mode="after")
    def _exactly_one_source(self) -> TaskCreateRequest:
        """确保样例和自由文本“有且只有一个”。

        `bool(None)` 是 False，`bool("  ")` 是 True，所以这里先 `strip()`，
        避免用户传一串空格也被当成有效输入。
        """
        has_sample = bool(self.sample_id and self.sample_id.strip())
        has_text = bool(self.ticket_text and self.ticket_text.strip())
        if has_sample == has_text:
            raise ValueError("sample_id 和 ticket_text 必须且只能提供一个")
        return self


def get_service(request: Request) -> TaskService:
    """从应用状态里取 TaskService。

    FastAPI 对每个请求都会调用这个函数；`request.app` 就是当前 FastAPI 实例。
    """
    return request.app.state.service  # type: ignore[no-any-return]


# `Annotated[类型, 依赖]` 是 FastAPI 推荐的写法：
# 参数类型一眼可见，也避免把 `Depends(get_service)` 写在默认值里。
# 例子：`async def route(service: ServiceDep)`，FastAPI 会自动注入。
ServiceDep = Annotated[TaskService, Depends(get_service)]


@router.get("/health")
async def health(service: ServiceDep) -> dict[str, Any]:
    """健康检查，前端/部署脚本可以用它确认服务已经起来。

    `model` 是服务端默认模型，不代表某个具体任务的选择；
    每个任务的模型看任务快照里的 `model_provider` / `model_name`。
    """
    return {
        "status": "ok",
        "mock": True,
        "time": utc_now(),
        "model": service.default_model_info(),
    }


@router.get("/models")
async def list_models(service: ServiceDep) -> dict[str, Any]:
    """返回页面可选的模型来源和可用性。

    前端创建任务时，把选中项的 `provider` 填进 `model_provider`，
    把 `default_model` 或下拉框里的某个 `models` 项填进 `model_name`。
    """
    return service.list_models()


@router.get("/samples")
async def list_samples(
    service: ServiceDep,
) -> list[dict[str, Any]]:
    """返回可选的模拟工单样例。"""
    return service.samples()


@router.get("/capabilities")
async def capabilities(
    service: ServiceDep,
) -> dict[str, Any]:
    """返回 Skill 和工具目录，前端“能力”页面直接展示。"""
    return service.capabilities()


@router.post("/tasks", status_code=202)
async def create_task(
    payload: TaskCreateRequest,
    service: ServiceDep,
) -> dict[str, Any]:
    """创建后台调查任务，立刻返回任务快照。"""
    return await service.create_task(
        sample_id=payload.sample_id,
        ticket_text=payload.ticket_text,
        tenant_id=payload.tenant_id,
        skill_name=payload.skill,
        max_turns=payload.max_turns,
        model_provider=payload.model_provider,
        model_name=payload.model_name,
    )


@router.get("/tasks/{task_id}")
async def get_task(
    task_id: str,
    service: ServiceDep,
) -> dict[str, Any]:
    """查询任务详情，包括状态、Topic、报告和能力快照。"""
    return service.get_task(task_id)


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(
    task_id: str,
    service: ServiceDep,
) -> JSONResponse:
    """请求取消任务。

    取消是协作式的，所以通常返回 `202`；只有任务已经到终态时才返回 `200`。
    """
    snapshot = await service.cancel_task(task_id)
    status_code = 200 if snapshot["status"] in TERMINAL_STATUSES else 202
    return JSONResponse(status_code=status_code, content=snapshot)


@router.get("/tasks/{task_id}/events")
async def list_events(
    service: ServiceDep,
    task_id: str,
    after_seq: int = Query(default=0, ge=0, description="只返回 seq 大于该值的事件"),
    limit: int = Query(default=200, ge=1, le=1000, description="本页最多多少条"),
) -> dict[str, Any]:
    """增量读取运行事件。前端默认 1.5 秒轮询一次。"""
    return service.read_events(task_id, after_seq=after_seq, limit=limit)


@router.get("/tasks/{task_id}/events/stream")
async def stream_events(
    task_id: str,
    request: Request,
    service: ServiceDep,
    after_seq: int = Query(default=0, ge=0),
) -> StreamingResponse:
    """SSE 事件流。事件形状与分页接口完全一致。"""
    service.ensure_task(task_id)
    return StreamingResponse(
        service.stream_events(task_id, after_seq=after_seq, request=request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Nginx 等反向代理默认会缓冲响应，SSE 需要关掉。
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/tasks/{task_id}/turns/{turn}/context")
async def get_turn_context(
    task_id: str,
    service: ServiceDep,
    turn: int = ApiPath(ge=1, description="第几轮，从 1 开始"),
) -> dict[str, Any]:
    """返回第 N 轮真正发给模型的消息、工具和预算。"""
    return service.read_turn_context(task_id, turn)


@router.get("/tasks/{task_id}/evidence")
async def list_evidence(
    task_id: str,
    service: ServiceDep,
) -> list[dict[str, Any]]:
    """返回证据目录卡片，不含全文。"""
    return service.list_evidence(task_id)


@router.get("/tasks/{task_id}/evidence/{evidence_id}")
async def read_evidence(
    task_id: str,
    evidence_id: str,
    service: ServiceDep,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=4000, ge=1, le=8000),
) -> dict[str, Any]:
    """按字符偏移读取证据原文，最多 8000 字符。"""
    return service.read_evidence(task_id, evidence_id, offset=offset, limit=limit)


@router.get("/tasks/{task_id}/report")
async def get_report(
    task_id: str,
    service: ServiceDep,
) -> dict[str, Any]:
    """返回最终 Markdown 报告。

    本轮不提供结构化报告，`structured` 固定为 `null`，前端不要据此猜测字段。
    """
    return {"markdown": service.get_report(task_id), "structured": None}


def _register_exception_handlers(app: FastAPI) -> None:
    """把领域异常统一转成 HTTP 响应。

    为什么不在每个路由里 try/except：
    - 同样一个 `TaskNotFound` 会在六七个接口里出现，重复处理容易漏；
    - 统一处理后，状态码和 `{"detail": "..."}` 形状保持一致。
    """

    @app.exception_handler(TaskNotFound)
    async def _task_not_found(_: Request, exc: TaskNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(SampleNotFound)
    async def _sample_not_found(_: Request, exc: SampleNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(EvidenceNotFound)
    async def _evidence_not_found(_: Request, exc: EvidenceNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(TurnContextNotFound)
    async def _turn_not_found(_: Request, exc: TurnContextNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ReportNotReady)
    async def _report_not_ready(_: Request, exc: ReportNotReady) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(InvalidTaskRequest)
    async def _invalid_request(_: Request, exc: InvalidTaskRequest) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    """创建 FastAPI 应用。

    测试可以传自己的临时目录配置；生产/本地默认从环境变量读。
    `app.state.service` 让依赖注入和 lifespan 共用同一个服务实例。
    """
    settings = settings or ApiSettings.from_env()
    service = TaskService(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> Any:
        await service.startup()
        try:
            yield
        finally:
            await service.shutdown()

    app = FastAPI(
        title="DBA 工单 Agent 小平台",
        version="0.4.0",
        description=(
            "本地模拟的 DBA 慢 SQL 调查服务。所有数据均为 mock，"
            "不连接真实数据库或监控系统。"
        ),
        lifespan=lifespan,
    )
    app.state.service = service
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _register_exception_handlers(app)
    app.include_router(router)
    return app


# uvicorn 的推荐写法是“模块:变量”，所以这里保留一个模块级 app。
# 例子：`uv run uvicorn app.api.main:app --port 8000`
app = create_app()


# 方便直接 `python -m app.api.main` 启动本地服务。
# `uvicorn.run` 会自己接管事件循环，不需要外面再包 asyncio.run。
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api.main:app", host="127.0.0.1", port=8000, reload=False)
