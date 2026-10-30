"""HTTP 服务配置。

配置项优先从环境变量读取，没有就用项目内的默认值。这样本地开发零配置，
部署时也能用环境变量换运行目录、数据库位置和前端地址。

环境变量：

```text
SUPPORT_PLATFORM_RUN_ROOT        运行记录目录，默认 var/runs
SUPPORT_PLATFORM_DB              任务 SQLite 文件，默认 var/tasks.db
SUPPORT_PLATFORM_SAMPLES         样例工单目录，默认 fixtures/tickets
SUPPORT_PLATFORM_CORS_ORIGINS    逗号分隔的前端地址
SUPPORT_PLATFORM_MODEL_DELAY     模拟模型停顿秒数，默认 0（接口演示最快）
SUPPORT_PLATFORM_MODEL_PROVIDER  默认模型来源：mock / deepseek，默认 mock
SUPPORT_PLATFORM_MODEL_NAME      DeepSeek 模型名，例如 deepseek-flash
SUPPORT_PLATFORM_MODEL_BASE_URL  DeepSeek 兼容接口根地址
SUPPORT_PLATFORM_DEEPSEEK_API_KEY DeepSeek Key；DEEPSEEK_API_KEY 也能被适配器识别
SUPPORT_PLATFORM_MODEL_TIMEOUT   模型读超时秒数，默认 120
SUPPORT_PLATFORM_MODEL_MAX_TOKENS 单轮最大输出 token，默认 4096
SUPPORT_PLATFORM_MODEL_TEMPERATURE 采样温度，默认 0.2
SUPPORT_PLATFORM_TOOL_TIMEOUT    工具超时秒数，默认 5
SUPPORT_PLATFORM_MAX_INLINE      单条工具结果内联字符上限，默认 4000
SUPPORT_PLATFORM_CONTEXT_BUDGET  模型输入字符预算，默认 12000
SUPPORT_PLATFORM_CONTEXT_MESSAGES 最近消息条数，默认 10
SUPPORT_PLATFORM_MAX_TURNS       默认最大轮数，默认 10

MCP 相关：

SUPPORT_PLATFORM_MCP_TRANSPORT  stdio / streamable-http / sse，默认 stdio
SUPPORT_PLATFORM_MCP_URL        HTTP/SSE 完整地址；不填则用 host/port 拼
SUPPORT_PLATFORM_MCP_COMMAND    stdio 启动命令；默认当前 Python + app.mcp.server
SUPPORT_PLATFORM_MCP_HOST       远程 MCP Server 地址，默认 127.0.0.1
SUPPORT_PLATFORM_MCP_PORT       远程 MCP Server 端口，默认 8765
SUPPORT_PLATFORM_MCP_TIMEOUT    连接/初始化/单次工具调用超时，默认 30
SUPPORT_PLATFORM_MCP_DATASET    MCP Server 读取的模拟数据文件

例子：本地默认 `stdio` 不需要启动任何服务；想观察 SSE，可以先运行
`uv run python -m app.mcp.server --transport sse --port 8765`，再设置
`SUPPORT_PLATFORM_MCP_TRANSPORT=sse` 启动 API。
```
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from app.adapters.models import SUPPORTED_MODEL_PROVIDERS, normalize_provider

# app/api/settings.py -> app/api -> app -> 项目根目录。
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173",
    "http://127.0.0.1:4173",
]

# MCP 传输方式。和 `app/mcp/client.py` 里的枚举保持一致；
# 这里单独写一份，是为了让配置层不依赖 MCP SDK 也能做启动校验。
MCP_TRANSPORTS = frozenset({"stdio", "streamable-http", "sse"})
DEFAULT_MCP_DATASET = PROJECT_ROOT / "fixtures" / "mock_dba" / "slow_sql_001.json"


def _env_path(name: str, default: Path) -> Path:
    """读路径环境变量；空字符串也按“没配置”处理。"""
    value = os.getenv(name, "").strip()
    return Path(value) if value else default


def _env_int(name: str, default: int) -> int:
    """读整数环境变量；写错时直接报错，避免悄悄用默认值掩盖配置问题。"""
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 必须是整数，当前值：{value}") from exc


def _env_float(name: str, default: float) -> float:
    """读浮点数环境变量。"""
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 必须是数字，当前值：{value}") from exc


@dataclass(slots=True)
class ApiSettings:
    """创建 FastAPI 应用所需的全部配置。

    `field(default_factory=list)` 给列表一个独立默认值；
    直接写 `= []` 会让所有实例共用同一个列表，是 Python 常见的坑。
    """

    run_root: Path = field(default_factory=lambda: PROJECT_ROOT / "var" / "runs")
    db_path: Path = field(default_factory=lambda: PROJECT_ROOT / "var" / "tasks.db")
    samples_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "fixtures" / "tickets")
    cors_origins: list[str] = field(default_factory=lambda: list(DEFAULT_CORS_ORIGINS))
    model_delay_s: float = 0.0
    # 默认走 mock，页面可以按任务覆盖成 deepseek。
    # 这里只存“默认值”，不强制每次任务都使用它。
    model_provider: str = "mock"
    model_name: str | None = None
    model_base_url: str | None = None
    model_api_key: str | None = None
    model_timeout_s: float = 120.0
    model_max_tokens: int = 4096
    model_temperature: float = 0.2
    tool_timeout_s: float = 5.0
    max_inline_chars: int = 4000
    context_budget: int = 12000
    context_messages: int = 10
    default_max_turns: int = 10
    # MCP 工具服务连接配置。默认 stdio：每个任务启动一个 MCP 子进程，
    # 不要求用户先手动开服务，适合本地开发和测试。
    mcp_transport: str = "stdio"
    mcp_url: str | None = None
    mcp_command: str | None = None
    mcp_host: str = "127.0.0.1"
    mcp_port: int = 8765
    mcp_timeout_s: float = 30.0
    mcp_dataset_path: Path = field(default_factory=lambda: DEFAULT_MCP_DATASET)

    def __post_init__(self) -> None:
        """校验 MCP 传输方式。

        例子：环境变量写成 `sse-http`（不存在的名字）时，服务启动就会失败，
        而不是等到用户点“开始调查”才报一个模糊的连接错误。
        """
        normalized = self.mcp_transport.strip().lower()
        if normalized not in MCP_TRANSPORTS:
            raise ValueError(
                "SUPPORT_PLATFORM_MCP_TRANSPORT 只能是 "
                f"{', '.join(sorted(MCP_TRANSPORTS))}，当前值：{self.mcp_transport}"
            )
        self.mcp_transport = normalized

    def mcp_endpoint(self) -> str:
        """返回展示和连接用的 MCP 地址。

        stdio 模式没有 URL，返回启动命令；HTTP/SSE 模式优先使用完整 URL，
        没配 URL 时按 host/port 和协议默认路径拼出来。
        例子：
        - stdio -> `.venv/bin/python -m app.mcp.server`
        - streamable-http -> `http://127.0.0.1:8765/mcp`
        - sse -> `http://127.0.0.1:8765/sse`
        """
        if self.mcp_transport == "stdio":
            # 不传 command 时，Runner 会用当前虚拟环境的 Python 解释器启动
            # `app.mcp.server`。这里展示同一个解释器路径，避免能力目录显示成
            # 一个可能指向系统 Python 的裸 `python`。
            return self.mcp_command or f"{sys.executable} -m app.mcp.server"
        if self.mcp_url:
            return self.mcp_url
        path = "/mcp" if self.mcp_transport == "streamable-http" else "/sse"
        return f"http://{self.mcp_host}:{self.mcp_port}{path}"

    @classmethod
    def from_env(cls) -> ApiSettings:
        """从环境变量创建配置。

        例子：测试里想用临时目录，可以直接
        `ApiSettings(run_root=tmp/"runs", db_path=tmp/"tasks.db")`，
        不必改环境变量。
        """
        origins_text = os.getenv("SUPPORT_PLATFORM_CORS_ORIGINS", "").strip()
        origins = (
            [item.strip() for item in origins_text.split(",") if item.strip()]
            if origins_text
            else list(DEFAULT_CORS_ORIGINS)
        )
        # provider 写错时启动就报错，比任务跑到一半才发现“模型不存在”更容易排查。
        model_provider = normalize_provider(
            os.getenv("SUPPORT_PLATFORM_MODEL_PROVIDER", "mock")
        )
        if model_provider not in SUPPORTED_MODEL_PROVIDERS:
            raise ValueError(
                "SUPPORT_PLATFORM_MODEL_PROVIDER 只能是 "
                f"{', '.join(sorted(SUPPORTED_MODEL_PROVIDERS))}，"
                f"当前值：{model_provider}"
            )
        return cls(
            run_root=_env_path("SUPPORT_PLATFORM_RUN_ROOT", PROJECT_ROOT / "var" / "runs"),
            db_path=_env_path("SUPPORT_PLATFORM_DB", PROJECT_ROOT / "var" / "tasks.db"),
            samples_dir=_env_path(
                "SUPPORT_PLATFORM_SAMPLES",
                PROJECT_ROOT / "fixtures" / "tickets",
            ),
            cors_origins=origins,
            model_delay_s=_env_float("SUPPORT_PLATFORM_MODEL_DELAY", 0.0),
            model_provider=model_provider,
            model_name=os.getenv("SUPPORT_PLATFORM_MODEL_NAME", "").strip() or None,
            model_base_url=os.getenv("SUPPORT_PLATFORM_MODEL_BASE_URL", "").strip() or None,
            model_api_key=(
                os.getenv("SUPPORT_PLATFORM_DEEPSEEK_API_KEY", "").strip() or None
            ),
            model_timeout_s=_env_float("SUPPORT_PLATFORM_MODEL_TIMEOUT", 120.0),
            model_max_tokens=_env_int("SUPPORT_PLATFORM_MODEL_MAX_TOKENS", 4096),
            model_temperature=_env_float("SUPPORT_PLATFORM_MODEL_TEMPERATURE", 0.2),
            tool_timeout_s=_env_float("SUPPORT_PLATFORM_TOOL_TIMEOUT", 5.0),
            max_inline_chars=_env_int("SUPPORT_PLATFORM_MAX_INLINE", 4000),
            context_budget=_env_int("SUPPORT_PLATFORM_CONTEXT_BUDGET", 12000),
            context_messages=_env_int("SUPPORT_PLATFORM_CONTEXT_MESSAGES", 10),
            default_max_turns=_env_int("SUPPORT_PLATFORM_MAX_TURNS", 10),
            mcp_transport=os.getenv("SUPPORT_PLATFORM_MCP_TRANSPORT", "stdio"),
            mcp_url=os.getenv("SUPPORT_PLATFORM_MCP_URL", "").strip() or None,
            mcp_command=os.getenv("SUPPORT_PLATFORM_MCP_COMMAND", "").strip() or None,
            mcp_host=os.getenv("SUPPORT_PLATFORM_MCP_HOST", "127.0.0.1").strip()
            or "127.0.0.1",
            mcp_port=_env_int("SUPPORT_PLATFORM_MCP_PORT", 8765),
            mcp_timeout_s=_env_float("SUPPORT_PLATFORM_MCP_TIMEOUT", 30.0),
            mcp_dataset_path=_env_path("SUPPORT_PLATFORM_MCP_DATASET", DEFAULT_MCP_DATASET),
        )
