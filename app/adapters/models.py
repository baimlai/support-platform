"""模型适配器工厂。

## 为什么要有这一层

CLI、测试和 HTTP 服务不应该分别判断“现在用的是模拟模型还是 DeepSeek”。
它们只传一个 `model_provider`，由这里决定构造哪个对象、模型标识是什么。

```text
provider="mock"     -> MockDbaModel，模型标识 "mock-dba"
provider="deepseek" -> DeepSeekModel，模型标识由参数/环境变量决定
```

这样以后再加别的模型（例如本地 Ollama 或其它 OpenAI 兼容服务），
只需要在这里加一个分支，循环和接口都不用改。

## 实现说明

- 返回值是 `(模型对象, 模型标识)`。模型标识会写进 `AgentLoopConfig.model`、
  每轮模型请求记录和事件里，排查时能看出“这次到底用了哪个模型”。
- 未知 provider 抛 `ModelConfigurationError`，CLI 和 HTTP 层都会把它转成
  用户能看懂的提示，而不是在后台悄悄退回模拟模型。
"""

from __future__ import annotations

import os
from typing import Any

from app.adapters.deepseek_model import (
    DEFAULT_MODEL,
    DeepSeekModel,
    ModelConfigurationError,
)
from app.adapters.mock_model import MockDbaModel

SUPPORTED_MODEL_PROVIDERS = frozenset({"mock", "deepseek"})

# 各 provider 的默认模型名。页面展示和任务快照都用这里的值，
# 避免 CLI、HTTP 和适配器三处各写一份字符串。
DEFAULT_MODEL_NAMES = {
    "mock": "mock-dba",
    "deepseek": DEFAULT_MODEL,
}

# 页面下拉框里可以展示的 DeepSeek 模型。它们只是“常用候选”，
# HTTP 接口仍允许传任意模型名，方便以后换成本地代理或新模型。
KNOWN_DEEPSEEK_MODELS = ("deepseek-flash", "deepseek-v4-pro")


def default_model_name(provider: str | None) -> str:
    """返回 provider 的默认模型名；未知 provider 直接报错。"""
    normalized = normalize_provider(provider)
    try:
        return DEFAULT_MODEL_NAMES[normalized]
    except KeyError as exc:
        raise ModelConfigurationError(
            f"不支持的模型 provider：{provider!r}。"
            f"可选值：{', '.join(sorted(SUPPORTED_MODEL_PROVIDERS))}"
        ) from exc


def resolve_model_name(provider: str | None, explicit: str | None = None) -> str:
    """确定一次任务实际使用的模型名。

    优先级（以 deepseek 为例）：

    ```text
    请求里显式传的 model_name
      -> 环境变量 DEEPSEEK_MODEL
      -> 环境变量 SUPPORT_PLATFORM_MODEL_NAME
      -> 代码默认值 deepseek-flash
    ```

    mock 的模型名固定是 `mock-dba`，因为模拟模型本来就只有一套剧本，
    传别的名字不会变出新的行为。
    """
    normalized = normalize_provider(provider)
    if normalized == "mock":
        return default_model_name(normalized)
    if normalized == "deepseek":
        return (
            (explicit or "").strip()
            or os.getenv("DEEPSEEK_MODEL", "").strip()
            or os.getenv("SUPPORT_PLATFORM_MODEL_NAME", "").strip()
            or default_model_name(normalized)
        )
    raise ModelConfigurationError(
        f"不支持的模型 provider：{provider!r}。"
        f"可选值：{', '.join(sorted(SUPPORTED_MODEL_PROVIDERS))}"
    )


def available_model_names(
    provider: str | None,
    configured: str | None = None,
) -> list[str]:
    """返回页面下拉框可展示的模型名列表。

    例子：deepseek 返回
    `["deepseek-flash", "deepseek-v4-pro"]`，第一项是默认值。

    `SUPPORT_PLATFORM_MODEL_OPTIONS` 可以用逗号追加自定义候选，
    例如私有代理提供的模型名。
    """
    normalized = normalize_provider(provider)
    if normalized == "mock":
        return [default_model_name(normalized)]
    if normalized != "deepseek":
        raise ModelConfigurationError(
            f"不支持的模型 provider：{provider!r}。"
            f"可选值：{', '.join(sorted(SUPPORTED_MODEL_PROVIDERS))}"
        )

    names = [resolve_model_name(normalized, configured)]
    for name in KNOWN_DEEPSEEK_MODELS:
        if name not in names:
            names.append(name)

    options_text = os.getenv("SUPPORT_PLATFORM_MODEL_OPTIONS", "").strip()
    for item in options_text.split(","):
        name = item.strip()
        if name and name not in names:
            names.append(name)
    return names


def normalize_provider(provider: str | None) -> str:
    """把 provider 规范成小写并去掉空格。

    例子：`" DeepSeek "` -> `"deepseek"`；空值按 `"mock"` 处理。
    """
    value = (provider or "").strip().lower()
    return value or "mock"


def build_model(
    provider: str | None,
    *,
    model_delay_s: float = 0.005,
    model_name: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    timeout_s: float = 120.0,
    max_tokens: int = 4096,
    temperature: float = 0.2,
) -> tuple[Any, str]:
    """构造模型适配器。

    返回 `(model, model_id)`：
    - `model` 只需要有 `stream(request)` 方法；
    - `model_id` 用于配置和记录，不代表真实网络请求里的模型名一定相同
      （DeepSeek 适配器会优先用请求里的 `model`）。
    """
    normalized = normalize_provider(provider)

    if normalized == "mock":
        return MockDbaModel(delay_s=model_delay_s), "mock-dba"

    if normalized == "deepseek":
        model = DeepSeekModel(
            api_key=api_key,
            base_url=base_url,
            model=model_name,
            timeout_s=timeout_s,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return model, model.model

    raise ModelConfigurationError(
        f"不支持的模型 provider：{provider!r}。"
        f"可选值：{', '.join(sorted(SUPPORTED_MODEL_PROVIDERS))}"
    )
