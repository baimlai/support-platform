"""DeepSeek 模型适配器。

## 这个文件解决什么问题

前面所有运行用的都是 `MockDbaModel`：它按固定剧本调用工具，能验证 Agent 循环，
但不会真正理解工单。现在把它换成一个真实模型，同时不改变循环、工具、证据、
Topic 和 HTTP 接口。

DeepSeek 提供 OpenAI 兼容的 `/chat/completions` 接口，所以这里做三件事：

```text
平台消息（SystemMessage / UserMessage / AssistantMessage / ToolResultMessage）
    -> OpenAI 格式（system / user / assistant / tool）
平台工具声明（ToolSpec）
    -> OpenAI function 格式（tools[].function）
OpenAI 流式分片（delta.content / delta.tool_calls）
    -> 平台的 AssistantStart / AssistantTextDelta / AssistantToolCallEvent / AssistantDone
```

## 为什么用 httpx，而不是再装一个厂商 SDK

- DeepSeek 的接口就是 OpenAI 兼容协议，直接发 JSON 最透明；
- 少一个依赖，少一层版本兼容问题；
- 测试里可以用 `httpx.MockTransport` 伪造 SSE，不需要真的联网。

## 密钥怎么放

只从环境变量读，**不要写进代码、README 或 git**：

```bash
export DEEPSEEK_API_KEY=sk-...
# 可选：换接口地址或模型
export DEEPSEEK_BASE_URL=https://api.deepseek.com
export DEEPSEEK_MODEL=deepseek-flash
```

`resolve_api_key()` 会明确报错，而不是拿空 Key 去请求然后收到一个难懂的 401。

## 实现说明（Python 语法）

- `async def` + `yield` 组成“异步生成器”。调用 `stream()` 不会立刻联网，
  只有循环 `async for` 时才开始执行。
- `async with client.stream(...) as response` 表示“流式响应也要在离开代码块时关闭”。
  网络流不关会泄漏连接。
- `dict.setdefault(key, 初始值)`：key 不存在时先放初始值，再返回它。
  这里用来按 `index` 累积同一个工具调用的分片。
- `line.startswith("data:")` 是 SSE 协议：每个事件一行 `data: JSON`，
  空行分隔；以冒号开头的行是注释（例如保活），直接跳过。
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.runtime.contracts import (
    AssistantDone,
    AssistantError,
    AssistantMessage,
    AssistantStart,
    AssistantStreamEvent,
    AssistantTextDelta,
    AssistantToolCallEvent,
    Content,
    ModelRequest,
    StopReason,
    TextContent,
    ToolCall,
    ToolResultMessage,
    ToolSpec,
    content_text,
    tool_result_text,
)

DEFAULT_BASE_URL = "https://api.deepseek.com"
# 本机已验证的 DeepSeek 模型是 deepseek-flash / deepseek-v4-pro。
# 公共 DeepSeek API 也可能提供 deepseek-chat；用 DEEPSEEK_MODEL 覆盖即可。
DEFAULT_MODEL = "deepseek-flash"
DEFAULT_TIMEOUT_S = 120.0
DEFAULT_MAX_TOKENS = 4096
DEFAULT_TEMPERATURE = 0.2

# OpenAI 的 finish_reason -> 平台 StopReason。
# `tool_calls` 表示模型要求执行工具，循环还要继续。
_FINISH_REASON_MAP: dict[str, StopReason] = {
    "stop": StopReason.STOP,
    "tool_calls": StopReason.TOOL_USE,
    "length": StopReason.LENGTH,
    "content_filter": StopReason.ERROR,
}


class ModelConfigurationError(RuntimeError):
    """模型配置缺失或不合法，例如没有设置 API Key。

    单独定义异常，是为了让 CLI 和 HTTP 层都能给出一句人话，而不是打印一整段堆栈。
    """


def _env_value(*names: str) -> str:
    """按顺序读取环境变量，返回第一个非空值。

    例子：`_env_value("DEEPSEEK_MODEL", "SUPPORT_PLATFORM_MODEL_NAME")`，
    前一个没设置就试后一个。
    """
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def resolve_api_key(explicit: str | None = None) -> str:
    """确定 API Key，缺失时抛出 `ModelConfigurationError`。

    顺序：
    1. 调用方显式传入（测试或上层配置）；
    2. `DEEPSEEK_API_KEY`；
    3. `SUPPORT_PLATFORM_DEEPSEEK_API_KEY`。

    不读取 `ANTHROPIC_API_KEY`：那是 Claude Code 自己的凭据，
    不应该被业务代码悄悄拿来用。
    """
    key = (explicit or "").strip() or _env_value(
        "DEEPSEEK_API_KEY",
        "SUPPORT_PLATFORM_DEEPSEEK_API_KEY",
    )
    if not key:
        raise ModelConfigurationError(
            "缺少 DeepSeek API Key。请先设置环境变量，例如：\n"
            "  export DEEPSEEK_API_KEY=sk-...\n"
            "不要把 Key 写进代码或提交到仓库。"
        )
    return key


class DeepSeekModel:
    """把 DeepSeek 的流式接口包装成平台模型适配器。

    参数：
    - `api_key`：显式 Key；为空时读环境变量；
    - `base_url`：OpenAI 兼容接口根地址，默认 `https://api.deepseek.com`；
    - `model`：模型名，默认 `deepseek-flash`；
    - `timeout_s`：读超时（流式响应里表示“多久没收到新数据就算超时”）；
    - `client`：测试可注入 `httpx.AsyncClient`，注入后由调用方负责关闭。
    """

    provider = "deepseek"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = resolve_api_key(api_key)
        self.base_url = (
            (base_url or "").strip()
            or _env_value("DEEPSEEK_BASE_URL", "SUPPORT_PLATFORM_MODEL_BASE_URL")
            or DEFAULT_BASE_URL
        ).rstrip("/")
        self.model = (
            (model or "").strip()
            or _env_value("DEEPSEEK_MODEL", "SUPPORT_PLATFORM_MODEL_NAME")
            or DEFAULT_MODEL
        )
        self.timeout_s = max(1.0, float(timeout_s))
        self.max_tokens = max(1, int(max_tokens))
        self.temperature = float(temperature)
        self._client = client
        # 模型偶尔不给工具调用 id 时，用这个计数器生成一个不重复的兜底 id。
        self._fallback_call_seq = 0

    def stream(self, request: ModelRequest) -> AsyncIterator[AssistantStreamEvent]:
        """模型适配器入口：返回异步事件流。

        注意这里没有 `async`。它只是一个普通函数，返回 `_stream()` 生成的异步生成器。
        真正联网发生在循环 `async for event in stream` 的时候。
        """
        return self._stream(request)

    async def _stream(self, request: ModelRequest) -> AsyncIterator[AssistantStreamEvent]:
        """调用 DeepSeek 并把 SSE 分片翻译成平台事件。"""
        model_id = request.model or self.model

        # 请求还没发出就被取消：直接返回取消事件，不要浪费一次网络调用。
        if request.cancellation.cancelled:
            yield self._error("用户已取消", StopReason.ABORTED, model_id)
            return

        payload = self.build_payload(request, model_id)
        client, owns_client = self._acquire_client()
        text_parts: list[str] = []
        tool_buffers: dict[int, dict[str, str]] = {}
        finish_reason: str | None = None

        try:
            async with client.stream(
                "POST",
                self._endpoint(),
                json=payload,
                headers=self._headers(),
            ) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    yield self._error(
                        _http_error_message(response.status_code, body),
                        StopReason.ERROR,
                        model_id,
                    )
                    return

                # 先发 start：循环会把一个“空消息”放进上下文，后续 delta 不断替换它。
                yield AssistantStart(
                    partial=self._partial([], StopReason.STOP, model_id)
                )

                async for line in response.aiter_lines():
                    # 网络读取可能很慢，每个分片都检查一次取消，用户点取消能尽快生效。
                    if request.cancellation.cancelled:
                        yield self._error("用户已取消", StopReason.ABORTED, model_id)
                        return

                    data = _parse_sse_data(line)
                    if data is None:
                        continue
                    if data == "[DONE]":
                        break

                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        # 单个坏分片不应该让整次调查失败，跳过并继续读后面的。
                        continue
                    if not isinstance(chunk, dict):
                        continue

                    choice = _first_choice(chunk)
                    delta = choice.get("delta") or {}

                    content = delta.get("content")
                    if isinstance(content, str) and content:
                        text_parts.append(content)
                        yield AssistantTextDelta(
                            delta=content,
                            partial=self._partial(text_parts, StopReason.STOP, model_id),
                        )

                    # DeepSeek 的思考模式会返回 reasoning_content；本轮只把最终回答
                    # 交给平台，思考内容不写进上下文，避免报告混入推理草稿。
                    for raw_call in delta.get("tool_calls") or []:
                        _accumulate_tool_call(tool_buffers, raw_call, self._next_fallback_call_id)

                    raw_finish = choice.get("finish_reason")
                    if raw_finish:
                        finish_reason = str(raw_finish)

            tool_calls, tool_error = self._finalize_tool_calls(tool_buffers)
            if tool_error:
                yield self._error(tool_error, StopReason.ERROR, model_id)
                return

            stop_reason = _FINISH_REASON_MAP.get(finish_reason or "stop", StopReason.STOP)
            if stop_reason is StopReason.ERROR:
                yield self._error(
                    f"模型停止原因：{finish_reason}",
                    StopReason.ERROR,
                    model_id,
                )
                return

            content_blocks: list[Content] = []
            text = "".join(text_parts)
            if text:
                content_blocks.append(TextContent(text=text))
            for tool_call in tool_calls:
                content_blocks.append(tool_call)
                # 每个工具调用单独发一个事件，前端可以逐个渲染卡片。
                yield AssistantToolCallEvent(
                    tool_call=tool_call,
                    partial=self._partial_from_blocks(content_blocks, stop_reason, model_id),
                )

            yield AssistantDone(
                message=self._partial_from_blocks(content_blocks, stop_reason, model_id)
            )

        except httpx.TimeoutException as exc:
            yield self._error(f"DeepSeek 请求超时：{exc}", StopReason.ERROR, model_id)
        except httpx.HTTPError as exc:
            yield self._error(f"DeepSeek 网络错误：{exc}", StopReason.ERROR, model_id)
        except OSError as exc:
            yield self._error(f"DeepSeek 连接失败：{exc}", StopReason.ERROR, model_id)
        finally:
            # 只有适配器自己创建的 client 才由适配器关闭；
            # 测试注入的 client 归测试管，提前关掉会让同一测试里的后续请求失败。
            if owns_client:
                await client.aclose()

    def build_payload(self, request: ModelRequest, model_id: str | None = None) -> dict[str, Any]:
        """把 `ModelRequest` 转成 DeepSeek/OpenAI 的请求体。

        单独抽成方法是为了单元测试：不联网也能断言消息和工具被正确转换。
        """
        payload: dict[str, Any] = {
            "model": model_id or request.model or self.model,
            "messages": [_to_openai_message(message) for message in request.messages],
            "stream": True,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        if request.tools:
            payload["tools"] = [_to_openai_tool(tool) for tool in request.tools]
            # auto：由模型自己决定这轮是否调用工具。
            payload["tool_choice"] = "auto"
        return payload

    def _endpoint(self) -> str:
        """拼出 chat completions 地址。

        例子：base_url 是 `https://api.deepseek.com`，
        得到 `https://api.deepseek.com/chat/completions`。
        """
        return f"{self.base_url}/chat/completions"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

    def _acquire_client(self) -> tuple[httpx.AsyncClient, bool]:
        """返回 `(client, 是否由适配器创建)`。

        每次运行创建一个短连接 client，结束时关闭；任务量很小，这样最省心。
        """
        if self._client is not None:
            return self._client, False
        timeout = httpx.Timeout(self.timeout_s, connect=10.0)
        return httpx.AsyncClient(timeout=timeout), True

    def _next_fallback_call_id(self) -> str:
        self._fallback_call_seq += 1
        return f"deepseek-call-{self._fallback_call_seq}"

    def _partial(
        self,
        text_parts: list[str],
        stop_reason: StopReason,
        model_id: str,
    ) -> AssistantMessage:
        """用“到目前为止的文本”构造一个 partial 消息。

        每次新建 TextContent，避免后续追加文本时改到已经发出的事件。
        """
        text = "".join(text_parts)
        content: list[Content] = [TextContent(text=text)] if text else []
        return self._partial_from_blocks(content, stop_reason, model_id)

    def _partial_from_blocks(
        self,
        blocks: list[Content],
        stop_reason: StopReason,
        model_id: str,
    ) -> AssistantMessage:
        """复制内容列表，构造助手消息。

        `list(blocks)` 是浅拷贝：列表对象是新的，但里面的 TextContent/ToolCall
        对象仍然共享。这里不会修改这些对象，所以浅拷贝足够。
        """
        return AssistantMessage(
            content=list(blocks),
            stop_reason=stop_reason,
            provider=self.provider,
            model=model_id,
        )

    def _error(
        self,
        message: str,
        stop_reason: StopReason,
        model_id: str,
    ) -> AssistantError:
        """构造一个统一的模型错误事件。"""
        return AssistantError(
            message=AssistantMessage(
                content=[],
                stop_reason=stop_reason,
                error_message=message,
                provider=self.provider,
                model=model_id,
            )
        )

    def _finalize_tool_calls(
        self,
        buffers: dict[int, dict[str, str]],
    ) -> tuple[list[ToolCall], str | None]:
        """把累积的工具调用分片转成完整的 `ToolCall`。

        返回 `(工具调用列表, 错误说明)`。错误说明不为空时调用方应发 AssistantError。
        """
        calls: list[ToolCall] = []
        for index in sorted(buffers):
            buffer = buffers[index]
            name = buffer.get("name", "").strip()
            if not name:
                return [], f"模型返回了没有名称的工具调用（index={index}）"

            raw_arguments = buffer.get("arguments", "").strip() or "{}"
            try:
                arguments = json.loads(raw_arguments)
            except json.JSONDecodeError as exc:
                return [], f"工具 {name} 的参数不是合法 JSON：{exc}"
            if not isinstance(arguments, dict):
                return [], f"工具 {name} 的参数必须是 JSON 对象"

            calls.append(
                ToolCall(
                    id=buffer.get("id") or self._next_fallback_call_id(),
                    name=name,
                    arguments=arguments,
                )
            )
        return calls, None


def _to_openai_message(message: Any) -> dict[str, Any]:
    """把平台消息转成 OpenAI 消息。

    四种角色的对应关系：

    | 平台 | OpenAI | 说明 |
    |---|---|---|
    | SystemMessage | system | 系统规则 + Skill + Topic |
    | UserMessage | user | 工单正文 |
    | AssistantMessage | assistant | 文本和/或 tool_calls |
    | ToolResultMessage | tool | 必须带 tool_call_id |
    """
    role = getattr(message, "role", "")
    if role == "system":
        return {"role": "system", "content": content_text(message.content)}

    if role == "user":
        return {"role": "user", "content": content_text(message.content)}

    if role == "assistant":
        text = "".join(
            block.text for block in message.content if isinstance(block, TextContent)
        )
        tool_calls = [
            {
                "id": block.id,
                "type": "function",
                "function": {
                    "name": block.name,
                    "arguments": json.dumps(block.arguments, ensure_ascii=False),
                },
            }
            for block in message.content
            if isinstance(block, ToolCall)
        ]
        payload: dict[str, Any] = {
            "role": "assistant",
            # OpenAI 允许 content 为 null，但有些兼容实现更接受空字符串。
            "content": text or None,
        }
        if tool_calls:
            payload["tool_calls"] = tool_calls
        return payload

    if isinstance(message, ToolResultMessage):
        return {
            "role": "tool",
            "tool_call_id": message.tool_call_id,
            "content": tool_result_text(message),
        }

    raise ValueError(f"不支持的消息类型：{type(message).__name__}")


def _to_openai_tool(tool: ToolSpec) -> dict[str, Any]:
    """把平台工具声明转成 OpenAI function 定义。

    例子：
        ToolSpec(name="slow_log_query", ...)
        -> {"type": "function", "function": {"name": "slow_log_query", ...}}
    """
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        },
    }


def _parse_sse_data(line: str) -> str | None:
    """从一行 SSE 里取出 `data:` 后面的内容。

    返回 None 表示这行不是数据（可能是空行、注释或普通文本）。
    例子：
        'data: {"choices": [...]}' -> '{"choices": [...]}'
        ': keep-alive'            -> None
        ''                        -> None
    """
    line = line.strip()
    if not line or line.startswith(":"):
        return None
    if not line.startswith("data:"):
        return None
    return line[len("data:") :].strip()


def _first_choice(chunk: dict[str, Any]) -> dict[str, Any]:
    """取出第一个 choice；没有就返回空字典，避免到处写存在性判断。"""
    choices = chunk.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        return choices[0]
    return {}


def _accumulate_tool_call(
    buffers: dict[int, dict[str, str]],
    raw_call: Any,
    fallback_id_factory: Any,
) -> None:
    """把一条流式工具调用分片合并到对应 index 的缓冲区。

    DeepSeek/OpenAI 会把同一个工具调用拆成多条：
    第一片带 id 和函数名，后面几片只带 arguments 的片段。
    所以 name 只在第一次出现时赋值，arguments 要一直追加。
    """
    if not isinstance(raw_call, dict):
        return
    try:
        index = int(raw_call.get("index", 0))
    except (TypeError, ValueError):
        index = 0

    buffer = buffers.setdefault(index, {"id": "", "name": "", "arguments": ""})
    call_id = raw_call.get("id")
    if isinstance(call_id, str) and call_id:
        buffer["id"] = call_id

    function = raw_call.get("function") or {}
    if not isinstance(function, dict):
        return
    name = function.get("name")
    if isinstance(name, str) and name and not buffer["name"]:
        buffer["name"] = name
    arguments = function.get("arguments")
    if isinstance(arguments, str) and arguments:
        buffer["arguments"] += arguments

    # 有些兼容实现只给 index，不给 id；在最终组装时用兜底 id。
    if not buffer["id"]:
        buffer["id"] = fallback_id_factory()


def _http_error_message(status_code: int, body: bytes) -> str:
    """把 HTTP 错误压成一行，既保留原因，又不把整页 HTML 塞进上下文。"""
    text = body.decode("utf-8", errors="replace").strip()
    if len(text) > 500:
        text = text[:500] + "..."
    hint = ""
    if status_code == 401:
        hint = "（请检查 DEEPSEEK_API_KEY 是否正确）"
    elif status_code == 402:
        hint = "（账户余额不足）"
    elif status_code == 429:
        hint = "（请求过于频繁，稍后重试）"
    return f"DeepSeek 接口返回 HTTP {status_code}{hint}：{text or '无响应内容'}"
