"""模型层（对应 langchain.llms / langchain.chat_models）。

- `BaseLanguageModel`：invoke / stream 抽象 + `bind_tools`。
- `OpenAICompatChatModel`：OpenAI 兼容 `/chat/completions`，**纯 stdlib
  urllib 实现**（显式 SSL 上下文 + 连接重试 + SSE 流），复用仓库 tutorial 08
  与 deerflow workbench 的网关调用模式。
- `AnthropicCompatChatModel`：Anthropic Messages API 兼容（`/v1/messages`，
  `x-api-key` + `anthropic-version` 头，SSE 流式；system 消息拆分到顶层字段）。
- `FakeLLM`：确定性假模型（测试回路，不依赖真实网关）。
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Sequence
from typing import Any

from .callback import get_callback_manager
from .runnable import ReactiveChainError, Runnable, State

DEFAULT_UA = "ReactiveChain/0.1"


class BaseLanguageModel(Runnable):
    """语言模型段抽象。输入键 `messages`（API dict 列表）或 `prompt`。"""

    writes: set[str] = {"output", "llm_message"}
    # 能力标志：tool calling 支持由具体模型声明（FakeLLM 等 mock 默认 False，
    # 供 create_tool_calling_agent 构造期校验，避免 hasattr 被抛错版 bind_tools 绕过）。
    supports_tool_calling: bool = False

    def __init__(
        self,
        *,
        input_key: str = "messages",
        name: str | None = None,
    ) -> None:
        self.input_key = input_key
        self._tools: list[dict[str, Any]] = []
        self.name = name or type(self).__name__
        self._last_tokens: int | None = None  # 最近一次 _call 的 usage（回调聚合用）

    @property
    def id(self) -> str:
        return f"llm:{self.name}"

    @property
    def reads(self) -> set[str]:  # type: ignore[override]
        return {self.input_key}

    def bind_tools(self, tools: Sequence[Any]) -> BaseLanguageModel:
        """绑定工具 schema（tool calling 模式）。tools 为 dict 或带
        `.to_schema()` 的对象（如 reactivechain.tools.BaseTool）。"""
        self._tools = [
            t.to_schema() if hasattr(t, "to_schema") else t for t in tools
        ]
        return self

    def _resolve_messages(self, state: State) -> list[dict[str, Any]]:
        payload = state.get(self.input_key)
        if payload is None and "prompt" in state:
            payload = [{"role": "user", "content": str(state["prompt"])}]
        if payload is None:
            raise ReactiveChainError(
                f"模型 '{self.name}' 缺少输入键 '{self.input_key}'（或有 'prompt'）— "
                f"Hint: 上游用 ChatPromptTemplate/PromptTemplate 生成输入"
            )
        if isinstance(payload, str):
            payload = [{"role": "user", "content": payload}]
        return [dict(m) for m in payload]

    def invoke(self, state: State) -> State:
        messages = self._resolve_messages(state)
        mgr = get_callback_manager()
        mgr.emit("on_llm_start", self, messages)
        start = time.perf_counter()
        message = self._call(messages)
        mgr.emit("on_llm_end", self, str(message.get("content") or ""),
                 (time.perf_counter() - start) * 1000, tokens=self._last_tokens)
        return {
            "output": str(message.get("content") or ""),
            "llm_message": message,
        }

    def stream(self, state: State, *, mode: str = "messages") -> Iterator[State]:
        for delta in self._stream_deltas(self._resolve_messages(state)):
            yield {"chunk": delta}

    def _call(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        raise NotImplementedError  # pragma: no cover

    def _stream_deltas(self, messages: list[dict[str, Any]]) -> Iterator[str]:
        raise NotImplementedError  # pragma: no cover


class FakeLLM(BaseLanguageModel):
    """确定性假模型：按顺序/规则返回固定文本，测试与教程用。"""

    def __init__(
        self,
        responses: Sequence[str] | None = None,
        *,
        input_key: str = "messages",
        name: str | None = None,
    ) -> None:
        super().__init__(input_key=input_key, name=name or "fake")
        self._responses = list(responses) if responses is not None else ["hello"]
        self._calls: list[list[dict[str, Any]]] = []

    @property
    def calls(self) -> list[list[dict[str, Any]]]:
        return self._calls

    def _pick(self) -> str:
        if not self._responses:
            raise ReactiveChainError("FakeLLM 已用完所有响应 — Hint: 传足够多的 responses")
        return self._responses.pop(0)

    def bind_tools(self, tools: Sequence[Any]) -> FakeLLM:
        raise ReactiveChainError(
            "FakeLLM 不支持 bind_tools / tool calling — Hint: 用 OpenAICompatChatModel"
            "（或继承 BaseLanguageModel 的 mock 模型）"
        )

    def _call(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        self._calls.append(messages)
        return {"role": "assistant", "content": self._pick()}

    def _stream_deltas(self, messages: list[dict[str, Any]]) -> Iterator[str]:
        self._calls.append(messages)
        for tok in self._pick().split():
            yield tok + " "


class OpenAICompatChatModel(BaseLanguageModel):
    """OpenAI 兼容 chat 模型（纯 stdlib urllib）。

    参数：
        base_url: 兼容服务根地址（如 https://api.openai.com/v1）。
        api_key: 密钥；None 时读取环境变量（参数 env_key，默认 OPENAI_API_KEY）。
        model: 模型名。
        temperature / max_tokens / top_p / stop: 采样参数。
        timeout_s: 单请求超时。
        trust_env: 是否走系统代理（默认 False 直连，避免 macOS 系统代理坑）。
    """

    supports_tool_calling = True

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        env_key: str = "OPENAI_API_KEY",
        model: str = "gpt-4o-mini",
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
        stop: Sequence[str] | None = None,
        timeout_s: float = 60.0,
        trust_env: bool = False,
        name: str | None = None,
        retries: int = 3,
    ) -> None:
        super().__init__(name=name or f"openai:{model}")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key if api_key is not None else os_getenv(env_key)
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.top_p = top_p
        self.stop = list(stop) if stop else None
        self.timeout_s = timeout_s
        self.trust_env = trust_env
        self.retries = retries

    @property
    def id(self) -> str:
        return f"llm:{self.model}@{self.base_url}"

    def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise ReactiveChainError(
                f"模型 '{self.name}' 未配置 api_key — Hint: 传参 api_key 或设环境变量"
            )
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": DEFAULT_UA,
            },
        )
        last: Exception | None = None
        for attempt in range(self.retries):
            try:
                with self._open(req) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                status = exc.code
                exc.close()  # 释放失败响应体，避免连接滞留依赖 GC
                if 400 <= status < 500 and status != 429:
                    # 客户端错误重试无意义（掩盖配置错误），直接抛出。
                    raise ReactiveChainError(
                        f"模型 '{self.name}' 请求被拒绝（HTTP {status}）— "
                        f"Hint: 检查 api_key/base_url/请求内容"
                    ) from exc
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        raise ReactiveChainError(
            f"模型 '{self.name}' 请求失败（{self.retries} 次尝试）：{last} — "
            f"Hint: 检查 base_url/api_key/网络"
        ) from last

    def _open(self, req: urllib.request.Request) -> Any:
        """打开请求：trust_env=True 走系统代理，否则显式直连（ProxyHandler({})）。"""
        if self.trust_env:
            return urllib.request.urlopen(req, timeout=self.timeout_s)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return opener.open(req, timeout=self.timeout_s)

    def _base_payload(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        payload: dict[str, Any] = {"model": self.model, "messages": messages}
        if self.temperature is not None:
            payload["temperature"] = self.temperature
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        if self.top_p is not None:
            payload["top_p"] = self.top_p
        if self.stop:
            payload["stop"] = self.stop
        if self._tools:
            payload["tools"] = self._tools
        return payload

    def _call(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        data = self._request(self._base_payload(messages))
        usage = (data.get("usage") or {}).get("total_tokens")
        if usage:
            self._last_tokens = int(usage)
        try:
            return dict(data["choices"][0]["message"])
        except (KeyError, IndexError) as exc:
            raise ReactiveChainError(
                f"模型 '{self.name}' 响应缺少 choices[0].message — Hint: 检查"
                f" base_url 是否 OpenAI 兼容；原始响应：{str(data)[:200]}"
            ) from exc

    def _stream_deltas(self, messages: list[dict[str, Any]]) -> Iterator[str]:
        payload = self._base_payload(messages)
        payload["stream"] = True
        if not self.api_key:
            raise ReactiveChainError(
                f"模型 '{self.name}' 未配置 api_key — Hint: 传参 api_key 或设环境变量"
            )
        body = json.dumps(payload).encode("utf-8")
        last: Exception | None = None
        for attempt in range(self.retries):
            req = urllib.request.Request(
                f"{self.base_url}/chat/completions",
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                    "User-Agent": DEFAULT_UA,
                    "Accept": "text/event-stream",
                },
            )
            try:
                with self._open(req) as r:
                    for raw in r:
                        line = raw.decode("utf-8", "replace").strip()
                        if not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if data == "[DONE]":
                            return
                        try:
                            chunk = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        piece = delta.get("content")
                        if piece:
                            yield piece
                return
            except urllib.error.HTTPError as exc:
                status = exc.code
                exc.close()
                if 400 <= status < 500 and status != 429:
                    raise ReactiveChainError(
                        f"模型 '{self.name}' 流式请求被拒绝（HTTP {status}）— "
                        f"Hint: 检查 api_key/base_url/请求内容"
                    ) from exc
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        raise ReactiveChainError(
            f"模型 '{self.name}' 流式请求失败（{self.retries} 次尝试）：{last} — "
            f"Hint: 检查 base_url/api_key/网络"
        ) from last


def os_getenv(key: str) -> str | None:
    """模块级封装便于测试替换。"""
    import os

    return os.environ.get(key)


class AnthropicCompatChatModel(BaseLanguageModel):
    """Anthropic Messages API 兼容模型（纯 stdlib urllib + SSE）。

    参数与 OpenAICompatChatModel 同构：`base_url` 为服务根地址（如
    https://api.anthropic.com），请求发往 `{base_url}/v1/messages`，认证头
    `x-api-key` + `anthropic-version: 2023-06-01`。`system` 角色消息自动拆分
    到请求体顶层 `system` 字段。本地模型走 OpenAI 兼容端点时请直接用
    `OpenAICompatChatModel`（Ollama 等），无需本适配。
    """

    supports_tool_calling = True

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        env_key: str = "ANTHROPIC_API_KEY",
        model: str = "claude-sonnet-4-5",
        max_tokens: int = 1024,
        temperature: float | None = None,
        top_p: float | None = None,
        stop: Sequence[str] | None = None,
        timeout_s: float = 60.0,
        trust_env: bool = False,
        name: str | None = None,
        retries: int = 3,
    ) -> None:
        super().__init__(name=name or f"anthropic:{model}")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key if api_key is not None else os_getenv(env_key)
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.top_p = top_p
        self.stop = list(stop) if stop else None
        self.timeout_s = timeout_s
        self.trust_env = trust_env
        self.retries = retries

    @property
    def id(self) -> str:
        return f"llm:anthropic:{self.model}@{self.base_url}"

    def _open(self, req: urllib.request.Request) -> Any:
        """打开请求：trust_env=True 走系统代理，否则显式直连（ProxyHandler({})）。"""
        if self.trust_env:
            return urllib.request.urlopen(req, timeout=self.timeout_s)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return opener.open(req, timeout=self.timeout_s)

    def _payload(self, messages: list[dict[str, Any]], *, stream: bool) -> dict[str, Any]:
        """Anthropic 请求体：system 角色拆分到顶层 `system` 字段。"""
        system: list[str] = []
        out: list[dict[str, Any]] = []
        for m in messages:
            role = m.get("role")
            content = m.get("content")
            if role == "system":
                system.append(str(content))
            else:
                out.append({"role": role, "content": content})
        payload: dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": out,
        }
        if system:
            payload["system"] = "\n".join(system)
        if self.temperature is not None:
            payload["temperature"] = self.temperature
        if self.top_p is not None:
            payload["top_p"] = self.top_p
        if self.stop:
            payload["stop"] = self.stop
        if stream:
            payload["stream"] = True
        return payload

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "x-api-key": self.api_key or "",
            "anthropic-version": "2023-06-01",
            "User-Agent": DEFAULT_UA,
            "Accept": "text/event-stream",
        }

    def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise ReactiveChainError(
                f"模型 '{self.name}' 未配置 api_key — Hint: 传参 api_key 或设环境变量"
            )
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/v1/messages",
            data=body,
            method="POST",
            headers=self._headers(),
        )
        last: Exception | None = None
        for attempt in range(self.retries):
            try:
                with self._open(req) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                status = exc.code
                exc.close()  # 释放失败响应体，避免连接滞留依赖 GC
                if 400 <= status < 500 and status != 429:
                    raise ReactiveChainError(
                        f"模型 '{self.name}' 请求被拒绝（HTTP {status}）— "
                        f"Hint: 检查 api_key/base_url/请求内容"
                    ) from exc
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        raise ReactiveChainError(
            f"模型 '{self.name}' 请求失败（{self.retries} 次尝试）：{last} — "
            f"Hint: 检查 base_url/api_key/网络"
        ) from last

    def _call(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        data = self._request(self._payload(messages, stream=False))
        usage = data.get("usage") or {}
        tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
        if tokens:
            self._last_tokens = int(tokens)
        try:
            text = "".join(
                b.get("text", "") for b in data["content"] if b.get("type") == "text"
            )
        except (KeyError, TypeError) as exc:
            raise ReactiveChainError(
                f"模型 '{self.name}' 响应缺少 content[].text — Hint: 检查"
                f" base_url 是否 Anthropic Messages 兼容；原始响应：{str(data)[:200]}"
            ) from exc
        return {"role": "assistant", "content": text}

    def _stream_deltas(self, messages: list[dict[str, Any]]) -> Iterator[str]:
        payload = self._payload(messages, stream=True)
        if not self.api_key:
            raise ReactiveChainError(
                f"模型 '{self.name}' 未配置 api_key — Hint: 传参 api_key 或设环境变量"
            )
        body = json.dumps(payload).encode("utf-8")
        last: Exception | None = None
        for attempt in range(self.retries):
            req = urllib.request.Request(
                f"{self.base_url}/v1/messages",
                data=body,
                method="POST",
                headers=self._headers(),
            )
            try:
                with self._open(req) as r:
                    for raw in r:
                        line = raw.decode("utf-8", "replace").strip()
                        if not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if not data:
                            continue
                        try:
                            ev = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                        if ev.get("type") == "content_block_delta":
                            piece = ev.get("delta", {}).get("text")
                            if piece:
                                yield piece
                return
            except urllib.error.HTTPError as exc:
                status = exc.code
                exc.close()
                if 400 <= status < 500 and status != 429:
                    raise ReactiveChainError(
                        f"模型 '{self.name}' 请求被拒绝（HTTP {status}）— "
                        f"Hint: 检查 api_key/base_url/请求内容"
                    ) from exc
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        raise ReactiveChainError(
            f"模型 '{self.name}' 请求失败（{self.retries} 次尝试）：{last} — "
            f"Hint: 检查 base_url/api_key/网络"
        ) from last