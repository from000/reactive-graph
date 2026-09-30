"""嵌入接口（对应 langchain_community.embeddings）。

- `Embeddings`：embed_documents / embed_query 抽象；
- `OpenAICompatEmbeddings`：OpenAI 兼容 /embeddings（纯 stdlib urllib）；
- `HashEmbeddings`：确定性 hash 嵌入——测试/离线演示，不依赖模型服务。
"""

from __future__ import annotations

import hashlib
import json
import ssl
import urllib.request

from .llm import os_getenv
from .runnable import ReactiveChainError

_SSL_CTX = ssl.create_default_context()


class Embeddings:
    """嵌入抽象。"""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents, one vector each."""
        raise NotImplementedError  # pragma: no cover

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query string."""
        raise NotImplementedError  # pragma: no cover


class OpenAICompatEmbeddings(Embeddings):
    """OpenAI 兼容嵌入（纯 stdlib）。

    参数：
        base_url / api_key / env_key: 同 OpenAICompatChatModel。
        model: 嵌入模型名（默认 text-embedding-3-small）。
        dims: 输出维度（hash 嵌入用；真实服务忽略）。
        timeout_s / trust_env: 网络参数。
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        env_key: str = "OPENAI_API_KEY",
        model: str = "text-embedding-3-small",
        timeout_s: float = 60.0,
        trust_env: bool = False,
        retries: int = 2,
    ) -> None:
        """Initialise the OpenAICompatEmbeddings."""
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key if api_key is not None else os_getenv(env_key)
        self.model = model
        self.timeout_s = timeout_s
        self.trust_env = trust_env
        self.retries = retries

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents, one vector each."""
        return [self._embed_one(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query string."""
        return self._embed_one(text)

    def _embed_one(self, text: str) -> list[float]:
        """Embed a single text, returning its vector."""
        if not self.api_key:
            raise ReactiveChainError(
                "OpenAICompatEmbeddings 未配置 api_key — Hint: 传参 api_key 或设环境变量"
            )
        body = json.dumps({"model": self.model, "input": text}).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}/embeddings",
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "ReactiveChain/0.1",
            },
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        # 5xx/网络错误重试（最多 retries 次额外尝试），4xx 不重试
        # （错误统一抛 ReactiveChainError，含 Hint）。
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                if self.trust_env:
                    with urllib.request.urlopen(
                        req, timeout=self.timeout_s, context=_SSL_CTX
                    ) as r:
                        data = json.loads(r.read().decode("utf-8"))
                else:
                    with opener.open(req, timeout=self.timeout_s) as r:
                        data = json.loads(r.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as exc:
                if exc.code >= 500 and attempt < self.retries:
                    last = exc
                    continue
                raise ReactiveChainError(
                    f"嵌入请求失败：HTTP {exc.code} — Hint: 检查 base_url/api_key/网络"
                ) from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                last = exc
                if attempt < self.retries:
                    continue
                raise ReactiveChainError(
                    f"嵌入请求失败：{exc} — Hint: 检查 base_url/api_key/网络"
                ) from exc
        else:
            assert last is not None
            raise ReactiveChainError(f"嵌入请求失败：{last}") from last
        try:
            return [float(v) for v in data["data"][0]["embedding"]]
        except (KeyError, IndexError) as exc:
            raise ReactiveChainError(
                "嵌入响应缺少 data[0].embedding — Hint: 检查 base_url 是否 OpenAI 兼容"
            ) from exc


class HashEmbeddings(Embeddings):
    """确定性 hash 嵌入：把 token 词袋哈希进固定维度（测试与离线演示）。"""

    def __init__(self, dims: int = 64) -> None:
        """Initialise the HashEmbeddings."""
        self.dims = dims

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents, one vector each."""
        return [self.embed_query(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query string."""
        vec = [0.0] * self.dims
        for token in text.lower().split():
            h = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:8], 16)
            vec[h % self.dims] += 1.0
        norm = sum(v * v for v in vec) ** 0.5 or 1.0
        return [v / norm for v in vec]