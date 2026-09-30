"""向量存储（对应 langchain_core.vectorstores）。

- `VectorStore`：add_documents / similarity_search / delete 抽象；
- `InMemoryVectorStore`：进程内余弦相似度（默认，测试/离线）；
- `DriverVectorStore`：对接 ReactiveGraph driver 的 VECTOR_UPSERT /
  VECTOR_SEARCH 协议（host 持久化，跨进程/重启存活）。
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Sequence
from typing import Any

from .documents import Document
from .embeddings import Embeddings
from .runnable import ReactiveChainError


class VectorStore:
    """向量存储抽象。"""

    def __init__(self, embeddings: Embeddings | None = None) -> None:
        self.embeddings = embeddings

    def add_documents(
        self,
        documents: Sequence[Document],
        *,
        embeddings: Embeddings | None = None,
    ) -> list[str]:
        raise NotImplementedError  # pragma: no cover

    def similarity_search(
        self,
        query: str,
        *,
        k: int = 4,
        embeddings: Embeddings | None = None,
    ) -> list[Document]:
        raise NotImplementedError  # pragma: no cover

    def delete(self, ids: Sequence[str]) -> None:
        raise NotImplementedError  # pragma: no cover


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


class InMemoryVectorStore(VectorStore):
    """进程内向量库：按命名空间分组，余弦相似度检索。"""

    def __init__(self, embeddings: Embeddings, *, namespace: str = "default") -> None:
        super().__init__(embeddings)
        self.namespace = namespace
        self._points: dict[str, tuple[list[float], Document]] = {}

    def add_documents(
        self,
        documents: Sequence[Document],
        *,
        embeddings: Embeddings | None = None,
    ) -> list[str]:
        emb = embeddings or self.embeddings
        if emb is None:
            raise ReactiveChainError("向量库未配置 embeddings — Hint: 构造时传入")
        ids: list[str] = []
        for doc in documents:
            doc_id = doc.id or str(uuid.uuid4())
            vector = emb.embed_query(doc.page_content)
            self._points[doc_id] = (vector, doc)
            ids.append(doc_id)
        return ids

    def similarity_search(
        self,
        query: str,
        *,
        k: int = 4,
        embeddings: Embeddings | None = None,
    ) -> list[Document]:
        emb = embeddings or self.embeddings
        if emb is None:
            raise ReactiveChainError("向量库未配置 embeddings — Hint: 构造时传入")
        qv = emb.embed_query(query)
        ranked = sorted(
            self._points.values(),
            key=lambda p: _cosine(qv, p[0]),
            reverse=True,
        )
        return [doc for _, doc in ranked[:k]]

    def delete(self, ids: Sequence[str]) -> None:
        for doc_id in ids:
            self._points.pop(doc_id, None)

    def __len__(self) -> int:
        return len(self._points)


class DriverVectorStore(VectorStore):
    """经 ReactiveGraph host 持久化的向量库（VECTOR_UPSERT / VECTOR_SEARCH）。

    用法：
        store = DriverVectorStore(host, embeddings, namespace=["rc", "demo"])
        store.add_documents([...])      # → VECTOR_UPSERT
        store.similarity_search("q")    # → VECTOR_SEARCH（余弦最近邻）
    """

    def __init__(
        self,
        host: Any,
        embeddings: Embeddings,
        *,
        namespace: Sequence[str] = ("rc", "default"),
    ) -> None:
        super().__init__(embeddings)
        if not hasattr(host, "vector_upsert") or not hasattr(host, "vector_search"):
            raise ReactiveChainError(
                "DriverVectorStore 需要支持 VECTOR_* 协议的 host — "
                f"Hint: 传入 ReactiveGraph.build 的 host 实例；得到 {type(host).__name__}"
            )
        self.host = host
        self.namespace = tuple(namespace or ())

    def add_documents(
        self,
        documents: Sequence[Document],
        *,
        embeddings: Embeddings | None = None,
    ) -> list[str]:
        emb = embeddings or self.embeddings
        if emb is None:
            raise ReactiveChainError("向量库未配置 embeddings — Hint: 构造时传入")
        ids: list[str] = []
        for doc in documents:
            doc_id = doc.id or str(uuid.uuid4())
            vector = emb.embed_query(doc.page_content)
            self.host.vector_upsert(
                vector,
                namespace=self.namespace,
                id=doc_id,
                metadata=doc.to_dict(),
            )
            ids.append(doc_id)
        return ids

    def similarity_search(
        self,
        query: str,
        *,
        k: int = 4,
        embeddings: Embeddings | None = None,
    ) -> list[Document]:
        emb = embeddings or self.embeddings
        if emb is None:
            raise ReactiveChainError("向量库未配置 embeddings — Hint: 构造时传入")
        results = self.host.vector_search(
            emb.embed_query(query),
            namespace=self.namespace,
            limit=k,
        )
        docs: list[Document] = []
        for item in results:
            meta = item.get("metadata") or {}
            doc = Document(
                page_content=meta.get("page_content", item.get("id", "")),
                metadata={
                    **{k: v for k, v in meta.items() if k != "page_content"},
                    "score": item.get("score"),
                },
                id=item.get("id"),
            )
            docs.append(doc)
        return docs

    def delete(self, ids: Sequence[str]) -> None:  # pragma: no cover - 协议无 DELETE
        raise ReactiveChainError("VECTOR_* 协议暂不支持删除 — Hint: 用 namespace 隔离生命周期")