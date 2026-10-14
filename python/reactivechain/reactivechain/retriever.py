"""检索器（对应 langchain.retrievers）。

所有检索器都是 Runnable：输入 `query` → 输出 `docs`（Document 列表）。
自建检索器：`RunnableLambda(fn, reads={"query"}, writes={"docs"})` 即可。
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Sequence

from .documents import Document
from .llm import BaseLanguageModel
from .runnable import ReactiveChainError, Runnable, State
from .vectorstore import VectorStore


class Retriever(Runnable):
    """检索器基类：reads={"query"}，writes={"docs"}。"""

    reads: set[str] = {"query"}
    writes: set[str] = {"docs"}

    def _search(self, query: str, state: State) -> list[Document]:
        """Run the backend search and return matching records."""
        raise NotImplementedError  # pragma: no cover

    def invoke(self, state: State) -> State:
        """Run this segment on *state* and return the keys it writes."""
        if "query" not in state:
            raise ReactiveChainError(
                "检索器缺少输入键 'query' — Hint: 上游段需写入 query"
            )
        return {"docs": self._search(str(state["query"]), state)}


class VectorStoreRetriever(Retriever):
    """向量库检索：query 嵌入 → 最近邻。

    确定性（同 query → 同 docs）：声明 pure 获得引擎输入指纹跳过
    （决策 R1；文档存储不变时跳过安全）。
    """

    pure: bool = True

    def __init__(
        self,
        vectorstore: VectorStore,
        *,
        k: int = 4,
        name: str | None = None,
    ) -> None:
        """Initialise the VectorStoreRetriever."""
        self.vectorstore = vectorstore
        self.k = k
        self.name = name or f"vector_retriever:{type(vectorstore).__name__}"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"retriever:{self.name}"

    def _search(self, query: str, state: State) -> list[Document]:
        """Run the backend search and return matching records."""
        return self.vectorstore.similarity_search(query, k=self.k)


class BM25Retriever(Retriever):
    """BM25 词频检索：语料内建（无需嵌入），适合小规模文档集。"""

    _TOKEN = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")

    def __init__(
        self,
        documents: Sequence[Document],
        *,
        k: int = 4,
        k1: float = 1.5,
        b: float = 0.75,
        name: str | None = None,
    ) -> None:
        """Initialise the BM25Retriever."""
        self.documents = list(documents)
        self.k = k
        self.k1 = k1
        self.b = b
        self.name = name or f"bm25:{len(documents)}docs"
        self._corpus = [self._tokenize(d.page_content) for d in self.documents]
        self._doc_lens = [len(t) for t in self._corpus]
        self._avg_len = (sum(self._doc_lens) / len(self._doc_lens)) if self._doc_lens else 1.0
        self._df: Counter = Counter()
        for tokens in self._corpus:
            self._df.update(set(tokens))

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"retriever:{self.name}"

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        return BM25Retriever._TOKEN.findall(text.lower())

    def _score(self, query_tokens: list[str], idx: int) -> float:
        tokens = self._corpus[idx]
        n = len(self.documents)
        tf = Counter(tokens)
        total = 0.0
        for qt in query_tokens:
            if qt not in tf:
                continue
            idf = math.log(1 + (n - self._df[qt] + 0.5) / (self._df[qt] + 0.5))
            tf_part = tf[qt] * (self.k1 + 1)
            norm = 1 - self.b + self.b * self._doc_lens[idx] / self._avg_len
            tf_part /= tf[qt] + self.k1 * norm
            total += idf * tf_part
        return total

    def _search(self, query: str, state: State) -> list[Document]:
        """Run the backend search and return matching records."""
        query_tokens = self._tokenize(query)
        if not query_tokens or not self.documents:
            return []
        # 一次性算分（避免 top-k 截断后重复计算）：正分项按分排序取前 k。
        scored = [(self._score(query_tokens, i), i) for i in range(len(self.documents))]
        return [
            self.documents[i]
            for score, i in sorted(scored, key=lambda pair: pair[0], reverse=True)
            if score > 0
        ][: self.k]


class MultiQueryRetriever(Retriever):
    """多查询检索：LLM 生成查询变体 → 合并结果去重。"""

    def __init__(
        self,
        base_retriever: Retriever,
        llm: BaseLanguageModel,
        *,
        n_queries: int = 3,
        name: str | None = None,
    ) -> None:
        """Initialise the MultiQueryRetriever."""
        self.base_retriever = base_retriever
        self.llm = llm
        self.n_queries = n_queries
        self.name = name or f"multi_query:{base_retriever.id}"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"retriever:{self.name}"

    def _search(self, query: str, state: State) -> list[Document]:
        """Run the backend search and return matching records."""
        variants = [query]
        if self.n_queries > 1:
            prompt = (
                f"生成 {self.n_queries - 1} 个与以下问题等价的变体查询，每行一个：\n{query}"
            )
            out = self.llm.invoke(
                {"messages": [{"role": "user", "content": prompt}]}
            )["output"]
            variants += [ln.strip() for ln in out.splitlines() if ln.strip()][
                : self.n_queries - 1
            ]
        seen: set[str] = set()
        merged: list[Document] = []
        for v in variants:
            for doc in self.base_retriever._search(v, state):
                key = doc.page_content
                if key not in seen:
                    seen.add(key)
                    merged.append(doc)
        return merged


class EnsembleRetriever(Retriever):
    """集成检索：多个检索器按 RRF 加权合并（去重）。"""

    def __init__(
        self,
        retrievers: Sequence[Retriever],
        *,
        weights: Sequence[float] | None = None,
        k: int = 4,
        name: str | None = None,
    ) -> None:
        """Initialise the EnsembleRetriever."""
        if not retrievers:
            raise ValueError("EnsembleRetriever 至少需要一个检索器")
        self.retrievers = list(retrievers)
        self.weights = (
            list(weights)
            if weights
            else [1.0 / len(retrievers)] * len(retrievers)
        )
        if len(self.weights) != len(retrievers):
            raise ValueError("weights 数量必须与 retrievers 一致")
        self.k = k
        self.name = name or "ensemble"

    @property
    def id(self) -> str:
        """Deterministic identifier used in cache keys and error messages."""
        return f"retriever:{self.name}"

    def _search(self, query: str, state: State) -> list[Document]:
        """Run the backend search and return matching records."""
        scores: dict[str, float] = {}
        docs: dict[str, Document] = {}
        for retriever, weight in zip(self.retrievers, self.weights, strict=False):
            for rank, doc in enumerate(retriever._search(query, state)):
                key = doc.page_content
                scores[key] = scores.get(key, 0.0) + weight / (60 + rank)
                docs[key] = doc
        ranked = sorted(scores, key=lambda k: scores[k], reverse=True)
        return [docs[key] for key in ranked[: self.k]]