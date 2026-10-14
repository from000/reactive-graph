"""ReactiveChain 真实场景验证套件（design: docs/plans/2026-09-17-
realworld-verification-design.md）。

离线单元测试见同目录 test_realworld.py；真实 API 集成由 REACTIVEGRAPH_REAL=1 门控
（main() 入口，后续任务实现）。
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path

from reactivechain.documents import Document
from reactivechain.embeddings import Embeddings, HashEmbeddings
from reactivechain.llm import BaseLanguageModel, FakeLLM
from reactivechain.parsers import StrOutputParser
from reactivechain.prompt import ChatPromptTemplate
from reactivechain.retriever import VectorStoreRetriever
from reactivechain.runnable import RunnableLambda
from reactivechain.vectorstore import InMemoryVectorStore

# 与 benchmarks/differential 同语料（5 段中文，固定 → 结果可复现、成本可控）
CORPUS = [
    "ReactiveGraph 是反应式图执行引擎，支持事件路由与指纹跳过。",
    "ReactiveChain 是构建在其上的声明式管道组件层。",
    "向量检索通过嵌入相似度做语义搜索，用于 RAG 召回。",
    "段级选择性执行：输入未变时跳过段调用，只重跑受影响的段。",
    "OpenAI 兼容模型通过 /chat/completions 接口接入。",
]

EmbedFn = Callable[[list[str]], list[list[float]]]


class EmbeddingCache:
    """OpenAI embeddings 本地缓存：语料向量只算一次，避免重复计费。

    key = sha1(json(texts))；命中时从 `<dir>/<key>.vec.json` 读，缺失时调
    `embed` 并落盘。`embed` 只在首次调用时执行。
    """

    def __init__(self, embed: EmbedFn, cache_dir: str | Path) -> None:
        self._embed = embed
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _key(self, texts: list[str]) -> str:
        return hashlib.sha1(json.dumps(texts, ensure_ascii=False).encode()).hexdigest()

    def get(self, texts: list[str]) -> list[list[float]]:
        key = self._key(texts)
        path = self._dir / f"{key}.vec.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        vectors = self._embed(texts)
        path.write_text(
            json.dumps(vectors, ensure_ascii=False),
            encoding="utf-8",
        )
        return vectors


# -- Task 2: 调用计数 + RAG 链 + 选择性收益 ------------------------------------


class CountingLLM(BaseLanguageModel):
    """包装 LLM：统计 `_call` 调用次数（effect 段不跳过，每次 invoke 计数）。"""

    def __init__(self, inner: BaseLanguageModel, *, name: str | None = None) -> None:
        super().__init__(name=name or f"counting-{inner.name}")
        self._inner = inner
        self.calls = 0

    def _call(self, messages: list[dict]) -> dict:
        self.calls += 1
        return self._inner._call(messages)

    def _stream_deltas(self, messages: list[dict]) -> object:
        self.calls += 1
        return self._inner._stream_deltas(messages)


class CountingEmbeddings(Embeddings):
    """包装 embeddings：统计 `embed_query` 调用（检索段命中缓存时不再调用）。"""

    def __init__(self, inner: Embeddings | None = None) -> None:
        super().__init__()
        self._inner = inner or HashEmbeddings(dims=8)
        self.calls = 0

    def embed_query(self, text: str) -> list[float]:
        self.calls += 1
        return self._inner.embed_query(text)


def build_rag(
    llm: BaseLanguageModel | None = None,
    embeddings: Embeddings | None = None,
) -> object:
    """真实场景 RAG 链（语义与 benchmarks/differential 相同：retriever 为
    pure 段获得指纹跳过；LLM 为 effect 段不跳过）。"""
    emb = embeddings or HashEmbeddings(dims=8)
    store = InMemoryVectorStore(emb)
    store.add_documents([Document(c, {"i": i}) for i, c in enumerate(CORPUS)])
    retriever = VectorStoreRetriever(store, k=2)

    def context(state: dict) -> dict:
        return {"context": "\n".join(d.page_content for d in state["docs"])}

    chain = (
        retriever
        | RunnableLambda(context, reads={"docs"}, writes={"context"})
        | ChatPromptTemplate(
            [("system", "仅基于资料回答：\n{context}"), ("human", "{query}")]
        )
        | (llm or FakeLLM([f"答案{i}" for i in range(20)]))
        | StrOutputParser()
    )
    return chain  # type: ignore[return-value]


def statistics_median(values: list[float]) -> float:
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    return ordered[mid] if n % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2


def run_workload(name: str, fn, rounds: int = 3) -> dict:
    """跑 fn 取 median 墙钟（含网络），输出采样结果。"""
    times: list[float] = []
    result = None
    for _ in range(rounds):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {
        "name": name,
        "median_ms": statistics_median(times),
        "output": result,
    }


def measure_skip_vs_full(chain, query: str, counter=None) -> dict:
    """同 query 重跑（skip：指纹缓存命中）vs 强制全量（clear_cache 后）的
    调用数/延迟对照——量化段级选择性执行的真实收益。`counter` 为带
    `.calls` 的对象（CountingEmbeddings/CountingLLM），用于取检索/LLM 调用数。"""
    calls0 = counter.calls if counter is not None else None

    t0 = time.perf_counter()
    chain.invoke({"query": query})
    skip_ms = (time.perf_counter() - t0) * 1000
    skip_calls = (counter.calls - calls0) if calls0 is not None else None

    chain.clear_cache()  # 强制全量：清空 pure 段指纹缓存
    calls1 = counter.calls if counter is not None else None

    t0 = time.perf_counter()
    chain.invoke({"query": query})
    full_ms = (time.perf_counter() - t0) * 1000
    full_calls = (counter.calls - calls1) if calls1 is not None else None

    return {
        "skip_ms": round(skip_ms, 4),
        "full_ms": round(full_ms, 4),
        "skip_retrieval_calls": skip_calls,
        "full_retrieval_calls": full_calls,
    }


# -- Task 3: 真实/离线 main() 入口 --------------------------------------------

QUERY_X = "ReactiveChain 的优势是什么？"
QUERY_Y = "向量检索如何工作？"
HERE = Path(__file__).resolve().parent


def main(results_dir: str | None = None) -> int:
    """真实场景验证入口（design §3 阶段 1）。

    `REACTIVEGRAPH_REAL=1`：真实 OpenAI（gpt-4o-mini + text-embedding-3-small）；
    否则离线壳（FakeLLM + HashEmbeddings）只做结构校验（CI/常规跑用）。
    结果写 `<results_dir>/realworld.json`（默认 benchmarks/realworld/.results/）。
    """
    import os
    import sys

    real = os.environ.get("REACTIVEGRAPH_REAL") == "1"
    api_key = os.environ.get("OPENAI_API_KEY")
    if real and not api_key:
        print("REACTIVEGRAPH_REAL=1 需要 OPENAI_API_KEY 环境变量", file=sys.stderr)
        return 2
    if real:
        from reactivechain.embeddings import OpenAICompatEmbeddings
        from reactivechain.llm import OpenAICompatChatModel

        llm = OpenAICompatChatModel(
            base_url="https://api.openai.com/v1",
            model="gpt-4o-mini",
            max_tokens=200,
            trust_env=True,  # 走系统/环境代理（真实环境经代理访问 OpenAI）
        )
        emb_inner: Embeddings = OpenAICompatEmbeddings(
            base_url="https://api.openai.com/v1",
            trust_env=True,
        )
    else:
        llm = FakeLLM([f"离线答案{i}" for i in range(20)])
        emb_inner = HashEmbeddings(dims=8)
    llm_c = CountingLLM(llm)
    emb_c = CountingEmbeddings(emb_inner)
    chain = build_rag(llm=llm_c, embeddings=emb_c)

    # 预热用不同 query（不污染 X 的首查缓存）
    chain.invoke({"query": QUERY_Y})

    def invoke_with_calls(q: str) -> tuple[dict, int]:
        c0 = emb_c.calls
        out = chain.invoke({"query": q})
        return out, emb_c.calls - c0

    out, first_calls = invoke_with_calls(QUERY_X)
    first_out = (out or {}).get("output", "")
    out, sel_calls = invoke_with_calls(QUERY_X)
    sel_out = (out or {}).get("output", "")

    # 收益量化：紧跟 sel（X 缓存最新鲜——fallback 缓存每段单槽，X/Y
    # 交替会互覆缓存，measure 必须紧贴同 query 重跑才反映纯段级收益）。
    measure = measure_skip_vs_full(chain, QUERY_X, counter=emb_c)

    out, diff_calls = invoke_with_calls(QUERY_Y)
    diff_out = (out or {}).get("output", "")

    w_first = run_workload(
        "rag_first", lambda: chain.invoke({"query": QUERY_X}).get("output", "")
    )
    w_sel = run_workload(
        "rag_selective", lambda: chain.invoke({"query": QUERY_X}).get("output", "")
    )
    w_diff = run_workload(
        "rag_different", lambda: chain.invoke({"query": QUERY_Y}).get("output", "")
    )

    report = {
        "mode": "real" if real else "offline",
        "rag_first": {**w_first, "retrieval_calls": first_calls, "output": first_out},
        "rag_selective": {**w_sel, "retrieval_calls": sel_calls, "output": sel_out},
        "rag_different": {**w_diff, "retrieval_calls": diff_calls, "output": diff_out},
        "measure_skip_vs_full": measure,
        "outputs_nonempty": bool(first_out) and bool(sel_out) and bool(diff_out),
        "selective_skips": sel_calls < first_calls,
    }
    out_dir = Path(results_dir) if results_dir else HERE / ".results"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "realworld.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if not report["outputs_nonempty"] or not report["selective_skips"]:
        print(
            f"[realworld] 断言失败：outputs_nonempty={report['outputs_nonempty']} "
            f"selective_skips={report['selective_skips']}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - 显式报错（design §4 成本护栏）
        import sys

        print(f"[realworld] 执行失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        if __import__("os").environ.get("REACTIVEGRAPH_REAL") == "1":
            print(
                "提示：真实模式需可达 api.openai.com 的网络与可用 api_key——"
                "403 unsupported_country_region_territory = 出口 IP 地域受限；"
                "trust_env=True 已启用（走系统代理）；限流=429（脚本自动重试 3 次）",
                file=sys.stderr,
            )
        raise SystemExit(3) from None
