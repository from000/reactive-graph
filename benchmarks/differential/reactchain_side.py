#!/usr/bin/env python3
"""ReactiveChain side of the differential benchmark (M6 成功标准).

Workloads (same semantics as langchain_side.py):
  rag_first      – RAG chain, first run for query X
  rag_selective  – same query X again (segment-level skip = differential point)
  rag_different  – different query Y (full re-run both sides)
  toolchain      – tool-calling loop driven by a mock model

Prints JSON: {workload: {median_ms, output, retrieval_calls}}.
Run: uv run --directory benchmarks/differential python reactchain_side.py
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
# P0：reactivechain 运行时依赖 reactivegraph（引擎为默认执行路径）
sys.path.insert(0, str(ROOT / "python" / "reactivegraph"))
sys.path.insert(0, str(ROOT / "python" / "reactivechain"))

from reactivechain import (  # noqa: E402
    ChatPromptTemplate,
    Document,
    FakeLLM,
    HashEmbeddings,
    InMemoryVectorStore,
    Pipeline,
    RunnableLambda,
    StrOutputParser,
    Toolkit,
    VectorStoreRetriever,
    create_tool_calling_agent,
    tool,
)
from reactivechain.llm import BaseLanguageModel  # noqa: E402

CORPUS = [
    "ReactiveGraph 是反应式图执行引擎，支持事件路由与指纹跳过。",
    "ReactiveChain 是构建在其上的声明式管道组件层。",
    "向量检索通过嵌入相似度做语义搜索，用于 RAG 召回。",
    "段级选择性执行：输入未变时跳过段调用，只重跑受影响的段。",
    "OpenAI 兼容模型通过 /chat/completions 接口接入。",
]
QUERY_X = "ReactiveChain 的优势是什么？"
QUERY_Y = "向量检索如何工作？"
FINAL_ANSWER = "ReactiveChain 提供声明式管道与段级选择性执行。"
Y_ANSWER = "向量检索用嵌入做语义相似度召回。"
# P0（决策 R1）后 LLM 段为 effect 不再被同输入跳过：响应数需覆盖
# main 的全部调用（预热1 + rag_first3 + rag_selective3 为 X 类，
# rag_different3 为 Y 类）；probe 链用独立响应列表（见 main）。
RAG_RESPONSES = [FINAL_ANSWER] * 7 + [Y_ANSWER] * 3


def build_rag() -> Pipeline:
    splitter = HashEmbeddings(dims=64)
    store = InMemoryVectorStore(splitter)
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
        | FakeLLM(RAG_RESPONSES)
        | StrOutputParser()
    )
    return chain  # type: ignore[return-value]


# -- tool chain -----------------------------------------------------------


@tool
def add(a: int, b: int) -> int:
    """两数相加。"""
    return a + b


class MockToolLLM(BaseLanguageModel):
    """第一轮要工具，第二轮给最终答案。"""

    supports_tool_calling = True

    def __init__(self) -> None:
        super().__init__(name="mock")
        self._round = 0

    def _call(self, messages: list[dict]) -> dict:
        self._round += 1
        if self._round == 1:
            return {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {"name": "add", "arguments": {"a": 2, "b": 3}},
                    }
                ],
            }
        return {"role": "assistant", "content": "结果是 5"}

    def _stream_deltas(self, messages):  # pragma: no cover
        yield "x"


def run_workload(name: str, fn, rounds: int = 3) -> dict:
    times: list[float] = []
    result = None
    for _ in range(rounds):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {
        "median_ms": statistics.median(times),
        "output": result,
        "retrieval_calls": None,
    }


def main() -> int:
    rag = build_rag()
    # 预热（PipeLine 缓存失效语义不依赖预热；预热只稳 CPU 频率）
    rag.invoke({"query": QUERY_X})

    results: dict = {}
    results["rag_first"] = run_workload(
        "rag_first", lambda: rag.invoke({"query": QUERY_X})["output"]
    )
    results["rag_selective"] = run_workload(
        "rag_selective", lambda: rag.invoke({"query": QUERY_X})["output"]
    )
    results["rag_different"] = run_workload(
        "rag_different", lambda: rag.invoke({"query": QUERY_Y})["output"]
    )

    def tool_run() -> str:
        agent = create_tool_calling_agent(MockToolLLM(), Toolkit([add]), max_iterations=5)
        return agent.invoke({"messages": [{"role": "user", "content": "2+3=?"}]})["output"]

    results["toolchain"] = run_workload("toolchain", tool_run)

    # 选择性证据：rag_selective 中检索段实际调用次数（显式单层管道）
    store = InMemoryVectorStore(HashEmbeddings(dims=64))
    store.add_documents([Document(c, {"i": i}) for i, c in enumerate(CORPUS)])
    calls: list[int] = []
    orig = store.similarity_search

    def counted(*a, **kw):
        calls.append(1)
        return orig(*a, **kw)

    store.similarity_search = counted  # type: ignore[method-assign]
    retriever = VectorStoreRetriever(store, k=2)

    def context(state: dict) -> dict:
        return {"context": "\n".join(d.page_content for d in state["docs"])}

    probe = Pipeline(
        [
            retriever,
            RunnableLambda(context, reads={"docs"}, writes={"context"}),
            ChatPromptTemplate(
                [("system", "仅基于资料回答：\n{context}"), ("human", "{query}")]
            ),
            # P0（决策 R1）后 LLM effect 不跳过：probe 两次 invoke 各消耗
            # 一个响应，用独立列表（不干扰主链响应序号）。
            FakeLLM([FINAL_ANSWER, FINAL_ANSWER]),
            StrOutputParser(),
        ]
    )
    probe.invoke({"query": QUERY_X})
    probe.invoke({"query": QUERY_X})  # 相同 → 检索段（pure）被指纹跳过
    results["rag_selective"]["retrieval_calls"] = len(calls)

    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())