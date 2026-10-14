#!/usr/bin/env python3
"""LangChain side of the ReactiveChain differential benchmark (M6).

Same workloads and semantics as reactchain_side.py, built with upstream
langchain-core LCEL components (FakeListChatModel, PromptTemplate,
RunnableLambda, StrOutputParser). No segment-level skip exists upstream —
retrieval runs on every invoke (that is the honest no-gain report).

Prints JSON: {workload: {median_ms, output, retrieval_calls}}.
Run: uv run --directory benchmarks/differential python langchain_side.py
"""

from __future__ import annotations

import json
import statistics
import sys
import time

from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableLambda, RunnableSequence

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
# 与 reactchain_side.py 对齐：X 类调用（预热1+rag_first3+rag_selective3）
# 返回 FINAL_ANSWER，Y 类（rag_different3）返回 Y_ANSWER——两侧响应表
# 对称，rag_different 输出断言才成立（不同查询不同答案）。
Y_ANSWER = "向量检索用嵌入做语义相似度召回。"

# 选择性证据：每次检索调用计数（langchain 无段级跳过，同 query 重跑仍执行）
RETRIEVAL_CALLS: list[int] = []


def _retrieve(query: str, k: int = 2) -> list[str]:
    """与 reactchain 侧同语义的检索：固定返回语料前 k 段（两侧输出一致）。"""
    RETRIEVAL_CALLS.append(1)
    return CORPUS[:k]


def build_rag() -> RunnableSequence:
    retriever = RunnableLambda(lambda inp: {"docs": _retrieve(inp["query"]), "query": inp["query"]})
    context = RunnableLambda(
        lambda inp: {"context": "\n".join(inp["docs"]), "query": inp["query"]}
    )
    prompt = PromptTemplate.from_template("仅基于资料回答：\n{context}\n问题：{query}")
    # 与 reactchain 侧对称：X 类 7 次（预热1+first3+selective3）、Y 类 3 次
    llm = FakeListChatModel(responses=[FINAL_ANSWER] * 7 + [Y_ANSWER] * 3)
    parser = StrOutputParser()

    return (retriever | context | prompt | llm | parser).with_config(  # type: ignore[return-value]
        {"run_name": "rag"}
    )


def run_workload(name: str, fn, rounds: int = 3) -> dict:
    times: list[float] = []
    result = None
    for _ in range(rounds):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {"median_ms": statistics.median(times), "output": result, "retrieval_calls": None}


def main() -> int:
    rag = build_rag()
    rag.invoke({"query": QUERY_X})  # 预热

    results: dict = {}
    results["rag_first"] = run_workload(
        "rag_first", lambda: rag.invoke({"query": QUERY_X})
    )
    results["rag_selective"] = run_workload(
        "rag_selective", lambda: rag.invoke({"query": QUERY_X})
    )
    results["rag_different"] = run_workload(
        "rag_different", lambda: rag.invoke({"query": QUERY_Y})
    )

    def tool_run() -> str:
        # 同语义工具链：走真实模型调用一轮（与 reactchain 侧 agent 循环等价开销）
        model = FakeListChatModel(responses=["结果是 5"])
        return str(model.invoke([{"role": "user", "content": "2+3=?"}]).content)

    results["toolchain"] = run_workload("toolchain", tool_run)

    # 诚实报告：同 query 重跑时 langchain 仍执行全部段（含检索）
    RETRIEVAL_CALLS.clear()
    probe = build_rag()
    probe.invoke({"query": QUERY_X})
    probe.invoke({"query": QUERY_X})
    results["rag_selective"]["retrieval_calls"] = len(RETRIEVAL_CALLS)

    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())