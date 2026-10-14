"""真实 LLM「选择性执行省钱」demo：pure 子任务在重跑时被跳过。

问题：换 ReactiveGraph 到底省钱吗？
答案（本脚本用真实 LLM 实测）：
- **agent 主链**（model/tools 是 effect 节点）→ **不省**，每次调用都发生。
- **确定性 LLM 子任务**（抽取/分类/摘要，声明 kind="pure" + reads）→ **省**：
  输入不变时指纹命中，直接跳过，不调 LLM。

场景（贴近真实）：
  1. 一份长文档先做一次"抽取"（LLM 调用 1），结果被后续步骤复用。
  2. 用户改了**与文档无关**的东西（比如语气/目标），整条 pipeline 重跑。
  3. LangGraph 语义 = 全量重跑 → 抽取再调一次 LLM。
     ReactiveGraph = 抽取的 reads 没变 → 跳过 → 不调 LLM。

需要 CUSTOM_LLM_URL / CUSTOM_LLM_API_KEY / CUSTOM_MODEL（默认 Agnes）。
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from openai import OpenAI  # noqa: E402
from reactivegraph.graph import GraphBuilder, ReactiveGraph  # noqa: E402

MODEL = os.environ.get("CUSTOM_MODEL", "agnes-2.5-flash")
CLIENT = OpenAI(
    base_url=os.environ["CUSTOM_LLM_URL"],
    api_key=os.environ["CUSTOM_LLM_API_KEY"],
    max_retries=0,
    timeout=300,
)

CALLS: list[dict] = []


def _llm(task: str, prompt: str, *, max_tokens: int = 600) -> str:
    t0 = time.perf_counter()
    r = CLIENT.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=max_tokens,
    )
    dt = time.perf_counter() - t0
    usage = r.usage
    CALLS.append({
        "task": task,
        "s": round(dt, 3),
        "tokens_in": getattr(usage, "prompt_tokens", None),
        "tokens_out": getattr(usage, "completion_tokens", None),
    })
    return (r.choices[0].message.content or "").strip()


DOCUMENT = (
    "Quarterly report: revenue grew 12% to $4.2M; churn fell to 2.1%; "
    "the APAC region contributed 38% of new bookings."
)

# 抽取结果缓存（真实场景里这是 LLM 的确定性输出）
EXTRACT_USD = 0.00012  # $0.12 / 1M input-like pricing proxy; demo 用估算


def build_extract(b: GraphBuilder) -> None:
    """阶段 1：确定性抽取（pure）。输入不变就该跳过。"""

    def extract(state):
        # 只依赖 document；tone 变化不应让它重跑
        text = _llm(
            "llm_extract",
            "Extract the three key figures as a comma-separated list. "
            f"Document: {state['document']}",
        )
        return {"metrics": text}

    b.task(
        "llm_extract",
        kind="pure",
        fn=extract,
        on=("run",),
        reads=("document",),
        writes=("metrics",),
        estimated_tokens_in=180,
        estimated_tokens_out=40,
        estimated_usd=EXTRACT_USD,
    )


def build_generate(b: GraphBuilder) -> None:
    """阶段 2：生成（依赖用户 tone，每次都真的要重跑）。"""

    def generate(state):
        text = _llm(
            "llm_generate",
            f"Rewrite in a {state['tone']} tone, one sentence, using: {state['metrics']}",
        )
        return {"answer": text}

    b.task(
        "llm_generate",
        kind="effect",
        fn=generate,
        on=("run",),
        reads=("metrics", "tone"),
        writes=("answer",),
    )


def build(b: GraphBuilder) -> None:
    build_extract(b)
    build_generate(b)


if __name__ == "__main__":
    graph = ReactiveGraph.build(build, graph_id="savings_demo")
    # 同输入跑两次：第二次 tone 变了，但 document 没变
    runs = [
        {"document": DOCUMENT, "tone": "formal"},
        {"document": DOCUMENT, "tone": "casual"},
    ]
    results = []
    for i, payload in enumerate(runs):
        out = graph.invoke("run", payload)
        results.append({"run": i, "tone": payload["tone"],
                        "metrics": out.get("metrics"),
                        "answer": out.get("answer")})
    explain = graph.explain_run()
    saved = graph.cost_saved()
    report = {
        "model": MODEL,
        "runs": len(runs),
        "llm_calls_total": len(CALLS),
        "llm_calls_by_task": {
            "llm_extract": sum(1 for c in CALLS if c["task"] == "llm_extract"),
            "llm_generate": sum(1 for c in CALLS if c["task"] == "llm_generate"),
        },
        "calls": CALLS,
        "results": results,
        "explain": explain,
        "cost_saved": saved,
        "why_skipped_extract": graph.why_skipped("llm_extract"),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
