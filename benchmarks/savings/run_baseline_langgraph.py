"""LangGraph baseline for the savings demo: same pipeline, no selective execution.

Runs the identical two-stage flow (extract -> generate) on LangGraph with the
same real LLM, twice with the same document but different tone. LangGraph has no
cross-run pure-task fingerprint skip, so `extract` is called both times.

This is the A/B partner of `run_savings.py`; together they answer "does swapping
to ReactiveGraph save money?" with measured call counts rather than claims.

Requires langgraph + langchain-core (available in the DeerFlow upstream venv).
Run with: UPSTREAM_PY=/path/to/deer-flow/backend/.venv/bin/python
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from openai import OpenAI

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


class State(TypedDict, total=False):
    document: str
    tone: str
    metrics: str
    answer: str


def extract_node(state: State) -> dict[str, Any]:
    text = _llm(
        "llm_extract",
        "Extract the three key figures as a comma-separated list. "
        f"Document: {state['document']}",
    )
    return {"metrics": text}


def generate_node(state: State) -> dict[str, Any]:
    text = _llm(
        "llm_generate",
        f"Rewrite in a {state['tone']} tone, one sentence, using: {state['metrics']}",
    )
    return {"answer": text}


def build_graph():
    g = StateGraph(State)
    g.add_node("llm_extract", extract_node)
    g.add_node("llm_generate", generate_node)
    g.add_edge(START, "llm_extract")
    g.add_edge("llm_extract", "llm_generate")
    g.add_edge("llm_generate", END)
    return g.compile()


if __name__ == "__main__":
    graph = build_graph()
    runs = [
        {"document": DOCUMENT, "tone": "formal"},
        {"document": DOCUMENT, "tone": "casual"},
    ]
    results = []
    for i, payload in enumerate(runs):
        out = graph.invoke(payload)
        results.append({"run": i, "tone": payload["tone"],
                        "metrics": out.get("metrics"), "answer": out.get("answer")})
    print(json.dumps({
        "engine": "langgraph",
        "model": MODEL,
        "runs": len(runs),
        "llm_calls_total": len(CALLS),
        "llm_calls_by_task": {
            "llm_extract": sum(1 for c in CALLS if c["task"] == "llm_extract"),
            "llm_generate": sum(1 for c in CALLS if c["task"] == "llm_generate"),
        },
        "tokens_in_total": sum(c["tokens_in"] or 0 for c in CALLS),
        "tokens_out_total": sum(c["tokens_out"] or 0 for c in CALLS),
        "calls": CALLS,
        "results": results,
    }, indent=2, ensure_ascii=False))
