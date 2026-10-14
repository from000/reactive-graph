"""LangGraph side of the differential benchmark (plan §16 reproducibility).

Runs the SAME deterministic graphs under the REAL upstream `langgraph`
(pin `langgraph==1.2.11`) and writes a JSON report
(workload -> output + median ms + per-run inputs increment so the reactive
side (harness.test.ts) evaluates identical per-run inputs). Orchestration and
output-equality assertion live in `run_differential.py`.

Workloads mirror docs/benchmarks.md:
- chat:      2-node graph, warm median invoke latency
- wide:      1000 independent nodes (fan-out), one field changed per run
- selective: 1000 nodes / 10 affected per run
- nogain:    1000-node dependency chain (every node depends on the seed)
"""

from __future__ import annotations

import json
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

HERE = Path(__file__).resolve().parent
RESULTS = HERE / ".results"
RUNS = 3
N = 1000


class Out(TypedDict):
    pass


def _chat() -> tuple[Any, Callable[[int], dict]]:
    class S(TypedDict, total=False):
        x: int
        y: int

    def nx(state: S) -> S:
        return {"x": 1}

    def ny(state: S) -> S:
        return {"y": 2}

    g = StateGraph(S)
    g.add_node("x", nx)
    g.add_node("y", ny)
    g.add_edge(START, "x")
    g.add_edge("x", "y")
    g.add_edge("y", END)
    return g.compile(), lambda _k: {"x": 0, "y": 0}


def _wide(affected: int) -> tuple[Any, Callable[[int], dict]]:
    fields: dict[str, type] = {}
    for i in range(N):
        fields[f"k{i}"] = int
        fields[f"v{i}"] = int
    schema = TypedDict("WideState", fields, total=False)  # noqa: C408

    g = StateGraph(schema)
    for i in range(N):
        def node(state: dict, _i: int = i) -> dict:
            k = state.get(f"k{_i}", 0)
            return {f"v{_i}": k + 1}

        g.add_node(f"t{i}", node)
        g.add_edge(START, f"t{i}")
        g.add_edge(f"t{i}", END)

    def inputs(k: int) -> dict:
        inp: dict[str, int] = {"k0": k}
        for i in range(1, N):
            inp[f"k{i}"] = k if i < affected else 0
        return inp

    return g.compile(), inputs


def _nogain() -> tuple[Any, Callable[[int], dict]]:
    fields: dict[str, type] = {"seed": int}
    for i in range(N):
        fields[f"v{i}"] = int
    schema = TypedDict("NogainState", fields, total=False)  # noqa: C408

    g = StateGraph(schema)
    for i in range(N):
        def node(state: dict, _i: int = i) -> dict:
            source = state.get("seed" if _i == 0 else f"v{_i-1}", 0)
            return {f"v{_i}": source + 1}

        g.add_node(f"t{i}", node)
        if i == 0:
            g.add_edge(START, "t0")
        else:
            g.add_edge(f"t{i-1}", f"t{i}")
    g.add_edge(f"t{N-1}", END)
    return g.compile(), lambda k: {"seed": k}


def measure(graph: Any, inputs_for: Callable[[int], dict]) -> dict:
    graph.invoke(inputs_for(0), {"configurable": {"thread_id": "warm"}})
    times: list[float] = []
    outputs: list[dict] = []
    for r in range(1, RUNS + 1):
        t0 = time.perf_counter()
        out = graph.invoke(inputs_for(r), {"configurable": {"thread_id": f"run{r}"}})
        times.append((time.perf_counter() - t0) * 1000)
        outputs.append(dict(out))
    return {"median_ms": round(statistics.median(times), 3), "output": outputs[-1]}


def main() -> None:
    report: dict[str, Any] = {"runs": RUNS, "nodes": N}
    report["chat"] = measure(*_chat())
    report["wide"] = measure(*_wide(1))
    report["selective"] = measure(*_wide(10))
    report["nogain"] = measure(*_nogain())
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "langgraph.json").write_text(json.dumps(report, sort_keys=True))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()