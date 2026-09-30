"""链路 6：interrupt / resume（HITL 确认 gate + 恢复继续工具链）。

对照 deer-flow v2 `runtime/` HITL 与 `tools/builtins/clarification_tool.py`：
图在执行工具前经 `n_gates` 段确认 gate——gate 抛 `Interrupt` 挂起 run，
host 记 interrupted；`resume(runId, {"confirmed_i": True})` 从挂起点继续，
通过全部 gate 后执行工具并 finalize。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from reactivegraph import GraphBuilder, GraphInterrupt, Interrupt, ReactiveGraph


def build_hitl_agent(
    host,
    calls: dict[str, int] | None = None,
    n_gates: int = 1,
    graph_id: str = "df_hitl",
) -> ReactiveGraph:
    """构造 HITL agent 图。

    - `n_gates`：确认 gate 段数（多段控制流演示，每段一个 `confirmed_i`）。
    - 链路：run → gate_0（interrupt?）→ ... → gate_{n-1} → run_tool → finalize。
    """
    counters: dict[str, int] = calls if calls is not None else {}

    def make_gate(i: int) -> Callable[[dict[str, Any]], dict[str, Any]]:
        def gate(s: dict[str, Any]) -> dict[str, Any]:
            counters[f"gate{i}"] = counters.get(f"gate{i}", 0) + 1
            if not s.get(f"confirmed_{i}"):
                raise GraphInterrupt([Interrupt({
                    "ask": f"confirm step {i}",
                    "tool": s.get("tool_request"),
                })])
            return {f"approved_{i}": True}

        return gate

    def run_tool(s: dict[str, Any]) -> dict[str, Any]:
        counters["tool"] = counters.get("tool", 0) + 1
        return {"tool_out": f"executed:{s['tool_request']}"}

    def finalize(s: dict[str, Any]) -> dict[str, Any]:
        counters["final"] = counters.get("final", 0) + 1
        return {"done": f"done:{s['tool_out']}"}

    def build(b: GraphBuilder) -> None:
        prev = "run"
        for i in range(n_gates):
            gate_id = f"gate{i}"
            b.task(gate_id, fn=make_gate(i), on=(prev,),
                   reads=(f"confirmed_{i}", "tool_request"),
                   writes=(f"approved_{i}",))
            prev = f"{gate_id}:written"
        b.task("run_tool", fn=run_tool, on=(prev,),
               reads=("tool_request",), writes=("tool_out",))
        b.task("finalize", fn=finalize, on=("run_tool:written",),
               reads=("tool_out",), writes=("done",))

    return ReactiveGraph.build(build, host=host, graph_id=graph_id)