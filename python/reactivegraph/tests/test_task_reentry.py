"""Re-entry budget (`max_runs`) + task-emitted follow-up events (`emits`).

Real agent loops need `model → tools → model → …` to be expressible. The
Driver's default task budget is at-most-once per run (a cycle terminates on the
second round); a task opts into re-entry with `max_runs`. Loop termination is
the task's own decision: it returns a `TaskOutcome` whose `emits` list overrides
the implicit `<task_id>:written` follow-up event, and `emits=()` stops the loop.

Both execution paths (in-process fallback and real Driver) must agree.
"""

from __future__ import annotations

import os
from pathlib import Path

from reactivegraph.graph import GraphBuilder, ReactiveGraph, TaskDef, TaskOutcome

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[3]


def _real_driver_env() -> dict:
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(REPO_ROOT / "packages" / "driver" / "dist" / "main.js")
    env["REACTIVEGRAPH_NODE_BIN"] = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    return env


def _loop_graph(calls: list[str], *, rounds: int = 3):
    """model → tools → model …; the model stops the loop after `rounds` turns."""

    def model(state: dict) -> TaskOutcome:
        n = int(state.get("n", 0)) + 1
        calls.append(f"model:{n}")
        return TaskOutcome(
            update={"n": n},
            # 收敛判定在任务侧：最后一轮不再发出下游事件。
            emits=() if n >= rounds else ("model:written",),
        )

    def tools(state: dict) -> TaskOutcome:
        calls.append(f"tools:{state['n']}")
        return TaskOutcome(update={}, emits=("tools:written",))

    def build(b: GraphBuilder) -> None:
        b.task("model", fn=model, on=("run", "tools:written"), writes=("n",), max_runs=rounds)
        b.task("tools", fn=tools, on=("model:written",), max_runs=rounds)

    return ReactiveGraph.build(build, graph_id="agent_loop")


class TestWireSpec:
    def test_wire_spec_carries_max_runs(self) -> None:
        builder = GraphBuilder("g")
        builder.task("loop", kind="effect", fn=lambda s: {}, on=("run",), max_runs=4)
        spec = ReactiveGraph(builder.build())._to_wire_spec()
        (task,) = spec["tasks"]
        assert task["maxRuns"] == 4

    def test_wire_spec_default_budget_is_at_most_once(self) -> None:
        builder = GraphBuilder("g")
        builder.task("once", kind="effect", fn=lambda s: {}, on=("run",))
        spec = ReactiveGraph(builder.build())._to_wire_spec()
        (task,) = spec["tasks"]
        assert task["maxRuns"] == 1

    def test_task_def_defaults_to_single_run(self) -> None:
        assert TaskDef(id="t").max_runs == 1


class TestTaskOutcome:
    def test_wrapper_forwards_emits(self) -> None:
        td = TaskDef(id="t", fn=lambda s: TaskOutcome(update={"a": 1}, emits=("custom:event",)))
        payload = ReactiveGraph._task_success_wrapper(td)({})
        assert payload["emits"] == ["custom:event"]
        assert payload["writes"] == ["a"]

    def test_wrapper_forwards_empty_emits_as_loop_stop(self) -> None:
        td = TaskDef(id="t", fn=lambda s: TaskOutcome(update={}, emits=()))
        payload = ReactiveGraph._task_success_wrapper(td)({})
        assert payload["emits"] == []

    def test_wrapper_omits_emits_when_not_declared(self) -> None:
        td = TaskDef(id="t", fn=lambda s: {"a": 1})
        payload = ReactiveGraph._task_success_wrapper(td)({})
        assert "emits" not in payload

    def test_wrapper_still_supports_legacy_update_receipt_tuple(self) -> None:
        td = TaskDef(id="t", kind="effect", fn=lambda s: ({"sent": True}, "rcpt"))
        payload = ReactiveGraph._task_success_wrapper(td)({})
        assert payload["external_receipts"] == [{"receipt": "rcpt"}]
        assert "emits" not in payload


class TestFallbackReentry:
    def test_max_runs_lets_a_model_tools_loop_converge(self) -> None:
        calls: list[str] = []
        out = _loop_graph(calls).invoke("run", {})
        assert calls == ["model:1", "tools:1", "model:2", "tools:2", "model:3"]
        assert out["n"] == 3

    def test_default_budget_keeps_a_cycle_at_most_once_per_task(self) -> None:
        calls: list[str] = []

        def a(state: dict) -> dict:
            calls.append("a")
            return {"x": state.get("x", 0) + 1}

        def b(state: dict) -> dict:
            calls.append("b")
            return {"y": state.get("y", 0) + 1}

        def build(bld: GraphBuilder) -> None:
            bld.task("a", fn=a, on=("run", "b:written"))
            bld.task("b", fn=b, on=("a:written",))

        out = ReactiveGraph.build(build).invoke("run", {})
        assert calls == ["a", "b"]
        assert out["x"] == 1 and out["y"] == 1


class TestDriverReentry:
    def test_driver_loop_matches_fallback(self) -> None:
        from reactivegraph.host import DriverHost

        fb_calls: list[str] = []
        fb = _loop_graph(fb_calls).invoke("run", {})

        dr_calls: list[str] = []
        graph = _loop_graph(dr_calls)
        host = DriverHost(env=_real_driver_env())
        try:
            host.start()
            host.handshake()
            dgraph = ReactiveGraph(graph.definition, host=host)
            out = dgraph.invoke("run", {})
        finally:
            host.close()
        assert out["n"] == fb["n"] == 3
        assert dr_calls == fb_calls

    def test_driver_default_budget_matches_fallback_cycle(self) -> None:
        from reactivegraph.host import DriverHost

        def build(bld: GraphBuilder) -> None:
            bld.task("a", fn=lambda s: {"x": s.get("x", 0) + 1}, on=("run", "b:written"))
            bld.task("b", fn=lambda s: {"y": s.get("y", 0) + 1}, on=("a:written",))

        graph = ReactiveGraph.build(build)
        fb = graph.invoke("run", {})
        host = DriverHost(env=_real_driver_env())
        try:
            host.start()
            host.handshake()
            out = ReactiveGraph(graph.definition, host=host).invoke("run", {})
        finally:
            host.close()
        assert out["x"] == fb["x"] == 1
        assert out["y"] == fb["y"] == 1
