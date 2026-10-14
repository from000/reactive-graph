"""LangGraph read-only importer tests.

The upstream package is a test-only dependency; the runtime importer never
imports langgraph and therefore adds no dependency to ReactiveChain users.
"""

from __future__ import annotations

from typing import Any

import pytest
from typing_extensions import TypedDict

from reactivechain.langgraph_import import import_langgraph, inspect_langgraph


class ChatState(TypedDict, total=False):
    x: int
    y: int


def _upstream_graph() -> Any:
    from langgraph.graph import END, START, StateGraph

    def nx(state: ChatState) -> ChatState:
        return {"x": 1}

    def ny(state: ChatState) -> ChatState:
        return {"y": 2}

    graph = StateGraph(ChatState)
    graph.add_node("x", nx)
    graph.add_node("y", ny)
    graph.add_edge(START, "x")
    graph.add_edge("x", "y")
    graph.add_edge("y", END)
    return graph


def test_inspect_langgraph_reports_nodes_edges_and_state_without_executing() -> None:
    report = inspect_langgraph(_upstream_graph())
    assert report["supported"] is True
    assert report["nodes"] == ["x", "y"]
    assert report["edges"] == [["START", "x"], ["x", "y"], ["y", "END"]]
    assert report["state_keys"] == ["x", "y"]
    assert report["unsupported"] == []


def test_import_langgraph_converts_nodes_and_edges_to_reactive_tasks() -> None:
    graph = import_langgraph(_upstream_graph())
    assert [task.id for task in graph.definition.tasks] == ["x", "y"]
    assert graph.definition.route_for("run") == ["x"]
    assert graph.definition.route_for("x:written") == ["y"]
    out = graph.invoke("run", {"x": 0, "y": 0})
    assert out == {"x": 1, "y": 2}


def test_importer_rejects_conditional_edges_with_actionable_report() -> None:
    from langgraph.graph import END, START, StateGraph

    def node(state: dict) -> dict:
        return {"x": 1}

    graph = StateGraph(dict)
    graph.add_node("n", node)
    graph.add_conditional_edges("n", lambda _s: END, {"end": END})
    graph.add_edge(START, "n")
    report = inspect_langgraph(graph)
    assert report["supported"] is False
    assert report["unsupported"] == ["conditional_edges"]
    with pytest.raises(ValueError, match="conditional_edges"):
        import_langgraph(graph)


def test_importer_has_no_runtime_langgraph_dependency() -> None:
    import subprocess
    import sys
    from pathlib import Path

    source = Path(__file__).parents[1] / "reactivechain" / "langgraph_import.py"
    code = subprocess.run(
        [sys.executable, "-c", f"import {source}; print('ok')"],
        capture_output=True,
        text=True,
    )
    # Importing the module must not import langgraph (runtime dependency boundary).
    assert code.returncode != 0 or "ok" in code.stdout
    assert "from langgraph" not in source.read_text()
    assert "import langgraph" not in source.read_text()
