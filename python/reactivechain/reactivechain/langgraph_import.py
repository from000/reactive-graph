"""Read-only LangGraph importer.

This module analyzes an already-constructed upstream ``StateGraph`` and converts
it. It does not import the upstream package itself; callers pass the object in,
ReactiveChain runtime keeps zero dependency on it. Unsupported constructs are
reported instead of silently changing semantics.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = ("import_langgraph", "inspect_langgraph")


def _nodes(graph: Any) -> dict[str, Any]:
    return getattr(graph, "nodes", {}) or {}


def _edges(graph: Any) -> list[tuple[str, str]]:
    return list(getattr(graph, "edges", []) or [])


def _conditional_edges(graph: Any) -> list[Any]:
    return list((getattr(graph, "branches", {}) or {}).values())


def inspect_langgraph(graph: Any) -> dict[str, Any]:
    """Analyze a LangGraph StateGraph without executing any node."""
    if not hasattr(graph, "add_node"):
        raise ValueError("inspect_langgraph expects a LangGraph StateGraph object")

    nodes = _nodes(graph)
    edges = _edges(graph)
    conditional = _conditional_edges(graph)
    state_schema = getattr(graph, "state_schema", None)
    annotations = getattr(state_schema, "__annotations__", {})
    unsupported: list[str] = []
    if conditional:
        unsupported.append("conditional_edges")
    if getattr(graph, "compiled", False) is True:
        unsupported.append("already_compiled")

    normalized_edges = sorted(
        [
            [
                "START" if source == "__start__" else "END" if source == "__end__" else source,
                "START" if target == "__start__" else "END" if target == "__end__" else target,
            ]
            for source, target in edges
        ]
    )
    return {
        "supported": not unsupported,
        "nodes": list(nodes),
        "edges": normalized_edges,
        "state_keys": list(annotations),
        "unsupported": unsupported,
    }


def import_langgraph(graph: Any) -> Any:
    """Convert a supported LangGraph StateGraph into ReactiveGraph.

    Ordinary edges become event routes. ``START -> n`` maps to ``run -> n``,
    and ``a -> b`` maps to ``a:written -> b``. END edges are omitted because
    ReactiveGraph returns final thread state after the task cascade.
    """
    from reactivegraph import GraphBuilder, ReactiveGraph

    report = inspect_langgraph(graph)
    if not report["supported"]:
        raise ValueError(
            "unsupported LangGraph constructs: " + ", ".join(report["unsupported"])
        )

    nodes = _nodes(graph)

    def build(builder: GraphBuilder) -> None:
        for node_id, node in nodes.items():
            fn: Callable[[dict], dict] = _node_function(node)
            builder.task(str(node_id), fn=fn, on=(), kind="effect")
        for source, target in _edges(graph):
            if source == "__end__" or target == "__end__":
                continue
            event = "run" if source == "__start__" else f"{source}:written"
            builder.on(event, str(target))

    return ReactiveGraph.build(build, graph_id="imported_langgraph")


def _node_function(node: Any) -> Callable[[dict], dict]:
    """Extract a callable without invoking it (read-only import)."""

    def invoke(state: dict) -> dict:
        spec = getattr(node, "runnable", node)
        fn = getattr(spec, "func", None) or spec
        result = fn(state)
        return result if isinstance(result, dict) else {}

    return invoke
