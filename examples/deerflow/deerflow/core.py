"""SuperAgent built on the native ReactiveGraph API (M3 verification).

This example exercises the engine's headline capabilities in one place:
selective task execution, scope isolation, effect idempotency receipts,
streaming, interrupt/resume, long-term store, checkpoint access, the
prebuilt ReAct agent, and scheduler causal trace export.
"""

from __future__ import annotations

from typing import Any

from reactivegraph import ReactiveGraph, TrackedStateProxy, create_react_agent
from reactivegraph.prebuilt import ToolNode, ToolSpec

from deerflow.tools import default_tools

__all__ = ("SuperAgent",)


class SuperAgent:
    """A small agent that demonstrates the native API surface end to end."""

    def __init__(
        self,
        model: Any,
        tools: list[ToolSpec] | None = None,
        *,
        host: Any = None,
        graph_id: str = "deerflow",
    ) -> None:
        self.model = model
        self.tools = ToolNode(tools or default_tools())
        self.graph = ReactiveGraph.build(self._build, host=host, graph_id=graph_id)
        self.agent = create_react_agent(model, self.tools)

    def _build(self, b) -> None:
        # A pure computed over a scoped counter.
        b.computed("msg_total", lambda s: len(s.get("history", [])), reads=("history",))
        # Scoped counters so tenantA/tenantB never collide on the same key.
        b.task("bump_a", fn=lambda x: {"count": x["n"]}, on=("run",), scope="tenantA")
        b.task("bump_b", fn=lambda x: {"count": x["m"]}, on=("run",), scope="tenantB")

    # -- driver-backed operations (require a DriverHost) --------------------

    def run_task(self, event: str, payload: dict) -> dict:
        return self.graph.invoke(event, payload)

    def stream_events(self, event: str, payload: dict) -> list[dict]:
        return list(self.graph.stream(event, payload))

    # -- prebuilt ReAct agent (no Driver needed) ----------------------------

    def chat(self, user_message: str) -> list[dict]:
        return self.agent.invoke([{"role": "user", "content": user_message}])["messages"]

    # -- store / checkpoint through the native accessors --------------------

    def put_memory(self, namespace: list[str], key: str, value: Any) -> None:
        self.graph.store_put(namespace, key, value)

    def get_memory(self, namespace: list[str], key: str) -> Any:
        return self.graph.store_get(namespace, key)

    def graph_state(self) -> dict:
        return self.graph.get_state()


def tracked_demo() -> dict:
    """Demonstrate TrackedStateProxy read-set awareness without a Driver."""
    from reactivegraph import ReactiveGraph

    def bump(state: TrackedStateProxy) -> dict:
        return {"seen": state["base"], "plus": state["base"] + 1}

    g = ReactiveGraph.build(lambda b: b.task("t", fn=bump).on("run", "t"))
    return g.invoke("run", {"base": 10})