"""Reverse integration: LangChain must be able to drive OUR graphs.

`langchain_core.runnables.base.coerce_to_runnable` dispatches on
``isinstance(obj, Runnable)`` — a structural look-alike is rejected with
"Expected a Runnable, callable or dict". Since `ReactiveGraph`/`AgentGraph`
inherit `Runnable` when langchain-core is importable (the same pattern
`reactivegraph.messages` uses for message classes), a ReactiveGraph can be
placed inside an existing LangChain pipeline.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("langchain_core", reason="needs the compat/langchain-ecosystem extra")

from langchain_core.runnables import Runnable, RunnableLambda  # noqa: E402

from reactivegraph.graph import GraphBuilder, ReactiveGraph  # noqa: E402


def _echo_graph() -> ReactiveGraph:
    def build(b: GraphBuilder) -> None:
        b.task(
            "t",
            fn=lambda s: {"out": s.get("x", 0) + 1},
            on=("run",),
            writes=("out",),
        )

    return ReactiveGraph.build(build)


class TestRunnableIdentity:
    def test_graph_is_a_langchain_runnable(self) -> None:
        assert isinstance(_echo_graph(), Runnable)

    def test_engine_still_works_without_langchain(self) -> None:
        """The base is optional: the graph keeps its own call surface."""
        graph = _echo_graph()
        for name in ("invoke", "batch", "stream", "ainvoke", "abatch", "astream"):
            assert callable(getattr(graph, name, None)), f"missing {name}"


class TestNativeBatch:
    def test_batch_preserves_order(self) -> None:
        graph = _echo_graph()
        out = graph.batch("run", [{"x": 1}, {"x": 2}, {"x": 3}])
        assert [r["out"] for r in out] == [2, 3, 4]

    @pytest.mark.asyncio
    async def test_abatch_matches_batch(self) -> None:
        graph = _echo_graph()
        sync = graph.batch("run", [{"x": 1}, {"x": 2}])
        asynced = await graph.abatch("run", [{"x": 1}, {"x": 2}])
        assert [r["out"] for r in outs(sync)] == [r["out"] for r in outs(asynced)]

    def test_batch_isolates_items(self) -> None:
        """Each item is its own run: no state leaks between them."""
        graph = _echo_graph()
        out = graph.batch("run", [{"x": 1}, {"x": 1}])
        assert [r["out"] for r in out] == [2, 2]


class TestLangChainComposition:
    def test_langchain_pipe_accepts_our_graph(self) -> None:
        """The exact call that used to raise "Expected a Runnable"."""

        class Bridge(Runnable):
            def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
                return _echo_graph().invoke("run", input, config=config)

        composed = RunnableLambda(lambda d: d) | Bridge()
        assert composed.invoke({"x": 1})["out"] == 2

    def test_langchain_batch_drives_our_graph(self) -> None:
        class Bridge(Runnable):
            def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
                return _echo_graph().invoke("run", input, config=config)

        composed = RunnableLambda(lambda d: d) | Bridge()
        results = composed.batch([{"x": 1}, {"x": 2}])
        assert [r["out"] for r in results] == [2, 3]

    def test_coerce_to_runnable_accepts_our_graph_directly(self) -> None:
        """No bridge needed for identity: the graph itself is Runnable."""
        from langchain_core.runnables.base import coerce_to_runnable

        graph = _echo_graph()
        assert coerce_to_runnable(graph) is graph


def outs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows


class TestDirectPipeIntegration:
    """The graph itself — no adapter — must satisfy the Runnable call contract.

    ``RunnableSequence`` invokes each step as ``step.invoke(input, config)`` and
    batches as ``step.batch(inputs, configs, return_exceptions=...)``. The
    native ``(event, payload)`` form stays valid; the Runnable form picks the
    entry event.
    """

    def test_pipe_invoke_uses_default_event(self) -> None:
        composed = RunnableLambda(lambda d: d) | _echo_graph()
        assert composed.invoke({"x": 1})["out"] == 2

    def test_pipe_batch(self) -> None:
        composed = RunnableLambda(lambda d: d) | _echo_graph()
        results = composed.batch([{"x": 1}, {"x": 2}])
        assert [r["out"] for r in results] == [2, 3]

    def test_pipe_stream(self) -> None:
        composed = RunnableLambda(lambda d: d) | _echo_graph()
        chunks = list(composed.stream({"x": 1}))
        assert chunks
        assert any("out" in str(chunk) for chunk in chunks)

    def test_runnable_wrappers_compose(self) -> None:
        graph = _echo_graph()
        assert graph.with_retry(stop_after_attempt=1).invoke({"x": 1})["out"] == 2
        assert graph.with_fallbacks([_echo_graph()]).invoke({"x": 1})["out"] == 2

    @pytest.mark.asyncio
    async def test_pipe_async_surface(self) -> None:
        composed = RunnableLambda(lambda d: d) | _echo_graph()
        out = await composed.ainvoke({"x": 1})
        assert out["out"] == 2
        results = await composed.abatch([{"x": 1}, {"x": 2}])
        assert [r["out"] for r in results] == [2, 3]
        chunks = [chunk async for chunk in composed.astream({"x": 1})]
        assert chunks

    def test_single_declared_event_becomes_the_default(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda s: {"seen": s["n"] * 2}, on=("visit",), writes=("seen",))

        graph = ReactiveGraph.build(build)
        assert graph.invoke({"n": 21})["seen"] == 42          # Runnable form
        assert graph.invoke("visit", {"n": 21})["seen"] == 42  # native form

    def test_ambiguous_default_event_requires_explicit_form(self) -> None:
        from reactivegraph.graph import GraphBuildError

        def build(b: GraphBuilder) -> None:
            b.task("a", fn=lambda s: {}, on=("x",))
            b.task("b", fn=lambda s: {}, on=("y",))

        graph = ReactiveGraph.build(build)
        with pytest.raises(GraphBuildError, match="event"):
            graph.invoke({"a": 1})

    def test_batch_return_exceptions_collects_failures(self) -> None:
        def boom(s: dict[str, Any]) -> dict[str, Any]:
            if s.get("x") == 0:
                raise ValueError("boom")
            return {"out": s["x"] + 1}

        def build(b: GraphBuilder) -> None:
            b.task("t", fn=boom, on=("run",), writes=("out",))

        graph = ReactiveGraph.build(build)
        results = graph.batch([{"x": 1}, {"x": 0}], return_exceptions=True)
        assert results[0]["out"] == 2
        assert isinstance(results[1], Exception)


_MSGS: dict[str, Any] = {"messages": [{"role": "user", "content": "hi"}]}


class TestAgentGraphRunnableBatch:
    """``create_agent`` graphs keep working when LangChain batches them."""

    @staticmethod
    def _agent():
        from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
        from langchain_core.messages import AIMessage

        from reactivegraph.create_agent import create_agent

        replies = iter([AIMessage(content="ok")] * 4)
        return create_agent(GenericFakeChatModel(messages=replies))

    def test_pipe_batch_accepts_per_item_configs(self) -> None:
        composed = RunnableLambda(lambda d: d) | self._agent()
        results = composed.batch([_MSGS, _MSGS])
        assert [r["messages"][-1].content for r in results] == ["ok", "ok"]

    @pytest.mark.asyncio
    async def test_pipe_abatch_accepts_per_item_configs(self) -> None:
        composed = RunnableLambda(lambda d: d) | self._agent()
        results = await composed.abatch([_MSGS, _MSGS])
        assert [r["messages"][-1].content for r in results] == ["ok", "ok"]


class TestCallShapeFailClosed:
    """Ambiguous or nonsensical shapes must raise, never silently re-route."""

    def test_string_input_resolves_to_payload_not_event(self) -> None:
        """A string in Runnable position is *input*, never the entry event."""
        from reactivegraph.graph_calls import resolve_call

        graph = _echo_graph()
        event, payload, config = resolve_call(graph.definition, "hello", None)
        assert (event, payload, config) == ("hello", None, None)
        # A declared event name in the same position keeps the native reading.
        event, payload, config = resolve_call(graph.definition, "run", {"x": 1})
        assert (event, payload) == ("run", {"x": 1})
        # A LangChain config (with an error-key shaped payload) selects the
        # Runnable reading so the string stays the payload.
        event, payload, config = resolve_call(
            graph.definition, "hello", {"callbacks": []}
        )
        assert (event, payload, config) == ("run", "hello", {"callbacks": []})

    def test_native_invoke_without_payload_raises(self) -> None:
        with pytest.raises(TypeError, match="payload"):
            _echo_graph().invoke("run")

    def test_bare_string_batch_raises(self) -> None:
        with pytest.raises(TypeError, match="sequence of inputs"):
            _echo_graph().batch("run")

    def test_mistyped_event_still_reports_no_route(self) -> None:
        from reactivegraph.graph import GraphBuildError

        with pytest.raises(GraphBuildError, match="no task routes"):
            _echo_graph().invoke("runs", {})
