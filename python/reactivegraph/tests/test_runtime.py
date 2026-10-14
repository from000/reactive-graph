"""Run-scoped runtime context, pinned against real LangGraph.

Tools and middleware read the active config/context through ambient helpers
instead of receiving them as parameters. The contract that matters is the
*outside-a-run* behaviour: ``get_config()`` must raise ``RuntimeError`` so
callers can fall back to a default, and the context must be scoped to the task
that set it.
"""

from __future__ import annotations

import asyncio

import pytest

from reactivegraph.runtime import (
    Runtime,
    get_config,
    get_runtime,
    get_stream_writer,
    runtime_context,
)


class TestGetConfig:
    def test_raises_outside_a_run(self) -> None:
        with pytest.raises(RuntimeError, match="outside of a runnable context"):
            get_config()

    def test_returns_the_bound_config(self) -> None:
        config = {"configurable": {"thread_id": "t1"}}
        with runtime_context(config=config):
            assert get_config() == config

    def test_restores_the_previous_value_on_exit(self) -> None:
        with runtime_context(config={"configurable": {"thread_id": "t1"}}):
            pass
        with pytest.raises(RuntimeError):
            get_config()

    def test_nested_contexts_shadow(self) -> None:
        outer = {"configurable": {"thread_id": "outer"}}
        inner = {"configurable": {"thread_id": "inner"}}
        with runtime_context(config=outer):
            with runtime_context(config=inner):
                assert get_config()["configurable"]["thread_id"] == "inner"
            assert get_config()["configurable"]["thread_id"] == "outer"


class TestGetRuntime:
    def test_raises_outside_a_run(self) -> None:
        with pytest.raises(RuntimeError):
            get_runtime()

    def test_returns_the_bound_runtime(self) -> None:
        runtime = Runtime(context={"user_id": "u1"})
        with runtime_context(runtime=runtime):
            assert get_runtime() is runtime

    def test_get_runtime_accepts_a_context_schema(self) -> None:
        runtime = Runtime(context={"user_id": "u1"})
        with runtime_context(runtime=runtime):
            assert get_runtime(dict).context == {"user_id": "u1"}


class TestGetStreamWriter:
    def test_returns_the_bound_writer(self) -> None:
        seen: list[object] = []
        runtime = Runtime(stream_writer=seen.append)
        with runtime_context(runtime=runtime):
            get_stream_writer()({"custom": 1})
        assert seen == [{"custom": 1}]

    def test_raises_outside_a_run(self) -> None:
        with pytest.raises(RuntimeError):
            get_stream_writer()

    def test_delegates_to_the_host_helper_outside_an_engine_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Outside an engine run the engine helper is the host helper.

        Host code (and its tests) install seams on ``langgraph.config``. If the
        engine helper resolved the writer by any other route, those seams would
        silently stop intercepting and the caller would lose its frames.
        """
        host_config = pytest.importorskip("langgraph.config")
        seen: list[object] = []
        monkeypatch.setattr(
            host_config, "get_stream_writer", lambda: seen.append
        )
        get_stream_writer()({"custom": 1})
        assert seen == [{"custom": 1}]

    def test_engine_runtime_still_wins_over_the_host(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        host_config = pytest.importorskip("langgraph.config")
        monkeypatch.setattr(
            host_config, "get_stream_writer", lambda: pytest.fail("host helper used")
        )
        seen: list[object] = []
        with runtime_context(runtime=Runtime(stream_writer=seen.append)):
            get_stream_writer()({"custom": 2})
        assert seen == [{"custom": 2}]

    def test_works_inside_a_host_langgraph_run(self) -> None:
        """Host-authored middleware may call either spelling.

        DeerFlow middleware originally imported ``get_stream_writer`` from
        ``langgraph.config``. When such a module is switched to the engine
        import, the helper must still resolve the *host's* ambient run —
        otherwise the middleware silently loses its custom frames (measured:
        six retry-event tests emitted nothing after the import swap).
        """
        pytest.importorskip("langgraph.config")
        from langgraph.graph import END, START, StateGraph
        from typing_extensions import TypedDict

        class State(TypedDict):
            seen: list[object]

        def node(state: State) -> State:
            get_stream_writer()({"custom": 1})
            return state

        graph = (
            StateGraph(State)
            .add_node("node", node)
            .add_edge(START, "node")
            .add_edge("node", END)
            .compile()
        )
        frames = [frame for frame in graph.stream({"seen": []}, stream_mode="custom")]
        assert frames == [{"custom": 1}]


class TestRuntime:
    def test_defaults(self) -> None:
        runtime = Runtime()
        assert runtime.context is None
        assert runtime.store is None
        assert runtime.previous is None

    def test_stream_writer_is_a_no_op_by_default(self) -> None:
        Runtime().stream_writer({"ignored": True})

    def test_override_replaces_selected_fields(self) -> None:
        runtime = Runtime(context={"a": 1}, previous="p")
        patched = runtime.override(context={"b": 2})
        assert patched.context == {"b": 2}
        assert patched.previous == "p"
        assert runtime.context == {"a": 1}

    def test_merge_prefers_other_when_set(self) -> None:
        base = Runtime(context={"a": 1}, previous="p")
        merged = base.merge(Runtime(context={"b": 2}))
        assert merged.context == {"b": 2}
        assert merged.previous == "p"


class TestTaskIsolation:
    def test_context_does_not_leak_across_tasks(self) -> None:
        async def main() -> tuple[bool, str]:
            async def child() -> str:
                with runtime_context(config={"configurable": {"thread_id": "child"}}):
                    await asyncio.sleep(0)
                    return get_config()["configurable"]["thread_id"]

            task = asyncio.create_task(child())
            await asyncio.sleep(0)
            leaked = False
            try:
                get_config()
                leaked = True
            except RuntimeError:
                pass
            return leaked, await task

        leaked, child_value = asyncio.run(main())
        assert leaked is False
        assert child_value == "child"


class TestLangGraphBridge:
    """Hosts call ``langgraph.config.get_config()`` directly.

    DeerFlow's middleware and tools import the ambient helpers from
    ``langgraph.config`` / ``langgraph.runtime``, not from ReactiveGraph.
    Upstream those are thin readers of
    ``langchain_core.runnables.config.var_child_runnable_config``, so the
    engine must publish that same ContextVar for a run to be a drop-in
    replacement — otherwise every such call raises "Called get_config
    outside of a runnable context" even though the run is live.
    """

    @staticmethod
    def _langgraph():
        return pytest.importorskip("langgraph.config"), pytest.importorskip(
            "langgraph.runtime"
        )

    def test_upstream_get_config_sees_the_run_config(self) -> None:
        langgraph_config, _ = self._langgraph()
        config = {"configurable": {"thread_id": "t1"}}
        with runtime_context(config=config, runtime=Runtime()):
            assert langgraph_config.get_config()["configurable"]["thread_id"] == "t1"

    def test_engine_get_runtime_delegates_inside_a_host_graph_node(self) -> None:
        """DeerFlow reads run context from inside a *host* LangGraph node.

        ``langgraph.config`` exposes ``get_config``/``get_stream_writer`` but
        has never exposed ``get_runtime``; that lives in ``langgraph.runtime``.
        An engine ``get_runtime()`` that only probes ``langgraph.config`` raises
        ``RuntimeError`` inside every host-run tool call, so request-scoped
        secrets are reported missing and the MCP interceptor fails closed.
        """
        pytest.importorskip("langgraph")
        from langgraph.graph import END, START
        from langgraph.graph import StateGraph as HostStateGraph

        from reactivegraph import get_runtime as engine_get_runtime

        class _State(dict):
            pass

        seen: dict[str, object] = {}

        def _probe(state):
            seen["context"] = engine_get_runtime().context
            return {}

        builder = HostStateGraph(dict, context_schema=dict)
        builder.add_node("probe", _probe)
        builder.add_edge(START, "probe")
        builder.add_edge("probe", END)
        graph = builder.compile()
        graph.invoke({}, context={"secrets": {"tenant_token": "t"}})
        assert seen["context"] == {"secrets": {"tenant_token": "t"}}

    def test_upstream_get_runtime_returns_our_runtime(self) -> None:
        _, langgraph_runtime = self._langgraph()
        runtime = Runtime(context={"user_id": "u1"})
        with runtime_context(config={"configurable": {}}, runtime=runtime):
            assert langgraph_runtime.get_runtime() is runtime

    def test_upstream_get_stream_writer_returns_our_writer(self) -> None:
        langgraph_config, _ = self._langgraph()
        seen: list[object] = []
        runtime = Runtime(stream_writer=seen.append)
        with runtime_context(config={"configurable": {}}, runtime=runtime):
            langgraph_config.get_stream_writer()({"custom": 1})
        assert seen == [{"custom": 1}]

    def test_bridge_works_without_a_caller_config(self) -> None:
        langgraph_config, langgraph_runtime = self._langgraph()
        runtime = Runtime(context={"user_id": "u2"})
        with runtime_context(runtime=runtime):
            assert langgraph_runtime.get_runtime() is runtime
            assert langgraph_config.get_config()["configurable"].get("thread_id") is None

    def test_caller_config_is_never_mutated(self) -> None:
        self._langgraph()
        config = {"configurable": {"thread_id": "t1"}}
        with runtime_context(config=config, runtime=Runtime()):
            pass
        assert config == {"configurable": {"thread_id": "t1"}}

    def test_bridge_is_cleaned_up_on_exit(self) -> None:
        langgraph_config, _ = self._langgraph()
        with runtime_context(config={"configurable": {}}, runtime=Runtime()):
            langgraph_config.get_config()
        with pytest.raises(RuntimeError, match="outside of a runnable context"):
            langgraph_config.get_config()

    def test_our_get_config_matches_upstream_shape(self) -> None:
        """Upstream's ``configurable`` carries the live Runtime under
        ``__pregel_runtime``; our own helper must agree with the bridged one."""
        langgraph_config, _ = self._langgraph()
        runtime = Runtime(context={"user_id": "u3"})
        with runtime_context(config={"configurable": {"thread_id": "t3"}}, runtime=runtime):
            ours = get_config()
            theirs = langgraph_config.get_config()
            assert ours["configurable"]["thread_id"] == "t3"
            assert ours["configurable"]["__pregel_runtime"] is runtime
            assert ours == theirs
