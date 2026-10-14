"""Run-scoped context must reach task bodies on every execution path.

Task bodies do not run on the thread that called ``invoke``/``astream``: the
Driver path executes them on ``DriverHost`` callback-pool workers, and the
async API drives the sync kernel on a producer thread. ``contextvars`` do not
travel across a ``Thread.start()``/``run_in_executor`` boundary, so without an
explicit re-entry the ambient helpers (``get_config``/``get_runtime``) raise
inside every task.

DeerFlow depends on the opposite: middleware and tools read
``get_config()['configurable']`` and ``get_runtime().context`` from inside the
graph, and its run worker pins the ``Runtime`` through
``config.configurable['__pregel_runtime']``.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

import pytest

from reactivegraph.graph import GraphBuilder, ReactiveGraph
from reactivegraph.host import DriverHost
from reactivegraph.runtime import Runtime, get_config, get_runtime, runtime_context

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _live_host() -> DriverHost:
    node = shutil.which("node")
    dist = _REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    host = DriverHost(env=env)
    host.start()
    host.handshake()
    return host


def _observing_graph(host: DriverHost | None, graph_id: str) -> ReactiveGraph:
    """A one-task graph whose body reports the ambient run context."""

    def build(b: GraphBuilder) -> None:
        def observe(state: dict) -> dict:
            runtime = get_runtime()
            config = get_config()
            return {
                "seen_context": runtime.context,
                "seen_thread": config.get("configurable", {}).get("thread_id"),
            }

        b.task("observe", fn=observe, on=("run",), writes=("seen_context", "seen_thread"))

    return ReactiveGraph.build(build, host=host, graph_id=graph_id)


class TestFallbackPath:
    """No DriverHost: the in-process kernel still runs on a foreign thread."""

    def test_invoke_sees_the_ambient_context(self) -> None:
        graph = _observing_graph(None, "ctx_fallback_invoke")
        context = {"thread_id": "T-invoke"}
        with runtime_context(
            runtime=Runtime(context=context),
            config={"configurable": {"thread_id": "T-invoke"}},
        ):
            state = graph.invoke("run", {})

        assert state["seen_context"] is context
        assert state["seen_thread"] == "T-invoke"

    async def test_ainvoke_sees_the_ambient_context(self) -> None:
        graph = _observing_graph(None, "ctx_fallback_ainvoke")
        context = {"thread_id": "T-ainvoke"}
        with runtime_context(
            runtime=Runtime(context=context),
            config={"configurable": {"thread_id": "T-ainvoke"}},
        ):
            state = await graph.ainvoke("run", {})

        assert state["seen_context"] is context
        assert state["seen_thread"] == "T-ainvoke"

    async def test_astream_sees_the_ambient_context(self) -> None:
        graph = _observing_graph(None, "ctx_fallback_astream")
        context = {"thread_id": "T-astream"}
        with runtime_context(
            runtime=Runtime(context=context),
            config={"configurable": {"thread_id": "T-astream"}},
        ):
            frames = [f async for f in graph.astream("run", {})]

        values = [f for f in frames if f.get("eventType") == "values"]
        assert values[-1]["payload"]["state"]["seen_context"] == context

    def test_stream_sees_the_ambient_context(self) -> None:
        graph = _observing_graph(None, "ctx_fallback_stream")
        context = {"thread_id": "T-stream"}
        with runtime_context(
            runtime=Runtime(context=context),
            config={"configurable": {"thread_id": "T-stream"}},
        ):
            frames = list(graph.stream("run", {}))

        values = [f for f in frames if f.get("eventType") == "values"]
        assert values[-1]["payload"]["state"]["seen_context"] == context


class TestDriverPath:
    """DriverHost executes task bodies on callback-pool worker threads."""

    def test_stream_sees_the_ambient_context(self) -> None:
        host = _live_host()
        try:
            graph = _observing_graph(host, "ctx_driver_stream")
            context = {"thread_id": "T-driver"}
            with runtime_context(
                runtime=Runtime(context=context),
                config={"configurable": {"thread_id": "T-driver"}},
            ):
                frames = list(graph.stream("run", {}))
            values = [f for f in frames if f.get("eventType") == "values"]
            assert values[-1]["payload"]["state"]["seen_context"] == context
            assert values[-1]["payload"]["state"]["seen_thread"] == "T-driver"
        finally:
            host.close()

    async def test_astream_sees_the_ambient_context(self) -> None:
        host = _live_host()
        try:
            graph = _observing_graph(host, "ctx_driver_astream")
            context = {"thread_id": "T-driver-async"}
            with runtime_context(
                runtime=Runtime(context=context),
                config={"configurable": {"thread_id": "T-driver-async"}},
            ):
                frames = [f async for f in graph.astream("run", {})]
            values = [f for f in frames if f.get("eventType") == "values"]
            assert values[-1]["payload"]["state"]["seen_context"] == context
        finally:
            host.close()

    def test_invoke_sees_the_ambient_context(self) -> None:
        host = _live_host()
        try:
            graph = _observing_graph(host, "ctx_driver_invoke")
            context = {"thread_id": "T-driver-invoke"}
            with runtime_context(runtime=Runtime(context=context)):
                state = graph.invoke("run", {})
            assert state["seen_context"] == context
        finally:
            host.close()

    def test_python_only_runtime_never_reaches_the_wire(self) -> None:
        """``configurable['__pregel_runtime']`` is a Python-side channel.

        The Driver receives the rest of the config (``recursionLimit`` etc.)
        but a ``Runtime`` object is not an RGP/1 canonical value and must be
        stripped before encoding instead of raising ``CodecError``.
        """
        host = _live_host()
        try:
            graph = _observing_graph(host, "ctx_driver_wire")
            context = {"thread_id": "T-wire"}
            runtime = Runtime(context=context)
            config = {
                "configurable": {"thread_id": "T-wire", "__pregel_runtime": runtime},
                "recursion_limit": 7,
            }
            state = graph.invoke("run", {}, config=config)
            assert state["seen_context"] == context
        finally:
            host.close()


class TestParallelBatch:
    """A parallel batch enters the run context from several threads at once."""

    def test_fanout_batch_all_see_the_context(self) -> None:
        """``runAll`` dispatches callbacks concurrently; a single shared
        ``Context`` object cannot be entered twice, which used to raise inside
        the worker and leave the Driver waiting for a reply until timeout."""
        host = _live_host()
        try:
            def build(b: GraphBuilder) -> None:
                def root(state: dict) -> dict:
                    return {"root": get_runtime().context}

                def make_branch(i: int):
                    def branch(state: dict) -> dict:
                        return {f"b{i}": get_runtime().context}

                    return branch

                b.task("root", fn=root, on=("run",), writes=("root",))
                for i in range(4):
                    b.task(
                        f"b{i}",
                        fn=make_branch(i),
                        on=("root:written",),
                        writes=(f"b{i}",),
                    )

            graph = ReactiveGraph.build(build, host=host, graph_id="ctx_driver_fanout")
            context = {"thread_id": "T-fanout"}
            with runtime_context(runtime=Runtime(context=context)):
                state = graph.invoke("run", {})

            assert state["root"] == context
            for i in range(4):
                assert state[f"b{i}"] == context
        finally:
            host.close()


class TestConcurrentRuns:
    """Context must not leak between concurrent runs of the same host."""

    def test_two_threads_keep_their_own_context(self, ) -> None:
        """Concurrent invokes must return their own run, not a shared slot.

        ``invoke`` used to assign the graph-wide ``self._last_run`` and then
        read the state back from that attribute, so two callers could receive
        each other's state. The subclass below blocks each thread immediately
        after that assignment; the pre-fix implementation therefore fails
        deterministically, while the returned local result is race-free.
        """
        host = _live_host()
        try:
            import threading

            seen: dict[str, object] = {}
            assigned = threading.Barrier(2)

            class CoordinatedGraph(ReactiveGraph):
                def __setattr__(self, name: str, value: object) -> None:
                    super().__setattr__(name, value)
                    if name == "_last_run":
                        assigned.wait(timeout=10)

            def build(b: GraphBuilder) -> None:
                def observe(state: dict) -> dict:
                    runtime = get_runtime()
                    config = get_config()
                    return {
                        "seen_context": runtime.context,
                        "seen_thread": config.get("configurable", {}).get("thread_id"),
                    }

                b.task(
                    "observe",
                    fn=observe,
                    on=("run",),
                    writes=("seen_context", "seen_thread"),
                )

            builder = GraphBuilder("ctx_driver_concurrent")
            build(builder)
            graph = CoordinatedGraph(builder.build(), host=host)

            def run(tag: str) -> None:
                config = {"configurable": {"thread_id": tag}}
                with runtime_context(
                    runtime=Runtime(context={"thread_id": tag}), config=config
                ):
                    seen[tag] = graph.invoke("run", {}, config=config)

            threads = [threading.Thread(target=run, args=(f"T{i}",)) for i in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            assert seen == {
                "T0": {"seen_context": {"thread_id": "T0"}, "seen_thread": "T0"},
                "T1": {"seen_context": {"thread_id": "T1"}, "seen_thread": "T1"},
            }
        finally:
            host.close()


class TestConcurrentThreadIsolation:
    """Concurrent runs on distinct thread ids must not share a store.

    ``graph.invoke`` did not forward ``config.configurable.thread_id`` to the
    Driver, so every concurrent run fell back to the Driver's ``"default"``
    store and the last writer won. The bug only surfaced under load (about
    1-3% of 400-round stress runs), which is why this test issues many rounds
    plus a barrier that forces both invokes to overlap.
    """

    def test_distinct_thread_ids_do_not_share_store(self) -> None:
        host = _live_host()
        try:
            import threading

            def make_builder(graph_id: str) -> GraphBuilder:
                builder = GraphBuilder(graph_id)

                def build(b: GraphBuilder) -> None:
                    def observe(state: dict) -> dict:
                        return {
                            "holder": get_config()
                            .get("configurable", {})
                            .get("thread_id")
                        }

                    b.task("observe", fn=observe, on=("run",), writes=("holder",))

                build(builder)
                return builder

            def make_graph(
                graph_id: str, barrier: threading.Barrier
            ) -> ReactiveGraph:
                class CoordinatedGraph(ReactiveGraph):
                    def __setattr__(self, name: str, value: object) -> None:
                        super().__setattr__(name, value)
                        if name == "_last_run":
                            try:
                                barrier.wait(timeout=5)
                            except threading.BrokenBarrierError:
                                pass

                return CoordinatedGraph(make_builder(graph_id).build(), host=host)

            for round_index in range(300):
                seen: dict[str, object] = {}
                assigned = threading.Barrier(2)
                graph = make_graph(f"ctx_iso_{round_index}", assigned)

                def run(tag: str, _graph: ReactiveGraph = graph, _seen: dict = seen) -> None:
                    config = {"configurable": {"thread_id": tag}}
                    with runtime_context(
                        runtime=Runtime(context={"thread_id": tag}), config=config
                    ):
                        _seen[tag] = _graph.invoke("run", {}, config=config)

                threads = [
                    threading.Thread(target=run, args=(f"T{i}",)) for i in range(2)
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

                assert seen == {
                    "T0": {"holder": "T0"},
                    "T1": {"holder": "T1"},
                }, f"round {round_index}: {seen}"
        finally:
            host.close()


class TestThreadHoppingStream:
    """A sync stream may be advanced by multiple executor threads.

    DeerFlow's async worker advances a synchronous graph iterator with one
    ``asyncio.to_thread(next, iterator)`` call per event. Each call runs in a
    fresh copy of the caller's Context, so a context manager spanning a
    ``yield`` would set its ContextVar in one copy and try to reset it in
    another. LangGraph supports this pattern; the engine must too.
    """

    async def test_stream_advance_on_different_threads_keeps_run_context(self) -> None:
        graph = _observing_graph(None, "ctx_thread_hopping_stream")
        context = {"thread_id": "T-thread-hopping"}
        config = {"configurable": {"thread_id": "T-thread-hopping"}}

        with runtime_context(runtime=Runtime(context=context), config=config):
            iterator = iter(graph.stream("run", {}))
            frames: list[dict] = []
            while True:
                def _advance() -> tuple[bool, dict | None]:
                    try:
                        return True, next(iterator)
                    except StopIteration:
                        return False, None

                has_frame, frame = await asyncio.to_thread(_advance)
                if not has_frame:
                    break
                assert frame is not None
                frames.append(frame)

        values = [frame for frame in frames if frame.get("eventType") == "values"]
        assert values[-1]["payload"]["state"]["seen_context"] == context
        assert values[-1]["payload"]["state"]["seen_thread"] == "T-thread-hopping"

        # The run-scoped variables must be restored when the pinned generator
        # is exhausted, not merely abandoned with their tokens still live.
        with pytest.raises(RuntimeError):
            get_config()
        with pytest.raises(RuntimeError):
            get_runtime()

    def test_abandoned_stream_restores_context_on_gc(self) -> None:
        graph = _observing_graph(None, "ctx_abandoned_stream")
        context = {"thread_id": "T-abandoned"}

        with runtime_context(
            runtime=Runtime(context=context),
            config={"configurable": {"thread_id": "T-abandoned"}},
        ):
            iterator = graph.stream("run", {})
            next(iterator)
            del iterator

        with pytest.raises(RuntimeError):
            get_config()
        with pytest.raises(RuntimeError):
            get_runtime()


class TestLangGraphAmbientBridge:
    """A host written against LangGraph must keep working unchanged.

    DeerFlow's ``ThreadDataMiddleware.before_agent`` calls
    ``langgraph.config.get_config()`` whenever the runtime carries no
    ``thread_id``. That helper reads langchain-core's
    ``var_child_runnable_config``, so the engine has to publish it on the
    same worker thread that runs the task body.
    """

    def test_driver_task_can_read_upstream_get_config(self) -> None:
        pytest.importorskip("langgraph.config")
        from langgraph.config import get_config as upstream_get_config

        host = _live_host()
        try:
            def build(b: GraphBuilder) -> None:
                def observe(state: dict) -> dict:
                    return {
                        "seen_thread": upstream_get_config()
                        .get("configurable", {})
                        .get("thread_id")
                    }

                b.task("observe", fn=observe, on=("run",), writes=("seen_thread",))

            graph = ReactiveGraph.build(build, host=host, graph_id="ctx_upstream_config")
            state = graph.invoke(
                "run", {}, config={"configurable": {"thread_id": "T-upstream"}}
            )
            assert state["seen_thread"] == "T-upstream"
        finally:
            host.close()

    def test_driver_task_can_read_upstream_runtime(self) -> None:
        pytest.importorskip("langgraph.runtime")
        from langgraph.runtime import get_runtime as upstream_get_runtime

        host = _live_host()
        try:
            def build(b: GraphBuilder) -> None:
                def observe(state: dict) -> dict:
                    return {"seen_context": upstream_get_runtime().context}

                b.task("observe", fn=observe, on=("run",), writes=("seen_context",))

            graph = ReactiveGraph.build(build, host=host, graph_id="ctx_upstream_runtime")
            context = {"thread_id": "T-upstream-runtime"}
            with runtime_context(runtime=Runtime(context=context)):
                state = graph.invoke("run", {})
            # Driver frames deepcopy payloads, so compare by value here.
            assert state["seen_context"] == context
        finally:
            host.close()

    def test_fallback_task_can_read_upstream_get_config(self) -> None:
        """The fallback kernel must publish the bridge too: hosts run the same
        middleware with and without a Driver attached."""
        pytest.importorskip("langgraph.config")
        from langgraph.config import get_config as upstream_get_config

        def build(b: GraphBuilder) -> None:
            def observe(state: dict) -> dict:
                return {
                    "seen_thread": upstream_get_config()
                    .get("configurable", {})
                    .get("thread_id")
                }

            b.task("observe", fn=observe, on=("run",), writes=("seen_thread",))

        graph = ReactiveGraph.build(build, host=None, graph_id="ctx_upstream_fallback")
        state = graph.invoke("run", {}, config={"configurable": {"thread_id": "T-fb"}})
        assert state["seen_thread"] == "T-fb"
