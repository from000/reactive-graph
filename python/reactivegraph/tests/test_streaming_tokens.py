"""End-to-end token streaming: a generator callback streams `messages`
events through the Driver to a remote `graph.stream()` consumer (M3).

The generator yields tokens/chunks; the Driver forwards them as transient
STREAM_EVENT messages before committing the callback's return value.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from reactivegraph.graph import GraphBuilder, ReactiveGraph


class TestStreamingTokens:
    def _live_host(self):
        from reactivegraph.host import DriverHost

        node = shutil.which("node")
        repo_root = Path(__file__).resolve().parents[3]
        dist = repo_root / "packages" / "driver" / "dist" / "main.js"
        if node is None or not dist.exists():
            pytest.skip("bundled Driver (node + packages/driver/dist) not available")
        assert node is not None
        env = dict(os.environ)
        env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
        env["REACTIVEGRAPH_NODE_BIN"] = node
        host = DriverHost(env=env)
        host.start()
        host.handshake()
        return host

    def test_generator_callback_streams_token_messages(self) -> None:
        host = self._live_host()
        try:

            def build(b: GraphBuilder) -> None:
                def llm(x):
                    # token-level generator: raw strings become messages chunks
                    yield "Hel"
                    yield "lo"
                    yield "!"
                    return {
                        "patches": [{"path": ["done"], "operation": "set", "value": True}],
                        "writes": ["done"],
                        "return_value": "Hello!",
                        "external_receipts": [],
                    }

                b.task("llm", fn=llm).on("run", "llm")

            graph = ReactiveGraph.build(build, host=host, graph_id="g_tokens")
            events = list(graph.stream("run", {}))
            msgs = [e for e in events if e.get("eventType") == "messages"]
            assert [m["payload"]["message"]["content"] for m in msgs] == ["Hel", "lo", "!"]
            # the committed state reflects the generator's return value
            values = [e for e in events if e.get("eventType") == "values"]
            assert values and values[-1]["payload"]["state"].get("done") is True
        finally:
            host.close()

    def test_async_generator_callback_streams_chunks(self) -> None:
        host = self._live_host()
        try:

            def build(b: GraphBuilder) -> None:
                async def llm(x):
                    # NOTE: Python forbids a value-bearing return in async
                    # generators — the async contract streams chunks only and
                    # commits an empty result; write state via a sync generator.
                    yield {"type": "custom", "payload": {"tag": "step1"}}
                    yield "final"

                b.task("llm", fn=llm).on("run", "llm")

            graph = ReactiveGraph.build(build, host=host, graph_id="g_atokens")
            events = list(graph.stream("run", {}))
            custom = [e for e in events if e.get("eventType") == "custom"]
            assert custom and custom[0]["payload"]["payload"] == {"tag": "step1"}
            msgs = [e for e in events if e.get("eventType") == "messages"]
            assert msgs and msgs[-1]["payload"]["message"]["content"] == "final"
        finally:
            host.close()


def test_fallback_stream_emits_segment_events() -> None:
    """P3-1 Task 1：fallback 流产出段边界事件（非单 values）。"""
    def build(b: GraphBuilder) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        def t1(s: dict) -> dict:
            return {"y": s["x"] * 2}

        b.task("t0", fn=t0, on=("run",), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), writes=("y",))

    g = ReactiveGraph.build(build)
    events = list(g.stream("run", {"n": 1}))
    kinds = [ev["eventType"] for ev in events]
    assert "task_start" in kinds and "task_end" in kinds and "values" in kinds
    assert kinds[-1] == "terminal"


@pytest.mark.parametrize(
    "noop_update",
    [{}, {"__reactivegraph_internal__": 1}],
    ids=["empty", "private-only"],
)
def test_fallback_stream_only_emits_values_for_public_writes(
    noop_update: dict,
) -> None:
    """A task emits ``values`` only when it commits public state.

    LangGraph emits at most one snapshot per Pregel super-step and suppresses
    it for tasks with no public channel write. Private engine bookkeeping must
    not make an otherwise no-op middleware look observable.
    """
    def build(b: GraphBuilder) -> None:
        b.task(
            "noop",
            fn=lambda state: dict(noop_update),
            on=("run",),
            writes=("x",),
        )
        b.task(
            "write",
            fn=lambda state: {"x": state["n"] + 1},
            on=("noop:written",),
            writes=("x",),
        )

    graph = ReactiveGraph.build(build)
    events = list(graph.stream("run", {"n": 1}))
    values = [event for event in events if event["eventType"] == "values"]

    assert len(values) == 2
    assert values[0]["payload"]["state"] == {"n": 1}
    assert values[-1]["payload"]["state"]["x"] == 2
    assert [
        event["task"] for event in events if event["eventType"] == "task_start"
    ] == ["noop", "write"]
    assert [
        event["task"] for event in events if event["eventType"] == "task_end"
    ] == ["noop", "write"]


def test_fallback_stream_error_event_then_raise() -> None:
    """P3-1 Task 1：段异常 → 调用方先消费到 task_error 事件，再收到异常。"""
    def build(b: GraphBuilder) -> None:
        def boom(s: dict) -> dict:
            raise RuntimeError("boom")

        b.task("t0", fn=boom, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build)
    seen: list[str] = []
    with pytest.raises(RuntimeError, match="boom"):
        for ev in g.stream("run", {"n": 1}):
            seen.append(ev["eventType"])
    assert "task_error" in seen
    assert "terminal" not in seen  # 异常路径不发 terminal


def test_fallback_stream_skip_still_emits_events() -> None:
    """P3-1 Task 1：pure 跳过（指纹缓存命中）时事件序列不塌缩。"""
    calls: list[int] = []

    def build(b: GraphBuilder) -> None:
        def counted(s: dict) -> dict:
            calls.append(1)
            return {"x": s["n"] + 1}

        b.task("t0", kind="pure", fn=counted, on=("run",), reads=("n",), writes=("x",))

    g = ReactiveGraph.build(build)
    list(g.stream("run", {"n": 1}))
    assert len(calls) == 1
    events2 = list(g.stream("run", {"n": 1}))  # 缓存命中 → 跳过执行
    assert len(calls) == 1
    kinds = [ev["eventType"] for ev in events2]
    assert "task_start" in kinds and "task_end" in kinds and kinds[-1] == "terminal"
