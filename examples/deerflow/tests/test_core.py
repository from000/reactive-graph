"""End-to-end verification of the native API capabilities via the example.

Every test asserts a distinct headline capability; skipped pieces are
documented with a reason (e.g. trace export is JS-side today).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
from reactivegraph import DriverHost

from deerflow.core import SuperAgent, tracked_demo


def _live_host() -> DriverHost:
    node = shutil.which("node")
    repo_root = Path(__file__).resolve().parents[3]  # tests -> deerflow -> examples -> repo
    dist = repo_root / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    host = DriverHost(env=env)
    host.start()
    host.handshake()
    return host


def fake_model(messages: list[dict]) -> dict:
    """Two-phase deterministic model: first tool call, then the answer."""
    if len(messages) == 1:
        return {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"id": "c1", "name": "get_weather", "arguments": {"city": "Paris"}}],
        }
    return {"role": "assistant", "content": "Paris is 20C and sunny."}


class TestHeadlineCapabilities:
    def test_prebuilt_react_agent_loops_tools_to_answer(self) -> None:
        agent = SuperAgent(fake_model)
        out = agent.chat("weather?")
        roles = [m["role"] for m in out]
        assert roles == ["user", "assistant", "tool", "assistant"]
        assert out[2]["name"] == "get_weather"
        assert out[-1]["content"] == "Paris is 20C and sunny."

    def test_tracked_state_proxy_read_set_awareness(self) -> None:
        # 引擎语义：invoke 返回 input 合并 writes 的 state——断言派生值，
        # 不锁定键集（input 键 base 会保留在返回 state 中）。
        out = tracked_demo()
        assert out["seen"] == 10 and out["plus"] == 11

    def test_scope_isolation_and_computed_via_driver(self) -> None:
        host = _live_host()
        try:
            agent = SuperAgent(fake_model, host=host, graph_id="g_scope")
            agent.run_task("run", {"n": 1, "m": 2})
            state = agent.graph_state()
            assert state["tenantA"]["count"] == 1
            assert state["tenantB"]["count"] == 2
        finally:
            host.close()

    def test_store_memory_via_driver(self) -> None:
        host = _live_host()
        try:
            agent = SuperAgent(fake_model, host=host, graph_id="g_mem")
            ns = ["deerflow", "alice"]
            agent.put_memory(ns, "pref", {"theme": "dark"})
            assert agent.get_memory(ns, "pref") == {"theme": "dark"}
        finally:
            host.close()

    def test_stream_events_via_driver(self) -> None:
        host = _live_host()
        try:
            agent = SuperAgent(fake_model, host=host, graph_id="g_stream")
            events = agent.stream_events("run", {"n": 1, "m": 2})
            values = [e for e in events if e.get("eventType") == "values"]
            assert len(values) >= 1
            assert values[-1]["payload"]["state"]["tenantA"]["count"] == 1
            assert values[-1]["payload"]["state"]["tenantB"]["count"] == 2
        finally:
            host.close()

    def test_checkpoint_state_via_driver(self) -> None:
        host = _live_host()
        try:
            agent = SuperAgent(fake_model, host=host, graph_id="g_ckpt")
            agent.run_task("run", {"n": 5, "m": 0})
            state = agent.graph_state()
            assert state["tenantA"]["count"] == 5
            cps = agent.graph.list_checkpoints()
            assert isinstance(cps, list)
        finally:
            host.close()