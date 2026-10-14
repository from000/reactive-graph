"""链路 6：interrupt / resume（HITL 确认 gate + 恢复继续工具链）。

对照 deer-flow v2 `runtime/` HITL 与 `tools/builtins/clarification_tool.py`
（向用户请求澄清）：agent 在执行工具前经确认 gate（抛 Interrupt 挂起）——
resume 携带用户确认载荷后从挂起点继续工具链直至完成；支持多段确认循环。
"""

from __future__ import annotations

from deerflow_port.hitl import build_hitl_agent
from reactivegraph import DriverHost


def test_link6_interrupt_asks_confirmation(df_host: DriverHost) -> None:
    """工具执行前 gate 挂起：invoke 返回 interrupted，等待用户确认。"""
    calls: dict[str, int] = {}
    g = build_hitl_agent(df_host, calls=calls, graph_id="df_hitl_1")
    g.invoke("run", {"tool_request": "charge"})
    assert g._last_run["interrupted"] is True
    assert calls["gate0"] == 1
    assert calls.get("tool", 0) == 0, "未确认前工具不得执行"


def test_link6_resume_continues_toolchain(df_host: DriverHost) -> None:
    """resume 携带确认载荷：从挂起点继续，工具执行并完成。"""
    calls: dict[str, int] = {}
    g = build_hitl_agent(df_host, calls=calls, graph_id="df_hitl_2")
    g.invoke("run", {"tool_request": "charge"})
    assert g._last_run["interrupted"] is True
    r = g.resume(g._last_run["runId"], {"confirmed_0": True})
    assert r.get("done") == "done:executed:charge", f"resume 应完成工具链，实际 {r}"
    assert calls["tool"] == 1 and calls["final"] == 1


def test_link6_multiple_interrupt_cycles(df_host: DriverHost) -> None:
    """多段确认：两处 gate 依次挂起，两次 resume 后全部通过。"""
    calls: dict[str, int] = {}
    g = build_hitl_agent(df_host, calls=calls, n_gates=2, graph_id="df_hitl_3")
    g.invoke("run", {"tool_request": "deploy"})
    assert g._last_run["interrupted"] is True
    r1 = g.resume(g._last_run["runId"], {"confirmed_0": True})
    assert r1.get("interrupted") is True, "第二段 gate 应再次挂起"
    r2 = g.resume(g._last_run["runId"], {"confirmed_1": True})
    assert r2.get("done") == "done:executed:deploy"
    assert calls["tool"] == 1