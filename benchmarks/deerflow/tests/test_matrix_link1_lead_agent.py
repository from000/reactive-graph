"""链路 1：lead_agent 多 agent 编排（planner→executor→verifier 事件路由）。

对照 deer-flow v2 `agents/lead_agent/agent.py:make_lead_agent`：多 agent 经图
编排 + 事件驱动路由；我们以声明式图（task/on/reads/writes）等价实现。
同一张图上同时断言选择性执行（跨 run 同输入跳过）——编排层即展示差异化。
"""

from __future__ import annotations

from deerflow_port.lead_agent import build_lead_agent
from reactivegraph import DriverHost


def test_link1_invoke_full_pipeline(df_host: DriverHost) -> None:
    """invoke 产出三阶段产物（plan → draft → answer），各阶段任务各执行一次。"""
    calls = {"planner": 0, "executor": 0, "verifier": 0}
    g = build_lead_agent(df_host, calls=calls)
    out = g.invoke("run", {"query": "deep research on reactive graphs"})
    assert out["plan"].startswith("plan:")
    assert out["draft"].startswith("draft:")
    assert out["answer"].startswith("verified:")
    assert calls == {"planner": 1, "executor": 1, "verifier": 1}


def test_link1_stream_three_phase_frames(df_host: DriverHost) -> None:
    """stream 值流帧覆盖三阶段，末帧含最终 answer（编排有序可观测）。"""
    calls = {"planner": 0, "executor": 0, "verifier": 0}
    g = build_lead_agent(df_host, calls=calls)
    events = list(g.stream("run", {"query": "q"}, thread_id="lead_t1"))
    values = [ev["payload"]["state"] for ev in events if ev.get("eventType") == "values"]
    # 三阶段各自推进一帧（planner→executor→verifier 状态逐帧可见）
    assert len(values) >= 3
    assert values[-1]["answer"].startswith("verified:")


def test_link1_cross_run_selective_skip(df_host: DriverHost) -> None:
    """跨 run 同输入：三阶段 pure 任务指纹跳过（calls 不增长）；新输入重跑。"""
    calls = {"planner": 0, "executor": 0, "verifier": 0}
    g = build_lead_agent(df_host, calls=calls)
    g.invoke("run", {"query": "same input"})
    g.invoke("run", {"query": "same input"})  # 同输入跨 run：pure 指纹跳过
    assert calls == {"planner": 1, "executor": 1, "verifier": 1}
    g.invoke("run", {"query": "different"})  # 新输入：全链路重跑
    assert calls == {"planner": 2, "executor": 2, "verifier": 2}
