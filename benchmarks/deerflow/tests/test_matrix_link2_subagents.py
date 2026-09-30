"""链路 2：sub-agents 派发执行（并行 + 容量控制）。

对照 deer-flow v2 `subagents/executor.py` + `registry.py`（内置 bash_agent /
general_purpose）+ `capacity.py`（并发上限）：主编排将 batch items 派发给
N 个并行 sub-agent（引擎事件订阅天然并行），capacity 截断超额派发。
"""

from __future__ import annotations

from deerflow_port.subagents import build_subagent_dispatcher
from reactivegraph import DriverHost


def test_link2_parallel_subagents_all_complete(df_host: DriverHost) -> None:
    """容量 ≥ 批次时：全部 sub-agent 各执行一次，结果聚合到 state。"""
    calls: dict[str, int] = {}
    n, capacity = 5, 5
    g = build_subagent_dispatcher(df_host, calls=calls,
                                  n_items=n, capacity=capacity, graph_id="df_sub_batch")
    out = g.invoke("run", {"items": [f"item{i}" for i in range(n)]})
    assert len(calls) == n, f"应派发 {n} 个子任务，实际 {calls}"
    assert all(calls[f"sub{i}"] == 1 for i in range(n))
    assert all(out[f"result_{i}"].startswith("done:") for i in range(n))


def test_link2_capacity_truncates(df_host: DriverHost) -> None:
    """容量 < 批次：超额 items 被截断（active/dropped 计数正确，超额的子任务不执行）。"""
    calls: dict[str, int] = {}
    n, capacity = 6, 3
    g = build_subagent_dispatcher(df_host, calls=calls,
                                  n_items=n, capacity=capacity, graph_id="df_sub_cap")
    out = g.invoke("run", {"items": [f"item{i}" for i in range(n)]})
    assert out["active"] == 3 and out["dropped"] == 3
    assert len(calls) == 3, f"应只执行 capacity 个子任务，实际 {calls}"


def test_link2_stream_frames_observable(df_host: DriverHost) -> None:
    """stream 值流帧覆盖派发 + 各 sub-agent 完成（≥ 初始 + capacity 帧）。"""
    calls: dict[str, int] = {}
    capacity = 4
    g = build_subagent_dispatcher(df_host, calls=calls,
                                  n_items=capacity, capacity=capacity, graph_id="df_sub_stream")
    events = list(g.stream("run", {"items": ["a", "b", "c", "d"]}, thread_id="sub_t1"))
    values = [ev["payload"]["state"] for ev in events if ev.get("eventType") == "values"]
    assert len(values) >= 1 + capacity, f"应至少 {1 + capacity} 帧，实际 {len(values)}"
    assert values[-1][f"result_{capacity - 1}"] .startswith("done:")