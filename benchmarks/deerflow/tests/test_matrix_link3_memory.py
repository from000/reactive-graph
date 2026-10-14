"""链路 3：长期记忆（跨 run 持久 + 检索注入 + 线程隔离）。

对照 deer-flow v2 `persistence/` + `runtime/store` + memory_middleware（记忆
注入 agent 上下文）：agent 图内含记忆任务——load_mem（pure，读 Driver
LongTermStore 注入 state）、answer（使用记忆作答）、save_mem（effect，
run 结束时把本轮事实写回 store，namespace 按记忆 key=线程维度隔离）。
"""

from __future__ import annotations

from deerflow_port.memory import build_memory_agent
from reactivegraph import DriverHost


def test_link3_memory_roundtrip_and_inject(df_host: DriverHost) -> None:
    """run1 学到事实 → save_mem 持久 → run2 同记忆 key load_mem 检索并用于回答。"""
    calls = {"load": 0, "answer": 0, "save": 0}
    g = build_memory_agent(df_host, calls=calls, graph_id="df_mem_1")
    out1 = g.invoke("run", {"mem_key": "t1", "learned": ["user likes Rust"]})
    assert out1["out"] == "no-memory"  # run1 开头无历史记忆；learned 在 run 末尾持久化
    # run2：同 host 同 graph 新 run，无新事实——检索到 run1 持久化记忆（跨 run）
    out2 = g.invoke("run", {"mem_key": "t1"})
    assert out2["out"] == "recalled:user likes Rust"
    assert calls["save"] == 2  # 每 run 末尾写回一次


def test_link3_memory_thread_isolation(df_host: DriverHost) -> None:
    """记忆按 key 隔离：t1 的记忆不污染 t2；全新 key 无记忆。"""
    g = build_memory_agent(df_host, graph_id="df_mem_2")
    g.invoke("run", {"mem_key": "t1", "learned": ["fact A"]})
    out_other = g.invoke("run", {"mem_key": "t2"})
    assert out_other["out"] == "no-memory"
    out_same = g.invoke("run", {"mem_key": "t1"})
    assert out_same["out"] == "recalled:fact A"


def test_link3_memory_survives_rebuild(df_host: DriverHost) -> None:
    """Driver store 持久：新 graph 实例（同 host 新 graph_id）仍能检索到旧记忆。"""
    g1 = build_memory_agent(df_host, graph_id="df_mem_3a")
    g1.invoke("run", {"mem_key": "persist", "learned": ["durable fact"]})
    g2 = build_memory_agent(df_host, graph_id="df_mem_3b")
    out = g2.invoke("run", {"mem_key": "persist"})
    assert out["out"] == "recalled:durable fact"