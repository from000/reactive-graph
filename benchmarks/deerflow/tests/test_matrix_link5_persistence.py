"""链路 5：线程持久化（checkpoint 落盘 + 重启恢复 + 断点继续 + durable 状态）。

对照 deer-flow v2 `persistence/engine.py` + `runtime/checkpointer` + `runtime/store`：
会话状态随 run 持久到 sqlite，DriverHost 重启后线程从 checkpoint 精确恢复，
从断点继续而非重头（A6/D4 模式）；多线程按 thread_id 隔离（invoke 无 thread
维度，线程会话走 stream(thread_id=...)，与 D5 同款）。
"""

from __future__ import annotations

from pathlib import Path

from deerflow_port.persistence import build_session_graph
from reactivegraph import DriverHost


def _run_thread(g, payload: dict, thread_id: str) -> None:
    """线程会话跑一轮（stream 携带 thread_id，invoke 无 thread 维度）。"""
    for _ev in g.stream("run", payload, thread_id=thread_id):
        pass


def test_link5_checkpoint_survives_restart(df_db: Path) -> None:
    """host 重启后多线程状态各自保留（落盘 + thread 隔离）。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host1.start()
    host1.handshake()
    try:
        g1 = build_session_graph(host1, graph_id="df_sess")
        _run_thread(g1, {}, "thr_a")
        _run_thread(g1, {}, "thr_a")
        _run_thread(g1, {}, "thr_b")
        assert g1.get_state(thread_id="thr_a")["n"] == 2
        assert g1.get_state(thread_id="thr_b")["n"] == 1
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host2.start()
    host2.handshake()
    try:
        g2 = build_session_graph(host2, graph_id="df_sess")
        assert g2.get_state(thread_id="thr_a")["n"] == 2, "重启后 thr_a 应恢复"
        assert g2.get_state(thread_id="thr_b")["n"] == 1, "重启后 thr_b 应恢复"
    finally:
        host2.close()


def test_link5_continue_from_checkpoint(df_db: Path) -> None:
    """恢复 run 从断点继续（第 3 轮计数=3，非重头）。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host1.start()
    host1.handshake()
    try:
        g1 = build_session_graph(host1, graph_id="df_sess2")
        _run_thread(g1, {}, "t")
        _run_thread(g1, {}, "t")
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host2.start()
    host2.handshake()
    try:
        g2 = build_session_graph(host2, graph_id="df_sess2")
        _run_thread(g2, {}, "t")
        assert g2.get_state(thread_id="t")["n"] == 3, "断点继续应得 3"
    finally:
        host2.close()


def test_link5_durable_state_and_trace(df_db: Path) -> None:
    """durable：重启后 checkpoint 精确恢复（未跑新 run 前）；stream 帧序列可复现。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host1.start()
    host1.handshake()
    try:
        g1 = build_session_graph(host1, graph_id="df_sess3")
        _run_thread(g1, {"seed": "x"}, "durable")
        frames1 = [ev.get("eventType") for ev in
                   g1.stream("run", {"seed": "y"}, thread_id="durable")]
        assert frames1, "stream 应产出事件帧（执行日志可观测）"
        before_n = g1.get_state(thread_id="durable")["n"]
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host2.start()
    host2.handshake()
    try:
        g2 = build_session_graph(host2, graph_id="df_sess3")
        # 未跑新 run：重启后 checkpoint 精确恢复（任务写入的计数逐字段一致）
        assert g2.get_state(thread_id="durable")["n"] == before_n, "重启后应精确恢复计数"
        # 同图同输入（effect 每次执行）：重启后帧序列确定一致（durable 回放可观测）
        frames2 = [ev.get("eventType") for ev in
                   g2.stream("run", {"seed": "z"}, thread_id="durable")]
        assert frames2 == frames1, f"重启后帧序列应一致：{frames1} vs {frames2}"
        # 恢复后继续：断点推进
        assert g2.get_state(thread_id="durable")["n"] == before_n + 1
    finally:
        host2.close()
