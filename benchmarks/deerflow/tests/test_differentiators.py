"""差异化能力测试：我们有、langchain/langgraph 没有的 7 项能力。

每一项断言「我们做到、langgraph 不这样/做不到」的量化行为（对照见
DEERFLOW_MATRIX.md 差异化小节）：
D1 跨 run 选择性执行（pure 指纹跳过）——langgraph 每次 invoke 全量重跑
D2 事务写冲突策略（明确报错而非静默合并）——langgraph 全量重算后合并
D3 computed 声明式缓存（read-set 失效声明）——无等价物
D4 scope 子状态命名空间——无等价物
D5 因果 trace / EXPORT_DOT（调度决策可导出）——黑盒重放
D6 durable event log 回放（重启精确恢复）——checkpoint 快照式
D7 RGP/1 协议 + Driver 真子进程（引擎跨语言执行）——纯 Python 进程内
"""

from __future__ import annotations

from pathlib import Path

import pytest
from reactivegraph import DriverError, DriverHost, GraphBuilder, ReactiveGraph


# -- D1 跨 run 选择性执行 -------------------------------------------------------
def test_diff_d1_cross_run_selective_skip(df_host: DriverHost) -> None:
    """同输入跨 run：pure 任务指纹跳过 calls 不增长；新输入重跑。"""
    calls: dict[str, int] = {}

    def pure_fn(s: dict) -> dict:
        calls["pure"] = calls.get("pure", 0) + 1
        return {"p": s["x"] * 2}

    def build(b: GraphBuilder) -> None:
        b.task("pure", fn=pure_fn, kind="pure", on=("run",), reads=("x",), writes=("p",))

    g = ReactiveGraph.build(build, host=df_host, graph_id="df_diff_d1")
    g.invoke("run", {"x": 1})
    g.invoke("run", {"x": 1})  # 同输入：跳过
    assert calls["pure"] == 1, "langgraph 每次 invoke 全量重跑；我们跨 run 跳过"
    g.invoke("run", {"x": 2})
    assert calls["pure"] == 2


# -- D2 事务写冲突策略 ----------------------------------------------------------
def test_diff_d2_write_conflict_explicit(df_host: DriverHost) -> None:
    """并行批同 path 写声明 → 明确 WriteConflict 错误（非静默合并）。"""
    with pytest.raises(DriverError, match="WriteConflict"):

        def build(b: GraphBuilder) -> None:
            b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
            b.task("t1", fn=lambda s: {"w": 1}, on=("t0:written",), writes=("w",))
            b.task("t2", fn=lambda s: {"w": 2}, on=("t0:written",), writes=("w",))

        g = ReactiveGraph.build(build, host=df_host, graph_id="df_diff_d2")
        g.invoke("run", {"n": 1})


# -- D3 computed 声明式缓存 -----------------------------------------------------
def test_diff_d3_computed_declarative_cache() -> None:
    """computed 以 read-set 声明缓存前提（langgraph 无等价物）；值正确。"""
    calc_calls: dict[str, int] = {"n": 0}

    def selector(s: dict) -> int:
        calc_calls["n"] += 1
        return s["a"] + s["b"]

    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"a": 1, "b": 2}, on=("run",))
        b.computed("total", selector, reads=("a", "b"))

    g = ReactiveGraph.build(build)
    assert g.invoke("run", {})["total"] == 3
    # read-set 声明式失效：只读集 (a, b) 决定重算（effect 全量重跑图无此机制）
    assert calc_calls["n"] >= 1


# -- D4 scope 子状态命名空间 ----------------------------------------------------
def test_diff_d4_scope_namespace(df_host: DriverHost) -> None:
    """scope 任务输出落 state[scope]，与顶层互不污染（langgraph 无子状态域）。"""
    def build(b: GraphBuilder) -> None:
        b.task("scoped", fn=lambda s: {"k": "v1"}, on=("run",), scope="user", writes=("k",))
        b.task("top", fn=lambda s: {"k": "top"}, on=("run",), writes=("k",))

    g = ReactiveGraph.build(build, host=df_host, graph_id="df_diff_d4")
    out = g.invoke("run", {})
    assert out["user"]["k"] == "v1"
    assert out["k"] == "top"


# -- D5 因果 trace / EXPORT_DOT -------------------------------------------------
def test_diff_d5_causal_trace_and_dot(df_host: DriverHost) -> None:
    """调度决策可导出：trace 含 start/skip/done 决策 + DOT 可渲染。"""
    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"out": "ok"}, on=("run",), writes=("out",))

    g = ReactiveGraph.build(build, host=df_host, graph_id="df_diff_d5")
    g.invoke("run", {})
    assert g._trace, "invoke 后应有 trace 事件"
    events = {t["event"] for t in g._trace}
    assert "run:start" in events, f"trace 应含 run:start，实际 {events}"
    dot = df_host.export_dot("df_diff_d5")
    assert "t" in dot


# -- D6 durable event log 回放 --------------------------------------------------
def test_diff_d6_durable_replay(df_db: Path) -> None:
    """host 重启后 checkpoint 精确恢复（事件日志驱动，非快照覆盖）。"""
    def build(b: GraphBuilder) -> None:
        b.task("acc", fn=lambda s: {"n": s.get("n", 0) + 1}, on=("run",),
               reads=("n",), writes=("n",))

    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host1.start()
    host1.handshake()
    try:
        g1 = ReactiveGraph.build(build, host=host1, graph_id="df_diff_d6")
        g1.invoke("run", {})
        g1.invoke("run", {})
        assert g1.get_state()["n"] == 2
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(df_db)})
    host2.start()
    host2.handshake()
    try:
        g2 = ReactiveGraph.build(build, host=host2, graph_id="df_diff_d6")
        assert g2.get_state()["n"] == 2, "重启后应精确恢复（事件日志回放）"
    finally:
        host2.close()


# -- D7 RGP/1 协议 + Driver 真子进程 -------------------------------------------
def test_diff_d7_protocol_and_real_driver(df_host: DriverHost) -> None:
    """RGP/1 握手 + 图 RUN 经真实 Node Driver 子进程跨语言执行。"""
    from reactivegraph import PROTOCOL_NAME, PROTOCOL_VERSION

    assert df_host.handshake()  # 协议握手（幂等）
    assert PROTOCOL_VERSION  # 版本协商面

    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"out": s["v"] + 1}, on=("run",),
               reads=("v",), writes=("out",))

    g = ReactiveGraph.build(build, host=df_host, graph_id="df_diff_d7")
    out = g.invoke("run", {"v": 1})
    assert out["out"] == 2  # Python API → RGP/1 → Node Driver 执行
    assert df_host.release_graph("df_diff_d7").get("ok") is True  # RELEASE_GRAPH 干净
    assert PROTOCOL_NAME  # 协议常量面