"""方案 C P2：引擎升级后 Driver 能力兑现的端到端测试。

覆盖（docs/plans/2026-09-15-reactivechain-engine-runtime.md §5 P2）：
1. 流式中断 resume（human-in-the-loop）：任务抛 GraphInterrupt（控制流
   信号，携带 Interrupt 载荷）→ run 立即
   返回 interrupted + checkpoint → resume 以用户载荷继续剩余链
2. checkpoint/恢复链侧接入：Pipeline.to_graph(host=...) → 图经真实
   Driver 执行 + get_state/list_checkpoints/restore_thread 可用
3. Memory 持久化：store_put/store_get 跨 run 持久（同 thread store）
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from reactivegraph import DriverHost, GraphInterrupt, Interrupt, ReactiveGraph

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def durable_host(tmp_path: Path):
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    assert node is not None
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    env["REACTIVEGRAPH_DB"] = str(tmp_path / "rgp.db")  # durable checkpoints
    host = DriverHost(env=env)
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()


# -- P3-4 checkpoint 多后端：DriverHost 便捷参数 ---------------------------


def _driver_baseline_env() -> dict:
    """Driver 基础 env（无任何 DB 配置——默认 memory 后端）。"""
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    assert node is not None
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    env.pop("REACTIVEGRAPH_DB", None)
    env.pop("REACTIVEGRAPH_PG_DSN", None)
    env.pop("REACTIVEGRAPH_REDIS_URL", None)
    return env


def _run_once(host: DriverHost, graph_id: str) -> list:
    def build(b) -> None:
        b.task("t", fn=lambda s: {"ok": True}, on=("run",), writes=("ok",))

    g = ReactiveGraph.build(build, host=host, graph_id=graph_id)
    g.invoke("run", {})
    return g.list_checkpoints()


def test_driverhost_checkpoint_backend_memory(tmp_path: Path) -> None:
    """memory：即使 env 携带 REACTIVEGRAPH_DB 也强制移除（不注入持久后端）。"""
    env = _driver_baseline_env()
    env["REACTIVEGRAPH_DB"] = str(tmp_path / "should_not_exist.db")
    host = DriverHost(env=env, checkpoint_backend="memory")
    host.start()
    host.handshake()
    try:
        cps = _run_once(host, "g_p34_mem")
        assert cps == []  # memory 后端不残留
    finally:
        host.close()
    assert not (tmp_path / "should_not_exist.db").exists()


def test_driverhost_checkpoint_backend_sqlite(tmp_path: Path) -> None:
    """sqlite：db_path 便捷参数等价 REACTIVEGRAPH_DB（持久 checkpoint）。"""
    db = tmp_path / "bk.db"
    host = DriverHost(env=_driver_baseline_env(),
                      checkpoint_backend="sqlite", db_path=str(db))
    host.start()
    host.handshake()
    try:
        cps = _run_once(host, "g_p34_sqlite")
        assert len(cps) >= 1  # run 落盘 checkpoint
    finally:
        host.close()
    assert db.exists()


def test_driverhost_checkpoint_backend_postgres() -> None:
    """postgres：pg_dsn 提供才测，否则 skip（外部 DB 依赖，design §8）。

    读取的是测试专用开关 ``REACTIVEGRAPH_TEST_PG_DSN``，不是 Driver 运行时
    的 ``REACTIVEGRAPH_PG_DSN``：后者是全局运行时配置，测试里从环境读取它
    会让每个 Driver 测试都被重定向到 Postgres（见 tests/conftest.py）。
    """
    dsn = os.environ.get("REACTIVEGRAPH_TEST_PG_DSN")
    if not dsn:
        pytest.skip("REACTIVEGRAPH_TEST_PG_DSN 未提供（外部 postgres 依赖）")
    host = DriverHost(env=_driver_baseline_env(),
                      checkpoint_backend="postgres", pg_dsn=dsn)
    host.start()
    host.handshake()
    try:
        cps = _run_once(host, "g_p34_pg")
        assert len(cps) >= 1
    finally:
        host.close()


# -- P2-2 流式中断 resume ------------------------------------------------


def test_interrupt_resume_end_to_end(durable_host: DriverHost) -> None:
    """任务抛 GraphInterrupt → interrupted=true + checkpoint；resume 以用户载荷
    继续（gate 放行 → 剩余链完成）。"""
    calls: list[str] = []

    def t0(state: dict) -> dict:
        calls.append("t0")
        return {"x": state["n"] + 1}

    def gate(state: dict) -> dict:
        """中断点：无 user_response 时请求人工输入，有则放行。

        ``Interrupt`` 是载荷而非异常；抛出的控制流信号是 ``GraphInterrupt``
        （与 langgraph 同构），host 按其类型名透传给 Driver。
        """
        calls.append("gate")
        if "user_response" not in state:
            raise GraphInterrupt([Interrupt("需要人工审批")])
        return {"decision": state["user_response"]}

    def t2(state: dict) -> dict:
        calls.append("t2")
        return {"out": state["x"] + (1 if state["decision"] else 0)}

    def build(b) -> None:
        b.task("t0", fn=t0, on=("run",), writes=("x",))
        b.task("gate", fn=gate, on=("t0:written",), reads=("x", "user_response"),
               writes=("decision",))
        b.task("t2", fn=t2, on=("gate:written",), reads=("x", "decision"),
               writes=("out",))

    g = ReactiveGraph.build(build, host=durable_host, graph_id="g_p2_interrupt")
    # 首次 run：gate 中断
    state1 = g.invoke("run", {"n": 4})
    assert g._last_run["interrupted"] is True
    assert state1["x"] == 5
    run_id = g._last_run["runId"]

    # resume：人工输入 True → gate 放行 → t2 完成（载荷为对象键，并入 store
    # 后中断任务与下游级联任务可读）；host.resume 返回裸 state
    resumed = g.resume(run_id, {"user_response": True})
    assert resumed["decision"] is True
    assert resumed["out"] == 6
    # 幂等重放：t0 重跑但同输入同输出；每任务至多执行一次（级联去重）
    assert calls.count("t0") == 2 and calls.count("gate") == 2 and calls.count("t2") == 1


# -- P2-3 Memory 持久化 --------------------------------------------------


def test_store_put_get_persist(durable_host: DriverHost) -> None:
    """store_put/store_get：long-term store 按 namespace/key 持久（跨 run
    同 thread 可读、覆盖写生效）。"""
    def build(b) -> None:
        b.task("t", fn=lambda s: {"ok": True}, on=("run",), writes=("ok",))

    g = ReactiveGraph.build(build, host=durable_host, graph_id="g_p2_store")
    g.store_put(("memory", "dialogue"), "k1", {"v": 42})
    assert g.store_get(("memory", "dialogue"), "k1") == {"v": 42}
    g.store_put(("memory", "dialogue"), "k1", {"v": 43})  # 覆盖
    assert g.store_get(("memory", "dialogue"), "k1")["v"] == 43
    # 空 namespace 列表、跨图共享（同 DB 文件）
    g2 = ReactiveGraph.build(build, host=durable_host, graph_id="g_p2_store2")
    assert g2.store_get(("memory", "dialogue"), "k1")["v"] == 43
    g2.store_delete(("memory", "dialogue"), "k1")
    assert g2.store_get(("memory", "dialogue"), "k1") is None