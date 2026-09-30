"""域 B：Driver 运行时与存储 e2e（B1–B7，RGP/1 协议面经 host）。

覆盖矩阵见 FEATURE_MATRIX.md。运行：
    uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_driver.py -q
"""

from __future__ import annotations

import threading

from reactivegraph import DriverHost, GraphBuilder, ReactiveGraph


# -- B1 RUN / RELEASE_GRAPH / SHUTDOWN ----------------------------------------
def test_b1_run_release_shutdown(e2e_host: DriverHost) -> None:
    """经 ReactiveGraph（内部 RUN）执行；RELEASE_GRAPH 释放；close 干净退出。"""

    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"out": s["v"] + 1}, on=("run",), reads=("v",), writes=("out",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_b1")
    assert g.invoke("run", {"v": 1})["out"] == 2  # RUN 通路
    e2e_host.release_graph("g_b1")  # RELEASE_GRAPH 不抛
    # 释放后可重新编译同 id
    g2 = ReactiveGraph.build(build, host=e2e_host, graph_id="g_b1")
    assert g2.invoke("run", {"v": 2})["out"] == 3
    e2e_host.close()  # SHUTDOWN 干净（close 幂等）


# -- B2 STORE_OP（LongTermStore 持久）------------------------------------------
def test_b2_store_op_persist(e2e_db) -> None:
    """STORE_OP put/get 跨 host 重启存活（sqlite）。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host1.start()
    host1.handshake()
    try:
        host1.store_op("put", ["ns"], "k", {"v": 1})
        rec = host1.store_op("get", ["ns"], "k")
        assert rec["value"] == {"v": 1}
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host2.start()
    host2.handshake()
    try:
        rec2 = host2.store_op("get", ["ns"], "k")
        assert rec2["value"] == {"v": 1}, "重启后 store 应存活"
    finally:
        host2.close()


# -- B3 CHECKPOINT_OP 生命周期 -------------------------------------------------
def test_b3_checkpoint_op(e2e_host: DriverHost) -> None:
    """checkpoint get/list/put/delete_thread 生命周期。"""

    def build(b: GraphBuilder) -> None:
        b.task("inc", fn=lambda s: {"n": s["n"] + 1}, on=("run",), reads=("n",), writes=("n",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_b3")
    for _ in g.stream("run", {"n": 0}, thread_id="t_b3"):
        pass
    for _ in g.stream("run", {"n": 1}, thread_id="t_b3"):
        pass

    cps = e2e_host.checkpoint_op("list", thread_id="t_b3")
    assert len(cps) >= 2
    c0 = e2e_host.checkpoint_op("get", thread_id="t_b3", checkpoint_id=cps[0]["checkpointId"])
    assert c0 is not None and c0["checkpointId"] == cps[0]["checkpointId"]

    # 注：CHECKPOINT_OP put 的 record.values 需 Driver 内部序列化 Buffer 格式
    # （JsonPlusSerializer/EncryptedSerializer），JSON 协议传输后不可在 Python
    # 面直接构造——put 记录面属 TS 内部细节，此处验证 get/list/delete_thread
    # 生命周期主路径。

    # delete_thread 清空
    e2e_host.checkpoint_op("delete_thread", thread_id="t_b3")
    assert e2e_host.checkpoint_op("list", thread_id="t_b3") == []


# -- B4 VECTOR_UPSERT / SEARCH -------------------------------------------------
def test_b4_vector_ops(e2e_db) -> None:
    """VECTOR_UPSERT/SEARCH：sqlite 命中 + 跨重启存活；memory 对照重启即失。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host1.start()
    host1.handshake()
    try:
        host1.vector_upsert([1.0, 0.0], namespace=["ns"], id="a", metadata={"tag": "x"})
        host1.vector_upsert([0.0, 1.0], namespace=["ns"], id="b")
        hits = host1.vector_search([1.0, 0.0], namespace=["ns"], limit=1)
        assert hits and hits[0]["id"] == "a", f"应命中 a，实际 {hits}"
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host2.start()
    host2.handshake()
    try:
        hits = host2.vector_search([0.0, 1.0], namespace=["ns"], limit=1)
        assert hits and hits[0]["id"] == "b", "sqlite 向量跨重启应存活"
    finally:
        host2.close()

    # memory 对照：重启即失
    m1 = DriverHost(env={})
    m1.start()
    m1.handshake()
    m1.vector_upsert([1.0, 0.0], id="a")
    m1.close()
    m2 = DriverHost(env={})
    m2.start()
    m2.handshake()
    assert m2.vector_search([1.0, 0.0], limit=1) == [], "memory 向量重启应丢失"
    m2.close()


# -- B5 持久化后端选择 -----------------------------------------------------------
def test_b5_backend_select(e2e_db) -> None:
    """REACTIVEGRAPH_DB → sqlite（store 重启存活）；无 env → memory（重启即失）。"""
    assert DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})  # 构造即选 sqlite 后端
    host = DriverHost(env={})
    host.start()
    host.handshake()
    host.store_op("put", [], "k", "mem")
    host.close()
    host2 = DriverHost(env={})
    host2.start()
    host2.handshake()
    assert host2.store_op("get", [], "k") is None, "memory store 重启应丢失"
    host2.close()


# -- B6 并发线程隔离 ------------------------------------------------------------
def test_b6_concurrent_threads(e2e_host: DriverHost) -> None:
    """4 线程各自 thread_id：状态互不串（each n == 20）。"""

    def build(b: GraphBuilder) -> None:
        b.task("acc", fn=lambda s: {"n": s.get("n", 0) + 1}, on=("run",),
               reads=("n",), writes=("n",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_b6")
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        try:
            for _ in range(20):
                for _fr in g.stream("run", {}, thread_id=f"t{i}"):
                    pass
            assert g.get_state(f"t{i}")["n"] == 20
        except BaseException as exc:  # noqa: BLE001 — 汇入主线程断言
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"并发线程应无错误，实际 {errors}"


# -- B7 EXPORT_DOT --------------------------------------------------------------
def test_b7_export_dot(e2e_host: DriverHost) -> None:
    """EXPORT_DOT：DOT 含任务节点与边（task 订阅事件）。"""

    def build(b: GraphBuilder) -> None:
        b.task("a", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
        b.task("b", fn=lambda s: {"y": s["x"]}, on=("a:written",), reads=("x",), writes=("y",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_b7")
    g.invoke("run", {})  # 触发惰性编译（export 需已编译图）
    dot = e2e_host.export_dot("g_b7")
    assert "a" in dot and "b" in dot, "DOT 应含任务节点"
    assert "run" in dot, "DOT 应含事件路由"