"""realworld 套件单元测试（fake/离线，不打真实 API）。

运行：uv run --directory python/reactivechain pytest benchmarks/realworld/test_realworld.py -q
真实 API 集成验证由 realworld_side.py main() 的 REACTIVEGRAPH_REAL 门控单独执行。
"""

from __future__ import annotations

import json
import tempfile

from realworld_side import EmbeddingCache


def test_embedding_cache_hits_after_first_call() -> None:
    calls: list[int] = []

    def embed(texts: list[str]) -> list[list[float]]:
        calls.append(1)
        return [[0.1, 0.2]] * len(texts)

    with tempfile.TemporaryDirectory() as d:
        cache = EmbeddingCache(embed, d)
        v1 = cache.get(["你好"])
        v2 = cache.get(["你好"])
        assert v1 == v2
        assert len(calls) == 1  # 第二次命中缓存，不重算


def test_cache_different_inputs_miss() -> None:
    calls: list[int] = []

    def embed(texts: list[str]) -> list[list[float]]:
        calls.append(1)
        return [[0.1, 0.2]] * len(texts)

    with tempfile.TemporaryDirectory() as d:
        cache = EmbeddingCache(embed, d)
        cache.get(["你好"])
        cache.get(["再见"])
        assert len(calls) == 2


# -- Task 2: RAG 链 + 调用计数 + 选择性收益 -----------------------------------


def test_selective_rerun_skips_retrieval() -> None:
    from reactivechain.llm import FakeLLM
    from realworld_side import CountingEmbeddings, CountingLLM, build_rag

    emb = CountingEmbeddings()
    llm = CountingLLM(FakeLLM([f"答案{i}" for i in range(6)]))
    chain = build_rag(llm=llm, embeddings=emb)
    chain.invoke({"query": "X"})  # 预热（入库 + 首查检索）
    calls_after_first = emb.calls
    chain.invoke({"query": "X"})
    assert emb.calls == calls_after_first  # 同 query 重跑：检索段跳过，embedding 不增
    chain.invoke({"query": "Y"})
    assert emb.calls == calls_after_first + 1  # 不同 query：全量重检索


def test_measure_skip_vs_full_quantifies_gain() -> None:
    from reactivechain.llm import FakeLLM
    from realworld_side import CountingEmbeddings, CountingLLM, build_rag, measure_skip_vs_full

    emb = CountingEmbeddings()
    llm = CountingLLM(FakeLLM([f"答案{i}" for i in range(8)]))
    chain = build_rag(llm=llm, embeddings=emb)
    chain.invoke({"query": "X"})  # 预热
    stat = measure_skip_vs_full(chain, "X", counter=emb)
    # skip 模式（缓存命中）检索调用数 < full 模式（clear_cache 后全量）
    assert stat["skip_retrieval_calls"] < stat["full_retrieval_calls"]
    assert stat["skip_ms"] < stat["full_ms"]


# -- Task 3: main() 离线壳（REACTIVEGRAPH_REAL=0 结构校验，无真实 API） -------


def test_main_offline_writes_report(tmp_path, monkeypatch) -> None:
    """离线壳：main() 用 FakeLLM 跑通并输出 .results/realworld.json，
    断言含调用计数/延迟字段且 selective 检索调用下降。"""
    monkeypatch.setenv("REACTIVEGRAPH_REAL", "0")
    from realworld_side import main

    rc = main(results_dir=str(tmp_path))
    assert rc == 0
    report = json.loads((tmp_path / "realworld.json").read_text(encoding="utf-8"))
    assert report["mode"] == "offline"
    assert report["outputs_nonempty"] is True
    assert report["selective_skips"] is True
    assert report["rag_first"]["retrieval_calls"] == 1
    assert report["rag_selective"]["retrieval_calls"] == 0


# -- Task 4: Driver 向量库持久化（sqlite 多后端，跨重启存活） -----------------


def test_driver_vector_store_persists_across_restart(tmp_path) -> None:
    """sqlite 持久化向量库：REACTIVEGRAPH_DB 存在时 VECTOR_UPSERT/SEARCH
    落到 sqlite，Driver 重启后检索仍命中（内存后端重启即失——对照见下）。"""
    from reactivechain.documents import Document
    from reactivechain.embeddings import HashEmbeddings
    from reactivechain.vectorstore import DriverVectorStore
    from reactivegraph import DriverHost

    db = tmp_path / "v.db"
    host = DriverHost(env={"REACTIVEGRAPH_DB": str(db)})
    host.start()
    host.handshake()
    try:
        store = DriverVectorStore(host, HashEmbeddings(dims=8), namespace=["rc", "test"])
        store.add_documents([Document("ReactiveChain 是声明式管道组件层。")])
        assert len(store.similarity_search("ReactiveChain", k=1)) == 1
    finally:
        host.close()

    # 重启（同 db）：持久化存活
    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(db)})
    host2.start()
    host2.handshake()
    try:
        store2 = DriverVectorStore(host2, HashEmbeddings(dims=8), namespace=["rc", "test"])
        hits = store2.similarity_search("ReactiveChain", k=1)
        assert len(hits) == 1  # 重启后仍命中（sqlite 持久化）
    finally:
        host2.close()


def test_driver_vector_store_memory_lost_on_restart(tmp_path) -> None:
    """对照：无 REACTIVEGRAPH_DB（内存后端）重启后检索为空——持久化收益
    由此差异体现。"""
    from reactivechain.documents import Document
    from reactivechain.embeddings import HashEmbeddings
    from reactivechain.vectorstore import DriverVectorStore
    from reactivegraph import DriverHost

    host = DriverHost(env={})
    host.start()
    host.handshake()
    try:
        store = DriverVectorStore(host, HashEmbeddings(dims=8), namespace=["rc", "test"])
        store.add_documents([Document("ReactiveChain 是声明式管道组件层。")])
        assert len(store.similarity_search("ReactiveChain", k=1)) == 1
    finally:
        host.close()

    host2 = DriverHost(env={})
    host2.start()
    host2.handshake()
    try:
        store2 = DriverVectorStore(host2, HashEmbeddings(dims=8), namespace=["rc", "test"])
        assert len(store2.similarity_search("ReactiveChain", k=1)) == 0  # 内存丢失
    finally:
        host2.close()


# -- Task 5: 长会话 / Interrupt-resume / 并发隔离 -----------------------------


def test_session_memory_accumulates_across_runs(tmp_path) -> None:
    """10 轮会话：同 thread_id 连续 invoke，状态（history）逐轮累积。"""
    from reactivegraph import DriverHost, ReactiveGraph

    db = tmp_path / "s.db"
    host = DriverHost(env={"REACTIVEGRAPH_DB": str(db)})
    host.start()
    host.handshake()
    try:

        def session_task(state: dict) -> dict:
            hist = list(state.get("history") or [])
            hist.append(state["query"])
            return {"history": hist}

        def build(b) -> None:
            b.task("session", fn=session_task, on=("run",), writes=("history",))

        g = ReactiveGraph.build(build, host=host, graph_id="g_rw_session")
        for i in range(10):
            for _frame in g.stream("run", {"query": f"q{i}"}, thread_id="s1"):
                pass
        assert g.get_state("s1")["history"] == [f"q{i}" for i in range(10)]
    finally:
        host.close()


def test_interrupt_resume_roundtrip(tmp_path) -> None:
    """确定性 Interrupt → resume(载荷) → 完成；状态正确（host + sqlite）。"""
    from reactivegraph import DriverHost, GraphInterrupt, Interrupt, ReactiveGraph

    db = tmp_path / "ir.db"
    host = DriverHost(env={"REACTIVEGRAPH_DB": str(db)})
    host.start()
    host.handshake()
    try:

        def gate(state: dict) -> dict:
            if "user_response" not in state:
                raise GraphInterrupt([Interrupt("需要人工审批")])
            return {"decision": state["user_response"]}

        def t2(state: dict) -> dict:
            return {"out": state["decision"]}

        def build(b) -> None:
            b.task("gate", fn=gate, on=("run",), writes=("decision",))
            b.task("t2", fn=t2, on=("gate:written",), reads=("decision",),
                   writes=("out",))

        g = ReactiveGraph.build(build, host=host, graph_id="g_rw_ir")
        g.invoke("run", {})
        assert g._last_run["interrupted"] is True
        r2 = g.resume(g._last_run["runId"], {"user_response": True})
        assert r2.get("out") is True
    finally:
        host.close()


def test_concurrent_threads_isolated(tmp_path) -> None:
    """4 线程 × 独立 thread_id：20 轮各自累积，状态互不串写。"""
    import threading

    from reactivegraph import DriverHost, ReactiveGraph

    db = tmp_path / "c.db"
    host = DriverHost(env={"REACTIVEGRAPH_DB": str(db)})
    host.start()
    host.handshake()
    try:

        def build(b) -> None:
            b.task("w", fn=lambda s: {"n": s.get("n", 0) + 1}, on=("run",),
                   writes=("n",))

        g = ReactiveGraph.build(build, host=host, graph_id="g_rw_conc")
        errors: list[Exception] = []

        def worker(i: int) -> None:
            try:
                for _ in range(20):
                    for _frame in g.stream("run", {"v": i}, thread_id=f"t{i}"):
                        pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        for i in range(4):
            assert g.get_state(f"t{i}")["n"] == 20  # 各自累积，互不串
    finally:
        host.close()