"""域 D：跨层端到端场景 e2e（D1–D6）。

覆盖矩阵见 FEATURE_MATRIX.md。运行：
    uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_scenarios.py -q
"""

from __future__ import annotations

import threading

from reactivechain import (
    ChatPromptTemplate,
    Document,
    FakeLLM,
    HashEmbeddings,
    InMemoryVectorStore,
    Pipeline,
    RunnableLambda,
    StrOutputParser,
    VectorStoreRetriever,
)
from reactivegraph import DriverHost, GraphBuilder, GraphInterrupt, Interrupt, ReactiveGraph


# -- D1 RAG 链 → Driver host ---------------------------------------------------
def test_d1_rag_chain_host(e2e_host: DriverHost) -> None:
    """retriever | prompt | llm | parser 全链挂 Driver 图执行。"""
    store = InMemoryVectorStore(HashEmbeddings(dims=8))
    store.add_documents([Document(page_content="反应式图执行引擎按依赖增量执行")])
    retriever = VectorStoreRetriever(store, k=1)

    chain = (
        RunnableLambda(lambda s: {"docs": retriever.invoke(s)["docs"]},
                       reads={"query"}, writes={"docs"})
        | ChatPromptTemplate([("human", "基于：{docs}\n回答：{query}")])
        | FakeLLM(["离线答案"])
        | StrOutputParser()
    )
    graph = chain.to_graph(host=e2e_host, graph_id="g_d1")
    out = graph.invoke("run", {"query": "引擎如何执行？"})
    assert "离线答案" in str(out)


# -- D2 ReAct agent（工具+记忆）在 Driver ---------------------------------------
def test_d2_react_agent_driver(e2e_host: DriverHost) -> None:
    """图内工具调用 + 记忆累积（store_put/get 跨 run 会话持久）。"""

    def tool_task(s: dict) -> dict:
        expr = s["expression"]
        result = eval(expr)  # noqa: S307 — e2e 固定输入
        return {"result": str(result)}

    def memory_task(s: dict) -> dict:
        history = list(s.get("history") or [])
        history.append({"q": s.get("query"), "a": s.get("result")})
        return {"history": history}

    def build(b: GraphBuilder) -> None:
        b.task("tool", fn=tool_task, on=("run",), reads=("expression",), writes=("result",))
        b.task("memory", fn=memory_task, on=("run",), reads=("history", "query", "result"),
               writes=("history",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_d2")
    g.invoke("run", {"expression": "1+2*3", "query": "q1"})
    g.invoke("run", {"expression": "10-4", "query": "q2"})
    history = g.get_state()["history"]
    assert len(history) == 2
    assert history[0]["q"] == "q1" and history[1]["q"] == "q2"


# -- D3 会话中断 → resume → 继续工具链 ------------------------------------------
def test_d3_interrupt_resume_chain(e2e_host: DriverHost) -> None:
    """中断后 resume 继续：工具结果与中断载荷衔接。"""

    def gate(s: dict) -> dict:
        if not s.get("user_response"):
            raise GraphInterrupt([Interrupt("需用户确认")])
        return {"confirmed": True}

    def tool_task(s: dict) -> dict:
        return {"result": s["confirmed"] and "ok"}

    def build(b: GraphBuilder) -> None:
        b.task("gate", fn=gate, on=("run",), reads=("user_response",), writes=("confirmed",))
        b.task("tool", fn=tool_task, on=("gate:written",), reads=("confirmed",), writes=("result",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_d3")
    g.invoke("run", {})
    assert g._last_run["interrupted"] is True
    r2 = g.resume(g._last_run["runId"], {"user_response": True})
    assert r2.get("result") == "ok", "resume 后工具链应继续完成"


# -- D4 重启恢复完整会话 ---------------------------------------------------------
def test_d4_restart_recover(e2e_db) -> None:
    """host 重启后会话状态（含累积历史）完整保留并继续。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host1.start()
    host1.handshake()
    try:
        def build(b: GraphBuilder) -> None:
            b.task("chat", fn=lambda s: {"history": list(s.get("history") or []) + [s["msg"]]},
                   on=("run",), reads=("history", "msg"), writes=("history",))

        g1 = ReactiveGraph.build(build, host=host1, graph_id="g_d4")
        g1.invoke("run", {"msg": "你好"})
        g1.invoke("run", {"msg": "继续"})
        assert g1.get_state()["history"] == ["你好", "继续"]
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host2.start()
    host2.handshake()
    try:
        def build2(b: GraphBuilder) -> None:
            b.task("chat", fn=lambda s: {"history": list(s.get("history") or []) + [s["msg"]]},
                   on=("run",), reads=("history", "msg"), writes=("history",))

        g2 = ReactiveGraph.build(build2, host=host2, graph_id="g_d4")
        assert g2.get_state()["history"] == ["你好", "继续"], "重启后会话应完整恢复"
        g2.invoke("run", {"msg": "再来"})
        assert g2.get_state()["history"] == ["你好", "继续", "再来"]
    finally:
        host2.close()


# -- D5 并发多线程会话隔离 -------------------------------------------------------
def test_d5_concurrent_threads(e2e_host: DriverHost) -> None:
    """4 线程各自 thread_id 会话隔离（各 20 轮，重启后各自计数保留）。"""

    def build(b: GraphBuilder) -> None:
        b.task("acc", fn=lambda s: {"n": s.get("n", 0) + 1}, on=("run",),
               reads=("n",), writes=("n",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_d5")
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
    # 会话隔离：各线程计数互不串
    counts = {f"t{i}": g.get_state(f"t{i}")["n"] for i in range(4)}
    assert counts == {"t0": 20, "t1": 20, "t2": 20, "t3": 20}


# -- D6 选择性执行收益（cache 命中跳过）------------------------------------------
def test_d6_selective_gain() -> None:
    """fallback 内核：同输入重跑 pure 段跳过（收益量化），改输入重算。"""
    calls: list[int] = []

    def costly(s: dict) -> dict:
        calls.append(1)
        return {"p": s["x"] * 2}

    chain = Pipeline([RunnableLambda(costly, pure=True, reads={"x"}, writes={"p"})])
    chain.invoke({"x": 1})
    chain.invoke({"x": 1})  # 同输入：pure 指纹跳过
    assert len(calls) == 1, f"同输入应跳过 costly 段，实际 {len(calls)} 次"
    chain.invoke({"x": 2})  # 新输入：重算
    assert len(calls) == 2
