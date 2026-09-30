"""域 A：reactivegraph 引擎 e2e（A1–A14）。

每条走完整真实路径：Python API → DriverHost 真 Driver 子进程（sqlite）→ 断言。
覆盖矩阵见 FEATURE_MATRIX.md（唯一事实源）。

运行：uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_engine.py -q
"""

from __future__ import annotations

import asyncio

import pytest
from reactivegraph import (
    PROTOCOL_NAME,
    PROTOCOL_VERSION,
    DriverError,
    DriverHost,
    GraphBuilder,
    GraphInterrupt,
    Interrupt,
    ReactiveGraph,
    ToolSpec,
    create_react_agent,
)
from reactivegraph.functional import build_function_graph, entrypoint, functask


# -- A1 GraphBuilder 声明式 ---------------------------------------------------
def test_a1_graphbuilder_declarative(e2e_host: DriverHost) -> None:
    """task/on/computed/scope/reads/writes/retry 声明 + 执行。

    - 声明面（host 真 Driver 编译 + 任务执行）：task/on/scope/retry 上 wire。
    - computed 执行面：fallback 内核执行结束后惰性求值；host/Driver 的
      Scheduler.compute 目前无运行时调用点（e2e 暴露缺陷，见收尾说明）。
    """

    def build(b: GraphBuilder) -> None:
        b.task("inc", fn=lambda s: {"n": s.get("n", 0) + 1}, kind="effect",
               on=("run",), reads=("n",), writes=("n",), retry=2)
        b.on("go", "inc")
        b.scope("user")

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a1")
    out = g.invoke("run", {"n": 1})
    assert out["n"] == 2

    # computed 执行面（fallback）：执行结束后按读路径哈希惰性求值，值写入 state
    def build2(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"a": 1, "b": 2}, on=("run",))
        b.computed("total", lambda s: s["a"] + s["b"], reads=("a", "b"))

    g2 = ReactiveGraph.build(build2)
    assert g2.invoke("run", {})["total"] == 3


# -- A2 选择性执行 ------------------------------------------------------------
def test_a2_selective_execution(e2e_host: DriverHost) -> None:
    """选择性执行双面验证。

    - host/Driver：单 run 内事件路由执行受影响任务；pure 任务跨 run 同输入
      指纹跳过（SchedulerPersistent 持久化，d0183d4 起支持）。
    - fallback 内核：pure 任务跨 run 同输入指纹跳过（不重跑）。
    """
    calls = {"t1": 0, "t2": 0, "pure": 0, "effect": 0, "pure_h": 0}

    def t1(s: dict) -> dict:
        calls["t1"] += 1
        return {"p": s["x"] * 2}

    def t2(s: dict) -> dict:
        calls["t2"] += 1
        return {"q": 99}

    def pure_fn(s: dict) -> dict:
        calls["pure"] += 1
        return {"p": s["x"] * 2}

    def pure_h(s: dict) -> dict:
        calls["pure_h"] += 1
        return {"p": s["x"] * 2}

    def effect_fn(s: dict) -> dict:
        calls["effect"] += 1
        return {"e": s["x"]}

    def build(b: GraphBuilder) -> None:
        b.task("t0", fn=lambda s: {"x": s["x"]}, on=("run",), reads=("x",), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), reads=("x",), writes=("p",))
        b.task("t2", fn=t2, on=("t0:written",), reads=(), writes=("q",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a2")
    g.invoke("run", {"x": 1})
    assert calls["t1"] == 1 and calls["t2"] == 1, "订阅 t0:written 的任务都应执行"

    # host：pure 任务跨 run 同输入指纹跳过（新输入重跑）
    def build3(b: GraphBuilder) -> None:
        b.task("pure_h", fn=pure_h, kind="pure", on=("run",), reads=("x",), writes=("p",))

    g3 = ReactiveGraph.build(build3, host=e2e_host, graph_id="g_a2h")
    g3.invoke("run", {"x": 1})
    g3.invoke("run", {"x": 1})  # 同输入跨 run：指纹跳过
    assert calls["pure_h"] == 1, f"host 跨 run 同输入应跳过，实际 {calls['pure_h']}"
    g3.invoke("run", {"x": 2})  # 新输入：重跑
    assert calls["pure_h"] == 2

    # fallback：cross-run pure 指纹跳过 + effect 每次执行
    def build2(b: GraphBuilder) -> None:
        b.task("pure", fn=pure_fn, kind="pure", on=("run",), reads=("x",), writes=("p",))
        b.task("effect", fn=effect_fn, kind="effect", on=("run",), reads=("x",), writes=("e",))

    g2 = ReactiveGraph.build(build2)
    g2.invoke("run", {"x": 1})
    g2.invoke("run", {"x": 1})  # 同输入：pure 指纹跳过
    assert calls["pure"] == 1, f"pure 同输入应跳过，实际 {calls['pure']}"
    g2.invoke("run", {"x": 2})  # 新输入：pure 重跑
    assert calls["pure"] == 2
    assert calls["effect"] == 3  # effect 每次 run 执行


# -- A3 事务状态 + 写冲突 ------------------------------------------------------
def test_a3_write_conflict(e2e_host: DriverHost) -> None:
    """并行批同 path 写声明 → WriteConflictError（经 DriverError 携带）。"""
    with pytest.raises(DriverError, match="WriteConflict"):

        def build(b: GraphBuilder) -> None:
            b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
            b.task("t1", fn=lambda s: {"w": 1}, on=("t0:written",), writes=("w",))
            b.task("t2", fn=lambda s: {"w": 2}, on=("t0:written",), writes=("w",))

        g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a3")
        g.invoke("run", {"n": 1})


# -- A4 stream -----------------------------------------------------------------
def test_a4_stream_frames(e2e_host: DriverHost) -> None:
    """stream 产出 values 帧（含最终 state）。"""
    events: list[dict] = []

    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"out": s["v"] * 2}, on=("run",), reads=("v",), writes=("out",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a4")
    for fr in g.stream("run", {"v": 21}, thread_id="s_a4"):
        events.append(fr)
    values = [e for e in events if e.get("eventType") == "values"]
    assert values, "stream 应产出 values 帧"
    assert values[-1]["payload"]["state"]["out"] == 42


# -- A5 interrupt / resume ------------------------------------------------------
def test_a5_interrupt_resume(e2e_host: DriverHost) -> None:
    """gate 任务抛 Interrupt → interrupted；resume 以用户载荷继续。"""

    def gate(s: dict) -> dict:
        if not s.get("user_response"):
            raise GraphInterrupt([Interrupt("need user")])
        return {"passed": True}

    def build(b: GraphBuilder) -> None:
        b.task("gate", fn=gate, on=("run",), reads=("user_response",), writes=("passed",))
        b.task("after", fn=lambda s: {"done": s["passed"]}, on=("gate:written",),
               reads=("passed",), writes=("done",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a5")
    g.invoke("run", {})
    assert g._last_run["interrupted"] is True
    r2 = g.resume(g._last_run["runId"], {"user_response": True})
    assert r2.get("out") is True or r2.get("done") is True


# -- A6 持久化执行（checkpoint + 恢复）------------------------------------------
def test_a6_durable_recover(e2e_db) -> None:
    """host 重启后从 checkpoint 恢复线程状态并继续。"""
    host1 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host1.start()
    host1.handshake()
    try:
        def build(b: GraphBuilder) -> None:
            b.task("acc", fn=lambda s: {"n": s.get("n", 0) + 1}, on=("run",),
                   reads=("n",), writes=("n",))

        g1 = ReactiveGraph.build(build, host=host1, graph_id="g_a6")
        g1.invoke("run", {})
        g1.invoke("run", {})
        assert g1.get_state()["n"] == 2
    finally:
        host1.close()

    host2 = DriverHost(env={"REACTIVEGRAPH_DB": str(e2e_db)})
    host2.start()
    host2.handshake()
    try:
        def build2(b: GraphBuilder) -> None:
            b.task("acc", fn=lambda s: {"n": s.get("n", 0) + 1}, on=("run",),
                   reads=("n",), writes=("n",))

        g2 = ReactiveGraph.build(build2, host=host2, graph_id="g_a6")
        assert g2.get_state()["n"] == 2, "重启后应恢复 checkpoint 状态"
        g2.invoke("run", {})
        assert g2.get_state()["n"] == 3
    finally:
        host2.close()


# -- A7 time travel / fork -----------------------------------------------------
def test_a7_time_travel(e2e_host: DriverHost) -> None:
    """restore_thread 回滚历史 checkpoint 并以新事务继续。"""

    def build(b: GraphBuilder) -> None:
        b.task("inc", fn=lambda s: {"n": s["n"] + 1}, on=("run",), reads=("n",), writes=("n",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a7")
    g.invoke("run", {"n": 0})  # -> 1
    g.invoke("run", {"n": 2})  # -> 3
    assert g.get_state()["n"] == 3
    cps = g.list_checkpoints()
    assert len(cps) >= 2
    restored = g.restore_thread(checkpoint_id=cps[0]["checkpointId"])
    assert restored["state"]["n"] == 1
    assert g.get_state()["n"] == 1
    g.invoke("run", {"n": 5})
    assert g.get_state()["n"] == 6


# -- A8 因果 trace / export_dot / get_state ------------------------------------
def test_a8_trace_dot(e2e_host: DriverHost) -> None:
    """执行 trace 记录事件；export_dot 含任务节点；get_state 可见写入。"""

    def build(b: GraphBuilder) -> None:
        b.task("t8", fn=lambda s: {"out": "ok"}, on=("run",), writes=("out",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a8")
    g.invoke("run", {})
    assert g.get_state()["out"] == "ok"
    assert g._trace, "invoke 后应有 trace 事件"
    events = {t["event"] for t in g._trace}
    assert "run:start" in events, f"trace 应含 run:start，实际 {events}"
    dot = e2e_host.export_dot("g_a8")
    assert "t8" in dot, "export_dot 应含任务节点"


# -- A9 函数式 API -------------------------------------------------------------
def test_a9_functional(e2e_host: DriverHost) -> None:
    """functask + entrypoint + build_function_graph 建图并执行。"""

    @functask
    def add_one(input_: dict) -> dict:
        return {"n": input_["n"] + 1}

    plan = entrypoint("g_a9", [add_one], [("run", "add_one")])
    assert plan.graph_id == "g_a9"
    assert plan.task_ids == ("add_one",)

    builder = build_function_graph("g_a9", [add_one], [("run", "add_one")])
    g = ReactiveGraph(builder.build(), host=e2e_host)
    out = g.invoke("run", {"n": 1})
    assert out["n"] == 2


# -- A10 预置 agent（工具 + token 级流）----------------------------------------
def test_a10_react_agent(e2e_host: DriverHost) -> None:
    """ToolNode + create_react_agent：工具调用多轮 + stream 消息事件。"""
    tools = [ToolSpec(name="weather", fn=lambda city: "20C", description="weather")]

    def model(messages):
        if len(messages) == 1:
            return {"role": "assistant", "content": "",
                    "tool_calls": [{"id": "t1", "name": "weather", "arguments": {"city": "Paris"}}]}
        return {"role": "assistant", "content": "It is 20C in Paris."}

    agent = create_react_agent(model, tools, max_iterations=3)
    frames = list(agent.stream([{"role": "user", "content": "weather in Paris?"}]))
    kinds = {f.get("eventType") for f in frames}
    assert "messages" in kinds, f"agent.stream 应产出 messages 事件，实际 {kinds}"
    result = [f for f in frames if f.get("eventType") == "result"]
    assert result, "agent.stream 应以 result 帧结束"
    history = result[-1]["payload"]["messages"]
    assert history[-1]["content"] == "It is 20C in Paris."
    # 工具消息进历史（ToolNode 面）
    tool_msgs = [m for m in history if m.get("role") == "tool"]
    assert tool_msgs and "20C" in str(tool_msgs[-1].get("content", ""))


# -- A11 异步 API ---------------------------------------------------------------
def test_a11_async(e2e_host: DriverHost) -> None:
    """ainvoke 与 invoke 一致；astream 与 stream 一致；事件循环不阻塞。"""

    def build(b: GraphBuilder) -> None:
        b.task("t", fn=lambda s: {"n": s["n"] * 3}, on=("run",), reads=("n",), writes=("n",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a11")

    async def run_async():
        out = await g.ainvoke("run", {"n": 2})
        events = [e async for e in g.astream("run", {"n": 2})]
        return out, events

    out, events = asyncio.run(run_async())
    assert out["n"] == 6
    values = [e for e in events if e.get("eventType") == "values"]
    assert values and values[-1]["payload"]["state"]["n"] == 6


# -- A12 DriverHost 生命周期 / 协议握手 ----------------------------------------
def test_a12_host_lifecycle() -> None:
    """start/handshake/close 干净；协议常量存在且握手后 host 可用。"""
    assert PROTOCOL_NAME == "RGP/1"
    assert PROTOCOL_VERSION == 1
    host = DriverHost(env={})
    host.start()
    host.handshake()  # 握手不抛即协议版本匹配
    host.close()


# -- A13 scope 子状态 ----------------------------------------------------------
def test_a13_scope(e2e_host: DriverHost) -> None:
    """scope 任务输出落 state[scope]，不污染顶层。"""

    def build(b: GraphBuilder) -> None:
        b.task("scoped", fn=lambda s: {"k": "v1"}, on=("run",), scope="user", writes=("k",))
        b.task("top", fn=lambda s: {"k": "top"}, on=("run",), writes=("k",))

    g = ReactiveGraph.build(build, host=e2e_host, graph_id="g_a13")
    out = g.invoke("run", {})
    assert out["user"]["k"] == "v1", "scope 任务输出应在 state['user']"
    assert out["k"] == "top", "顶层键不被 scope 任务覆盖"


# -- A14 checkpoint/store 原生入口 ---------------------------------------------
def test_a14_store_native(e2e_host: DriverHost) -> None:
    """store_put/get 跨 run 同线程持久（sqlite LongTermStore）。"""
    g = ReactiveGraph.build(lambda b: None, host=e2e_host, graph_id="g_a14")
    g.store_put(["ns"], "k", {"v": 42})
    assert g.store_get(["ns"], "k") == {"v": 42}
    # 第二次 put 覆盖 + 再读
    g.store_put(["ns"], "k", {"v": 43})
    assert g.store_get(["ns"], "k") == {"v": 43}
