"""Native ReactiveGraph Python API tests (Task 7 / D.5.43-56)."""

from __future__ import annotations

import pytest

from reactivegraph.graph import GraphBuilder, GraphBuildError, ReactiveGraph


class TestGraphConstruction:
    def test_builds_and_validates(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("greet", fn=lambda x: {"msg": f"hi {x['name']}"}).on("visit", "greet")

        graph = ReactiveGraph.build(build, graph_id="g")
        assert graph.definition.id == "g"
        assert graph.definition.route_for("visit") == ["greet"]

    def test_duplicate_task_id_rejected(self) -> None:
        with pytest.raises(GraphBuildError, match="duplicate"):
            GraphBuilder("g").task("a").task("a").build()

    def test_route_to_unknown_task_rejected(self) -> None:
        with pytest.raises(GraphBuildError, match="unknown task"):
            GraphBuilder("g").on("e", "nope").build()


class TestNativeInvoke:
    def test_event_route_invokes_task(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda x: {"seen": x["n"] * 2}).on("run", "t")

        graph = ReactiveGraph.build(build)
        out = graph.invoke("run", {"n": 21})
        assert out["seen"] == 42

    def test_no_route_raises(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda x: {})

        graph = ReactiveGraph.build(build)
        with pytest.raises(GraphBuildError, match="no task routes"):
            graph.invoke("missing", {})

    def test_scope_recorded(self) -> None:
        b = GraphBuilder("g")
        b.scope("user")
        assert b.build().scopes == ["user"]

    def test_multi_task_chain_accumulates_state(self) -> None:
        """多任务同事件：in-process fallback 按注册顺序链式累积（段级图的基础）。

        第二个任务必须读到第一个任务写入的键；当前实现（每任务同一 payload）
        会 KeyError，此测试驱动 fallback 改为累积 state。
        """

        def build(b: GraphBuilder) -> None:
            b.task("inc", fn=lambda s: {"x": s["n"] + 1}, on=("run",), writes=("x",))
            b.task("dbl", fn=lambda s: {"y": s["x"] * 2}, on=("run",), writes=("y",))

        graph = ReactiveGraph.build(build)
        out = graph.invoke("run", {"n": 4})
        assert out == {"n": 4, "x": 5, "y": 10}

    def test_fallback_pure_skip_skips_unchanged_input(self) -> None:
        """in-process fallback：pure 任务同输入指纹跳过（对齐 Driver 选择性）。"""
        calls: list[int] = []

        def counted(state: dict) -> dict:
            calls.append(1)
            return {"v": int(state["k"]) + 1}

        def build(b: GraphBuilder) -> None:
            b.task("t", kind="pure", fn=counted, on=("run",), reads=("k",))

        graph = ReactiveGraph.build(build)
        graph.invoke("run", {"k": 1})
        graph.invoke("run", {"k": 1})  # 相同输入 → 跳过
        assert len(calls) == 1
        graph.invoke("run", {"k": 2})  # 不同输入 → 执行
        assert len(calls) == 2

    def test_fallback_skip_only_for_pure(self) -> None:
        """effect/opaque 任务不跳过（有副作用语义，见方案 C 决策 R1）。"""
        calls: list[int] = []

        def eff(state: dict) -> dict:
            calls.append(1)
            return {"v": 1}

        def build(b: GraphBuilder) -> None:
            b.task("t", kind="effect", fn=eff, on=("run",))

        graph = ReactiveGraph.build(build)
        graph.invoke("run", {})
        graph.invoke("run", {})
        assert len(calls) == 2

    def test_fallback_pure_cache_not_polluted(self) -> None:
        """缓存存 deepcopy：调用方修改返回 dict 不污染后续跳过结果。"""

        def identity(state: dict) -> dict:
            # 从代理 state 构造普通 list（不返回代理引用）
            return {"v": [int(i) for i in state["k"]]}

        def build(b: GraphBuilder) -> None:
            b.task("t", kind="pure", fn=identity, on=("run",), reads=("k",))

        graph = ReactiveGraph.build(build)
        out1 = graph.invoke("run", {"k": [1]})
        out1["v"].append(2)  # 调用方污染返回 dict
        out2 = graph.invoke("run", {"k": [1]})
        assert out2["v"] == [1]  # 缓存未被污染

    def test_fallback_non_json_input_not_cached(self) -> None:
        """不可 JSON 序列化的输入不缓存（对齐 chain `_fingerprint` 语义）。"""
        calls: list[int] = []

        def counted(state: dict) -> dict:
            calls.append(1)
            return {"v": 1}

        def build(b: GraphBuilder) -> None:
            b.task("t", kind="pure", fn=counted, on=("run",))

        graph = ReactiveGraph.build(build)
        graph.invoke("run", {"obj": object()})
        graph.invoke("run", {"obj": object()})
        assert len(calls) == 2

    def test_fallback_event_propagation_subscription(self) -> None:
        """P1：fallback 事件传播——任务订阅上游 `{tid}:written` 事件。"""
        seen: list[str] = []

        def t1(state: dict) -> dict:
            seen.append("t1")
            return {"x": int(state["k"]) + 1}

        def t2(state: dict) -> dict:
            seen.append("t2")
            return {"y": state["x"] * 2}

        def t3(state: dict) -> dict:
            seen.append("t3")
            return {"z": state["y"] + 1}

        def build(b: GraphBuilder) -> None:
            b.task("t1", fn=t1, on=("run",), writes=("x",))
            b.task("t2", fn=t2, on=("t1:written",), writes=("y",))
            b.task("t3", fn=t3, on=("t2:written",), writes=("z",))

        graph = ReactiveGraph.build(build)
        out = graph.invoke("run", {"k": 4})
        assert out == {"k": 4, "x": 5, "y": 10, "z": 11}
        assert seen == ["t1", "t2", "t3"]  # 传播顺序=订阅链顺序

    def test_fallback_event_cycle_guard_raises(self) -> None:
        """P1：订阅成环（A→B→A）时 seen 防重入优雅终止（不挂起）。"""

        def a(state: dict) -> dict:
            return {"x": 1}

        def b(state: dict) -> dict:
            return {"y": 2}

        def build(bld: GraphBuilder) -> None:
            bld.task("a", fn=a, on=("run", "b:written"))
            bld.task("b", fn=b, on=("a:written",))

        graph = ReactiveGraph.build(build)
        # (事件, 任务) 对去重：环在第二轮被截断，正常返回不无限循环
        out = graph.invoke("run", {})
        assert out["x"] == 1 and out["y"] == 2

    def test_fallback_computed_evaluated(self) -> None:
        """P1-2：fallback 执行结束后 computeds 按读路径哈希惰性求值。"""
        calls: list[int] = []

        def sel(s: dict) -> int:
            calls.append(1)
            return s["a"] + s["b"]

        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda s: {"a": 1, "b": 2}, on=("run",))
            b.computed("total", sel, reads=("a", "b"))

        graph = ReactiveGraph.build(build)
        out = graph.invoke("run", {})
        assert out["total"] == 3
        # 读路径未变 → 哈希缓存命中，selector 不重算
        graph.invoke("run", {})
        assert len(calls) == 1


class TestComputed:
    def test_selector_caches_and_invalidates_on_read_set(self) -> None:
        state: dict = {"a": 1, "b": 2}
        calls = []

        def sel(s: dict) -> int:
            calls.append(1)
            return s["a"] + s["b"]

        b = GraphBuilder("g")
        b.computed("total", sel, reads=("a", "b"))
        gd = b.build()
        c = gd._computed_by_id["total"]
        assert c.evaluate(state) == 3
        assert c.evaluate(state) == 3  # cache hit
        assert len(calls) == 1
        # irrelevant-path change -> cache survives
        state["zzz"] = 99
        assert c.evaluate(state) == 3
        assert len(calls) == 1
        # relevant-path change -> invalidates
        state["a"] = 10
        assert c.evaluate(state) == 12
        assert len(calls) == 2


class TestWireSuccess:
    """Driver-facing TaskSuccess payload: real read paths + declared writes/retry."""

    def _wrapper(self, **task_kwargs):
        from reactivegraph.graph import TaskDef

        td = TaskDef(id="t", fn=lambda x: {"msg": f"hi {x['name']}"}, **task_kwargs)
        return ReactiveGraph._task_success_wrapper(td)

    def test_reads_collected_from_task_input_access(self) -> None:
        callback = self._wrapper()
        payload = callback({"name": "Ada", "ignored": 1})
        assert payload["reads"] == ["name"]
        assert payload["patches"] == [
            {"path": ["msg"], "operation": "set", "value": "hi Ada", "taskId": "t"}
        ]
        assert payload["writes"] == ["msg"]

    def test_direct_mutation_returns_recorded_patches(self) -> None:
        from reactivegraph.graph import TaskDef
        from reactivegraph.state import TrackedStateProxy

        def mutate(x: TrackedStateProxy) -> TrackedStateProxy:
            x["count"] = x.get("count", 0) + 1
            return x

        td = TaskDef(id="m", fn=mutate)
        payload = ReactiveGraph._task_success_wrapper(td)({"count": 1})
        assert payload["reads"] == ["count"]
        assert payload["writes"] == ["count"]
        assert len(payload["patches"]) == 1
        assert payload["patches"][0]["path"] == ("count",)
        assert payload["patches"][0]["operation"] == "set"

    def test_wire_spec_carries_writes_retry_and_scope(self) -> None:
        builder = GraphBuilder("g")
        builder.task(
            "t",
            kind="effect",
            fn=lambda x: {"msg": "hi"},
            on=("run",),
            writes=("msg",),
            retry={"maxAttempts": 3, "backoffFactor": 2.0},
            scope="tenantA",
        )
        graph = ReactiveGraph(builder.build())
        spec = graph._to_wire_spec()
        (task,) = spec["tasks"]
        assert task["writes"] == ["msg"]
        assert task["retry"] == {"maxAttempts": 3, "backoffFactor": 2.0}
        assert task["scope"] == "tenantA"
        assert "reads" not in task  # reads are runtime-collected, not declared

    def test_in_process_fallback_accepts_plain_dict_return(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda x: {"seen": x["n"] * 2}).on("run", "t")

        graph = ReactiveGraph.build(build)
        out = graph.invoke("run", {"n": 21})
        # fallback 返回完整累积 state（与 Driver run 返回形状一致）
        assert out["seen"] == 42
        assert out["n"] == 21

    def test_tuple_return_records_effect_receipt(self) -> None:
        from reactivegraph.graph import TaskDef

        def side_effect(x) -> tuple[dict, str]:
            return ({"sent": True}, f"rcpt-{x['id']}")

        td = TaskDef(id="mail", fn=side_effect, kind="effect")
        payload = ReactiveGraph._task_success_wrapper(td)({"id": 7})
        assert payload["external_receipts"] == [{"receipt": "rcpt-7"}]
        assert payload["writes"] == ["sent"]
        assert payload["patches"][0]["path"] == ["sent"]

    def test_plain_return_has_no_receipt(self) -> None:
        from reactivegraph.graph import TaskDef

        td = TaskDef(id="t", fn=lambda x: {"ok": True})
        payload = ReactiveGraph._task_success_wrapper(td)({})
        assert payload["external_receipts"] == []

    def test_fallback_ignores_receipt_component_of_tuple(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda x: ({"seen": x["n"]}, "rcpt-x")).on("run", "t")

        graph = ReactiveGraph.build(build)
        out = graph.invoke("run", {"n": 5})
        assert out["seen"] == 5
        assert out["n"] == 5


class TestStreaming:
    """stream() end-to-end through a real bundled Driver (requires node)."""

    @staticmethod
    def _live_host():
        import os
        import shutil
        from pathlib import Path

        from reactivegraph.host import DriverHost

        node = shutil.which("node")
        repo_root = Path(__file__).resolve().parents[3]
        dist = repo_root / "packages" / "driver" / "dist" / "main.js"
        if node is None or not dist.exists():
            pytest.skip("bundled Driver (node + packages/driver/dist) not available")
        assert node is not None
        env = dict(os.environ)
        env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
        env["REACTIVEGRAPH_NODE_BIN"] = node
        host = DriverHost(env=env)
        host.start()
        host.handshake()
        return host

    def test_stream_yields_values_events(self) -> None:
        host = self._live_host()
        try:
            def build(b: GraphBuilder) -> None:
                b.task("t", fn=lambda x: {"n": x["n"] + 1}).on("run", "t")

            graph = ReactiveGraph.build(build, host=host, graph_id="g_stream")
            events = list(graph.stream("run", {"n": 1}))
            values = [e for e in events if e.get("eventType") == "values"]
            assert len(values) >= 1
            last = values[-1]["payload"]["state"]
            assert last["n"] == 2
        finally:
            host.close()

    def test_resume_requires_host(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda x: {})

        graph = ReactiveGraph.build(build)
        with pytest.raises(GraphBuildError, match="DriverHost"):
            graph.resume("run-1", {})


class TestPersistence:
    """checkpoint / long-term store accessors through a real bundled Driver."""

    def test_store_roundtrip_via_real_driver(self) -> None:
        host = TestStreaming._live_host()
        try:
            def build(b: GraphBuilder) -> None:
                b.task("t", fn=lambda x: {})

            graph = ReactiveGraph.build(build, host=host)
            ns = ["users", "alice"]
            graph.store_put(ns, "prefs", {"theme": "dark"})
            assert graph.store_get(ns, "prefs") == {"theme": "dark"}
            hits = graph.store_search(["users"], filter_={"theme": "dark"})
            assert any(h["key"] == "prefs" for h in hits)
            assert any("/".join(ns) == "users/alice" for ns in graph.list_namespaces())
            graph.store_delete(ns, "prefs")
            assert graph.store_get(ns, "prefs") is None
        finally:
            host.close()

    def test_get_state_and_checkpoints_via_real_driver(self) -> None:
        host = TestStreaming._live_host()
        try:
            def build(b: GraphBuilder) -> None:
                b.task("t", fn=lambda x: {"n": x["n"] + 1}).on("run", "t")

            graph = ReactiveGraph.build(build, host=host, graph_id="g_persist")
            graph.invoke("run", {"n": 1})
            assert graph.get_state()["n"] == 2
            # checkpoint list exists after a run (memory saver: run checkpoints)
            cps = graph.list_checkpoints()
            assert isinstance(cps, list)
        finally:
            host.close()

    def test_persistence_accessors_require_host(self) -> None:
        def build(b: GraphBuilder) -> None:
            b.task("t", fn=lambda x: {})

        graph = ReactiveGraph.build(build)
        for fn in (
            lambda: graph.get_state(),
            lambda: graph.store_put(["a"], "k", 1),
            lambda: graph.list_namespaces(),
        ):
            with pytest.raises(GraphBuildError, match="DriverHost"):
                fn()


class TestScopeIsolation:
    """scope: named sub-state namespaces enforced by the Driver scheduler."""

    def test_scoped_tasks_isolate_state_via_real_driver(self) -> None:
        host = TestStreaming._live_host()
        try:
            def build(b: GraphBuilder) -> None:
                b.task("a", fn=lambda x: {"n": 1}, on=("run",), scope="s1")
                b.task("b", fn=lambda x: {"n": 2}, on=("run",), scope="s2")

            graph = ReactiveGraph.build(build, host=host, graph_id="g_scope")
            graph.invoke("run", {})
            state = graph.get_state()
            assert state["s1"]["n"] == 1
            assert state["s2"]["n"] == 2
        finally:
            host.close()
