"""P3-3 Task 1：fallback 执行的 trace 记录（附录 C 映射落地点）。

`graph._trace` 为执行期间追加的事件列表（{"event": ..., "task"?,
"computed_id"?}）：`task:{tid}:start` / `task:{tid}`（end）/
`cache_hit`（pure 指纹跳过）/ `computed:{id}:hit` / 段错误带 error 标记。
"""

from __future__ import annotations

import pytest

from reactivegraph.graph import GraphBuildError, ReactiveGraph
from reactivegraph.host import DriverError


def _events(g: ReactiveGraph) -> list[str]:
    return [t["event"] for t in g._trace]


def test_fallback_trace_start_end_events() -> None:
    """task start/end 与段执行一一对应。"""

    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        b.task("t0", fn=t0, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build)
    g.invoke("run", {"n": 1})
    events = _events(g)
    assert "task:t0:start" in events
    assert "task:t0" in events
    assert events.index("task:t0:start") < events.index("task:t0")


def test_fallback_trace_cache_hit_on_skip() -> None:
    """pure 段同输入第二次执行 → cache_hit 记录（append 到 trace）。"""

    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        b.task("t0", kind="pure", fn=t0, on=("run",), reads=("n",), writes=("x",))

    g = ReactiveGraph.build(build)
    g.invoke("run", {"n": 1})
    assert "cache_hit" not in _events(g)
    g.invoke("run", {"n": 1})  # 指纹命中 → skip
    assert "cache_hit" in _events(g)


def test_fallback_trace_computed_hit() -> None:
    """computed 求值 → computed:{id}:hit 记录。"""

    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        b.task("t0", fn=t0, on=("run",), writes=("x",))
        b.computed("double", selector=lambda s: s["x"] * 2, reads=("x",))

    g = ReactiveGraph.build(build)
    out = g.invoke("run", {"n": 1})
    events = _events(g)
    assert "computed:double:hit" in events
    assert out["double"] == 4


def test_fallback_trace_error_marked() -> None:
    """段抛错 → invoke 抛错且 trace 记录带 error 标记（异常安全、不阻断）。"""

    def build(b) -> None:
        def boom(s: dict) -> dict:
            raise RuntimeError("boom")

        b.task("t0", fn=boom, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build)
    with pytest.raises(RuntimeError, match="boom"):
        g.invoke("run", {"n": 1})
    end = [t for t in g._trace if t["event"] == "task:t0"]
    assert end and end[-1].get("error") is True


# -- P3-3 Task 3：Driver 模式 span trace 桥接（复用 run_stream custom 通道） --

import os  # noqa: E402
import shutil  # noqa: E402
from pathlib import Path  # noqa: E402

from reactivegraph.host import DriverHost  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def driver_host(tmp_path: Path):
    node = shutil.which("node")
    dist = _REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    assert node is not None  # guarded by skip above
    env["REACTIVEGRAPH_NODE_BIN"] = node
    host = DriverHost(env=env)
    host.start()
    host.handshake()
    try:
        yield host
    finally:
        host.close()


def test_driver_stream_produces_span_trace(driver_host) -> None:
    """Driver 模式 stream 后 Python 侧拿到 span 级 trace（task:{tid}:start）。"""

    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        b.task("t0", fn=t0, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build, host=driver_host)
    frames = list(g.stream("run", {"n": 1}, trace=True))
    assert len(frames) >= 1  # 至少 values 帧（host 分支无 terminal 收尾帧）
    events = [t["event"] for t in g._trace]
    assert "task:t0:start" in events
    assert "task:t0" in events
    assert events.index("task:t0:start") < events.index("task:t0")
    # custom span 帧仍透传给调用方（不吞帧）
    assert any(f.get("eventType") == "custom" for f in frames)


def test_driver_stream_default_no_custom_frames(driver_host) -> None:
    """默认（trace=False）不产生 custom span 帧——既有帧流形状不变。"""

    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        b.task("t0", fn=t0, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build, host=driver_host)
    frames = list(g.stream("run", {"n": 1}))
    assert not any(f.get("eventType") == "custom" for f in frames)
    assert g._trace == []  # 未开 trace → 无 span 记录


def test_driver_stream_span_trace_error_marked(driver_host) -> None:
    """Driver 段抛错：stream 上抛前 _trace 记录带 error 标记的 task end。"""

    def build(b) -> None:
        def boom(s: dict) -> dict:
            raise RuntimeError("boom")

        b.task("t0", fn=boom, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build, host=driver_host)
    with pytest.raises(Exception):  # noqa: B017 - Driver 错误帧透传
        list(g.stream("run", {"n": 1}, trace=True))
    end = [t for t in g._trace if t["event"] == "task:t0"]
    assert end and end[-1].get("error") is True


def test_explain_run_and_why_skipped() -> None:
    calls: list[int] = []

    def build(b) -> None:
        def pure(s: dict) -> dict:
            calls.append(1)
            return {"x": s["n"] + 1}

        b.task("pure", kind="pure", fn=pure, on=("run",), reads=("n",), writes=("x",))

    g = ReactiveGraph.build(build)
    g.invoke("run", {"n": 1})
    g.invoke("run", {"n": 1})

    explanation = g.explain_run()
    # fallback emits both task execution and skip; Driver trace reflects latest run only
    assert explanation["executed"] in {0, 1}
    assert explanation["skipped"] == 1
    assert explanation["cache_hits"] == 1
    decision = g.why_skipped("pure")
    assert decision["task"] == "pure"
    assert decision["reason"] == "fingerprint_unchanged"
    assert g.cost_saved()["skipped"] == 1
    assert calls == [1]


def test_driver_explain_run_after_trace_stream(driver_host) -> None:
    calls: list[int] = []

    def build(b) -> None:
        def pure(s: dict) -> dict:
            calls.append(1)
            return {"x": s["n"] + 1}

        b.task("pure", kind="pure", fn=pure, on=("run",), reads=("n",), writes=("x",))

    g = ReactiveGraph.build(build, host=driver_host)
    list(g.stream("run", {"n": 1}, thread_id="explain-1", trace=True))
    frames = list(g.stream("run", {"n": 1}, thread_id="explain-1", trace=True))
    explanation = g.explain_run()
    assert explanation["executed"] == 0  # latest run skipped the only task
    assert explanation["skipped"] == 1
    assert explanation["errors"] == 0
    assert g.why_skipped("pure")["reason"] == "fingerprint_unchanged"
    assert g.cost_saved()["skipped"] == 1
    assert calls == [1]
    assert any(frame.get("eventType") == "values" for frame in frames)


def test_why_invalidated_for_computed_read_set() -> None:
    calls: list[int] = []

    def build(b) -> None:
        def set_a(s: dict) -> dict:
            calls.append(1)
            return {"a": s["next_a"]}

        b.task("set_a", fn=set_a, on=("run",), writes=("a",))
        b.computed("double", selector=lambda s: s["a"] * 2, reads=("a",))

    g = ReactiveGraph.build(build)
    g.invoke("run", {"a": 1, "next_a": 2})
    g.invoke("run", {"a": 1, "next_a": 3})

    decision = g.why_invalidated("double")
    assert decision == {
        "selector": "double",
        "kind": "computed",
        "reason": "read_set_changed",
        "reads": ["a"],
    }
    assert calls == [1, 1]


def test_driver_computed_invalidation_explanation(driver_host) -> None:
    """RGP/1 wire computeds: the Driver emits `span:invalidate` frames whose
    `reads` attribute feeds `why_invalidated()` — the same explainability
    contract as the fallback kernel, now over the Driver boundary."""

    def build(b) -> None:
        b.task("set_a", fn=lambda s: {"a": s["next_a"]}, on=("run",), writes=("a",))
        b.computed("double", selector=lambda s: s["a"] * 2, reads=("a",))

    g = ReactiveGraph.build(build, host=driver_host)
    frames = list(g.stream("run", {"a": 1, "next_a": 2}, thread_id="invalidate-1", trace=True))
    assert any(frame.get("eventType") == "values" for frame in frames)
    assert g.why_invalidated("double") == {
        "selector": "double",
        "kind": "computed",
        "reason": "read_set_changed",
        "reads": ["a"],
    }


def test_driver_computed_execution_and_invalidation(driver_host) -> None:
    calls: list[int] = []

    def build(b) -> None:
        def set_a(s: dict) -> dict:
            calls.append(1)
            return {"a": s["next_a"]}

        b.task("set_a", fn=set_a, on=("run",), writes=("a",))
        b.computed("double", selector=lambda s: s["a"] * 2, reads=("a",))

    g = ReactiveGraph.build(build, host=driver_host)
    out1 = g.invoke("run", {"a": 1, "next_a": 2})
    out2 = g.invoke("run", {"a": 1, "next_a": 3})
    assert out1["double"] == 4
    assert out2["double"] == 6
    assert calls == [1, 1]
    list(g.stream("run", {"a": 1, "next_a": 4}, thread_id="computed-wire", trace=True))
    assert g.why_invalidated("double") == {
        "selector": "double",
        "kind": "computed",
        "reason": "read_set_changed",
        "reads": ["a"],
    }


def test_driver_declared_write_conflict_explanation(driver_host) -> None:
    """P4.1：声明写冲突 → explain_conflict 返回 loser/winner/path/write sets。"""

    def build(b) -> None:
        b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
        b.task("a", fn=lambda s: {"w": 1}, on=("t0:written",), writes=("w",))
        b.task("b", fn=lambda s: {"w": 2}, on=("t0:written",), writes=("w",))

    g = ReactiveGraph.build(build, host=driver_host)
    try:
        list(g.stream("run", {"n": 1}, trace=True))
    except DriverError:
        pass
    else:
        pytest.fail("expected WriteConflictError")

    assert g.explain_conflict() == {
        "kind": "write_conflict",
        "reason": "declared_write_conflict",
        "task": "b",
        "winner": "a",
        "path": "w",
        "declared_writes": {"a": ["w"], "b": ["w"]},
        "actual_writes": {"a": ["w"]},
    }


def test_driver_undeclared_write_conflict_explanation(driver_host) -> None:
    """P4.1：欠声明实际写冲突 → reason 区分 actual_write_conflict。"""

    def build(b) -> None:
        b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
        b.task("a", fn=lambda s: {"w": 1}, on=("t0:written",), writes=("a",))
        b.task("b", fn=lambda s: {"w": 2}, on=("t0:written",), writes=("b",))

    g = ReactiveGraph.build(build, host=driver_host)
    try:
        list(g.stream("run", {"n": 1}, trace=True))
    except DriverError:
        pass
    else:
        pytest.fail("expected WriteConflictError")

    assert g.explain_conflict() == {
        "kind": "write_conflict",
        "reason": "actual_write_conflict",
        "task": "b",
        "winner": "a",
        "path": "w",
        "declared_writes": {"t0": ["x"], "a": ["a"], "b": ["b"]},
        "actual_writes": {"a": ["w"], "b": ["w"]},
    }


def test_driver_historical_explain_run_after_restart(tmp_path) -> None:
    """P4.2：durable log 支持重启后按 runId 查询历史执行解释。"""
    import os
    import shutil

    db_path = tmp_path / "history.db"
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(_REPO_ROOT / "packages" / "driver" / "dist" / "main.js")
    env["REACTIVEGRAPH_NODE_BIN"] = shutil.which("node") or "node"
    env["REACTIVEGRAPH_DB"] = str(db_path)

    def build(b) -> None:
        b.task(
            "pure",
            kind="pure",
            fn=lambda s: {"x": s["n"] + 1},
            on=("run",),
            reads=("n",),
            writes=("x",),
        )

    run_id = "history-run-1"
    host = DriverHost(env=env)
    host.start()
    host.handshake()
    try:
        g = ReactiveGraph.build(build, host=host, graph_id="history-graph")
        result = g.stream("run", {"n": 1}, run_id=run_id, trace=True)
        list(result)
    finally:
        host.close()

    host2 = DriverHost(env=env)
    host2.start()
    host2.handshake()
    try:
        g2 = ReactiveGraph.build(build, host=host2, graph_id="history-graph")
        explanation = g2.explain_run(run_id=run_id)
        assert explanation["runId"] == run_id
        assert explanation["executed"] == 1
        assert explanation["events"]
        assert explanation["decisionsPersisted"] is False
    finally:
        host2.close()


def test_driver_historical_explain_unknown_run_errors(driver_host) -> None:
    """P4.2：历史 run 查询是确定错误，不伪造空解释。"""
    g = ReactiveGraph.build(lambda b: b.task("t", fn=lambda s: {}, on=("run",)), host=driver_host)
    with pytest.raises(DriverError, match="historical explain requires a durable log"):
        g.explain_run(run_id="missing-run")


def test_export_causal_trace_json_and_dot(driver_host) -> None:
    """P4.3：最近 run 的 causal trace 支持 JSON/DOT 导出。"""

    def build(b) -> None:
        b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
        b.computed("double", selector=lambda s: s["x"] * 2, reads=("x",))

    g = ReactiveGraph.build(build, host=driver_host)
    list(g.stream("run", {"n": 1}, trace=True))

    js = g.export_trace()
    assert js["protocol"] == "reactivegraph.causal-trace.v1"
    assert js["runId"]
    kinds = [event["kind"] for event in js["events"]]
    assert "span_start" in kinds
    assert "span_end" in kinds

    dot = g.export_trace(format="dot")
    assert dot.startswith("digraph")
    assert "digraph" in dot
    assert "t0" in dot


def test_export_causal_trace_fallback_json_and_dot() -> None:
    """P4.3：fallback 与 Driver 导出同一 causal-trace v1 形状。"""

    def build(b) -> None:
        b.task("t0", fn=lambda s: {"x": s["n"] + 1}, on=("run",), writes=("x",))

    g = ReactiveGraph.build(build)
    g.invoke("run", {"n": 1})
    js = g.export_trace()
    assert js["protocol"] == "reactivegraph.causal-trace.v1"
    assert js["runId"] == "latest"
    assert js["events"]
    dot = g.export_trace(format="dot")
    assert dot.startswith("digraph")
    assert 'digraph "latest"' in dot
    assert "->" in dot


def test_export_causal_trace_rejects_unknown_format() -> None:
    g = ReactiveGraph.build(lambda b: b.task("t", fn=lambda s: {}, on=("run",)))
    g.invoke("run", {})
    with pytest.raises(GraphBuildError, match="unsupported trace format"):
        g.export_trace(format="yaml")


def test_cost_saved_reports_declared_task_estimates() -> None:
    """P4.4：任务级 cost 元数据把跳过数升级为 token/usd 估算。"""
    calls: list[int] = []

    def llm(s: dict) -> dict:
        calls.append(1)
        return {"x": s["n"] + 1}

    def build(b) -> None:
        b.task(
            "llm",
            kind="pure",
            fn=llm,
            on=("run",),
            reads=("n",),
            writes=("x",),
            estimated_tokens_in=100,
            estimated_tokens_out=50,
            estimated_usd=0.02,
        )

    g = ReactiveGraph.build(build)
    g.invoke("run", {"n": 1})
    g.invoke("run", {"n": 1})
    report = g.cost_saved()
    assert report == {
        "executions": 1,
        "skipped": 1,
        "llm_calls_saved": 1,
        "tools_saved": 0,
        "tokens_in_saved": 100,
        "tokens_out_saved": 50,
        "estimated_usd_saved": 0.02,
        "cost_known": True,
    }
    assert calls == [1]


def test_cost_saved_unknown_cost_is_explicit() -> None:
    def build(b) -> None:
        b.task("pure", kind="pure", fn=lambda s: {"x": 1}, on=("run",), reads=("n",))

    g = ReactiveGraph.build(build)
    g.invoke("run", {"n": 1})
    g.invoke("run", {"n": 1})
    report = g.cost_saved()
    assert report == {
        "executions": 1,
        "skipped": 1,
        "llm_calls_saved": 0,
        "tools_saved": 0,
        "tokens_in_saved": 0,
        "tokens_out_saved": 0,
        "estimated_usd_saved": 0.0,
        "cost_known": False,
    }


def test_driver_field_permission_rejects_tool_patch_before_commit(driver_host) -> None:
    """P5.4：工具 patch 在 Driver 提交点受字段权限拒绝。"""

    def build(b) -> None:
        b.task("write_secret", fn=lambda s: {"secret": 1}, on=("run",), writes=("secret",))

    g = ReactiveGraph.build(build, host=driver_host)
    with pytest.raises(DriverError, match="permission denied"):
        g.invoke("run", {}, config={"allowWritePaths": ["public"]})


def test_driver_budget_policy_rejects_before_side_effect(driver_host) -> None:
    calls: list[int] = []

    def build(b) -> None:
        b.task("spend", fn=lambda s: calls.append(1) or {"n": 1}, on=("run",), writes=("n",))

    g = ReactiveGraph.build(build, host=driver_host)
    with pytest.raises(DriverError, match="policy denied"):
        g.invoke("run", {}, config={"budgetLimit": 0})
    assert calls == []


def test_driver_run_policy_configuration_is_not_leaked_into_trace(driver_host) -> None:
    """P5.4：策略配置本身不出现在 trace/schema，不泄漏运行时内部细节。"""

    def build(b) -> None:
        b.task("t", fn=lambda s: {"ok": 1}, on=("run",), writes=("ok",))

    g = ReactiveGraph.build(build, host=driver_host)
    list(g.stream("run", {}, trace=True, config={"allowWritePaths": ["ok"], "budgetLimit": 1}))
    exported = g.export_trace()
    assert "allowWritePaths" not in repr(exported)
    assert "budgetLimit" not in repr(exported)
