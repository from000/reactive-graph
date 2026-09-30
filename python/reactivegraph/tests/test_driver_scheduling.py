"""P1-3（方案 C，E4 修订）：链编译产物在真实 Driver 下的端到端验证。

依赖 packages/driver/dist/main.js（仓库已构建，`pnpm --filter
@reactivegraph/driver build`）+ Node。与 test_driver_lifecycle.py 的
real_driver_env 用法一致。

引擎升级（E2，runtime.ts run/resume 改造）后的语义：
- **任务级联**：每任务完成后，订阅其 `{tid}:written` 的任务入队执行
  （广度优先，`queued` 去重终止环）——订阅链图在 Driver 与 fallback
  行为一致
- **任务输入 = run 载荷 merge 线程 store 累积**——链式管道读上游写
  正常（多段链 Driver 输出与 fallback 一致）
- 剩余能力边界：无持久化时选择性（pure 指纹/effect 幂等 receipts）
  不跨 run 生效（Scheduler 每次 run 重建）——P2 接入持久化后兑现
"""

from __future__ import annotations

import os
from pathlib import Path

from reactivegraph.graph import ReactiveGraph
from reactivegraph.host import DriverError, DriverHost

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[3]


def _real_driver_env() -> dict:
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(REPO_ROOT / "packages" / "driver" / "dist" / "main.js")
    env["REACTIVEGRAPH_NODE_BIN"] = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    return env


def test_driver_single_task_matches_fallback() -> None:
    """单任务图 Driver 输出与 fallback 一致（to_graph 产物双执行器通用）。"""
    calls: list[int] = []

    def compute(state: dict) -> dict:
        calls.append(1)
        return {"v": state["n"] * 3}

    def build(b) -> None:
        b.task("t", fn=compute, on=("run",), reads=("n",), writes=("v",))

    graph = ReactiveGraph.build(build)
    fb = graph.invoke("run", {"n": 4})

    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(graph.definition, host=host)
        out = dgraph.invoke("run", {"n": 4})
        assert out["v"] == fb["v"] == 12
    finally:
        host.close()


def test_driver_chain_reads_upstream_writes() -> None:
    """E4：多段链 Driver 任务输入 = run 载荷 merge store 累积——第 2 段
    读到第 1 段写，输出与 fallback 一致（任务级联输入构造升级生效）。"""
    seen: list[str] = []

    def t0(state: dict) -> dict:
        seen.append("t0")
        return {"x": state["n"] + 1}

    def t1(state: dict) -> dict:
        seen.append("t1")
        return {"y": state["x"] * 2}

    def build(b) -> None:
        b.task("t0", fn=t0, on=("run",), writes=("x",))
        b.task("t1", fn=t1, on=("run",), reads=("x",), writes=("y",))

    graph = ReactiveGraph.build(build)
    fb = graph.invoke("run", {"n": 4})
    assert fb == {"n": 4, "x": 5, "y": 10}
    seen.clear()  # fallback 执行也 append；Driver 段重新计数

    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(graph.definition, host=host)
        out = dgraph.invoke("run", {"n": 4})
        assert out["y"] == fb["y"] == 10
        assert seen == ["t0", "t1"]
    finally:
        host.close()


def test_driver_written_subscription_cascades() -> None:
    """E4：订阅链（on=("t0:written",)）在 Driver 级联执行，输出与 fallback
    一致（written 事件级联升级生效）。"""
    seen: list[str] = []

    def t0(state: dict) -> dict:
        seen.append("t0")
        return {"x": state["n"] + 1}

    def t1(state: dict) -> dict:
        seen.append("t1")
        return {"y": state["x"] + 1}

    def t2(state: dict) -> dict:
        seen.append("t2")
        return {"z": state["x"] * 2}

    def build(b) -> None:
        b.task("t0", fn=t0, on=("run",), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), writes=("y",))
        b.task("t2", fn=t2, on=("t0:written",), writes=("z",))

    graph = ReactiveGraph.build(build)
    fb = graph.invoke("run", {"n": 4})
    assert fb == {"n": 4, "x": 5, "y": 6, "z": 10}
    seen.clear()

    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(graph.definition, host=host)
        out = dgraph.invoke("run", {"n": 4})
        assert out["x"] == 5 and out["y"] == 6 and out["z"] == 10
        assert seen == ["t0", "t1", "t2"]
    finally:
        host.close()


def test_driver_subscription_cycle_terminates() -> None:
    """E4：订阅环（t0→t1→t0）Driver 级联 queued 去重优雅终止（不挂死、
    不抛错），与 fallback 的 seen 去重语义一致。"""
    seen: list[str] = []

    def t0(state: dict) -> dict:
        seen.append("t0")
        return {"x": state.get("x", 0) + 1}

    def t1(state: dict) -> dict:
        seen.append("t1")
        return {"y": state.get("y", 0) + 1}

    def build(b) -> None:
        b.task("t0", fn=t0, on=("run", "t1:written"), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), writes=("y",))

    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(ReactiveGraph.build(build).definition, host=host)
        out = dgraph.invoke("run", {})
        # 每个任务至多执行一次（queued 去重）→ x/y 各 1
        assert out["x"] == 1 and out["y"] == 1
        assert seen == ["t0", "t1"]
    finally:
        host.close()


def test_driver_fanout_batch_parallel_matches_fallback() -> None:
    """P3-2 Task 1：扇出图（t0 → t1/t2 → t3 汇总）Driver 与 fallback 一致。

    事件轮次批次改造后：t1/t2 同批 runAll 并行、t3 汇总——输出不因
    并行化改变（等价守护，改造前后均须绿）。
    """
    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        def t1(s: dict) -> dict:
            return {"y": s["x"] * 2}

        def t2(s: dict) -> dict:
            return {"z": s["x"] + 10}

        def t3(s: dict) -> dict:
            return {"w": s["y"] + s["z"]}

        b.task("t0", fn=t0, on=("run",), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), writes=("y",))
        b.task("t2", fn=t2, on=("t0:written",), writes=("z",))
        b.task("t3", fn=t3, on=("t1:written", "t2:written"), writes=("w",))

    graph = ReactiveGraph.build(build)
    fb = graph.invoke("run", {"n": 1})  # x=2, y=4, z=12, w=16

    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(graph.definition, host=host)
        out = dgraph.invoke("run", {"n": 1})
        assert out["w"] == fb["w"] == 16
    finally:
        host.close()


def test_driver_declared_write_conflict_raises() -> None:
    """P3-2 Task 2：声明写冲突（同批同 path 声明）→ WriteConflictError。

    t1/t2 都订阅 t0:written（同批并行）且都声明 writes=("w",)——
    runAll 的声明写冲突检测上抛，经 DriverError 携带到 Python。
    """
    import pytest

    def t1(state: dict) -> dict:
        return {"w": 1}

    def t2(state: dict) -> dict:
        return {"w": 2}

    def build(b) -> None:
        b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), writes=("w",))
        b.task("t2", fn=t2, on=("t0:written",), writes=("w",))

    graph = ReactiveGraph.build(build)
    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(graph.definition, host=host)
        with pytest.raises(DriverError, match="WriteConflict"):
            dgraph.invoke("run", {"n": 1})
    finally:
        host.close()


def test_driver_undeclared_write_conflict_raises() -> None:
    """P3-2 Task 2：欠声明实际写冲突 → WriteConflictError。

    t1/t2 声明 writes=("a",)/("b",) 但实际都写 w——runAll 以实际写集
    替换 claims 后检测到冲突，同样上抛。
    """
    import pytest

    def t1(state: dict) -> dict:
        return {"w": 1}

    def t2(state: dict) -> dict:
        return {"w": 2}

    def build(b) -> None:
        b.task("t0", fn=lambda s: {"x": 1}, on=("run",), writes=("x",))
        b.task("t1", fn=t1, on=("t0:written",), writes=("a",))
        b.task("t2", fn=t2, on=("t0:written",), writes=("b",))

    graph = ReactiveGraph.build(build)
    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(graph.definition, host=host)
        with pytest.raises(DriverError, match="WriteConflict"):
            dgraph.invoke("run", {"n": 1})
    finally:
        host.close()


def test_driver_pure_task_executes() -> None:
    """Driver 可执行 pure 任务且输出正确（无持久化的选择性语义 = 每次
    run 执行，见模块 docstring 剩余能力边界）。"""
    calls: list[int] = []

    def counted(state: dict) -> dict:
        calls.append(1)
        return {"v": state["k"] + 1}

    def build(b) -> None:
        b.task("t", kind="pure", fn=counted, on=("run",), reads=("k",), writes=("v",))

    host = DriverHost(env=_real_driver_env())
    try:
        host.start()
        host.handshake()
        graph = ReactiveGraph.build(build)
        dgraph = ReactiveGraph(graph.definition, host=host)
        out1 = dgraph.invoke("run", {"k": 1})
        out2 = dgraph.invoke("run", {"k": 2})
        assert out1["v"] == 2 and out2["v"] == 3
    finally:
        host.close()


def test_driver_concurrency1_matches_default() -> None:
    """P3-2 Task 3：同一扇出图 concurrency=1（env 串行兜底）输出等价。

    默认并行 vs REACTIVEGRAPH_CONCURRENCY=1 vs fallback——三路输出一致
    （并发/串行不改变最终 state，仅执行序差异）。
    """
    import time

    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        def make_branch(i: int):
            def branch(s: dict) -> dict:
                time.sleep(0.01)  # 拉长执行窗口，让并行/串行差异可观察
                return {f"y{i}": s["x"] * 2}
            return branch

        b.task("t0", fn=t0, on=("run",), writes=("x",))
        for i in range(4):
            b.task(f"b{i}", fn=make_branch(i), on=("t0:written",), writes=(f"y{i}",))
        b.task("t9", fn=lambda s: {"z": sum(s[f"y{i}"] for i in range(4))},
               on=tuple(f"b{i}:written" for i in range(4)), writes=("z",))

    graph = ReactiveGraph.build(build)
    fb = graph.invoke("run", {"n": 1})  # x=2, y_i=4, z=16

    # 默认（并行）
    host = DriverHost(env=_real_driver_env())
    # concurrency=1（串行兜底）
    env1 = _real_driver_env()
    env1["REACTIVEGRAPH_CONCURRENCY"] = "1"
    host1 = DriverHost(env=env1)
    try:
        host.start()
        host.handshake()
        out_par = ReactiveGraph(graph.definition, host=host).invoke("run", {"n": 1})
        host1.start()
        host1.handshake()
        out_ser = ReactiveGraph(graph.definition, host=host1).invoke("run", {"n": 1})
    finally:
        host.close()
        host1.close()
    assert out_par["z"] == out_ser["z"] == fb["z"] == 16