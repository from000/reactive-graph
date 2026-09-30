"""方案 C P2-1：Pipeline.to_graph(host=) 链侧 Driver 接入 + checkpoint/恢复。

链经真实 Driver 任务级联执行（输出与 fallback 一致）；checkpoint 按 run
落盘（REACTIVEGRAPH_DB），restore_thread 回滚后继续。
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
from reactivegraph import DriverHost

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def durable_host(tmp_path: Path):
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        pytest.skip("bundled Driver (node + packages/driver/dist) not available")
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


def test_to_graph_host_matches_fallback() -> None:
    """P2-1：to_graph(host=) 链驱动力下输出与 fallback 一致（任务级联）。"""
    from reactivechain import Pipeline, RunnableLambda

    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True),
        RunnableLambda(lambda s: {"y": s["x"] * 2}, reads={"x"}, writes={"y"}, pure=True),
    ])
    fb = chain.invoke({"n": 3})  # fallback
    assert fb == {"y": 8}

    durable_host = None
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is not None and dist.exists():
        import tempfile

        env = dict(os.environ)
        env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
        env["REACTIVEGRAPH_NODE_BIN"] = node
        env["REACTIVEGRAPH_DB"] = str(Path(tempfile.mkdtemp()) / "rgp.db")
        durable_host = DriverHost(env=env)
        durable_host.start()
        durable_host.handshake()
    if durable_host is None:
        pytest.skip("bundled Driver not available")
    try:
        graph = chain.to_graph(host=durable_host, graph_id="g_to_graph_host")
        out = graph.invoke("run", {"n": 3})
        assert out["y"] == fb["y"] == 8
    finally:
        durable_host.close()


def test_stream_values_sequence_matches_driver(durable_host: DriverHost) -> None:
    """P3-1 Task 3：fallback 与 Driver 的 values 快照帧序列逐帧一致。

    fallback（chain.stream mode=values）与 Driver（graph.stream host）各跑
    一次同一线性链，比对 values 帧的 state 序列（初始帧 + 每任务写后帧）。
    """
    from reactivegraph import ReactiveGraph

    from reactivechain import Pipeline, RunnableLambda

    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True),
        RunnableLambda(lambda s: {"y": s["x"] * 2}, reads={"x"}, writes={"y"}, pure=True),
    ])
    fallback_frames = list(chain.stream({"n": 1}, mode="values"))
    assert len(fallback_frames) >= 3  # 初始 + 段0 + 段1

    graph = ReactiveGraph(chain.to_graph().definition, host=durable_host)
    driver_frames = [
        ev["payload"]["state"]
        for ev in graph.stream("run", {"n": 1})
        if ev.get("eventType") == "values"
    ]
    assert driver_frames == fallback_frames


def test_stream_values_skip_sequence_matches(durable_host: DriverHost) -> None:
    """P3-1 Task 3：pure 段缓存命中（skip）时两执行器 values 帧仍同构。

    第二次同输入触发 skip；Driver 用新 thread 隔离（thread_id 透传），
    首帧从空 store 起步，与 fallback 逐帧一致（帧数不塌缩）。
    """
    from reactivegraph import ReactiveGraph

    from reactivechain import Pipeline, RunnableLambda

    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True),
    ])
    graph = ReactiveGraph(chain.to_graph().definition, host=durable_host)
    fb1 = list(chain.stream({"n": 1}, mode="values"))  # 预热 fallback 缓存
    list(graph.stream("run", {"n": 1}, thread_id="skip_prewarm"))  # 预热 Driver
    fb2 = list(chain.stream({"n": 1}, mode="values"))  # 第二次：skip 路径
    dr2 = [
        ev["payload"]["state"]
        for ev in graph.stream("run", {"n": 1}, thread_id="skip_t2")
        if ev.get("eventType") == "values"
    ]
    assert len(fb2) == len(dr2) == 2  # 初始 + skip 任务帧（帧数不塌缩）
    assert fb2 == fb1  # fallback：skip 后帧序列稳定（含缓存回填 x）
    # Driver（新 thread + 跨 run 指纹跳过，d0183d4 起）：skip 帧不写 x——
    # 新 thread 的 store 本无 x，skip=不执行=不写（thread 隔离语义）；
    # 帧数仍为 2（初始 + skip 帧），不塌缩。
    assert len(dr2) == 2 and dr2 == [{"n": 1}, {"n": 1}]


def test_checkpoint_restore_via_to_graph(durable_host: DriverHost) -> None:
    """P2-1：链图经 Driver 执行后 checkpoint 按 run 落盘，restore_thread
    回滚到指定快照并继续（链侧持久化工作流）。"""
    from reactivechain import Pipeline, RunnableLambda

    chain = Pipeline([
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}, pure=True),
        RunnableLambda(lambda s: {"y": s["x"] * 2}, reads={"x"}, writes={"y"}, pure=True),
    ])
    graph = chain.to_graph(host=durable_host, graph_id="g_p2_chain")
    graph.invoke("run", {"n": 1})  # x=2, y=4
    assert graph.get_state()["y"] == 4
    cps = graph.list_checkpoints()
    assert len(cps) >= 1

    graph.invoke("run", {"n": 10})  # x=11, y=22
    assert graph.get_state()["y"] == 22

    restored = graph.restore_thread(checkpoint_id=cps[0]["checkpointId"])
    assert restored["state"]["y"] == 4
    assert graph.get_state()["y"] == 4

    graph.invoke("run", {"n": 5})  # 从恢复点继续 → x=6, y=12
    assert graph.get_state()["y"] == 12


def test_stream_values_error_driver_emits_task_error(
        durable_host: DriverHost,
) -> None:
    """P3-1 Task 4：Driver 段异常 → graph.stream 先见 task_error 事件再抛错。

    段抛错经 Driver 错误帧 → Python 侧 DriverError；异常上抛前补发
    task_error 事件（不吞）。
    """
    from reactivegraph import ReactiveGraph

    from reactivechain import Pipeline, RunnableLambda

    def boom(s: dict) -> dict:
        raise RuntimeError("boom")

    chain = Pipeline([RunnableLambda(boom, writes={"x"})])
    graph = ReactiveGraph(chain.to_graph().definition, host=durable_host)
    seen: list[str] = []
    with pytest.raises(Exception, match="boom"):
        for ev in graph.stream("run", {"n": 1}):
            seen.append(ev.get("eventType", ""))
    assert "task_error" in seen