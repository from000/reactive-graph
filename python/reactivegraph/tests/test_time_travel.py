"""Time travel (replay/fork): restore_thread rolls a thread back to a
historical checkpoint and subsequent runs continue from it.

Requires the bundled Driver with a durable checkpoint backend
(REACTIVEGRAPH_DB), so it is skipped when node/dist are unavailable.
"""

import os
import shutil
from pathlib import Path

import pytest

from reactivegraph.graph import GraphBuilder, ReactiveGraph
from reactivegraph.host import DriverHost

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture()
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


def test_restore_thread_rolls_back_and_continues(durable_host: DriverHost) -> None:
    def build(b: GraphBuilder) -> None:
        b.task("inc", fn=lambda x: {"n": x["n"] + 1}).on("run", "inc")

    g = ReactiveGraph.build(build, host=durable_host, graph_id="g_tt")
    g.invoke("run", {"n": 0})  # n -> 1  (checkpoint 1)
    g.invoke("run", {"n": 2})  # n -> 3  (checkpoint 2)
    assert g.get_state()["n"] == 3

    cps = g.list_checkpoints()
    assert len(cps) >= 2

    restored = g.restore_thread(checkpoint_id=cps[0]["checkpointId"])
    assert restored["state"]["n"] == 1
    assert g.get_state()["n"] == 1

    g.invoke("run", {"n": 5})  # continues from the restored snapshot
    assert g.get_state()["n"] == 6


def test_restore_unknown_checkpoint_raises(durable_host: DriverHost) -> None:
    def build(b: GraphBuilder) -> None:
        b.task("inc", fn=lambda x: {"n": x["n"] + 1}).on("run", "inc")

    g = ReactiveGraph.build(build, host=durable_host, graph_id="g_tt2")
    g.invoke("run", {"n": 0})
    with pytest.raises(Exception, match="Hint:"):
        g.restore_thread(checkpoint_id="cp-999")
