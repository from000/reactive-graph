"""04 · 流式 —— DriverHost 下 stream() 逐事件产出,不再阻塞到结束。

运行前提:node 可用且已构建 Driver:
    pnpm --filter @reactivegraph/driver build
"""

import os
import shutil
from pathlib import Path

from reactivegraph import GraphBuilder, ReactiveGraph
from reactivegraph.host import DriverHost

REPO_ROOT = Path(__file__).resolve().parents[3]  # docs/tutorials/code -> repo root


def live_host() -> DriverHost:
    node = shutil.which("node")
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if node is None or not dist.exists():
        raise SystemExit("需要先构建 Driver: pnpm --filter @reactivegraph/driver build")
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(dist)
    env["REACTIVEGRAPH_NODE_BIN"] = node
    h = DriverHost(env=env)
    h.start()
    h.handshake()
    return h


def build(b: GraphBuilder) -> None:
    b.task("step", fn=lambda x: {"n": x["n"] + 1}).on("run", "step")


h = live_host()
try:
    g = ReactiveGraph.build(build, host=h)
    events = list(g.stream("run", {"n": 1}))
    values = [e for e in events if e.get("eventType") == "values"]
    print("stream 事件序列:", [e["eventType"] for e in events])
    print("final state:", values[-1]["payload"]["state"])
    assert values[-1]["payload"]["state"]["n"] == 2
    print("OK: stream() 边跑边出事件,state 收敛正确。")
finally:
    h.close()