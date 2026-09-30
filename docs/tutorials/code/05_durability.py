"""05 · 持久化 —— checkpoint 与 long-term store 原生入口(经 DriverHost)。

store_* 跨线程/跨进程共享(long-term store);get_state 反映 thread 当前状态。
运行前提同 04(已构建 Driver)。
"""

import os
import shutil
from pathlib import Path

from reactivegraph import GraphBuilder, ReactiveGraph
from reactivegraph.host import DriverHost

REPO_ROOT = Path(__file__).resolve().parents[3]


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
    b.task("t", fn=lambda x: {}).on("run", "t")


h = live_host()
try:
    g = ReactiveGraph.build(build, host=h, graph_id="g_persist")
    g.invoke("run", {})

    state = g.get_state()  # 默认 thread
    print("thread state:", state)
    assert state == {}

    # long-term store:namespace 前缀搜索
    g.store_put(["users", "alice"], "prefs", {"theme": "dark"})
    assert g.store_get(["users", "alice"], "prefs") == {"theme": "dark"}
    hits = g.store_search(["users"], filter_={"theme": "dark"})
    assert any(h["key"] == "prefs" for h in hits)
    assert any("/".join(ns) == "users/alice" for ns in g.list_namespaces())
    print("OK: checkpoint 可见 + store 读写/搜索/命名空间枚举通过。")
finally:
    h.close()