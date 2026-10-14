"""06 · 第一个 1000 节点应用 —— fan-out 吞吐实战。

1000 个 pure 任务监听同一事件、并行写各自字段。展示:
- 构建 + 一次 invoke 的真实开销(本机 ~几十 ms 量级);
- 选择性执行发生在 Driver 调度层:同一 run 内相同输入的任务会被
  fingerprint 跳过(见 benchmarks/differential/harness.test.ts 的 wide 场景);
  computed 按读集失效只重算受影响派生值(见 03)。
运行前提同 04(已构建 Driver)。
"""

import os
import shutil
import time
from pathlib import Path

from reactivegraph import GraphBuilder, ReactiveGraph
from reactivegraph.host import DriverHost

REPO_ROOT = Path(__file__).resolve().parents[3]
N = 1000


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
    for i in range(N):
        def f(x: dict, _i: int = i) -> dict:
            return {f"v{_i}": x.get("value", 0) + _i}

        b.task(f"t{i}", kind="pure", fn=f).on("run", f"t{i}")


h = live_host()
try:
    t_build = time.perf_counter()
    g = ReactiveGraph.build(build, host=h, graph_id="g_1000")
    build_ms = (time.perf_counter() - t_build) * 1000

    t0 = time.perf_counter()
    out = g.invoke("run", {"value": 1})
    run_ms = (time.perf_counter() - t0) * 1000

    assert len(out) == N, f"expect {N} fields, got {len(out)}"
    assert out["v0"] == 1 and out["v999"] == 1000

    print(f"1000 节点:编译 {build_ms:.1f} ms | invoke {run_ms:.2f} ms | 字段 {len(out)}")
    print("OK: 事件一次路由 1000 个任务,状态合并完整。")
    print("选择性执行(指纹跳过/computed 失效)在 Driver 调度层,见差分基准与 03。")
finally:
    h.close()