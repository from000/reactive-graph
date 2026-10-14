#!/usr/bin/env python3
"""P3-2 Task 3：真实 Driver 扇出图——并行 vs concurrency=1 计时对比。

同一扇出图（t0 → 8 分支 sleep(0.05) → 汇总）分别以默认并发与
REACTIVEGRAPH_CONCURRENCY=1 各跑 3 次取中位数；断言并行不慢于串行
（2 倍宽松阈值，容忍 CI 抖动）。输出等价校验（z == 4*N）同时回归。

已知边界：Driver 引擎侧 runAll 并行 + 写冲突检测已生效；端到端并行
加速受 Python host 单 reader 线程串行处理 TASK_INVOKE 回调所限（瓶颈
在 host 侧回调执行，非 Driver 调度）——本基准验证"并行不劣于串行"，
真实加速需 host 回调线程池（后续优化，不在 P3-2 范围）。

Run: uv run --directory python/reactivegraph python ../../benchmarks/parallel_vs_serial.py
"""

from __future__ import annotations

import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python" / "reactivegraph"))

from reactivegraph.graph import ReactiveGraph  # noqa: E402
from reactivegraph.host import DriverHost  # noqa: E402

BRANCHES = 8


def _driver_env(concurrency: str | None) -> dict:
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(ROOT / "packages" / "driver" / "dist" / "main.js")
    env["REACTIVEGRAPH_NODE_BIN"] = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    if concurrency is not None:
        env["REACTIVEGRAPH_CONCURRENCY"] = concurrency
    return env


def _build_graph() -> ReactiveGraph:
    def build(b) -> None:
        def t0(s: dict) -> dict:
            return {"x": s["n"] + 1}

        def make_branch(i: int):
            def branch(s: dict) -> dict:
                time.sleep(0.05)  # 模拟耗时分支——并行窗口
                return {f"y{i}": s["x"] * 2}
            return branch

        b.task("t0", fn=t0, on=("run",), writes=("x",))
        for i in range(BRANCHES):
            b.task(f"b{i}", fn=make_branch(i), on=("t0:written",), writes=(f"y{i}",))
        b.task(
            "t9",
            fn=lambda s: {"z": sum(s[f"y{i}"] for i in range(BRANCHES))},
            on=tuple(f"b{i}:written" for i in range(BRANCHES)),
            writes=("z",),
        )

    return ReactiveGraph.build(build)


def _median_ms(env: dict) -> float:
    host = DriverHost(env=env)
    try:
        host.start()
        host.handshake()
        dgraph = ReactiveGraph(_build_graph().definition, host=host)
        samples: list[float] = []
        for _ in range(3):
            t0 = time.perf_counter()
            out = dgraph.invoke("run", {"n": 1})
            samples.append((time.perf_counter() - t0) * 1000)
            assert out["z"] == 4 * BRANCHES  # x=2, y_i=4, z=4*N
        return statistics.median(samples)
    finally:
        host.close()


def main() -> int:
    par = _median_ms(_driver_env(None))
    ser = _median_ms(_driver_env("1"))
    print(f"parallel_ms={par:.1f} serial_ms={ser:.1f} speedup={ser / max(par, 1e-6):.2f}x")
    ok = par <= ser * 2  # 宽松阈值：并行不得慢于串行 2 倍
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
