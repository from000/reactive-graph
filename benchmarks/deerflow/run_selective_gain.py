"""DeerFlow 场景的选择性执行收益量化（选择性执行 vs 全量重跑）。

场景：链路 1 lead_agent 图（planner→executor→verifier，3 个 pure 任务），
host 模式（真实 Node Driver，RGP/1）：

- `same_input`（gain）：同输入多轮——首次执行 3 任务，跨 run 指纹跳过后续
  （langgraph Pregel 每次 invoke 全量重跑 = 3×轮数次）；
- `varying_input`（no-gain）：每轮不同输入——全部执行（无 skip，诚实报告
  no-gain，不 cherry-pick）。

指标：任务执行数（决定性）+ wall-clock median/p95（参考，RGP/1 IPC 主导）。

运行（differential discipline：--repeats ≥ 3 取 median of medians）：
    uv run --directory python/reactivechain \
      python $PWD/benchmarks/deerflow/run_selective_gain.py --rounds 50 --repeats 3
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))  # import deerflow_port（脚本非安装包）

from deerflow_port.lead_agent import build_lead_agent  # noqa: E402
from reactivegraph import DriverHost  # noqa: E402


def _env() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "node": shutil.which("node") or "?",
        "os": f"{platform.system()}/{platform.machine()}",
    }


def _median(samples: list[float]) -> float:
    return statistics.median(samples)


def _p95(samples: list[float]) -> float:
    ordered = sorted(samples)
    return ordered[int(0.95 * (len(ordered) - 1))]


def measure(host: DriverHost, inputs: list[dict], rounds: int, graph_id: str,
            warmup_input: dict | None = None) -> dict:
    """跑 rounds 轮（warmup 1），返回任务执行数与耗时样本。"""
    calls: dict[str, int] = {}
    g = build_lead_agent(host, calls=calls, graph_id=graph_id)
    wi = warmup_input if warmup_input is not None else inputs[0]
    g.invoke("run", wi)  # warmup（不采样；同输入场景在此完成首次执行）
    warmup_executed = sum(calls.values())
    calls.clear()
    samples: list[float] = []
    for r in range(rounds):
        t0 = time.perf_counter()
        g.invoke("run", inputs[r % len(inputs)])
        samples.append((time.perf_counter() - t0) * 1000)
    executed = sum(calls.values())
    return {"executed": executed, "samples_ms": samples,
            "warmup_executed": warmup_executed}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rounds", type=int, default=50)
    ap.add_argument("--repeats", type=int, default=3, help="differential discipline ≥3")
    args = ap.parse_args()

    rounds_n, reps = args.rounds, args.repeats
    n_tasks = 3  # planner + executor + verifier
    host = DriverHost(env={})  # memory 后端：聚焦调度收益，排除磁盘噪声
    host.start()
    host.handshake()
    try:
        same_q = {"query": "quantify selective execution"}
        same = [measure(host, [same_q], rounds_n, f"gain_q_{i}",
                        warmup_input=same_q) for i in range(reps)]
        # varying：warmup 用独立输入，避免与首个采样轮同输入误 skip
        varying = [
            measure(
                host,
                [{"query": f"input number {i}"} for i in range(rounds_n)],
                rounds_n,
                f"nogain_q_{i}",
                warmup_input={"query": "warmup probe"},
            )
            for i in range(reps)
        ]
    finally:
        host.close()

    def summarize(runs: list[dict]) -> dict:
        med = [_median(r["samples_ms"]) for r in runs]
        p95 = [_p95(r["samples_ms"]) for r in runs]
        return {
            "repeats": len(runs),
            "rounds": rounds_n,
            "tasks_executed_total": sum(r["executed"] for r in runs),
            "tasks_per_round": [r["executed"] for r in runs],
            "warmup_executed_total": sum(r["warmup_executed"] for r in runs),
            "median_ms": med,
            "median_of_medians_ms": _median(med),
            "p95_ms": p95,
        }

    same_s = summarize(same)
    vary_s = summarize(varying)
    naive = reps * rounds_n * n_tasks
    same_s["naive_tasks_total"] = naive
    same_s["skip_rate"] = 1 - same_s["tasks_executed_total"] / naive
    vary_s["naive_tasks_total"] = naive
    vary_s["skip_rate"] = 1 - vary_s["tasks_executed_total"] / naive

    report = {
        "scenario": (
            "DeerFlow lead_agent (planner→executor→verifier, "
            "3 pure tasks, host Driver RGP/1)"
        ),
        "same_input_gain": same_s,
        "varying_input_nogain": vary_s,
        "env": _env(),
    }
    out_dir = _HERE / ".results"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "selective_gain.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False)
    )

    print("DeerFlow 选择性执行收益（lead_agent 3 任务，host Driver）")
    warmup_each = same_s["warmup_executed_total"] // reps
    print(
        f"  同输入 gain  : 采样 {rounds_n} 轮执行 "
        f"{same_s['tasks_executed_total']}/{same_s['naive_tasks_total']} 任务"
        f"（skip_rate={same_s['skip_rate']:.0%}；warmup 执行首次 {warmup_each} 任务/轮）"
        f" median={same_s['median_of_medians_ms']:.2f}ms（per-run）"
    )
    print(
        f"  变输入 no-gain: 执行 {vary_s['tasks_executed_total']}/"
        f"{vary_s['naive_tasks_total']} 任务"
        f"（skip_rate={vary_s['skip_rate']:.0%}）"
        f" median={vary_s['median_of_medians_ms']:.2f}ms（per-run）"
    )
    print(f"  环境: {report['env']}")
    print(f"  结果: {out_dir / 'selective_gain.json'}")


if __name__ == "__main__":
    main()
