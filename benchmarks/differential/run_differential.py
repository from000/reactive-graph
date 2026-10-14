#!/usr/bin/env python3
"""Differential benchmark orchestrator (plan §16 reproducibility).

Runs BOTH sides on this machine:
  1. reactive side: `vitest run benchmarks/differential/harness.test.ts` (native Driver)
  2. upstream side: `uv run ... langgraph_side.py` (real langgraph==1.2.11)
then asserts the two sides produce identical outputs for every workload
(projected to comparable fields) and prints the comparison table.

Usage:
    uv run --directory benchmarks/differential python -m run_differential  # noqa: E501
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RESULTS = HERE / ".results"

# (lg field, rg out index) per workload — the outputs must be equal after
# projection; this is the semantic-equivalence assertion.
PROJECTIONS: dict[str, list[tuple[str, int]]] = {
    "chat": [("x", 0), ("y", 1)],
    "wide": [("v0", 0), ("v999", 999)],
    "selective": [("v0", 0), ("v9", 9), ("v999", 999)],
    "nogain": [("v999", 999)],
}


def run_reactive() -> None:
    cmd = ["pnpm", "exec", "vitest", "run", "benchmarks/differential/harness.test.ts"]
    print(f"[reactive] {' '.join(cmd)}")
    subprocess.run(cmd, cwd=ROOT, check=True)


def run_langgraph() -> dict:
    cmd = ["uv", "run", "--directory", str(HERE), "python", "langgraph_side.py"]
    print(f"[langgraph] {' '.join(cmd)}")
    out = subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True)
    return json.loads(out.stdout)


def main() -> int:
    if not (RESULTS / "reactive.json").exists() or "--refresh" in sys.argv:
        run_reactive()
    reactive = json.loads((RESULTS / "reactive.json").read_text())
    langgraph = run_langgraph()

    failures: list[str] = []
    for wl, pairs in PROJECTIONS.items():
        rg_out = reactive[wl]["output"].get("out", {})
        for lg_field, rg_idx in pairs:
            lv = langgraph[wl]["output"].get(lg_field)
            rv = rg_out[rg_idx] if isinstance(rg_out, list) else rg_out.get(str(rg_idx))
            if lv != rv:
                failures.append(f"{wl}: langgraph.{lg_field}={lv} != reactive.out[{rg_idx}]={rv}")

    if failures:
        print("OUTPUT MISMATCH:", *failures, sep="\n  ")
        return 1

    print("\nDifferential benchmark (median of runs, same machine)")
    print(f"{'workload':<10}{'reactive ms':>12}{'langgraph ms':>14}{'ratio':>10}")
    for wl in PROJECTIONS:
        rms = reactive[wl]["median_ms"]
        lms = langgraph[wl]["median_ms"]
        ratio = lms / rms if rms else float("inf")
        print(f"{wl:<10}{rms:>12.3f}{lms:>14.3f}{ratio:>9.1f}x")
    print("\nreactive executedPerRun: " + ", ".join(
        f"{wl}={reactive[wl]['executedPerRun']:.0f}" for wl in PROJECTIONS))
    print("Outputs identical for every workload ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())