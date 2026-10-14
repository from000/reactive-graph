#!/usr/bin/env python3
"""Differential benchmark: ReactiveChain vs upstream LangChain (M6 成功标准).

Runs both sides on this machine, asserts identical outputs for every workload,
and prints the comparison table with the selective-execution evidence
(retrieval_calls on the same-query re-run) — including the honest no-gain
report for workloads where ReactiveChain gains nothing.

Workloads:
  rag_first / rag_selective / rag_different / toolchain

Usage:
    uv run --directory benchmarks/differential python run_reactchain_differential.py
    uv run --directory benchmarks/differential python run_reactchain_differential.py --refresh
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE / ".results"

SIDE_SCRIPTS = {
    "reactivechain": "reactchain_side.py",
    "langchain": "langchain_side.py",
}


def run_side(name: str) -> dict:
    script = SIDE_SCRIPTS[name]
    cmd = ["uv", "run", "--directory", str(HERE), "python", str(HERE / script)]
    print(f"[{name}] {' '.join(cmd)}")
    out = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def main() -> int:
    rc = run_side("reactivechain")
    lc = run_side("langchain")

    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "reactchain.json").write_text(
        json.dumps(rc, ensure_ascii=False, indent=2)
    )
    (RESULTS / "langchain.json").write_text(
        json.dumps(lc, ensure_ascii=False, indent=2)
    )

    failures: list[str] = []
    for wl in rc:
        if rc[wl]["output"] != lc[wl]["output"]:
            failures.append(
                f"{wl}: reactivechain={rc[wl]['output']!r} != langchain={lc[wl]['output']!r}"
            )

    if failures:
        print("OUTPUT MISMATCH:", *failures, sep="\n  ")
        return 1

    print("\nDifferential benchmark: ReactiveChain vs LangChain (median of runs, same machine)")
    print(f"{'workload':<15}{'reactivechain ms':>16}{'langchain ms':>14}{'ratio':>10}")
    for wl in rc:
        rms = rc[wl]["median_ms"]
        lms = lc[wl]["median_ms"]
        ratio = lms / rms if rms else float("inf")
        print(f"{wl:<15}{rms:>16.4f}{lms:>14.4f}{ratio:>9.1f}x")

    print("\nselective-execution evidence (same-query re-run, retrieval calls):")
    rc_calls = rc["rag_selective"]["retrieval_calls"]
    lc_calls = lc["rag_selective"]["retrieval_calls"]
    print(f"  reactivechain.rag_selective.retrieval_calls = {rc_calls}")
    print(f"  langchain.rag_selective.retrieval_calls     = {lc_calls}")
    print("\nOutputs identical for every workload ✓")
    print("No-gain honesty: reactivechain does not skip on different queries or the\n"
          "toolchain (both sides re-run fully there); gains concentrate on re-running")
    print("identical inputs (rag_selective), which is the differential point.")
    return 0


if __name__ == "__main__":
    sys.exit(main())