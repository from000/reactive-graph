#!/usr/bin/env python3
"""Tutorial code check: every tutorial code file must run successfully.

01–03 run without the bundled Driver (pure Python); 04–06 need node +
`packages/driver/dist/main.js` so they are exercised locally, skipped here.
`reactchain_*.py` tutorials run under the reactivechain project env.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CODE = ROOT / "docs" / "tutorials" / "code"
REQUIRES_DRIVER = {"04_streams_resume.py", "05_durability.py", "06_1000_nodes.py"}

failures: list[tuple[str, str]] = []
ran = 0
for py in sorted(CODE.glob("*.py")):
    if py.name in REQUIRES_DRIVER:
        continue
    if py.name.startswith("reactchain_"):
        proj = ROOT / "python" / "reactivechain"
    else:
        proj = ROOT / "python" / "reactivegraph"
    ran += 1
    r = subprocess.run(
        ["uv", "run", "--directory", str(proj), "python", str(py)],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        failures.append((py.name, r.stderr[-800:]))
    else:
        print(f"  ok  {py.name}")

if failures:
    print("Tutorial code check FAILED:")
    for name, err in failures:
        print(f"  [{name}] {err}")
    sys.exit(1)

print(f"Tutorial code check OK ({ran} scripts ran; 04-06 require bundled Driver, run locally).")