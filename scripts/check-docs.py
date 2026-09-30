#!/usr/bin/env python3
"""Docs health check: every relative markdown link in docs/ must resolve.

Checks [text](relative-path) and image links (excluding anchors #...,
external http(s) URLs, and mailto). Exits non-zero on the first broken link —
this runs in CI (`.github/workflows/ci.yml`) as the docs gate.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")

errors: list[str] = []
checked = 0

for md in sorted(DOCS.rglob("*.md")):
    for line_no, line in enumerate(md.read_text(encoding="utf-8").splitlines(), start=1):
        for target in LINK_RE.findall(line):
            target = target.strip()
            if target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            if " " in target:  # skip links with titles like (url "title")
                target = target.split(" ")[0]
            checked += 1
            anchor = ""
            if "#" in target:
                target, anchor = target.split("#", 1)
            resolved = (md.parent / target).resolve()
            if not resolved.exists():
                errors.append(f"{md.relative_to(ROOT)}:{line_no}: broken link -> {target}")

if errors:
    print("Docs link check FAILED:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

print(f"Docs link check OK ({checked} relative links resolved).")
