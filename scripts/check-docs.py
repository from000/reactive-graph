#!/usr/bin/env python3
"""Docs health check: every relative markdown link must resolve.

Covers docs/ plus the user-facing READMEs that ship inside published
artifacts: the repo READMEs, every Python package README (sdist readme), and
every npm package README. A broken link in one of those documents ships to
npm/PyPI readers, where it cannot be fixed by a docs rebuild.

Checks [text](relative-path) and image links (excluding anchors #...,
external http(s) URLs, and mailto). Exits non-zero on the first broken link —
this runs in CI (`.github/workflows/ci.yml`) as the docs gate.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# docs/ is the documentation site: every markdown file there is scanned.
DOCS = ROOT / "docs"

# Package READMEs are packaged into published npm/PyPI artifacts, so their
# links are just as user-visible as the site's. Only READMEs (not every
# markdown file) are scanned here — npm and PyPI ship exactly these.
PACKAGE_README_ROOTS = [ROOT / "packages", ROOT / "python"]
EXTRA_FILES = [ROOT / "README.md", ROOT / "README.en.md"]

LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")

errors: list[str] = []
checked = 0

targets: list[Path] = list(DOCS.rglob("*.md"))
for base in PACKAGE_README_ROOTS:
    targets.extend(base.rglob("README.md"))
targets.extend(EXTRA_FILES)
markdown_files = sorted({p for p in targets if p.exists()})

for md in markdown_files:
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

# Nav completeness: a page that builds but is absent from ``mkdocs.yml`` is
# unreachable from the rendered site (this had already happened to the whole
# tutorials/ tree). ``README.md`` is an internal index and is exempt.
import re as _re

nav_text = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
nav_entries = set(_re.findall(r"([A-Za-z0-9_\-/]+\.md)", nav_text))
# Internal working notes are deliberately not part of the published site:
# ``plans/`` holds dated design/implementation records (the roadmap page links
# the ones that matter) and ``research/`` holds background investigations.
_INTERNAL_DOC_PREFIXES = ("tutorials/code/", "plans/", "research/")
site_pages = {
    rel
    for rel in (path.relative_to(DOCS).as_posix() for path in DOCS.rglob("*.md"))
    if not rel.startswith(_INTERNAL_DOC_PREFIXES)
}
unlisted = sorted(p for p in site_pages if p not in nav_entries and p != "README.md")
if unlisted:
    print("Docs nav check FAILED: pages exist but are not in mkdocs.yml nav:")
    for page in unlisted:
        print(f"  docs/{page}")
    sys.exit(1)

if errors:
    print("Docs link check FAILED:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

print(
    f"Docs link check OK ({checked} relative links resolved "
    f"across {len(markdown_files)} markdown files; "
    f"{len(site_pages)} pages all present in nav)."
)
