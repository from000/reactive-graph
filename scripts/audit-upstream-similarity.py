#!/usr/bin/env python3
"""Find ReactiveGraph symbols that resemble upstream LangGraph/LangChain code.

This is a review aid, not a gate. It parses the local Python sources and every
upstream package it can find, normalises both (comments, docstrings and blank
lines removed) and reports function/class level similarity. Run it before
publishing, then record whatever you adapted in
``scripts/third-party-attribution.txt``.

Usage:
    python3 scripts/audit-upstream-similarity.py [--upstream PATH ...]
                                                 [--threshold 0.6]

``PATH`` may be a venv root, a ``site-packages`` directory, a uv cache
(``uv/archive-v0``), or any directory that holds ``langgraph`` /
``langchain_core`` / ``langchain`` package directories. With no ``--upstream``
the script looks in ``benchmarks/differential/.venv`` and the uv cache.

Note that ``langchain`` (the distribution that carries
``langchain/agents/middleware``) lives in the uv cache even when the
differential venv does not install it, so the cache is scanned too by default.
"""

from __future__ import annotations

import argparse
import ast
import difflib
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUR_ROOTS = [
    ROOT / "python/reactivegraph/reactivegraph",
    ROOT / "python/reactivechain/reactivechain",
]
PKG_NAMES = ("langgraph", "langchain_core", "langchain")
# 0.45 catches heavily rewritten ports whose control flow still matches
# upstream (e.g. the add_messages merge loop scored 0.57); genuine
# re-implementations that merely share an idiom stay well below this.
DEFAULT_THRESHOLD = 0.45
MIN_LINES = 10

# Upstream installs we deliberately track. Newer copies of the same major
# version are near-identical, so only the newest of each is scanned.
VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")


def normalize(segment: str) -> list[str]:
    """Strip docstrings, comments, indentation and blank lines."""
    segment = re.sub(r'"""[\s\S]*?"""', "", segment)
    segment = re.sub(r"'''[\s\S]*?'''", "", segment)
    lines = []
    for line in segment.splitlines():
        line = re.sub(r"#.*$", "", line).strip()
        if line:
            lines.append(line)
    return lines


def extract_defs(path: Path) -> dict[str, list[list[str]]]:
    """Map every top-level-ish def/class name to its normalised source lines."""
    try:
        src = path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(src)
    except (OSError, SyntaxError, ValueError):
        return {}
    defs: dict[str, list[list[str]]] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            segment = ast.get_source_segment(src, node)
            if not segment:
                continue
            lines = normalize(segment)
            if len(lines) >= MIN_LINES:
                defs.setdefault(node.name, []).append(lines)
    return defs


def our_symbols() -> dict[str, list[list[str]]]:
    """Every sufficiently large def/class defined in our own packages."""
    symbols: dict[str, list[list[str]]] = {}
    for our_root in OUR_ROOTS:
        if not our_root.exists():
            continue
        for path in sorted(our_root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            for symbol, lines_list in extract_defs(path).items():
                symbols.setdefault(symbol, []).extend(lines_list)
    return symbols


def site_packages(venv: Path) -> Path | None:
    """Resolve a venv root to its site-packages directory (if any)."""
    lib = venv / "lib"
    if not lib.is_dir():
        return None
    for python_dir in sorted(lib.glob("python*")):
        candidate = python_dir / "site-packages"
        if candidate.is_dir():
            return candidate
    return None


def package_dirs(root: Path) -> list[tuple[str, Path]]:
    """Find ``langgraph``/``langchain_core``/... dirs directly under *root*.

    Handles both an installed ``site-packages`` (packages sit one level down)
    and a package cache such as ``uv/archive-v0`` (one dir per resolved wheel).
    """
    found: list[tuple[str, Path]] = []
    for name in PKG_NAMES:
        if root.name == name and root.is_dir():
            found.append((name, root))
            continue
        direct = root / name
        if direct.is_dir():
            found.append((name, direct))
        for candidate in root.glob(f"*/{name}"):
            if candidate.is_dir() and candidate != direct:
                found.append((name, candidate))
    return found


def cache_roots() -> list[Path]:
    """Package trees in the uv cache that the differential venv does not have.

    ``langchain`` is the important one: it carries
    ``langchain/agents/middleware`` (TodoListMiddleware, SummarizationMiddleware,
    AgentMiddleware) and is deliberately absent from the benchmark venv, so
    without this the audit silently skips everything ported from it.
    """
    archive = Path(os.environ.get("UV_CACHE_DIR", Path.home() / ".cache/uv"))
    archive = archive / "archive-v0"
    if not archive.is_dir():
        return []
    roots = []
    for entry in sorted(archive.iterdir()):
        if not entry.is_dir():
            continue
        if (entry / "langchain").is_dir() and (entry / "langchain/agents").is_dir():
            roots.append(entry)
    return roots


def candidate_roots(explicit: list[Path] | None) -> list[Path]:
    if explicit:
        return explicit
    roots = []
    venv = ROOT / "benchmarks/differential/.venv"
    if venv.exists():
        roots.append(venv)
    roots.extend(cache_roots())
    return roots


def version_of(pkg: Path) -> tuple[int, int, int]:
    """Version of the distribution that ships *pkg*, from the sibling dist-info."""
    dist_names = {pkg.name, pkg.name.replace("_", "-"), pkg.name.replace("-", "_")}
    best = (0, 0, 0)
    for dist_info in pkg.parent.glob("*.dist-info"):
        stem = dist_info.name.rsplit(".dist-info", 1)[0]
        dist, _, version = stem.rpartition("-")
        if dist.replace("_", "-").lower() not in {d.replace("_", "-").lower() for d in dist_names}:
            continue
        match = VERSION_RE.match(version)
        if match:
            candidate = tuple(int(part) for part in match.groups())
            best = max(best, candidate)
    return best


def resolve(roots: list[Path], explicit_given: bool = False) -> list[Path]:
    """Expand venv roots, drop duplicates, keep the newest copy of each package."""
    newest: dict[str, tuple[tuple[int, int, int], Path]] = {}
    for root in roots:
        root = root.resolve()
        if not root.exists():
            if explicit_given:
                print(f"warning: skipping missing upstream {root}", file=sys.stderr)
            continue
        base = site_packages(root) or root
        found = package_dirs(base)
        if not found:
            continue
        for name, pkg in found:
            resolved = pkg.resolve()
            version = version_of(resolved)
            if name not in newest or version > newest[name][0]:
                newest[name] = (version, resolved)
    return [path for _, path in newest.values()]


def build_index(roots: list[Path], wanted: set[str]) -> dict[str, list[tuple[list[str], str]]]:
    index: dict[str, list[tuple[list[str], str]]] = {}
    scanned = 0
    for tree in roots:
        label = f"{tree.parent.name}/{tree.name}"
        for path in tree.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            try:
                src = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            # Cheap pre-filter: only parse files that mention a symbol we have.
            if not any(name in src for name in wanted):
                continue
            scanned += 1
            for symbol, entries in extract_defs(path).items():
                if symbol not in wanted:
                    continue
                for lines in entries:
                    index.setdefault(symbol, []).append(
                        (lines, f"{label}/{path.relative_to(tree)}")
                    )
    print(f"parsed {scanned} upstream files -> {len(index)} shared symbols",
          file=sys.stderr)
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", action="append", type=Path, default=None,
                        help="venv, site-packages or package dir (repeatable)")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    args = parser.parse_args()

    ours = our_symbols()
    if not ours:
        print("no local reactivegraph/reactivechain sources found", file=sys.stderr)
        return 2
    wanted = set(ours)

    roots = resolve(candidate_roots(args.upstream), explicit_given=bool(args.upstream))
    if not roots:
        print("no upstream packages found; pass --upstream PATH", file=sys.stderr)
        return 2
    index = build_index(roots, wanted)
    if not index:
        print("no shared symbols between our code and upstream", file=sys.stderr)
        return 2

    findings = []
    for our_root in OUR_ROOTS:
        if not our_root.exists():
            continue
        for path in sorted(our_root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            for symbol, entries in extract_defs(path).items():
                if symbol not in index:
                    continue
                for lines in entries:
                    best = (0.0, "", [])
                    for upstream_lines, upstream_path in index[symbol]:
                        ratio = difflib.SequenceMatcher(
                            None, lines, upstream_lines, autojunk=False
                        ).ratio()
                        if ratio > best[0]:
                            best = (ratio, upstream_path, upstream_lines)
                    if best[0] >= args.threshold:
                        findings.append((best[0], symbol, path, best[1], lines, best[2]))

    findings.sort(key=lambda f: -f[0])
    print(f"{'similarity':>10}  {'symbol':<34} {'file':<52} upstream")
    print("-" * 150)
    for ratio, symbol, path, upstream, lines, upstream_lines in findings:
        shared = len(set(lines) & set(upstream_lines))
        print(f"{ratio * 100:>9.1f}%  {symbol:<34} "
              f"{str(path.relative_to(ROOT)):<52} {upstream} ({shared} shared lines)")
    print(f"\n{len(findings)} matches at >= {args.threshold:.0%} similarity")
    print("Review each one and record adapted files in "
          "scripts/third-party-attribution.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
