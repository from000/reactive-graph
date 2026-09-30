#!/usr/bin/env python3
"""Gate the third-party attribution surface.

Four things are checked, all of which have silently broken in the past:

1. Every file listed in ``scripts/third-party-attribution.txt`` exists and
   points at THIRD_PARTY_NOTICES.md. The manifest is produced with
   ``scripts/audit-upstream-similarity.py``.
2. Every packaged copy of THIRD_PARTY_NOTICES.md is byte-identical to the root
   copy, so a licence fix cannot land in the repository but miss the wheels,
   sdists and npm tarballs.
3. Every distributing package actually ships the file, via its ``pyproject``
   ``license-files`` or its npm ``files`` allow-list.
4. No source file claims to be ported/copied verbatim from upstream without
   being listed. This is the check that catches a new port: the similarity
   audit is a periodic review, but the acknowledgement in the docstring is
   written at porting time, so the two must agree.

Run: ``python3 scripts/check-third-party-attribution.py``
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        tomllib = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "scripts/third-party-attribution.txt"
NOTICE_NAME = "THIRD_PARTY_NOTICES.md"
ROOT_NOTICE = ROOT / NOTICE_NAME

# Directories whose contents are published to PyPI / npm.
PYTHON_PACKAGES = (
    "python/reactivegraph",
    "python/reactivechain",
    "python/reactivegraph_sdk",
    "python/reactivegraph_cli",
)
NPM_PACKAGES = (
    "packages/driver",
    "packages/sdk-js",
    "packages/protocol",
    "packages/devtools-protocol",
)
SOURCE_ROOTS = (
    "python/reactivegraph/reactivegraph",
    "python/reactivechain/reactivechain",
)
# Phrases that announce derivative work in a module docstring. The last one is
# the canonical attribution header itself, so a file that carries it is listed
# in the manifest by definition.
DERIVATION_RE = re.compile(
    r"(ported from|copied verbatim|kept verbatim|verbatim from|adapted from"
    r"|portions of this module are adapted)",
    re.IGNORECASE,
)


def read_license_files(pyproject: Path) -> list[str] | None:
    """Return the ``project.license-files`` list, or None if unparsable.

    ``tomllib`` is only in the standard library from 3.11, and CI runs this
    gate on 3.10 as well, so fall back to a narrow scan of the key. The
    fallback fails closed: no match means the caller reports the problem.
    """
    text = pyproject.read_text(encoding="utf-8")
    if tomllib is not None:
        try:
            data = tomllib.loads(text)
        except tomllib.TOMLDecodeError:
            return None
        found = data.get("project", {}).get("license-files")
        return found if isinstance(found, list) else None
    match = re.search(r"^license-files\s*=\s*\[(.*?)\]", text, re.M | re.S)
    if not match:
        return None
    return [
        quoted or single for quoted, single in re.findall(r'"([^"]+)"|\'([^\']+)\'', match.group(1))
    ]


failures: list[str] = []


def check_manifest() -> int:
    entries = 0
    for raw in MANIFEST.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.startswith("#"):
            continue
        parts = raw.split("\t")
        if len(parts) < 2:
            failures.append(f"{MANIFEST.name}: malformed entry: {raw!r}")
            continue
        rel, upstream = parts[0].strip(), parts[1].strip()
        entries += 1
        path = ROOT / rel
        if not path.exists():
            failures.append(f"{rel}: listed as adapted from {upstream} but missing")
            continue
        if NOTICE_NAME not in path.read_text(encoding="utf-8", errors="ignore"):
            failures.append(f"{rel}: adapted from {upstream} but does not point at {NOTICE_NAME}")
    return entries


def check_copies() -> int:
    if not ROOT_NOTICE.exists():
        failures.append(f"{NOTICE_NAME}: missing at the repository root")
        return 0
    expected = ROOT_NOTICE.read_bytes()
    checked = 0
    for rel in (*PYTHON_PACKAGES, *NPM_PACKAGES):
        copy = ROOT / rel / NOTICE_NAME
        checked += 1
        if not copy.exists():
            failures.append(f"{rel}/{NOTICE_NAME}: missing")
        elif copy.read_bytes() != expected:
            failures.append(f"{rel}/{NOTICE_NAME}: differs from the root copy")
    return checked


def check_python_manifests() -> None:
    for rel in PYTHON_PACKAGES:
        pyproject = ROOT / rel / "pyproject.toml"
        license_files = read_license_files(pyproject)
        if license_files is None:
            failures.append(f"{rel}/pyproject.toml: cannot read project.license-files")
        elif NOTICE_NAME not in license_files:
            failures.append(f"{rel}/pyproject.toml: license-files does not include {NOTICE_NAME}")


def check_npm_manifests() -> None:
    for rel in NPM_PACKAGES:
        package_json = ROOT / rel / "package.json"
        try:
            data = json.loads(package_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            failures.append(f"{rel}/package.json: unreadable ({exc})")
            continue
        if NOTICE_NAME not in data.get("files", []):
            failures.append(f"{rel}/package.json: files does not include {NOTICE_NAME}")


def check_declared_derivations(listed: set[str]) -> None:
    """Flag modules that declare a port but are missing from the manifest.

    This is what catches a newly ported module: the similarity audit is a
    periodic review, but the acknowledgement is written at porting time.
    """
    for rel in SOURCE_ROOTS:
        for path in sorted((ROOT / rel).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            header = text.split('"""', 2)[1] if text.count('"""') >= 2 else text[:2000]
            match = DERIVATION_RE.search(header)
            if not match:
                continue
            relative = str(path.relative_to(ROOT))
            if relative not in listed:
                failures.append(
                    f"{relative}: declares '{match.group(0)}' but is not in "
                    f"{MANIFEST.name}; add it or remove the claim"
                )


entries = check_manifest()
declared = {
    raw.split("\t")[0].strip()
    for raw in MANIFEST.read_text(encoding="utf-8").splitlines()
    if raw.strip() and not raw.startswith("#") and "\t" in raw
}
check_declared_derivations(declared)
copies = check_copies()
check_python_manifests()
check_npm_manifests()

if failures:
    print("third-party attribution check FAILED:", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    sys.exit(1)

print(
    f"third-party attribution check: OK "
    f"({entries} adapted files, {copies} packaged notices in sync)"
)
