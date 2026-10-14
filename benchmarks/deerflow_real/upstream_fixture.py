"""Locate an optional bytedance/deer-flow checkout used by real-path benchmarks."""

from __future__ import annotations

import os
from pathlib import Path

ENV_VAR = "DEERFLOW_UPSTREAM_ROOT"


def upstream_root() -> Path | None:
    value = os.environ.get(ENV_VAR)
    if not value:
        return None
    return Path(value).expanduser().resolve()


def upstream_backend() -> Path | None:
    root = upstream_root()
    if root is None:
        return None
    return root / "backend"


def upstream_available() -> bool:
    backend = upstream_backend()
    return backend is not None and (backend / "pyproject.toml").is_file()


def require_upstream_backend() -> Path:
    backend = upstream_backend()
    if backend is None or not (backend / "pyproject.toml").is_file():
        raise RuntimeError(
            f"set {ENV_VAR} to a bytedance/deer-flow checkout before running real-path benchmarks"
        )
    return backend
