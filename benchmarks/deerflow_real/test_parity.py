from __future__ import annotations

import importlib.util
import sys

import pytest

from upstream_fixture import require_upstream_backend, upstream_available


def _load_deerflow_factory():
    deerflow_root = require_upstream_backend() / "packages" / "harness"
    spec = importlib.util.spec_from_file_location(
        "deerflow_factory",
        deerflow_root / "deerflow" / "agents" / "factory.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load DeerFlow factory")
    module = importlib.util.module_from_spec(spec)
    sys.modules["deerflow_factory"] = module
    spec.loader.exec_module(module)
    return module


def test_deerflow_factory_imports_real_path() -> None:
    if not upstream_available():
        pytest.skip("real bytedance/deer-flow checkout not available")
    try:
        module = _load_deerflow_factory()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"DeerFlow source imports require its full environment: {exc}")
    assert callable(module.create_deerflow_agent)
