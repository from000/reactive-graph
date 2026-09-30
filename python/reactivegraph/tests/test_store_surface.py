"""The engine-owned long-term-store base class hosts subclass.

DeerFlow's store providers annotate ``BaseStore`` and its thread-metadata
backend subclasses nothing but must be *assignable* where ``BaseStore`` is
expected. When LangGraph is installed the engine hands back its class so host
``isinstance`` checks keep working; without LangGraph the native base carries
the same eight-method surface.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from reactivegraph import BaseStore, MemoryStore


def test_host_base_class_wins_when_langgraph_is_installed() -> None:
    try:
        from langgraph.store.base import BaseStore as HostBase
    except ImportError:  # pragma: no cover - langgraph is an optional dep
        pytest.skip("langgraph not installed")
    assert BaseStore is HostBase


def test_memory_store_satisfies_the_engine_store_surface() -> None:
    store = MemoryStore()
    for method in (
        "get", "put", "delete", "search", "list_namespaces", "batch",
        "aget", "aput", "adelete", "asearch", "alist_namespaces", "abatch",
    ):
        assert callable(getattr(store, method)), method


def test_base_store_without_langgraph() -> None:
    code = textwrap.dedent(
        """
        import importlib.abc, sys

        class _Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split(".")[0] == "langgraph":
                    raise ImportError("langgraph is not installed")
                return None

        sys.meta_path.insert(0, _Blocker())

        from reactivegraph import BaseStore, MemoryStore

        assert issubclass(MemoryStore, object)
        for method in ("get", "put", "delete", "search", "list_namespaces", "batch"):
            assert callable(getattr(BaseStore, method, None)), method
        for method in ("aget", "aput", "adelete", "asearch", "alist_namespaces", "abatch"):
            assert callable(getattr(BaseStore, method, None)), method

        # The abstract base fails closed for an unimplemented backend.
        try:
            BaseStore().get(("ns",), "key")
        except (NotImplementedError, TypeError):
            pass
        else:
            raise AssertionError("native BaseStore must fail closed")
        print("OK")
        """
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-3000:]
    assert "OK" in result.stdout
