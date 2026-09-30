"""The engine-owned checkpoint base class hosts subclass.

DeerFlow's ``ReactiveGraphSaver``/``CachedHistorySaver`` subclass
``BaseCheckpointSaver``; the class must be importable from ReactiveGraph. When
LangGraph is installed the engine hands back *its* class, because other host
code still does ``isinstance(..., langgraph.checkpoint.base.BaseCheckpointSaver)``.
Without LangGraph the engine's native base carries the same surface.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from reactivegraph import BaseCheckpointSaver, PendingWrite, empty_checkpoint, uuid6


def test_checkpoint_primitives_are_exported_and_typed() -> None:
    checkpoint = empty_checkpoint()
    assert checkpoint["v"] == 2
    assert checkpoint["channel_values"] == {}
    assert uuid6().version == 6

    saver = BaseCheckpointSaver()
    # Both the host class and the native fallback are abstract storage bases:
    # an unimplemented backend must raise rather than report an empty thread.
    with pytest.raises(NotImplementedError):
        saver.get(_config())
    assert saver.get_next_version(None, "messages") == 1
    assert saver.get_next_version(3, "messages") == 4
    assert PendingWrite.__origin__ is tuple


def test_host_base_class_wins_when_langgraph_is_installed() -> None:
    try:
        from langgraph.checkpoint.base import BaseCheckpointSaver as HostBase
    except ImportError:  # pragma: no cover - langgraph is an optional dep
        pytest.skip("langgraph not installed")
    assert BaseCheckpointSaver is HostBase


def test_base_checkpoint_saver_without_langgraph() -> None:
    code = textwrap.dedent(
        """
        import importlib.abc, sys

        class _Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split(".")[0] == "langgraph":
                    raise ImportError("langgraph is not installed")
                return None

        sys.meta_path.insert(0, _Blocker())

        from reactivegraph import BaseCheckpointSaver, PendingWrite, empty_checkpoint

        class Saver(BaseCheckpointSaver[str]):
            pass

        saver = Saver()
        assert saver.get_next_version(None, "messages") == 1
        assert empty_checkpoint()["v"] == 2
        assert PendingWrite.__origin__ is tuple
        for call in (
            lambda: saver.get({"configurable": {"thread_id": "t"}}),
            lambda: saver.get_tuple({"configurable": {"thread_id": "t"}}),
        ):
            try:
                call()
            except NotImplementedError:
                pass
            else:
                raise AssertionError("native saver must fail closed")
        print("OK")
        """
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-3000:]
    assert "OK" in result.stdout


def _config() -> dict:
    return {"configurable": {"thread_id": "t", "checkpoint_ns": ""}}
