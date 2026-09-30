"""Interrupt payload + graph constants, pinned against real LangGraph.

``langgraph.types.Interrupt`` is *not* an exception: it is the payload a
node's ``interrupt()`` call surfaces to the client, identified by a stable id
derived from the node's namespace. DeerFlow serialises it straight out of a
checkpoint, so its field names, id derivation and slot-only storage are an
engine contract.
"""

from __future__ import annotations

import dataclasses
import pickle

import pytest

from reactivegraph.constants import (
    END,
    REMOVE_ALL_MESSAGES,
    START,
    TAG_HIDDEN,
    TAG_NOSTREAM,
)
from reactivegraph.types import Interrupt


class TestInterruptShape:
    def test_is_a_slotted_dataclass_with_value_and_id(self) -> None:
        interrupt = Interrupt("ask")
        assert [f.name for f in dataclasses.fields(interrupt)] == ["value", "id"]
        assert Interrupt.__slots__ == ("value", "id")
        assert interrupt.value == "ask"
        assert interrupt.id == "placeholder-id"

    def test_default_id_is_the_upstream_placeholder(self) -> None:
        assert Interrupt(value="v", id="i").id == "i"

    def test_positional_construction_matches_upstream(self) -> None:
        assert Interrupt("v", "my-id").id == "my-id"

    def test_equality_is_by_value_and_unhashable(self) -> None:
        assert Interrupt("v") == Interrupt("v")
        assert Interrupt("v", "a") != Interrupt("v", "b")
        with pytest.raises(TypeError):
            hash(Interrupt("v"))

    def test_value_requires_an_argument(self) -> None:
        with pytest.raises(TypeError):
            Interrupt()  # type: ignore[call-arg]

    def test_has_no_instance_dict(self) -> None:
        # Serializers fall back to ``model_dump``/``dict``/``__dict__`` before
        # an isinstance check; a dict would make Interrupt serialise as {}.
        assert not hasattr(Interrupt("v"), "__dict__")

    def test_round_trips_through_pickle(self) -> None:
        assert pickle.loads(pickle.dumps(Interrupt({"q": 1}, "i"))) == Interrupt({"q": 1}, "i")

    def test_is_final(self) -> None:
        """The native fallback is always runtime-marked final.

        Our own class uses ``typing_extensions.final``, which writes
        ``__final__`` on every supported Python. Upstream
        ``langgraph.types.Interrupt`` uses stdlib ``typing.final``, which only
        started writing the runtime-visible attribute in CPython 3.11 — on
        3.10 the marker is visible to type checkers only. Assert the stronger
        guarantee (native class) unconditionally, and the host class according
        to the stdlib semantics of the running interpreter.
        """
        import sys

        from reactivegraph.types import HostInterrupt, _NativeInterrupt

        assert getattr(_NativeInterrupt, "__final__", False) is True
        if HostInterrupt is not None:
            host_final = getattr(HostInterrupt, "__final__", None)
            if sys.version_info >= (3, 11):
                assert host_final is True
            else:
                assert host_final is None
        # ``Interrupt`` resolves to the host class when langgraph is installed
        # and to the native class otherwise; both must carry the final intent.
        if sys.version_info >= (3, 11):
            assert getattr(Interrupt, "__final__", False) is True

    def test_is_not_an_exception(self) -> None:
        # Middleware catches ``Exception`` to wrap ordinary failures and must
        # not swallow a control-flow payload; keeping this out of the
        # exception hierarchy is what makes that safe.
        assert not issubclass(Interrupt, BaseException)


class TestInterruptFromNs:
    def test_id_is_the_xxh3_128_of_the_namespace(self) -> None:
        assert Interrupt.from_ns({"q": 1}, "node:1").id == "bd00ab1d89bcbb85b2b33c0e6efdaeb0"

    def test_empty_namespace_still_hashes(self) -> None:
        assert Interrupt.from_ns("v", "").id == "99aa06d3014798d86001c324468d497f"

    def test_carries_the_value_unchanged(self) -> None:
        assert Interrupt.from_ns("question", "n").value == "question"

    def test_same_namespace_yields_the_same_id(self) -> None:
        assert Interrupt.from_ns(1, "a|b").id == Interrupt.from_ns(2, "a|b").id


class TestInterruptDeprecatedKwargs:
    def test_ns_sequence_kwarg_becomes_the_id(self) -> None:
        assert Interrupt("v", ns=["a", "b"]).id == "e4bb8f8c00c16987c7ee863ab9632249"

    def test_explicit_id_wins_over_ns(self) -> None:
        assert Interrupt("v", id="x", ns=["a", "b"]).id == "x"

    def test_non_sequence_ns_is_ignored(self) -> None:
        assert Interrupt("v", ns=5).id == "placeholder-id"

    def test_str_ns_is_a_sequence_and_is_joined(self) -> None:
        # ``str`` is a ``Sequence[str]``, so it takes the join path.
        assert Interrupt("v", ns="str").id == "32ba4123dfb3b73d05b766dea33af2a2"

    def test_empty_sequence_ns_hashes_the_empty_string(self) -> None:
        assert Interrupt("v", ns=[]).id == "99aa06d3014798d86001c324468d497f"

    def test_unknown_deprecated_kwargs_are_ignored(self) -> None:
        assert Interrupt("v", resumable=True).id == "placeholder-id"

    def test_interrupt_id_property_is_deprecated_but_works(self) -> None:
        with pytest.warns(DeprecationWarning):
            assert Interrupt("v").interrupt_id == "placeholder-id"


class TestGraphConstants:
    def test_start_and_end_match_upstream(self) -> None:
        assert START == "__start__"
        assert END == "__end__"

    def test_tags_match_upstream(self) -> None:
        assert TAG_NOSTREAM == "nostream"
        assert TAG_HIDDEN == "langsmith:hidden"

    def test_remove_all_messages_matches_upstream(self) -> None:
        assert REMOVE_ALL_MESSAGES == "__remove_all__"

    def test_constants_are_interned_strings(self) -> None:
        # Upstream interns them; identity comparisons in hot paths rely on it.
        import sys

        assert START is sys.intern("__start__")
        assert END is sys.intern("__end__")
        assert TAG_NOSTREAM is sys.intern("nostream")
        assert TAG_HIDDEN is sys.intern("langsmith:hidden")


class TestCheckpointerAlias:
    def test_accepts_the_three_upstream_switch_values(self) -> None:
        from typing import get_args

        from reactivegraph.types import Checkpointer

        assert type(None) in get_args(Checkpointer)
        assert bool in get_args(Checkpointer)

    def test_includes_the_native_checkpoint_store(self) -> None:
        from typing import get_args

        from reactivegraph.checkpoint import CheckpointStore
        from reactivegraph.types import Checkpointer

        assert CheckpointStore in get_args(Checkpointer)


class TestHostInterruptInterop:
    def test_engine_interrupt_is_the_host_class_when_available(self) -> None:
        """The payload crosses package boundaries as one identity.

        DeerFlow's serializer does ``isinstance(obj, reactivegraph.Interrupt)``
        on payloads produced by ``langgraph.types.interrupt`` inside graphs
        that still use the host runtime. ``isinstance`` would accept a
        subclass, but the reverse direction (host code checking an
        engine-produced payload) would not; aliasing keeps both true.
        """
        host_types = pytest.importorskip("langgraph.types")
        assert Interrupt is host_types.Interrupt

    def test_host_payload_satisfies_engine_checks(self) -> None:
        host_types = pytest.importorskip("langgraph.types")
        payload = host_types.Interrupt(value={"q": 1}, id="i")
        assert isinstance(payload, Interrupt)
        assert payload.value == {"q": 1}
        assert payload.id == "i"

    def test_without_langgraph_the_native_interrupt_still_works(self) -> None:
        """The bridge must be optional: no module-scope langgraph import."""
        import subprocess
        import sys
        import textwrap

        code = textwrap.dedent(
            """
            import importlib.abc, sys

            class _Blocker(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname.split(".")[0] in ("langchain", "langgraph"):
                        raise ImportError(fullname + " is not installed")
                    return None

            sys.meta_path.insert(0, _Blocker())

            from reactivegraph.types import Interrupt
            assert Interrupt("v", id="i").id == "i"
            assert Interrupt.from_ns("v", "n").id
            print("OK")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert "OK" in result.stdout
