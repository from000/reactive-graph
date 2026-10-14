"""Native engine primitives: behaviour pinned against real LangGraph.

DeerFlow's harness consumes ``Overwrite`` on reducer channels and
``empty_checkpoint``/``uuid6`` when it writes marker checkpoints. Those
primitives are engine contracts, not host contracts, so they live here —
probed against the real ``langgraph`` implementation rather than assumed.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

from reactivegraph.checkpoint import empty_checkpoint, uuid6
from reactivegraph.types import OVERWRITE, Command, Overwrite, Send


class TestOverwrite:
    def test_fields_and_discriminator(self) -> None:
        overwrite = Overwrite(value=[1, 2])
        assert overwrite.value == [1, 2]
        assert overwrite.type == OVERWRITE == "__overwrite__"

    def test_is_mutable_slotted_dataclass(self) -> None:
        overwrite = Overwrite(value=1)
        assert [f.name for f in dataclasses.fields(overwrite)] == ["value", "type"]
        assert Overwrite.__slots__ == ("value", "type")
        overwrite.value = 2
        assert overwrite.value == 2

    def test_equality_by_value_and_unhashable(self) -> None:
        assert Overwrite(value=[1]) == Overwrite(value=[1])
        assert Overwrite(value=1) != Overwrite(value=2)
        with pytest.raises(TypeError):
            hash(Overwrite(value=1))

    def test_positional_construction_matches_upstream(self) -> None:
        # Upstream is ``@dataclass(slots=True)`` with ``value`` first, so the
        # wrapper is constructible positionally.
        assert Overwrite("x").value == "x"

    def test_json_erased_form_is_recognised(self) -> None:
        # The dataclass-erased shape survives JSON round-trips; channels must
        # keep treating it as an overwrite.
        assert Overwrite.is_overwrite({"value": [1], "type": OVERWRITE}) == (True, [1])
        assert Overwrite.is_overwrite({OVERWRITE: [1]}) == (True, [1])
        assert Overwrite.is_overwrite(Overwrite(value=[1])) == (True, [1])

    def test_plain_values_are_not_overwrites(self) -> None:
        assert Overwrite.is_overwrite([1, 2]) == (False, None)
        assert Overwrite.is_overwrite({"value": [1]}) == (False, None)
        assert Overwrite.is_overwrite({"type": OVERWRITE}) == (False, None)
        assert Overwrite.is_overwrite({"value": 1, "type": "other"}) == (False, None)


class TestHostOverwriteInterop:
    """Hosts that annotate ``isinstance(x, langgraph.types.Overwrite)``.

    LangGraph's own ``BinaryOperatorAggregate.update`` checks the *host* class
    before treating a write as a reducer bypass. An engine-produced
    ``Overwrite`` therefore has to be an instance of the host class, exactly
    like ``Command``; otherwise host subgraphs hosted inside DeerFlow try to
    run ``operator.add`` on the wrapper and raise ``TypeError``.
    """

    def test_engine_overwrite_is_a_host_overwrite_when_available(self) -> None:
        host_types = pytest.importorskip("langgraph.types")
        assert issubclass(Overwrite, host_types.Overwrite)
        assert isinstance(Overwrite(value=[1]), host_types.Overwrite)

    def test_host_and_engine_overwrites_are_mutually_recognised(self) -> None:
        host_types = pytest.importorskip("langgraph.types")
        assert isinstance(host_types.Overwrite(value=[1]), Overwrite)
        assert Overwrite.is_overwrite(host_types.Overwrite(value=[1])) == (True, [1])

    def test_engine_overwrite_survives_a_host_channel_update(self) -> None:
        """The measured DeerFlow failure: a host reducer channel must unwrap it."""
        channels = pytest.importorskip("langgraph.channels")
        channel = channels.BinaryOperatorAggregate(list, lambda a, b: a + b)
        channel.key = "messages"
        channel.update([["START"]])
        channel.update([Overwrite(["only"])])
        assert channel.get() == ["only"]

    def test_engine_surface_is_unchanged_by_the_bridge(self) -> None:
        overwrite = Overwrite(value=[1, 2])
        assert overwrite.value == [1, 2]
        assert overwrite.type == OVERWRITE
        assert [f.name for f in dataclasses.fields(overwrite)] == ["value", "type"]
        assert Overwrite.__slots__ == ("value", "type")
        overwrite.value = 3
        assert overwrite.value == 3
        assert Overwrite(value=[1]) == Overwrite(value=[1])
        assert Overwrite(value=1) != Overwrite(value=2)
        with pytest.raises(TypeError):
            hash(Overwrite(value=1))


class TestUuid6:
    def test_is_version_6(self) -> None:
        value = uuid6()
        assert isinstance(value, uuid.UUID)
        assert value.version == 6

    def test_defaults_are_unique(self) -> None:
        assert uuid6() != uuid6()

    def test_monotonic_even_with_fixed_clock_seq(self) -> None:
        # ``empty_checkpoint`` pins clock_seq=-2 so ids stay ordered and
        # collision-free within one process; that is the contract the
        # checkpoint marker path relies on.
        first = uuid6(clock_seq=-2)
        second = uuid6(clock_seq=-2)
        assert first < second
        assert first.clock_seq == 16382

    def test_explicit_node_is_used(self) -> None:
        assert uuid6(node=0x123456789ABC).node == 0x123456789ABC


class TestEmptyCheckpoint:
    def test_shape_matches_upstream(self) -> None:
        checkpoint = empty_checkpoint()
        assert list(checkpoint) == [
            "v",
            "id",
            "ts",
            "channel_values",
            "channel_versions",
            "versions_seen",
            "pending_sends",
            "updated_channels",
        ]
        assert checkpoint["v"] == 2
        assert checkpoint["channel_values"] == {}
        assert checkpoint["channel_versions"] == {}
        assert checkpoint["versions_seen"] == {}
        assert checkpoint["pending_sends"] == []
        assert checkpoint["updated_channels"] is None

    def test_id_is_ordered_uuid6_string(self) -> None:
        first = empty_checkpoint()
        second = empty_checkpoint()
        assert uuid.UUID(first["id"]).version == 6
        assert first["id"] < second["id"]

    def test_ts_is_utc_isoformat(self) -> None:
        checkpoint = empty_checkpoint()
        assert checkpoint["ts"].endswith("+00:00")
        assert "T" in checkpoint["ts"]

    def test_instances_do_not_share_mutable_state(self) -> None:
        first = empty_checkpoint()
        first["channel_values"]["messages"] = ["x"]
        assert empty_checkpoint()["channel_values"] == {}


class TestCommand:
    def test_fields_and_defaults(self) -> None:
        command = Command()
        assert command.graph is None
        assert command.update is None
        assert command.resume is None
        assert command.goto == ()

    def test_constructs_from_keywords(self) -> None:
        command = Command(graph="sub", update={"x": 1}, resume="answer", goto="tools")
        assert command.graph == "sub"
        assert command.update == {"x": 1}
        assert command.resume == "answer"
        assert command.goto == "tools"

    def test_goto_accepts_sequence(self) -> None:
        assert Command(goto=("a", "b")).goto == ("a", "b")

    def test_equality_is_by_value(self) -> None:
        assert Command(update={"x": 1}, goto="a") == Command(update={"x": 1}, goto="a")
        assert Command(update={"x": 1}) != Command(update={"x": 2})

    def test_unhashable(self) -> None:
        with pytest.raises(TypeError):
            hash(Command(update={"x": 1}))

    def test_slots(self) -> None:
        assert Command.__slots__ == ("graph", "update", "resume", "goto")

    def test_repr_omits_empty_fields(self) -> None:
        assert repr(Command(update={"x": 1}, goto="a")) == "Command(update={'x': 1}, goto='a')"

    def test_is_subscriptable_like_the_host_command(self) -> None:
        """``Command[Any]`` is a public host annotation surface.

        LangGraph's ``ToolNode`` resolves node signatures with
        ``typing.get_type_hints``. Upstream's todo tools are annotated
        ``-> Command[Any]``, so the engine's host-bridged ``Command`` must
        remain generic even though it is defined by subclassing.
        """
        import typing

        host_types = pytest.importorskip("langgraph.types")
        assert host_types.Command[typing.Any]
        assert Command[typing.Any]
        assert Command.__parameters__ != ()


class TestSend:
    def test_carries_node_and_state(self) -> None:
        send = Send("worker", {"n": 1})
        assert send.node == "worker"
        assert send.arg == {"n": 1}

    def test_equality_is_by_value(self) -> None:
        assert Send("w", {"n": 1}) == Send("w", {"n": 1})
        assert Send("w", {"n": 1}) != Send("w", {"n": 2})


class TestHostCommandInterop:
    """Hosts that annotate ``isinstance(x, langgraph.types.Command)``.

    LangGraph's ``Command`` is a plain class (not an ABC), so virtual
    subclass registration is impossible. The engine therefore subclasses the
    host class when it is importable, which keeps every host ``isinstance``
    check true while preserving the engine's own surface.
    """

    def test_engine_command_is_a_host_command_when_available(self) -> None:
        host_types = pytest.importorskip("langgraph.types")
        assert issubclass(Command, host_types.Command)
        assert isinstance(Command(update={"x": 1}), host_types.Command)

    def test_host_and_engine_commands_are_mutually_recognised(self) -> None:
        """Both directions must hold so mixed imports cannot disagree.

        DeerFlow still imports ``Command`` from ``langgraph.types`` in the
        gateway while its middleware returns the engine's ``Command``; either
        side must satisfy the other's ``isinstance`` check.
        """
        host_types = pytest.importorskip("langgraph.types")
        assert isinstance(host_types.Command(update={"x": 1}), Command)
        assert isinstance(Command(update={"x": 1}), host_types.Command)

    def test_engine_surface_is_unchanged_by_the_bridge(self) -> None:
        command = Command(graph="sub", update={"x": 1}, resume="answer", goto="tools")
        assert (command.graph, command.update, command.resume, command.goto) == (
            "sub",
            {"x": 1},
            "answer",
            "tools",
        )
        assert Command.__slots__ == ("graph", "update", "resume", "goto")
        assert repr(Command(update={"x": 1}, goto="a")) == "Command(update={'x': 1}, goto='a')"
        with pytest.raises(TypeError):
            hash(Command(update={"x": 1}))

    def test_without_langgraph_the_native_command_still_works(self) -> None:
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

            from reactivegraph.types import Command
            assert Command(update={"x": 1}).update == {"x": 1}
            print("OK")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert "OK" in result.stdout
