"""The channel classes hosts subclass must exist in the engine.

DeerFlow annotates state fields with ``DeltaChannel`` and defines its own
``BinaryOperatorAggregate`` subclass (``TaskNotesChannel``). Both used to come
from ``langgraph.channels``; the engine now owns the classes so a host can drop
LangGraph without rewriting its state schemas. These tests pin the behaviour
against the real LangGraph implementation, because a channel that folds writes
differently silently corrupts checkpointed state.
"""

from __future__ import annotations

from typing import Annotated

import pytest
from typing_extensions import TypedDict

from reactivegraph.channels import (
    BaseChannel,
    BinaryOperatorAggregate,
    DeltaChannel,
    channel_reducer,
)
from reactivegraph.errors import EmptyChannelError, InvalidUpdateError
from reactivegraph.types import Overwrite

try:  # pragma: no cover - langgraph is an optional comparison dependency
    from langgraph import channels as lg_channels
except ImportError:  # pragma: no cover
    lg_channels = None

requires_lg = pytest.mark.skipif(lg_channels is None, reason="langgraph not installed")


def _sum_reducer(state, writes):
    for w in writes:
        state = state + w
    return state


_COMPILER_PROBE_CHANNEL = DeltaChannel(_sum_reducer, list, snapshot_frequency=7)


class _CompilerProbeState(TypedDict, total=False):
    messages: Annotated[list, _COMPILER_PROBE_CHANNEL]


# ---------------------------------------------------------------------------
# BaseChannel / BinaryOperatorAggregate
# ---------------------------------------------------------------------------


def test_base_channel_is_abstract() -> None:
    with pytest.raises(TypeError):
        BaseChannel(dict)  # type: ignore[abstract]


def test_binop_folds_each_write_and_reports_empty() -> None:
    ch = BinaryOperatorAggregate(list, lambda a, b: a + b)
    # ``list()`` is a valid seed value, so a fresh list channel is available
    # and empty -- exactly like LangGraph's. ``EmptyChannelError`` is only
    # reachable for types whose constructor raises (e.g. ``int``).
    assert ch.is_available() is True
    assert ch.get() == []
    class _Uninstantiable:
        def __init__(self) -> None:
            raise TypeError("no default value")

    with pytest.raises(EmptyChannelError):
        BinaryOperatorAggregate(_Uninstantiable, lambda a, b: a).get()

    assert ch.update([["a"], ["b"]]) is True
    assert ch.get() == ["a", "b"]
    assert ch.is_available() is True
    assert ch.update([]) is False


def test_binop_overwrite_replaces_and_rejects_two_per_step() -> None:
    ch = BinaryOperatorAggregate(list, lambda a, b: a + b)
    ch.update([["a"], ["b"]])
    ch.update([Overwrite(["z"])])
    assert ch.get() == ["z"]

    with pytest.raises(InvalidUpdateError):
        ch.update([Overwrite(["x"]), Overwrite(["y"])])


def test_binop_unwraps_an_overwrite_first_write() -> None:
    """A replace-style write into a fresh Union channel must not store the wrapper.

    LangGraph has this bug (DeerFlow #4380) and the harness carries an
    import-time monkeypatch for it. The engine implements the patched
    behaviour directly, so that host patch becomes a no-op.
    """
    ch = BinaryOperatorAggregate(dict | None, lambda a, b: b)
    ch.update([Overwrite({"probe": True})])
    assert ch.get() == {"probe": True}

    with pytest.raises(InvalidUpdateError):
        BinaryOperatorAggregate(dict | None, lambda a, b: b).update(
            [Overwrite({"a": 1}), Overwrite({"b": 2})]
        )


def test_binop_accepts_every_serialised_overwrite_form() -> None:
    """A state update may round-trip through JSON and lose the dataclass."""
    for payload in ({"__overwrite__": ["z"]}, {"type": "__overwrite__", "value": ["z"]}):
        ch = BinaryOperatorAggregate(list, lambda a, b: a + b)
        ch.update([["a"]])
        ch.update([payload])
        assert ch.get() == ["z"], payload


def test_binop_accepts_a_host_overwrite_instance() -> None:
    """A host graph's ``Overwrite`` is a different class object.

    DeerFlow's patched write paths build ``langgraph.types.Overwrite`` while
    the channels they target are the engine's; recognising only our own class
    stores the wrapper literally (DeerFlow #4380 resurfacing through the
    migration).
    """
    host_types = pytest.importorskip("langgraph.types")
    ch = BinaryOperatorAggregate(dict | None, lambda existing, new: new)
    ch.key = "probe"

    ch.update([host_types.Overwrite({"sandbox_id": "local:thread-2"})])

    stored = ch.get()
    assert not isinstance(stored, host_types.Overwrite)
    assert stored == {"sandbox_id": "local:thread-2"}


def test_binop_copy_and_checkpoint_round_trip() -> None:
    ch = BinaryOperatorAggregate(list, lambda a, b: a + b)
    ch.update([["a"]])
    clone = ch.copy()
    assert clone.get() == ["a"]
    clone.update([["b"]])
    assert ch.get() == ["a"], "copy must not share mutable state"

    restored = ch.from_checkpoint(ch.checkpoint())
    assert restored.get() == ["a"]


def test_binop_empty_channel_checkpoint_is_a_sentinel_not_none() -> None:
    ch = BinaryOperatorAggregate(list, lambda a, b: a + b)
    assert ch.checkpoint() is not None


# ---------------------------------------------------------------------------
# DeltaChannel
# ---------------------------------------------------------------------------


def test_delta_reducer_receives_the_whole_batch() -> None:
    ch = DeltaChannel(_sum_reducer, list, snapshot_frequency=10)
    ch.update([[1], [2], [3]])
    assert ch.get() == [1, 2, 3]


def test_delta_is_batching_invariant() -> None:
    """Folding xs then ys must equal folding xs+ys — the replay guarantee."""
    batched = DeltaChannel(_sum_reducer, list)
    batched.update([[1], [2]])
    batched.update([[3]])

    once = DeltaChannel(_sum_reducer, list)
    once.update([[1], [2], [3]])

    assert batched.get() == once.get()


def test_delta_rejects_a_non_positive_snapshot_frequency() -> None:
    with pytest.raises(ValueError):
        DeltaChannel(_sum_reducer, list, snapshot_frequency=0)


def test_delta_checkpoint_is_always_empty_sentinel() -> None:
    """Snapshots are decided by create_checkpoint, not by the channel."""
    ch = DeltaChannel(_sum_reducer, list)
    ch.update([[1]])
    assert ch.checkpoint() is not None
    assert ch.is_available() is True


def test_delta_replay_writes_folds_oldest_to_newest() -> None:
    ch = DeltaChannel(_sum_reducer, list)
    ch.value = []
    ch.replay_writes([("t1", "messages", [1]), ("t2", "messages", [2])])
    assert ch.get() == [1, 2]


def test_delta_replay_writes_restarts_after_the_last_overwrite() -> None:
    ch = DeltaChannel(_sum_reducer, list)
    ch.value = []
    ch.replay_writes(
        [("t1", "messages", [1]), ("t2", "messages", Overwrite([9])), ("t3", "messages", [4])]
    )
    assert ch.get() == [9, 4]


def test_delta_overwrite_on_empty_channel_is_not_stored_verbatim() -> None:
    """Regression guard for the Overwrite-on-empty-channel bug (#4380)."""
    ch = DeltaChannel(_sum_reducer, list)
    ch.update([Overwrite([7])])
    assert ch.get() == [7]


# ---------------------------------------------------------------------------
# channel_reducer
# ---------------------------------------------------------------------------


def test_channel_reducer_adapts_delta_batch_to_single_write() -> None:
    ch = DeltaChannel(_sum_reducer, list)
    fold = channel_reducer(ch)
    assert fold is not None
    assert fold([1], [2]) == [1, 2]


def test_channel_reducer_returns_binop_operator_verbatim() -> None:
    def op(a, b):
        return a + b

    ch = BinaryOperatorAggregate(list, op)
    assert channel_reducer(ch) is op


# ---------------------------------------------------------------------------
# Cross-engine equality (the swap must not change folding)
# ---------------------------------------------------------------------------


@requires_lg
def test_our_binop_matches_langgraph_fold_for_fold() -> None:
    # The serialised Overwrite form is understood by both engines; each
    # engine's native ``Overwrite`` dataclass is private to it.
    values = [["a"], ["b"], {"__overwrite__": ["z"]}, ["c"]]
    ours = BinaryOperatorAggregate(list, lambda a, b: a + b)
    theirs = lg_channels.BinaryOperatorAggregate(list, lambda a, b: a + b)
    for chunk in ([values[0]], [values[1], values[2]], [values[3]]):
        ours.update(chunk)
        theirs.update(chunk)
    assert ours.get() == theirs.get()


@requires_lg
def test_our_delta_matches_langgraph_fold_for_fold() -> None:
    ours = DeltaChannel(_sum_reducer, list, snapshot_frequency=5)
    theirs = lg_channels.DeltaChannel(_sum_reducer, list, snapshot_frequency=5)
    for chunk in ([[1], [2]], [[3]], []):
        assert ours.update(chunk) == theirs.update(chunk)
    assert ours.get() == theirs.get()


# ---------------------------------------------------------------------------
# Host identity bridge
# ---------------------------------------------------------------------------


@requires_lg
def test_our_delta_channel_is_recognised_by_the_host_compiler() -> None:
    """Upstream ``StateGraph`` only honours its own ``BaseChannel`` instances.

    DeerFlow's ``DeltaThreadState`` is annotated with *our* ``DeltaChannel``;
    without the bridge upstream silently compiles ``messages`` as
    ``LastValue``, dropping the delta/replay contract and its snapshot cadence.
    """
    from langgraph.graph import StateGraph

    builder = StateGraph(_CompilerProbeState)
    builder.add_node("noop", lambda state: {})
    builder.set_entry_point("noop")
    graph = builder.compile()
    compiled = graph.channels["messages"]
    assert isinstance(compiled, lg_channels.DeltaChannel)
    assert compiled.snapshot_frequency == 7
    assert compiled.reducer is _sum_reducer


@requires_lg
def test_host_delta_channel_is_recognised_by_the_engine() -> None:
    """The reverse direction: a schema may still carry a host channel."""
    host_channel = lg_channels.DeltaChannel(_sum_reducer, list, snapshot_frequency=9)
    assert isinstance(host_channel, DeltaChannel)


@requires_lg
def test_engine_delta_channel_accepts_host_checkpoint_snapshot() -> None:
    """Upstream's ``_DeltaSnapshot`` must restore through our channel.

    ``StateGraph`` writes host ``_DeltaSnapshot`` blobs into checkpoints; the
    engine's ``from_checkpoint`` has to recognise that shape or every restored
    delta thread fails closed with an opaque attribute error.
    """
    from langgraph.checkpoint.serde.types import _DeltaSnapshot

    ch = DeltaChannel(_sum_reducer, list, snapshot_frequency=3)
    restored = ch.from_checkpoint(_DeltaSnapshot([1, 2, 3]))
    assert restored.get() == [1, 2, 3]


def test_delta_channel_imports_without_langgraph() -> None:
    """The bridge must stay optional, exactly like the middleware identity."""
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

        from reactivegraph.channels import DeltaChannel
        channel = DeltaChannel(lambda state, writes: state + writes, list)
        channel.update([[1], [2]])
        assert channel.get() == [[1], [2]]
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr[-3000:]
    assert "OK" in result.stdout


@requires_lg
def test_engine_delta_channel_accepts_host_missing_sentinel() -> None:
    """Upstream Pregel passes *its* ``MISSING`` into ``from_checkpoint``.

    ``channels_from_checkpoint`` calls ``spec.from_checkpoint(...get(k, MISSING))``
    with LangGraph's private sentinel. A channel that only recognises its own
    sentinel stores that object as the channel value, and the first reducer
    call then folds ``<object object>`` into user state (observed as
    ``NotImplementedError: Unsupported message type: <class 'object'>``).
    """
    from langgraph._internal._typing import MISSING as HOST_MISSING

    ch = DeltaChannel(_sum_reducer, list, snapshot_frequency=3)
    restored = ch.from_checkpoint(HOST_MISSING)
    assert restored.get() == []
    assert restored.is_available()


@requires_lg
def test_engine_delta_channel_checkpoint_uses_host_missing_sentinel() -> None:
    """The outbound direction: upstream skips only *its own* sentinel.

    ``create_checkpoint`` writes ``values[k] = v`` whenever
    ``v is not MISSING``. Returning a private sentinel makes upstream persist
    a placeholder object as if it were real state.
    """
    from langgraph._internal._typing import MISSING as HOST_MISSING

    ch = DeltaChannel(_sum_reducer, list, snapshot_frequency=3)
    assert ch.checkpoint() is HOST_MISSING


@requires_lg
def test_upstream_pregel_round_trip_through_engine_delta_channel() -> None:
    """End-to-end: a real host ``StateGraph`` must fold engine-channel writes.

    This is the DeerFlow delta-mode path: the state schema carries our
    ``DeltaChannel``, upstream compiles and runs the graph, and the first
    super-step must not leak a sentinel into the reducer.
    """
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import StateGraph

    builder = StateGraph(_CompilerProbeState)
    builder.add_node("writer", lambda state: {"messages": [1, 2]})
    builder.set_entry_point("writer")
    builder.set_finish_point("writer")
    graph = builder.compile(checkpointer=InMemorySaver())
    graph.invoke({}, {"configurable": {"thread_id": "delta-sentinel"}})
    assert graph.get_state({"configurable": {"thread_id": "delta-sentinel"}}).values[
        "messages"
    ] == [1, 2]


@requires_lg
def test_engine_empty_channel_error_is_a_host_empty_channel_error() -> None:
    """Upstream catches *its own* ``EmptyChannelError`` by identity.

    ``read_channel`` wraps ``channels[chan].get()`` in
    ``except EmptyChannelError``; an engine channel raising a different class
    aborts the whole read instead of being treated as "empty", which is
    exactly the DeerFlow delta-replay failure.
    """
    from langgraph.checkpoint.base import EmptyChannelError as HostEmptyChannelError

    assert issubclass(EmptyChannelError, HostEmptyChannelError)


@requires_lg
def test_upstream_read_channel_swallows_engine_empty_error() -> None:
    """Behavioural form of the same contract, using upstream's own helper."""
    from langgraph.pregel._io import read_channels

    # A Union-typed channel has no constructible default, so it stays
    # genuinely empty until its first write -- the TaskNotesChannel shape.
    channel = BinaryOperatorAggregate(int | str, lambda a, b: b)
    assert not channel.is_available()
    assert read_channels({"task_notes": channel}, ["task_notes"]) == {}


@requires_lg
def test_resolving_a_host_channel_class_does_not_mutate_it() -> None:
    """A schema may annotate a field with a host channel *class*.

    ``copy.copy`` on a class returns the same class object, so writing the
    field name onto the "copy" wrote ``key`` onto LangGraph's
    ``EphemeralValue`` class itself. That class attribute shadowed
    ``BaseChannel``'s slot descriptor process-wide, and every later
    ``EphemeralValue(typ)`` anywhere in the interpreter died with
    ``AttributeError: 'EphemeralValue' object attribute 'key' is read-only``.
    Measured: 23 fork tests failed on graphs built after such a schema was
    resolved, even though the engine's own channels were unaffected.
    """
    from reactivegraph.channels import resolve_channels

    host_class = lg_channels.EphemeralValue
    assert "key" not in host_class.__dict__

    class State(TypedDict):
        jump_to: Annotated[object, host_class]

    State.__annotations__["jump_to"] = Annotated[object, host_class]
    resolved = resolve_channels([State])
    assert "key" not in host_class.__dict__, "resolving must not write to the host class"
    assert isinstance(resolved["jump_to"], host_class)
    assert resolved["jump_to"].key == "jump_to"
    host_class(object)  # the host class must remain constructible


@requires_lg
def test_host_binop_channel_is_recognised_by_the_engine() -> None:
    """The reverse of the delta bridge: a schema may carry a host binop channel.

    ``_channel_for`` hands back the *host* class for a two-argument reducer
    annotation (the storage layer isinstance-checks against it). Consumers that
    ask the engine whether a channel folds writes — DeerFlow's
    ``graph_reducer_channels``, which decides where an ``Overwrite`` wrapper is
    required — must recognise that instance, or replacement writes silently
    merge into the reducer instead of replacing.
    """
    host_channel = lg_channels.BinaryOperatorAggregate(list, _sum_reducer)
    assert isinstance(host_channel, BinaryOperatorAggregate)
    assert channel_reducer(host_channel) is _sum_reducer


@requires_lg
def test_host_binop_identity_does_not_widen_sibling_subclasses() -> None:
    """Only the host's own base is bridged, never an unrelated sibling."""

    class Unrelated:
        pass

    assert not isinstance(Unrelated(), BinaryOperatorAggregate)


@requires_lg
def test_resolved_binop_channel_is_recognised_as_a_reducer() -> None:
    """A schema-annotated reducer must be visible as a reducer after resolution."""
    from reactivegraph.channels import resolve_channels

    class ReducerState(TypedDict):
        notes: Annotated[dict, _sum_reducer]

    resolved = resolve_channels([ReducerState])
    assert channel_reducer(resolved["notes"]) is _sum_reducer


@requires_lg
def test_engine_ephemeral_marker_compiles_to_host_ephemeral_channel() -> None:
    """The host compiler must see the engine marker as its own channel.

    ``AgentState.jump_to`` is annotated with the engine's ``EphemeralValue``.
    LangGraph's ``StateGraph`` resolves channel annotations by checking
    ``issubclass(item, BaseChannel)``. A marker that is not a channel is
    silently compiled into a persistent ``LastValue`` instead, so a middleware
    ``jump_to`` directive survives every subsequent super-step.
    """
    from langgraph.checkpoint.base import EmptyChannelError as HostEmptyChannelError
    from langgraph.graph.state import _get_channel

    from reactivegraph.message_state import EphemeralValue as EngineEphemeralValue

    channel = _get_channel("jump_to", Annotated[object, EngineEphemeralValue])
    assert isinstance(channel, EngineEphemeralValue)
    assert isinstance(channel, lg_channels.EphemeralValue)

    with pytest.raises(HostEmptyChannelError):
        channel.get()
    assert channel.update(["model"]) is True
    assert channel.get() == "model"
    assert channel.update([]) is True
    assert channel.is_available() is False



class TestDeltaChannelContract:
    """DeltaChannel's snapshot/replay contract is what delta checkpoints rely on."""

    @staticmethod
    def _channel() -> DeltaChannel[list[int]]:
        return DeltaChannel(lambda current, writes: current + sum(writes, []), list)

    def test_copy_is_independent_and_preserves_configuration(self) -> None:
        channel = self._channel()
        channel.key = "messages"
        channel.update([[1], [2]])
        clone = channel.copy()
        clone.update([[3]])
        assert channel.get() == [1, 2]
        assert clone.get() == [1, 2, 3]
        assert clone.key == "messages"
        assert clone.snapshot_frequency == channel.snapshot_frequency

    def test_from_checkpoint_distinguishes_missing_plain_and_snapshot(self) -> None:
        from reactivegraph.channels import _MISSING, _DeltaSnapshot

        channel = self._channel()
        assert channel.from_checkpoint(_MISSING).get() == []
        assert channel.from_checkpoint([1, 2]).get() == [1, 2]
        assert channel.from_checkpoint(_DeltaSnapshot([3])).get() == [3]

    def test_replay_writes_uses_the_last_overwrite_as_the_base(self) -> None:
        from reactivegraph.types import Overwrite

        channel = self._channel()
        channel.replay_writes(
            [
                ("t", "messages", [1]),
                ("t", "messages", [2]),
                ("t", "messages", Overwrite([9])),
                ("t", "messages", [3]),
            ]
        )
        assert channel.get() == [9, 3]

    def test_two_overwrites_in_one_superstep_are_rejected(self) -> None:
        from reactivegraph.errors import InvalidUpdateError
        from reactivegraph.types import Overwrite

        channel = self._channel()
        with pytest.raises(InvalidUpdateError, match="only one Overwrite"):
            channel.update([Overwrite([1]), Overwrite([2])])

    def test_checkpoint_always_omits_delta_state(self) -> None:
        from reactivegraph.channels import _MISSING

        channel = self._channel()
        channel.update([[1]])
        assert channel.checkpoint() is _MISSING


def test_binop_compatibility_import_path_exports_the_engine_channel() -> None:
    from reactivegraph.channels import BinaryOperatorAggregate
    from reactivegraph.channels.binop import BinaryOperatorAggregate as CompatBinop

    assert CompatBinop is BinaryOperatorAggregate
