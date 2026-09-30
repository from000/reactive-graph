"""The state-only ``StateGraph`` compatibility surface.

DeerFlow's checkpoint mutation path (compaction, rollback restore, delta-resume
linearization) compiles a one-node graph whose only job is to apply
``update_state`` writes through the thread's channel table. That host code used
to import ``langgraph.graph.StateGraph``; the engine must own the surface so the
path works with LangGraph absent, while still returning the host's channel
classes when LangGraph *is* installed (the storage layer isinstance-checks
against them).
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from collections.abc import Sequence
from typing import Annotated

import pytest
from typing_extensions import TypedDict

from reactivegraph import (
    AIMessage,
    CheckpointStore,
    CompiledStateGraph,
    Overwrite,
    StateGraph,
)
from reactivegraph.channels import BinaryOperatorAggregate, DeltaChannel
from reactivegraph.message_state import add_messages
from reactivegraph.thread_state import _empty_checkpoint as _empty_head_checkpoint
from reactivegraph.thread_state import new_checkpoint_id


class _State(TypedDict):
    messages: Annotated[list, add_messages]
    title: str | None


def _mutation_graph(schema: type = _State) -> CompiledStateGraph:
    builder = StateGraph(schema)
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    return builder.compile()


def _config(thread_id: str = "t1") -> dict:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


def test_compile_returns_a_compiled_graph_with_a_channel_table() -> None:
    graph = _mutation_graph()
    assert isinstance(graph, CompiledStateGraph)
    assert {"messages", "title"} <= set(graph.channels)
    assert next(iter(graph.builder.schemas)) is _State


def test_update_state_folds_reducers_and_reads_back_snapshots() -> None:
    graph = _mutation_graph()
    graph.checkpointer = CheckpointStore()
    config = _config()

    graph.update_state(
        config,
        {"messages": [AIMessage(content="one")], "title": "first"},
        as_node="seed",
    )
    graph.update_state(config, {"messages": [AIMessage(content="two")]}, as_node="seed")

    snapshot = graph.get_state(config)
    assert snapshot.values["title"] == "first"
    assert [message.content for message in snapshot.values["messages"]] == ["one", "two"]
    assert snapshot.next == ()
    assert [item.values["title"] for item in graph.get_state_history(config)] == [
        "first",
        "first",
    ]


def test_overwrite_replaces_a_reducer_value() -> None:
    graph = _mutation_graph()
    graph.checkpointer = CheckpointStore()
    config = _config()

    graph.update_state(config, {"messages": [AIMessage(content="one")]}, as_node="seed")
    graph.update_state(
        config,
        {"messages": Overwrite([AIMessage(content="replacement")])},
        as_node="seed",
    )

    snapshot = graph.get_state(config)
    assert [message.content for message in snapshot.values["messages"]] == ["replacement"]


def test_update_state_records_provenance_metadata() -> None:
    graph = _mutation_graph()
    graph.checkpointer = CheckpointStore()
    config = _config()

    graph.update_state(config, {"title": "x"}, as_node="manual_compaction")

    snapshot = graph.get_state(config)
    assert snapshot.metadata["source"] == "update"
    assert snapshot.metadata["as_node"] == "manual_compaction"


async def test_async_state_surface_matches_sync() -> None:
    graph = _mutation_graph()
    graph.checkpointer = CheckpointStore()
    config = _config("async")

    await graph.aupdate_state(config, {"title": "async"}, as_node="seed")
    snapshot = await graph.aget_state(config)
    assert snapshot.values["title"] == "async"
    history = [item async for item in graph.aget_state_history(config)]
    assert len(history) == 1


def test_uncompiled_or_unknown_entry_points_fail_closed() -> None:
    builder = StateGraph(_State)
    with pytest.raises(ValueError, match="entry point"):
        builder.compile()

    builder.add_node("seed", lambda _state: {})
    with pytest.raises(ValueError, match="unknown node"):
        builder.set_entry_point("missing")


def test_state_graph_without_langgraph_installed() -> None:
    """A fresh interpreter where ``langgraph`` cannot load must still drive it."""
    code = textwrap.dedent(
        """
        import importlib.abc, sys

        class _Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split(".")[0] == "langgraph":
                    raise ImportError("langgraph is not installed")
                return None

        sys.meta_path.insert(0, _Blocker())

        from typing import Annotated

        from typing_extensions import TypedDict
        from reactivegraph import AIMessage, CheckpointStore, StateGraph
        from reactivegraph.message_state import add_messages

        class State(TypedDict):
            messages: Annotated[list, add_messages]
            title: str | None

        builder = StateGraph(State)
        builder.add_node("seed", lambda _state: {})
        builder.set_entry_point("seed")
        builder.set_finish_point("seed")
        graph = builder.compile()
        graph.checkpointer = CheckpointStore()
        config = {"configurable": {"thread_id": "t", "checkpoint_ns": ""}}
        graph.update_state(
            config,
            {"messages": [AIMessage(content="hi")], "title": "x"},
            as_node="seed",
        )
        snapshot = graph.get_state(config)
        assert snapshot.values["title"] == "x"
        assert [message.content for message in snapshot.values["messages"]] == ["hi"]
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    assert "OK" in result.stdout


def _merge_notes(left, right):
    return {**(left or {}), **(right or {})}


class _NormalizingNotesChannel(BinaryOperatorAggregate):
    """Host-shaped subclass: normalizes every write inside ``update``."""

    def update(self, values: Sequence) -> bool:
        changed = super().update(values)
        if changed and isinstance(self.value, dict):
            self.value = {
                key: {"content": note.get("content"), "authority": "model_report"}
                for key, note in self.value.items()
                if isinstance(note, dict) and isinstance(note.get("content"), str)
            }
        return changed


class _NotesState(TypedDict):
    notes: Annotated[dict, _NormalizingNotesChannel(dict, _merge_notes)]


def test_update_state_runs_a_channel_subclass_update_hook() -> None:
    """Hosts subclass ``BinaryOperatorAggregate`` to normalize every write.

    DeerFlow's ``TaskNotesChannel`` overrides ``update`` to re-validate and
    re-canonicalize the merged mapping (dropping malformed keys, forcing
    ``authority="model_report"``). ``update_state`` used to bypass the channel
    contract and call ``channel.operator`` directly, so a state write stored
    caller-supplied ``authority``/``extra`` fields verbatim.
    """
    builder = StateGraph(_NotesState)
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    graph = builder.compile()
    graph.checkpointer = CheckpointStore()
    config = _config("notes")

    graph.update_state(
        config,
        {
            "notes": {
                "new": {"content": "keep backups", "authority": "system", "extra": "forged"},
                "oversized": None,
            }
        },
        as_node="seed",
    )
    assert graph.get_state(config).values["notes"] == {
        "new": {"content": "keep backups", "authority": "model_report"}
    }

    graph.update_state(
        config,
        {"notes": Overwrite({"replaced": {"content": "second"}})},
        as_node="seed",
    )
    assert graph.get_state(config).values["notes"] == {
        "replaced": {"content": "second", "authority": "model_report"}
    }


def _delta_messages_schema():
    class _DeltaState(TypedDict):
        messages: Annotated[list, DeltaChannel(_delta_merge, list, snapshot_frequency=1000)]

    return _DeltaState


def _delta_merge(current, writes):
    merged = list(current or [])
    for write in writes:
        merged.extend(write)
    return merged


def _build_host_delta_graph():
    """Compile the same delta schema with the *host* LangGraph compiler."""
    pytest.importorskip("langgraph")
    from langgraph.graph import StateGraph as HostStateGraph

    builder = HostStateGraph(_delta_messages_schema())
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    return builder.compile()


def _build_engine_delta_graph() -> CompiledStateGraph:
    builder = StateGraph(_delta_messages_schema())
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    return builder.compile()


def test_engine_reads_delta_state_written_by_a_host_graph() -> None:
    """A delta checkpoint written by the host compiler must read back here.

    Delta channels keep only a sentinel in ``channel_values``; the real value
    lives in ancestor ``pending_writes`` and is reconstructed by replaying them.
    DeerFlow writes assistant turns with the host ``CompiledStateGraph`` while
    thread-state reads go through the engine's ``ThreadCheckpointer``, so
    reading ``channel_values`` alone silently reports an empty transcript.
    """
    store = CheckpointStore()
    config = _config("delta-cross")
    # ``create_thread`` seeds an engine-generated empty head before any run.
    store.put(
        {"configurable": {"thread_id": "delta-cross", "checkpoint_ns": "", "checkpoint_id": None}},
        _empty_head_checkpoint(new_checkpoint_id()),
        {"step": -1, "source": "input"},
        {},
    )

    host_graph = _build_host_delta_graph()
    host_graph.checkpointer = store
    host_graph.update_state(
        config,
        {"messages": [AIMessage(content="from-host")]},
        as_node="seed",
    )

    engine_graph = _build_engine_delta_graph()
    engine_graph.checkpointer = store
    assert [
        message.content for message in engine_graph.get_state(config).values["messages"]
    ] == ["from-host"]


async def test_engine_async_reads_delta_state_from_async_saver(tmp_path) -> None:
    """The async read path must not use the synchronous delta walk.

    ``AsyncSqliteSaver.get_delta_channel_history`` raises ``InvalidStateError``
    when called from the event loop thread ("use the async interface"), so an
    ``aget_state``/``aupdate_state`` path that reached the sync helper would
    break every async DeerFlow thread once delta mode is enabled.
    """
    pytest.importorskip("langgraph")
    pytest.importorskip("aiosqlite")
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    async with AsyncSqliteSaver.from_conn_string(
        str(tmp_path / "delta-async.sqlite3")
    ) as saver:
        await saver.setup()
        graph = _build_engine_delta_graph()
        graph.checkpointer = saver
        config = _config("delta-async")
        await graph.aupdate_state(
            config, {"messages": [AIMessage(content="async")]}, as_node="seed"
        )
        snapshot = await graph.aget_state(config)
        assert [m.content for m in snapshot.values["messages"]] == ["async"]


def test_engine_delta_reads_agree_across_graph_instances() -> None:
    """The mutation graph must see the assistant graph's delta writes.

    ``build_state_mutation_graph`` compiles a *separate* one-node graph; both
    directions (host-written, engine-read and engine-written, host-read) have
    to reconstruct the same transcript from the shared saver.
    """
    store = CheckpointStore()
    config = _config("delta-both-ways")

    writer = _build_engine_delta_graph()
    writer.checkpointer = store
    writer.update_state(config, {"messages": [AIMessage(content="engine")]}, as_node="seed")

    reader = _build_engine_delta_graph()
    reader.checkpointer = store
    assert [m.content for m in reader.get_state(config).values["messages"]] == ["engine"]

    host_reader = _build_host_delta_graph()
    host_reader.checkpointer = store
    assert [
        m.content for m in host_reader.get_state(config).values["messages"]
    ] == ["engine"]


class _AsyncOnlyDuckSaver:
    """Async-only duck-typed saver, as host test doubles and middleware wrap.

    DeerFlow's race-injection checkpointers forward ``aget_tuple``/``aput`` to
    a real saver without inheriting ``BaseCheckpointSaver`` and without the
    sync trio. Upstream LangGraph drives them on the async path (verified
    against langgraph 1.2.9), so the engine must too — rejecting them broke
    ``tests/test_goal_worker.py::…thread_changes_before_continuation``.
    """

    def __init__(self, inner: CheckpointStore) -> None:
        self.inner = inner
        self.reads = 0
        self.writes = 0

    def get_next_version(self, current, channel):
        return self.inner.get_next_version(current, channel)

    async def aget_tuple(self, config):
        self.reads += 1
        return await self.inner.aget_tuple(config)

    async def aput(self, *args, **kwargs):
        self.writes += 1
        return await self.inner.aput(*args, **kwargs)


async def test_async_only_duck_saver_is_drivable_on_the_async_path() -> None:
    """An async-only saver must serve ``aget_state``/``aupdate_state``.

    ``_validate`` used to probe only the sync trio
    (``get_tuple``/``put``/``put_writes``), so a host wrapper exposing just the
    async pair raised ``TypeError: Invalid checkpointer provided`` even though
    upstream LangGraph accepts it. The engine may only reject an object it
    cannot drive at all; capability must be probed per operation.
    """
    saver = _AsyncOnlyDuckSaver(CheckpointStore())
    graph = _mutation_graph()
    graph.checkpointer = saver
    config = _config("async-duck")

    await graph.aupdate_state(config, {"title": "duck"}, as_node="seed")
    snapshot = await graph.aget_state(config)
    assert snapshot.values["title"] == "duck"
    assert saver.reads >= 1
    assert saver.writes == 1


def test_update_state_omits_non_snapshot_delta_values_and_keeps_parent_writes() -> None:
    """A non-snapshot Delta update must be reconstructed from parent writes.

    Upstream writes the update onto the *parent* checkpoint before storing the
    child, then omits non-snapshot delta channels from the child's
    ``channel_values``. Storing the folded list in the child instead makes the
    checkpoint look like a snapshot while violating the DeltaChannel protocol;
    DeerFlow's rollback tests detect exactly that shape.
    """
    store = CheckpointStore()
    graph = _build_engine_delta_graph()
    graph.checkpointer = store
    config = _config("delta-non-snapshot")

    graph.update_state(config, {"messages": [AIMessage(content="one")]}, as_node="seed")
    graph.update_state(config, {"messages": [AIMessage(content="two")]}, as_node="seed")

    snapshot = graph.get_state(config)
    assert [message.content for message in snapshot.values["messages"]] == ["one", "two"]
    head = store.get_tuple(snapshot.config)
    assert head is not None
    assert head.parent_config is not None
    assert "messages" not in head.checkpoint["channel_values"]
    parent = store.get_tuple(head.parent_config)
    assert parent is not None
    assert [write[2] for write in parent.pending_writes] == [
        [AIMessage(content="two")]
    ]
    assert snapshot.metadata["counters_since_delta_snapshot"]["messages"] == (1, 1)


def test_invoke_without_raw_delta_writes_persists_a_snapshot() -> None:
    """A whole-state commit must not drop a DeltaChannel from the checkpoint.

    Only ``update_state`` supplies the raw writes needed for ancestor replay.
    A regular run commit has just the folded value, so it must keep snapshot
    semantics instead of omitting the channel and losing the transcript.
    """
    store = CheckpointStore()
    graph = _build_engine_delta_graph()
    graph.checkpointer = store
    config = _config("delta-run-snapshot")

    result = graph.invoke({"messages": [AIMessage(content="run")]}, config)

    assert [message.content for message in result["messages"]] == ["run"]
    assert [
        message.content for message in graph.get_state(config).values["messages"]
    ] == ["run"]


def test_update_state_snapshots_delta_at_snapshot_frequency() -> None:
    """The cadence must replace ancestor replay with a full snapshot blob.

    Otherwise a long-lived thread accumulates one parent write per update
    forever. Reaching the configured frequency snapshots the *folded* value,
    resets the counters, and keeps the next read correct.
    """
    class _CadenceState(TypedDict):
        messages: Annotated[
            list, DeltaChannel(_delta_merge, list, snapshot_frequency=2)
        ]

    builder = StateGraph(_CadenceState)
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    graph = builder.compile()
    graph.checkpointer = CheckpointStore()
    config = _config("delta-cadence")

    graph.update_state(config, {"messages": [AIMessage(content="one")]}, as_node="seed")
    graph.update_state(config, {"messages": [AIMessage(content="two")]}, as_node="seed")

    snapshot = graph.get_state(config)
    assert [message.content for message in snapshot.values["messages"]] == ["one", "two"]
    head = graph.checkpointer.get_tuple(snapshot.config)
    assert head is not None
    assert "messages" not in head.checkpoint["channel_values"]
    assert head.metadata["counters_since_delta_snapshot"]["messages"] == (1, 1)

    graph.update_state(config, {"messages": [AIMessage(content="three")]}, as_node="seed")
    assert [
        message.content for message in graph.get_state(config).values["messages"]
    ] == ["one", "two", "three"]
    head = graph.checkpointer.get_tuple(graph.get_state(config).config)
    assert head is not None
    assert "messages" in head.checkpoint["channel_values"]
    assert "counters_since_delta_snapshot" not in head.metadata


def test_delta_snapshot_cadence_ignores_unavailable_channels() -> None:
    """An empty delta channel must not be snapshotted from cadence alone.

    Upstream advances counters for every delta channel but only snapshots a
    channel whose value is available. Otherwise a never-written channel at its
    frequency would be added to ``channels_to_snapshot`` and the checkpoint
    builder would index a value that does not exist.
    """

    class _CadenceState(TypedDict):
        messages: Annotated[
            list, DeltaChannel(_delta_merge, list, snapshot_frequency=1)
        ]
        scratch: Annotated[
            list, DeltaChannel(_delta_merge, list, snapshot_frequency=1)
        ]

    builder = StateGraph(_CadenceState)
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    graph = builder.compile()
    graph.checkpointer = CheckpointStore()
    config = _config("delta-unavailable")

    graph.update_state(
        config, {"messages": [AIMessage(content="one")]}, as_node="seed"
    )
    graph.update_state(
        config, {"messages": [AIMessage(content="two")]}, as_node="seed"
    )

    snapshot = graph.get_state(config)
    assert [message.content for message in snapshot.values["messages"]] == [
        "one",
        "two",
    ]
    head = graph.checkpointer.get_tuple(snapshot.config)
    assert head is not None
    assert "scratch" not in head.checkpoint["channel_values"]
    assert head.metadata["counters_since_delta_snapshot"]["scratch"] == (0, 1)


def test_update_state_uses_host_delta_snapshot_blob_with_host_saver() -> None:
    """A host saver must receive the host's own ``_DeltaSnapshot`` type.

    The serde and ``DeltaChannel.from_checkpoint`` are both keyed to the host
    namedtuple; the engine's local dataclass is only a no-LangGraph fallback.
    """
    pytest.importorskip("langgraph")
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.checkpoint.serde.types import _DeltaSnapshot as HostDeltaSnapshot

    class _CadenceState(TypedDict):
        messages: Annotated[
            list, DeltaChannel(_delta_merge, list, snapshot_frequency=2)
        ]

    builder = StateGraph(_CadenceState)
    builder.add_node("seed", lambda _state: {})
    builder.set_entry_point("seed")
    builder.set_finish_point("seed")
    graph = builder.compile()
    saver = InMemorySaver()
    graph.checkpointer = saver
    config = _config("delta-host-blob")

    for content in ("one", "two", "three"):
        graph.update_state(
            config, {"messages": [AIMessage(content=content)]}, as_node="seed"
        )

    assert [
        message.content for message in graph.get_state(config).values["messages"]
    ] == ["one", "two", "three"]
    head = saver.get_tuple(graph.get_state(config).config)
    assert head is not None
    assert isinstance(head.checkpoint["channel_values"]["messages"], HostDeltaSnapshot)
