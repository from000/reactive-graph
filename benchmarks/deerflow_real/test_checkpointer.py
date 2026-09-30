"""Contract tests for the ReactiveGraph checkpointer replacement.

Derived from the real upstream ``backend/tests/test_checkpointer.py`` and the
consumer surface in ``deerflow/client.py`` / ``deerflow/runtime/checkpoint_state.py``:

* ``get_tuple`` / ``aget_tuple``      -> latest or explicit checkpoint
* ``list`` / ``alist``                -> newest-first history, limit, before cursor
* ``put`` / ``aput``                  -> immutable snapshot + parent chain
* ``put_writes``                      -> pending writes attached to a checkpoint
* ``delete_thread``                   -> lifecycle purge
* provider singleton / reset / config  -> parity with upstream factory

No LangChain/LangGraph import is permitted in the implementation.
"""

from __future__ import annotations

import pytest

from deerflow_reactive import (
    ReactiveCheckpointSaver,
    ReactiveCheckpointStore,
    create_deerflow_agent,
    get_checkpointer,
    reset_checkpointer,
)


class FakeModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def invoke(self, messages):
        self.calls += 1
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]


def _cp(checkpoint_id: str, values: dict, ts: str = "2026-01-01T00:00:00+00:00") -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": ts,
        "channel_values": values,
        "channel_versions": {k: 1 for k in values},
        "versions_seen": {},
    }


def _cfg(thread_id: str, checkpoint_id: str | None = None, checkpoint_ns: str = "") -> dict:
    configurable: dict = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


# ---------------------------------------------------------------------------
# Store contract
# ---------------------------------------------------------------------------


def test_store_put_and_get_by_checkpoint_id() -> None:
    store = ReactiveCheckpointStore()
    cfg = _cfg("t1", "cp-1")
    store.put(cfg, _cp("cp-1", {"n": 1}), {"source": "loop"}, {"n": 1})
    tup = store.get_tuple(cfg)
    assert tup is not None
    assert tup.checkpoint["channel_values"] == {"n": 1}
    assert tup.metadata == {"source": "loop"}
    assert tup.config["configurable"]["checkpoint_id"] == "cp-1"


def test_store_get_tuple_without_id_returns_latest() -> None:
    store = ReactiveCheckpointStore()
    store.put(_cfg("t1", "cp-1"), _cp("cp-1", {"n": 1}, "2026-01-01T00:00:00+00:00"), {}, {})
    store.put(_cfg("t1", "cp-2"), _cp("cp-2", {"n": 2}, "2026-01-01T00:00:01+00:00"), {}, {})
    tup = store.get_tuple(_cfg("t1"))
    assert tup is not None
    assert tup.checkpoint["channel_values"] == {"n": 2}


def test_store_get_tuple_unknown_returns_none() -> None:
    store = ReactiveCheckpointStore()
    assert store.get_tuple(_cfg("missing")) is None
    assert store.get_tuple(_cfg("t1", "nope")) is None


def test_store_isolates_threads_and_namespaces() -> None:
    store = ReactiveCheckpointStore()
    store.put(_cfg("a", "cp-1"), _cp("cp-1", {"n": "a"}), {}, {})
    store.put(_cfg("b", "cp-1"), _cp("cp-1", {"n": "b"}), {}, {})
    store.put(_cfg("a", "cp-1", "ns"), _cp("cp-1", {"n": "ns"}), {}, {})
    assert store.get_tuple(_cfg("a", "cp-1")).checkpoint["channel_values"] == {"n": "a"}
    assert store.get_tuple(_cfg("b", "cp-1")).checkpoint["channel_values"] == {"n": "b"}
    assert store.get_tuple(_cfg("a", "cp-1", "ns")).checkpoint["channel_values"] == {"n": "ns"}
    # A namespace-less lookup resolves the root namespace, not the child ns.
    assert store.get_tuple(_cfg("a")).checkpoint["channel_values"] == {"n": "a"}


def test_store_records_parent_chain_and_lists_newest_first() -> None:
    store = ReactiveCheckpointStore()
    first = store.put(_cfg("t1", "cp-1"), _cp("cp-1", {"n": 1}, "2026-01-01T00:00:00+00:00"), {}, {})
    second = store.put(first, _cp("cp-2", {"n": 2}, "2026-01-01T00:00:01+00:00"), {}, {})
    store.put(second, _cp("cp-3", {"n": 3}, "2026-01-01T00:00:02+00:00"), {}, {})

    history = list(store.list(_cfg("t1")))
    assert [t.checkpoint["id"] for t in history] == ["cp-3", "cp-2", "cp-1"]
    assert history[0].parent_config["configurable"]["checkpoint_id"] == "cp-2"
    assert history[-1].parent_config is None


def test_store_list_supports_limit_and_before_cursor() -> None:
    store = ReactiveCheckpointStore()
    cfg = _cfg("t1", "cp-1")
    for i in range(1, 5):
        cfg = store.put(cfg, _cp(f"cp-{i}", {"n": i}, f"2026-01-01T00:00:0{i}+00:00"), {}, {})

    assert [t.checkpoint["id"] for t in store.list(_cfg("t1"), limit=2)] == ["cp-4", "cp-3"]
    rest = list(store.list(_cfg("t1"), before=_cfg("t1", "cp-3")))
    assert [t.checkpoint["id"] for t in rest] == ["cp-2", "cp-1"]


def test_store_pending_writes_are_attached_to_checkpoint() -> None:
    store = ReactiveCheckpointStore()
    cfg = store.put(_cfg("t1", "cp-1"), _cp("cp-1", {"n": 1}), {}, {})
    store.put_writes(
        cfg,
        [("task-a", "messages", ["m"]), ("task-a", "scratch", 1)],
        "task-a",
        "path",
    )

    tup = store.get_tuple(cfg)
    assert tup is not None
    assert tup.pending_writes == [
        ("task-a", "messages", ["m"]),
        ("task-a", "scratch", 1),
    ]


def test_store_writes_may_precede_their_checkpoint() -> None:
    """Pregel commits task writes before the checkpoint row is visible."""
    store = ReactiveCheckpointStore()
    cfg = _cfg("t1", "cp-1")
    store.put_writes(cfg, [("task-a", "messages", ["m"])], "task-a")
    assert store.get_tuple(cfg) is None

    store.put(cfg, _cp("cp-1", {"n": 1}), {}, {})
    tup = store.get_tuple(cfg)
    assert tup is not None
    assert tup.pending_writes == [("task-a", "messages", ["m"])]


def test_store_delete_thread_purges_only_that_thread() -> None:
    store = ReactiveCheckpointStore()
    cfg = store.put(_cfg("t1", "cp-1"), _cp("cp-1", {"n": 1}), {}, {})
    store.put_writes(cfg, [("task-a", "messages", ["m"])], "task-a")
    store.put(_cfg("t2", "cp-1"), _cp("cp-1", {"n": 2}), {}, {})

    store.delete_thread("t1")

    assert store.get_tuple(_cfg("t1")) is None
    assert list(store.list(_cfg("t1"))) == []
    assert store.get_tuple(_cfg("t2")) is not None


def test_store_put_is_idempotent_for_same_checkpoint_id() -> None:
    store = ReactiveCheckpointStore()
    cfg = _cfg("t1", "cp-1")
    store.put(cfg, _cp("cp-1", {"n": 1}), {}, {})
    store.put(cfg, _cp("cp-1", {"n": 1}), {}, {})
    assert [t.checkpoint["id"] for t in store.list(_cfg("t1"))] == ["cp-1"]


def test_store_rejects_checkpoint_without_id() -> None:
    store = ReactiveCheckpointStore()
    with pytest.raises(ValueError, match="id"):
        store.put(_cfg("t1"), {"channel_values": {}}, {}, {})


def test_store_get_next_version_increments() -> None:
    store = ReactiveCheckpointStore()
    assert store.get_next_version(None, "messages") == 1
    assert store.get_next_version(1, "messages") == 2
    assert store.get_next_version("3", "messages") == 4


# ---------------------------------------------------------------------------
# Async contract
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_store_async_surface_matches_sync() -> None:
    store = ReactiveCheckpointStore()
    cfg = await store.aput(_cfg("t1", "cp-1"), _cp("cp-1", {"n": 1}), {}, {})
    await store.aput_writes(cfg, [("task-a", "messages", ["m"])], "task-a")

    tup = await store.aget_tuple(cfg)
    assert tup is not None
    assert tup.checkpoint["channel_values"] == {"n": 1}
    assert tup.pending_writes == [("task-a", "messages", ["m"])]

    listed = [t async for t in store.alist(_cfg("t1"))]
    assert [t.checkpoint["id"] for t in listed] == ["cp-1"]

    await store.adelete_thread("t1")
    assert await store.aget_tuple(_cfg("t1")) is None


# ---------------------------------------------------------------------------
# Provider contract (upstream get_checkpointer/reset_checkpointer parity)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset():
    reset_checkpointer()
    yield
    reset_checkpointer()


def test_provider_defaults_to_reactive_memory_saver() -> None:
    cp = get_checkpointer()
    assert isinstance(cp, ReactiveCheckpointSaver)


def test_provider_memory_singleton_and_reset() -> None:
    first = get_checkpointer()
    assert get_checkpointer() is first
    reset_checkpointer()
    assert get_checkpointer() is not first


def test_provider_config_memory_explicit() -> None:
    cp = get_checkpointer({"type": "memory"})
    assert isinstance(cp, ReactiveCheckpointSaver)


def test_provider_unknown_backend_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown"):
        get_checkpointer({"type": "unknown"})


def test_provider_postgres_without_connection_string_fails_closed() -> None:
    with pytest.raises(ValueError, match="connection_string is required"):
        get_checkpointer({"type": "postgres"})


# ---------------------------------------------------------------------------
# Graph wiring: the saver is the source of truth, not a local dict
# ---------------------------------------------------------------------------


def test_graph_persists_state_through_saver_and_shares_across_instances() -> None:
    saver = ReactiveCheckpointSaver()
    first_model = FakeModel([{"role": "assistant", "content": "first"}])
    graph = create_deerflow_agent(first_model, checkpointer=saver)
    config = _cfg("shared")

    state = graph.invoke({"messages": [{"role": "user", "content": "one"}]}, config)
    assert graph.get_state(config) == state

    second_model = FakeModel([{"role": "assistant", "content": "second"}])
    other = create_deerflow_agent(second_model, checkpointer=saver)
    assert other.get_state(config) == state

    continued = other.invoke({"messages": [{"role": "user", "content": "two"}]}, config)
    assert [m["content"] for m in continued["messages"]] == ["one", "first", "two", "second"]


def test_graph_writes_one_checkpoint_per_invoke_with_parent_chain() -> None:
    saver = ReactiveCheckpointSaver()
    model = FakeModel([{"role": "assistant", "content": "a"}, {"role": "assistant", "content": "b"}])
    graph = create_deerflow_agent(model, checkpointer=saver)
    config = _cfg("chain")
    graph.invoke({"messages": [{"role": "user", "content": "one"}]}, config)
    graph.invoke({"messages": [{"role": "user", "content": "two"}]}, config)

    history = list(saver.list(_cfg("chain")))
    assert len(history) == 2
    assert history[0].parent_config["configurable"]["checkpoint_id"] == history[1].checkpoint["id"]


def test_graph_without_checkpointer_does_not_use_saver() -> None:
    saver = ReactiveCheckpointSaver()
    model = FakeModel([{"role": "assistant", "content": "a"}])
    graph = create_deerflow_agent(model)
    graph.invoke({"messages": [{"role": "user", "content": "one"}]}, _cfg("t"))
    assert list(saver.list(_cfg("t"))) == []
