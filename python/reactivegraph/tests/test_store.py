"""Native ReactiveGraph store: behaviour pinned against real LangGraph.

These tests encode the *observed* contract of ``langgraph.store.memory
.InMemoryStore`` (probed, not assumed). A host that already depends on
LangGraph adapts this store in its own package; the semantics live here so
every host shares one tested implementation.
"""

from __future__ import annotations

import time

import pytest

from reactivegraph.store import (
    InvalidNamespaceError,
    MemoryStore,
    PutOp,
    SearchItem,
    get_store,
    reset_store,
)

# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------


def test_put_get_roundtrip() -> None:
    store = MemoryStore()
    store.put(("users", "123"), "prefs", {"theme": "dark"})
    item = store.get(("users", "123"), "prefs")
    assert item is not None
    assert item.value == {"theme": "dark"}
    assert item.key == "prefs"
    assert item.namespace == ("users", "123")


def test_get_missing_returns_none() -> None:
    store = MemoryStore()
    assert store.get(("nope",), "missing") is None


def test_delete_removes_item() -> None:
    store = MemoryStore()
    store.put(("n",), "k", {"v": 1})
    store.delete(("n",), "k")
    assert store.get(("n",), "k") is None


def test_put_overwrite_replaces_value() -> None:
    store = MemoryStore()
    store.put(("n",), "k", {"v": 1})
    store.put(("n",), "k", {"v": 2})
    item = store.get(("n",), "k")
    assert item is not None
    assert item.value == {"v": 2}


def test_put_overwrite_advances_updated_at() -> None:
    """Probed: upstream resets created_at/updated_at on every put."""
    store = MemoryStore()
    store.put(("n",), "k", {"v": 1})
    first = store.get(("n",), "k")
    time.sleep(0.01)
    store.put(("n",), "k", {"v": 2})
    second = store.get(("n",), "k")
    assert first is not None and second is not None
    assert second.updated_at > first.updated_at


def test_get_does_not_create_namespace_for_listing() -> None:
    """Probed upstream quirk: ``get`` on an unknown ns materialises it."""
    store = MemoryStore()
    assert store.get(("ghost",), "k") is None
    assert store.list_namespaces() == [("ghost",)]


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def test_search_prefix_filters_namespace() -> None:
    store = MemoryStore()
    store.put(("docs", "a"), "1", {"v": 1})
    store.put(("other",), "2", {"v": 2})
    keys = {item.key for item in store.search(("docs",))}
    assert keys == {"1"}


def test_search_respects_limit_and_offset() -> None:
    store = MemoryStore()
    for i in range(5):
        store.put(("docs",), f"k{i}", {"v": i})
    assert len(store.search(("docs",), limit=2)) == 2
    assert len(store.search(("docs",), limit=2, offset=4)) == 1


def test_search_returns_search_items_with_none_score() -> None:
    store = MemoryStore()
    store.put(("docs",), "k", {"v": 1})
    results = store.search(("docs",))
    assert isinstance(results[0], SearchItem)
    assert results[0].score is None


def test_search_filter_exact_match() -> None:
    store = MemoryStore()
    store.put(("f",), "1", {"status": "open"})
    store.put(("f",), "2", {"status": "closed"})
    assert [i.key for i in store.search(("f",), filter={"status": "open"})] == ["1"]


def test_search_filter_nested_dict() -> None:
    store = MemoryStore()
    store.put(("f",), "1", {"meta": {"owner": "alice"}})
    store.put(("f",), "2", {"meta": {"owner": "bob"}})
    assert [i.key for i in store.search(("f",), filter={"meta": {"owner": "bob"}})] == ["2"]


def test_search_filter_operators() -> None:
    store = MemoryStore()
    store.put(("f",), "1", {"n": 1})
    store.put(("f",), "2", {"n": 5})
    assert [i.key for i in store.search(("f",), filter={"n": {"$gt": 3}})] == ["2"]
    assert [i.key for i in store.search(("f",), filter={"n": {"$lte": 1}})] == ["1"]
    assert [i.key for i in store.search(("f",), filter={"n": {"$ne": 1}})] == ["2"]


def test_search_filter_unknown_operator_fails_closed() -> None:
    store = MemoryStore()
    store.put(("f",), "1", {"n": 1})
    with pytest.raises(ValueError, match="Unsupported operator"):
        store.search(("f",), filter={"n": {"$bogus": 1}})


def test_search_filter_list_equality() -> None:
    store = MemoryStore()
    store.put(("f",), "1", {"tags": ["a", "b"]})
    store.put(("f",), "2", {"tags": ["a"]})
    assert [i.key for i in store.search(("f",), filter={"tags": ["a", "b"]})] == ["1"]


# ---------------------------------------------------------------------------
# list_namespaces
# ---------------------------------------------------------------------------


def test_list_namespaces_sorted() -> None:
    store = MemoryStore()
    store.put(("b",), "k", {})
    store.put(("a",), "k", {})
    assert store.list_namespaces() == [("a",), ("b",)]


def test_list_namespaces_max_depth_truncates_and_dedupes() -> None:
    store = MemoryStore()
    store.put(("a", "b"), "k", {})
    store.put(("a", "c"), "k", {})
    assert store.list_namespaces(max_depth=1) == [("a",)]


def test_list_namespaces_prefix_and_suffix() -> None:
    store = MemoryStore()
    store.put(("a", "b", "c"), "k", {})
    store.put(("x", "y"), "k", {})
    assert store.list_namespaces(prefix=("a",)) == [("a", "b", "c")]
    assert store.list_namespaces(suffix=("y",)) == [("x", "y")]


def test_list_namespaces_wildcard() -> None:
    store = MemoryStore()
    store.put(("a", "b"), "k", {})
    store.put(("a", "c"), "k", {})
    assert store.list_namespaces(prefix=("a", "*")) == [("a", "b"), ("a", "c")]


def test_list_namespaces_offset_limit() -> None:
    store = MemoryStore()
    for name in ("a", "b", "c"):
        store.put((name,), "k", {})
    assert store.list_namespaces(offset=1, limit=1) == [("b",)]


# ---------------------------------------------------------------------------
# Namespace validation
# ---------------------------------------------------------------------------


def test_empty_namespace_rejected() -> None:
    store = MemoryStore()
    with pytest.raises(InvalidNamespaceError, match="Namespace cannot be empty"):
        store.put((), "k", {})


def test_non_string_namespace_label_rejected() -> None:
    store = MemoryStore()
    with pytest.raises(InvalidNamespaceError, match="must be strings"):
        store.put(("a", 1), "k", {})  # type: ignore[arg-type]


def test_dotted_namespace_label_rejected() -> None:
    store = MemoryStore()
    with pytest.raises(InvalidNamespaceError, match="cannot contain periods"):
        store.put(("a.b",), "k", {})


def test_ttl_fails_closed_when_unsupported() -> None:
    store = MemoryStore()
    with pytest.raises(NotImplementedError, match="TTL is not supported"):
        store.put(("a",), "k", {}, ttl=5)


def test_ttl_none_is_allowed() -> None:
    store = MemoryStore()
    store.put(("a",), "k", {}, ttl=None)
    assert store.get(("a",), "k") is not None


# ---------------------------------------------------------------------------
# batch / abatch
# ---------------------------------------------------------------------------


def test_batch_get_and_put() -> None:
    store = MemoryStore()
    results = store.batch([("put", ("n",), "k", {"v": 1})])
    assert results == [None]
    assert store.get(("n",), "k") is not None


def test_batch_duplicate_put_is_last_write_wins() -> None:
    """Probed: upstream dedupes by (namespace, key) before applying."""
    store = MemoryStore()
    store.batch([("put", ("d",), "x", {"i": 1}), ("put", ("d",), "x", {"i": 2})])
    item = store.get(("d",), "x")
    assert item is not None
    assert item.value == {"i": 2}


def test_batch_unknown_op_fails_closed() -> None:
    store = MemoryStore()
    with pytest.raises(ValueError, match="Unknown operation type"):
        store.batch([("nonsense",)])


# ---------------------------------------------------------------------------
# Async surface
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_async_crud() -> None:
    store = MemoryStore()
    await store.aput(("n",), "k", {"v": 1})
    item = await store.aget(("n",), "k")
    assert item is not None and item.value == {"v": 1}
    await store.adelete(("n",), "k")
    assert await store.aget(("n",), "k") is None


@pytest.mark.anyio
async def test_async_search_and_list() -> None:
    store = MemoryStore()
    await store.aput(("docs",), "a", {"status": "open"})
    results = await store.asearch(("docs",), filter={"status": "open"})
    assert [i.key for i in results] == ["a"]
    assert await store.alist_namespaces() == [("docs",)]


@pytest.mark.anyio
async def test_async_batch() -> None:
    store = MemoryStore()
    await store.abatch([("put", ("n",), "k", {"v": 1})])
    assert await store.aget(("n",), "k") is not None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------


def test_get_store_returns_singleton() -> None:
    reset_store()
    assert get_store() is get_store()


def test_reset_store_replaces_instance() -> None:
    reset_store()
    first = get_store()
    reset_store()
    assert get_store() is not first


def test_item_is_hashable_by_identity() -> None:
    store = MemoryStore()
    store.put(("n",), "k", {"v": 1})
    item = store.get(("n",), "k")
    assert item is not None
    assert item in {item}


# ---------------------------------------------------------------------------
# Host-operation interoperability
# ---------------------------------------------------------------------------


def test_accepts_foreign_namedtuple_ops() -> None:
    """A host engine passes its own (NamedTuple) ops straight into ``batch``.

    NamedTuples are tuples, so a naive ``isinstance(op, tuple)`` check would
    route them to the inline-tuple branch and lose every field. This pins the
    structural dispatch that keeps host ops working.
    """
    import collections

    ForeignGet = collections.namedtuple("GetOp", "namespace key refresh_ttl")
    ForeignPut = collections.namedtuple("PutOp", "namespace key value index ttl")

    store = MemoryStore()
    store.batch([
        ForeignGet(("g",), "k", True),
        ForeignPut(("g",), "k", {"v": 9}, None, None),
        ForeignGet(("g",), "k", True),
    ])
    assert store.get(("g",), "k").value == {"v": 9}


def test_inline_tuple_ops_still_work() -> None:
    """Inline ``(kind, namespace, key, value)`` tuples remain supported."""
    store = MemoryStore()
    store.batch([("put", ("t",), "k", {"v": 1})])
    assert store.get(("t",), "k").value == {"v": 1}


def test_put_op_ttl_keyword_is_not_treated_as_index() -> None:
    """``PutOp``'s 4th field is ``index``; TTL must be passed by keyword."""
    store = MemoryStore()
    with pytest.raises(NotImplementedError):
        store.batch([PutOp(("a",), "k", {"v": 1}, ttl=3)])
