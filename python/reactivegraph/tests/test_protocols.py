"""Public extension contracts must accept real implementations.

The point of these Protocols is that a host can plug in its own backend
without inheriting from engine classes. The tests below prove three things:
the engine's own backends satisfy the contracts, a *from-scratch* implementation
satisfies them, and a broken one fails with a message naming the missing
methods instead of a bare TypeError deep inside a run.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from reactivegraph.checkpoint import CheckpointStore
from reactivegraph.protocols import (
    CheckpointSaverProtocol,
    RetrieverProtocol,
    StoreProtocol,
    check_protocol,
)
from reactivegraph.store import MemoryStore, SqliteStore
from reactivegraph.store_types import Item, SearchItem


class CustomStore:
    """A store written from scratch — no engine base class."""

    def __init__(self) -> None:
        self._data: dict[tuple[str, ...], dict[str, Any]] = {}

    def get(
        self, namespace: Sequence[str], key: str, *, refresh_ttl: bool | None = None
    ) -> Item | None:
        return self._data.get(tuple(namespace), {}).get(key)

    def put(
        self,
        namespace: Sequence[str],
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
    ) -> None:
        del ttl
        self._data.setdefault(tuple(namespace), {})[key] = value

    def delete(self, namespace: Sequence[str], key: str) -> None:
        self._data.get(tuple(namespace), {}).pop(key, None)

    def search(
        self,
        namespace_prefix: Sequence[str],
        /,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[SearchItem]:
        del query, filter, limit, offset
        return []

    def list_namespaces(
        self,
        *,
        prefix: Sequence[str] | None = None,
        suffix: Sequence[str] | None = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]:
        del prefix, suffix, max_depth, limit, offset
        return list(self._data)


class BrokenStore:
    """Missing search/list_namespaces on purpose."""

    def get(
        self, namespace: Sequence[str], key: str, *, refresh_ttl: bool | None = None
    ) -> Item | None:
        return None

    def put(
        self,
        namespace: Sequence[str],
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
    ) -> None:
        return

    def delete(self, namespace: Sequence[str], key: str) -> None:
        return


class CustomRetriever:
    def get_relevant_documents(self, query: str, **kwargs: Any) -> list[Any]:
        del kwargs
        return [f"doc-for-{query}"]


class TestProtocolsAcceptEngineBackends:
    @pytest.mark.parametrize("cls", [MemoryStore, SqliteStore])
    def test_builtin_stores_satisfy_store_protocol(self, cls: type) -> None:
        assert isinstance(cls(), StoreProtocol)

    def test_checkpoint_store_satisfies_saver_protocol(self) -> None:
        assert isinstance(CheckpointStore(), CheckpointSaverProtocol)


class TestProtocolsAcceptCustomImplementations:
    def test_from_scratch_store_satisfies_protocol(self) -> None:
        assert isinstance(CustomStore(), StoreProtocol)

    def test_from_scratch_retriever_satisfies_protocol(self) -> None:
        assert isinstance(CustomRetriever(), RetrieverProtocol)


class TestProtocolErrors:
    def test_missing_methods_are_named(self) -> None:
        with pytest.raises(TypeError, match="search"):
            check_protocol(BrokenStore(), StoreProtocol, context="graph.store")

    def test_error_names_the_context(self) -> None:
        with pytest.raises(TypeError, match="graph.store"):
            check_protocol(BrokenStore(), StoreProtocol, context="graph.store")

    def test_conforming_object_passes(self) -> None:
        check_protocol(CustomStore(), StoreProtocol)  # must not raise
