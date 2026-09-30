"""Typed-serialization contract: every supported value must round-trip.

Checkpoint and store durability rest on this module, so these are contract
tests rather than implementation-detail tests: an object the serializer claims
to support must come back as the same value with the same concrete type.
"""

from __future__ import annotations

import datetime as dt
import decimal
import pathlib
import re
import uuid
from collections import deque
from enum import Enum

import pytest
from pydantic import BaseModel

from reactivegraph.serde import ReactiveSerializer, SerializationError


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Payload(BaseModel):
    count: int
    label: str


class TestTypedRoundTrip:
    """Small scalar/container values used by real checkpoint states."""

    @pytest.mark.parametrize(
        "value",
        [
            None,
            b"bytes",
            bytearray(b"bytearray"),
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            decimal.Decimal("12.50"),
            dt.datetime(2026, 1, 2, 3, 4, 5, 6),
            dt.date(2026, 1, 2),
            dt.time(3, 4, 5, 6),
            dt.timedelta(days=1, seconds=2, microseconds=3),
            dt.timezone(dt.timedelta(hours=8)),
            pathlib.Path("/tmp/state.json"),
            re.compile(r"^state-\d+$", re.IGNORECASE),
            {1, 2, 3},
            frozenset({"a", "b"}),
            deque([1, 2, 3]),
            (1, "two", 3.0),
            {"nested": [1, (2, 3), {"ok": True}]},
        ],
    )
    def test_msgpack_round_trip_preserves_type_and_value(self, value: object) -> None:
        serializer = ReactiveSerializer()
        type_, payload = serializer.dumps_typed(value)
        restored = serializer.loads_typed((type_, payload))
        assert restored == value
        assert type(restored) is type(value)

    def test_pydantic_model_round_trips(self) -> None:
        serializer = ReactiveSerializer()
        restored = serializer.loads_typed(serializer.dumps_typed(Payload(count=2, label="x")))
        assert restored == Payload(count=2, label="x")

    def test_enum_round_trips_as_the_same_member(self) -> None:
        serializer = ReactiveSerializer()
        restored = serializer.loads_typed(serializer.dumps_typed(Color.RED))
        assert restored is Color.RED

    def test_delta_snapshot_round_trips(self) -> None:
        delta_types = pytest.importorskip("langgraph.checkpoint.serde.types")
        marker = delta_types._DeltaSnapshot({"n": 1})  # noqa: SLF001 - host contract

        serializer = ReactiveSerializer()
        restored = serializer.loads_typed(serializer.dumps_typed(marker))
        assert restored == marker


class TestJsonRoundTrip:
    """The long-term store uses the JSON codec, not msgpack."""

    @pytest.mark.parametrize(
        "value",
        [
            b"bytes",
            bytearray(b"bytearray"),
            dt.datetime(2026, 1, 2, 3, 4, 5),
            dt.date(2026, 1, 2),
            dt.time(3, 4, 5),
            dt.timedelta(seconds=7),
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            decimal.Decimal("0.125"),
            (1, 2),
            {1, 2},
            frozenset({"x"}),
            deque([1, 2]),
            Color.BLUE,
            Payload(count=1, label="json"),
        ],
    )
    def test_json_round_trip_preserves_type_and_value(self, value: object) -> None:
        serializer = ReactiveSerializer()
        restored = serializer.loads_json(serializer.dumps_json({"v": value}))["v"]
        assert restored == value
        assert type(restored) is type(value)

    def test_json_rejects_non_string_mapping_keys(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="string dictionary keys"):
            serializer.dumps_json({1: "x"})

    def test_json_rejects_unknown_python_objects(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="cannot serialize"):
            serializer.dumps_json(object())


class TestSafetyBoundary:
    def test_pickle_payload_is_refused(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="pickle deserialization is disabled"):
            serializer.loads_typed(("pickle", b"\x80\x04."))

    def test_unknown_serialization_type_is_refused(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="unknown serialization type"):
            serializer.loads_typed(("yaml", b"x: 1"))

    def test_callable_is_refused_instead_of_pickled(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="pickle fallback is disabled"):
            serializer.dumps_typed(len)

    def test_invalid_msgpack_payload_fails_closed(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="invalid msgpack"):
            serializer.loads_typed(("msgpack", b"\xc1"))

    def test_unknown_extension_code_is_refused(self) -> None:
        serializer = ReactiveSerializer()
        with pytest.raises(SerializationError, match="blocked msgpack extension code"):
            serializer._ext_hook(99, b"\x00")  # noqa: SLF001 - codec boundary

    def test_constructor_for_unloaded_module_is_refused(self) -> None:
        import msgpack

        serializer = ReactiveSerializer()
        body = serializer._pack_prepared(  # noqa: SLF001 - codec boundary
            ("not_loaded_anywhere", "Thing", (1,))
        )
        forged = msgpack.packb(msgpack.ExtType(1, body))
        with pytest.raises(SerializationError, match="blocked constructor"):
            serializer.loads_typed(("msgpack", forged))
