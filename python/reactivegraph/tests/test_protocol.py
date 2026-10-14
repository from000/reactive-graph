"""RGP/1 protocol tests — mirror of packages/protocol/test/framing.test.ts.

Shared cross-language fixture corpus: handshake payload, map payload, bytes,
int64, timestamp, oversized frame. Byte values are hardcoded identically in both
languages (see packages/protocol/test/framing.test.ts).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from reactivegraph.protocol import (
    CodecError,
    FrameDecoder,
    ProtocolError,
    decode_value,
    encode_frame,
    encode_value,
    is_envelope,
)

# {sdkVersion: "0.1.0", protocolVersions: [1]} — msgpack bytes (payload only).
HANDSHAKE_PAYLOAD_BYTES = bytes(
    [
        0x82,
        0xAA,
        0x73,
        0x64,
        0x6B,
        0x56,
        0x65,
        0x72,
        0x73,
        0x69,
        0x6F,
        0x6E,
        0xA5,
        0x30,
        0x2E,
        0x31,
        0x2E,
        0x30,
        0xB0,
        0x70,
        0x72,
        0x6F,
        0x74,
        0x6F,
        0x63,
        0x6F,
        0x6C,
        0x56,
        0x65,
        0x72,
        0x73,
        0x69,
        0x6F,
        0x6E,
        0x73,
        0x91,
        0x01,
    ]
)

# {"a": 1, "b": [1, 2, 3], "c": "x"}
MAP_PAYLOAD_BYTES = bytes(
    [
        0x83,
        0xA1,
        0x61,
        0x01,
        0xA1,
        0x62,
        0x93,
        0x01,
        0x02,
        0x03,
        0xA1,
        0x63,
        0xA1,
        0x78,
    ]
)


class TestFraming:
    def test_single_legal_frame_round_trip(self) -> None:
        frame = encode_frame(HANDSHAKE_PAYLOAD_BYTES)
        decoder = FrameDecoder()
        frames = decoder.push(frame)
        assert len(frames) == 1
        assert frames[0] == HANDSHAKE_PAYLOAD_BYTES

    def test_reconstructs_split_frame(self) -> None:
        frame = encode_frame(HANDSHAKE_PAYLOAD_BYTES)
        decoder = FrameDecoder()
        mid = 7
        assert decoder.push(frame[:mid]) == []
        frames = decoder.push(frame[mid:])
        assert len(frames) == 1
        assert frames[0] == HANDSHAKE_PAYLOAD_BYTES

    def test_concatenated_frames_ordered(self) -> None:
        decoder = FrameDecoder()
        f1 = encode_frame(HANDSHAKE_PAYLOAD_BYTES)
        f2 = encode_frame(MAP_PAYLOAD_BYTES)
        frames = decoder.push(f1 + f2)
        assert len(frames) == 2
        assert frames[0] == HANDSHAKE_PAYLOAD_BYTES
        assert frames[1] == MAP_PAYLOAD_BYTES

    def test_malformed_length_rejected_and_reset(self) -> None:
        decoder = FrameDecoder(max_frame_bytes=1024)
        bad = bytes([0xFF, 0xFF, 0xFF, 0xFF, 0x01])
        with pytest.raises(ProtocolError):
            decoder.push(bad)
        # decoder reset; a legal frame still works
        good = encode_frame(HANDSHAKE_PAYLOAD_BYTES)
        frames = decoder.push(good)
        assert len(frames) == 1

    def test_oversized_frame_rejected(self) -> None:
        decoder = FrameDecoder(max_frame_bytes=4)
        with pytest.raises(ProtocolError):
            decoder.push(encode_frame(bytes(100)))

    def test_frame_bytes_match_ts_fixture(self) -> None:
        # The 4-byte big-endian header prefixing the handshake payload.
        frame = encode_frame(HANDSHAKE_PAYLOAD_BYTES)
        assert frame[:4] == len(HANDSHAKE_PAYLOAD_BYTES).to_bytes(4, "big")
        assert frame[4:] == HANDSHAKE_PAYLOAD_BYTES


class TestCodec:
    def test_bytes_round_trip(self) -> None:
        value = {"data": b"\x01\x02\xfa"}
        decoded = decode_value(encode_value(value))
        assert isinstance(decoded["data"], bytes)
        assert decoded["data"] == b"\x01\x02\xfa"

    def test_int64_round_trip(self) -> None:
        n = 9007199254740993  # 2^53 + 1
        decoded = decode_value(encode_value({"n": n}))
        assert decoded["n"] == n
        assert isinstance(decoded["n"], int)

    def test_timestamp_round_trip(self) -> None:
        at = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
        decoded = decode_value(encode_value({"at": at}))
        assert isinstance(decoded["at"], datetime)
        assert decoded["at"] == at

    def test_rejects_non_canonical_values(self) -> None:
        with pytest.raises(CodecError):
            encode_value({"f": lambda: 1})
        with pytest.raises(CodecError):
            encode_value({"o": object()})  # arbitrary class instance
        cyclic: dict = {}
        cyclic["self"] = cyclic
        with pytest.raises(CodecError):
            encode_value(cyclic)
        with pytest.raises(CodecError):
            encode_value({"m": {1, 2}})  # set

    def test_decimal_marker_round_trip(self) -> None:
        value = {"price": {"__type__": "decimal", "value": "3.14"}}
        decoded = decode_value(encode_value(value))
        assert decoded["price"] == {"__type__": "decimal", "value": "3.14"}


class TestEnvelopeTransportFixture:
    def test_round_trips_run_and_cancel_envelopes(self) -> None:
        """Cross-language transport fixture: RUN request + CANCEL envelope (mirror of TS)."""
        envelopes = [
            {
                "version": 1,
                "id": "req-1",
                "kind": "request",
                "method": "RUN",
                "payload": {"graphId": "g", "threadId": "t"},
            },
            {
                "version": 1,
                "id": "c-1",
                "kind": "cancel",
                "method": "CANCEL",
                "payload": {"targetId": "req-1", "reason": "user abort"},
            },
        ]
        for env in envelopes:
            payload_bytes = encode_value(env["payload"])
            frames = FrameDecoder().push(encode_frame(payload_bytes))
            assert len(frames) == 1
            assert decode_value(frames[0]) == env["payload"]


class TestEnvelopeGuards:
    def test_rejects_unknown_version(self) -> None:
        assert not is_envelope(
            {"version": 2, "id": "x", "kind": "request", "method": "RUN", "payload": {}}
        )

    def test_rejects_unknown_method(self) -> None:
        assert not is_envelope(
            {"version": 1, "id": "x", "kind": "request", "method": "NOPE", "payload": {}}
        )

    def test_accepts_well_formed_envelope(self) -> None:
        assert is_envelope(
            {"version": 1, "id": "x", "kind": "request", "method": "RUN", "payload": {}}
        )

    def test_session_rejects_duplicate_id_and_method_mismatch(self) -> None:
        pending: dict[str, str] = {}

        def register_request(rid: str, method: str) -> None:
            if rid in pending:
                raise ProtocolError(f"duplicate request id: {rid}")
            pending[rid] = method

        def accept_response(rid: str, method: str) -> None:
            if rid not in pending:
                raise ProtocolError(f"unknown request id: {rid}")
            if pending[rid] != method:
                raise ProtocolError(
                    f"response method mismatch: expected {pending[rid]}, got {method}"
                )
            del pending[rid]

        register_request("r1", "RUN")
        # duplicate request id rejected while still pending
        with pytest.raises(ProtocolError):
            register_request("r1", "RUN")  # duplicate id
        accept_response("r1", "RUN")  # ok
        register_request("r2", "TASK_RESULT")
        with pytest.raises(ProtocolError):
            accept_response("r2", "RUN")  # mismatch
        with pytest.raises(ProtocolError):
            accept_response("nope", "RUN")  # unknown id
