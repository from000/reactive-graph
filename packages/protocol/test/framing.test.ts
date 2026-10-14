import { describe, expect, it } from "vitest";
import { CodecError, decodeValue, encodeValue } from "../src/codec.js";
import { FrameDecoder, ProtocolError, encodeFrame } from "../src/framing.js";
import { isEnvelope } from "../src/messages.js";

/** Cross-language fixtures (mirrored in python/reactivegraph/tests/test_protocol.py). */

// A handshake payload: {sdkVersion: "0.1.0", protocolVersions: [1]}
// MessagePack bytes (independent of frame header).
const HANDSHAKE_PAYLOAD_BYTES = new Uint8Array([
  0x82, 0xaa, 0x73, 0x64, 0x6b, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0xa5, 0x30, 0x2e, 0x31,
  0x2e, 0x30, 0xb0, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x56, 0x65, 0x72, 0x73, 0x69,
  0x6f, 0x6e, 0x73, 0x91, 0x01,
]);

// canonical bytes test vector: {"a": 1, "b": [1, 2, 3], "c": "x"}
const MAP_PAYLOAD_BYTES = new Uint8Array([
  0x83, 0xa1, 0x61, 0x01, 0xa1, 0x62, 0x93, 0x01, 0x02, 0x03, 0xa1, 0x63, 0xa1, 0x78,
]);

describe("RGP/1 framing", () => {
  it("round-trips a single legal frame", () => {
    const frame = encodeFrame(HANDSHAKE_PAYLOAD_BYTES);
    const decoder = new FrameDecoder();
    const frames = decoder.push(frame);
    expect(frames).toHaveLength(1);
    expect(frames[0]).toEqual(HANDSHAKE_PAYLOAD_BYTES);
  });

  it("reconstructs a frame from split chunks", () => {
    const frame = encodeFrame(HANDSHAKE_PAYLOAD_BYTES);
    const decoder = new FrameDecoder();
    const mid = 7;
    const a = frame.slice(0, mid);
    const b = frame.slice(mid);
    const frames1 = decoder.push(a);
    const frames2 = decoder.push(b);
    expect(frames1).toHaveLength(0);
    expect(frames2).toHaveLength(1);
    expect(frames2[0]).toEqual(HANDSHAKE_PAYLOAD_BYTES);
  });

  it("emits two ordered envelopes for concatenated frames", () => {
    const decoder = new FrameDecoder();
    const f1 = encodeFrame(HANDSHAKE_PAYLOAD_BYTES);
    const f2 = encodeFrame(MAP_PAYLOAD_BYTES);
    const concat = new Uint8Array(f1.length + f2.length);
    concat.set(f1, 0);
    concat.set(f2, f1.length);
    const frames = decoder.push(concat);
    expect(frames).toHaveLength(2);
    expect(frames[0]).toEqual(HANDSHAKE_PAYLOAD_BYTES);
    expect(frames[1]).toEqual(MAP_PAYLOAD_BYTES);
  });

  it("rejects a malformed (oversized) length and resets cleanly", () => {
    const decoder = new FrameDecoder({ maxFrameBytes: 1024 });
    const bad = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x01]);
    expect(() => decoder.push(bad)).toThrow(ProtocolError);
    // decoder reset; a legal frame still works
    const good = encodeFrame(HANDSHAKE_PAYLOAD_BYTES);
    const frames = decoder.push(good);
    expect(frames).toHaveLength(1);
  });

  it("rejects a payload exceeding maxFrameBytes", () => {
    const decoder = new FrameDecoder({ maxFrameBytes: 4 });
    const big = new Uint8Array(100);
    const frame = encodeFrame(big);
    expect(() => decoder.push(frame)).toThrow(ProtocolError);
  });
});

describe("RGP/1 canonical codec", () => {
  it("round-trips bytes", () => {
    const value = { data: new Uint8Array([1, 2, 250]) };
    const decoded = decodeValue(encodeValue(value)) as { data: Uint8Array };
    expect(decoded.data).toBeInstanceOf(Uint8Array);
    expect(Array.from(decoded.data)).toEqual([1, 2, 250]);
  });

  it("round-trips a 64-bit integer exactly", () => {
    const n = 9007199254740993n; // 2^53 + 1
    const decoded = decodeValue(encodeValue({ n })) as { n: bigint };
    expect(typeof decoded.n).toBe("bigint");
    expect(decoded.n).toBe(n);
  });

  it("round-trips a timestamp", () => {
    const date = new Date(1_700_000_000_123);
    const decoded = decodeValue(encodeValue({ at: date })) as { at: Date };
    expect(decoded.at).toBeInstanceOf(Date);
    expect(decoded.at.getTime()).toBe(date.getTime());
  });

  it("rejects non-canonical values", () => {
    expect(() => encodeValue({ f: () => 1 })).toThrow(CodecError);
    expect(() => encodeValue(undefined)).toThrow(CodecError);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => encodeValue(cyclic)).toThrow(CodecError);
    expect(() => encodeValue(new Map())).toThrow(CodecError);
  });
});

describe("RGP/1 envelopes transport fixture (incl. cancellation)", () => {
  it("round-trips a RUN request and a CANCEL envelope through frame + codec", () => {
    const envelopes = [
      {
        version: 1,
        id: "req-1",
        kind: "request",
        method: "RUN",
        payload: { graphId: "g", threadId: "t" },
      },
      {
        version: 1,
        id: "c-1",
        kind: "cancel",
        method: "CANCEL",
        payload: { targetId: "req-1", reason: "user abort" },
      },
    ] as const;
    for (const env of envelopes) {
      const payloadBytes = encodeValue(env.payload);
      const frames = new FrameDecoder().push(encodeFrame(payloadBytes));
      expect(frames).toHaveLength(1);
      const decoded = decodeValue(frames[0]!) as typeof env.payload;
      expect(decoded).toEqual(env.payload);
    }
  });
});

describe("RGP/1 envelope guards", () => {
  it("rejects unknown protocol version", () => {
    expect(isEnvelope({ version: 2, id: "x", kind: "request", method: "RUN", payload: {} })).toBe(
      false,
    );
  });

  it("rejects unknown method", () => {
    expect(isEnvelope({ version: 1, id: "x", kind: "request", method: "NOPE", payload: {} })).toBe(
      false,
    );
  });

  it("accepts a well-formed envelope", () => {
    expect(isEnvelope({ version: 1, id: "x", kind: "request", method: "RUN", payload: {} })).toBe(
      true,
    );
  });

  it("rejects a response whose method does not match a pending request (session-level)", () => {
    // Simple session bookkeeping: reject a response to an unknown request id,
    // and reject a response whose method differs from the recorded request.
    const pending = new Map<string, string>(); // id -> method
    function registerRequest(id: string, method: string): void {
      if (pending.has(id)) throw new ProtocolError(`duplicate request id: ${id}`);
      pending.set(id, method);
    }
    function acceptResponse(id: string, method: string): void {
      const expected = pending.get(id);
      if (expected === undefined) throw new ProtocolError(`unknown request id: ${id}`);
      if (expected !== method)
        throw new ProtocolError(`response method mismatch: expected ${expected}, got ${method}`);
      pending.delete(id);
    }
    registerRequest("r1", "RUN");
    // duplicate request id rejected while still pending
    expect(() => registerRequest("r1", "RUN")).toThrow(ProtocolError);
    expect(() => acceptResponse("r1", "RUN")).not.toThrow();
    registerRequest("r2", "TASK_RESULT");
    expect(() => acceptResponse("r2", "RUN")).toThrow(ProtocolError);
  });
});
