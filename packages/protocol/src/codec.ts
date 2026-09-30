/**
 * RGP/1 canonical value codec (docs/spec/rgp-1.md §3, docs/research/msgpack-cross-language.md).
 * TS mirror of python/reactivegraph/reactivegraph/protocol.py encode/decode value.
 *
 * Canonical values: null, boolean, number (safe-int encodes as int; float as float),
 * bigint (int64/uint64), string, Uint8Array (bin), Array, plain object with string
 * keys, Date (msgpack timestamp ext -1), decimal (custom ext type 0, UTF-8 decimal string).
 *
 * Non-canonical values (functions, undefined at value position, cyclic references,
 * Map/Set, class instances) are rejected with CodecError.
 */

import { ExtensionCodec, decode, encode, type ExtensionCodecType } from "@msgpack/msgpack";

const DECIMAL_EXT_TYPE = 0;

export class CodecError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CodecError";
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** True if `value` is our tagged decimal marker object. */
function isDecimalMarker(value: unknown): value is { __type__: "decimal"; value: string } {
  return (
    isPlainObject(value) &&
    (value as Record<string, unknown>)["__type__"] === "decimal" &&
    typeof (value as Record<string, unknown>)["value"] === "string"
  );
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function encodeDecimal(value: string): Uint8Array {
  return textEncoder.encode(value);
}

function decodeDecimal(data: Uint8Array): { __type__: "decimal"; value: string } {
  return { __type__: "decimal", value: textDecoder.decode(data) };
}

function buildExtensionCodec(): ExtensionCodecType<undefined> {
  const codec = new ExtensionCodec();
  // custom ext type 0: decimal as UTF-8 decimal string
  codec.register({
    type: DECIMAL_EXT_TYPE,
    encode: (object) => {
      if (isDecimalMarker(object)) {
        return encodeDecimal(object.value);
      }
      return null;
    },
    decode: decodeDecimal,
  });
  return codec;
}

const codec = buildExtensionCodec();

const MAX_DEPTH = 256;

function assertCanonical(value: unknown, path: string, seen: WeakSet<object>, depth: number): void {
  if (depth > MAX_DEPTH) {
    throw new CodecError(`max depth exceeded at ${path}`);
  }
  if (value === null) return;
  switch (typeof value) {
    case "boolean":
    case "number":
    case "string":
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new CodecError(`non-finite number at ${path}`);
      }
      return;
    case "bigint":
      return;
    case "undefined":
      throw new CodecError(`undefined at ${path}`);
    case "function":
    case "symbol":
      throw new CodecError(`non-canonical ${typeof value} at ${path}`);
    case "object": {
      if (value instanceof Uint8Array) return;
      if (value instanceof Date) {
        if (Number.isNaN(value.getTime())) {
          throw new CodecError(`invalid Date at ${path}`);
        }
        return;
      }
      if (isDecimalMarker(value)) return;
      if (value instanceof ArrayBuffer) {
        throw new CodecError(`ArrayBuffer at ${path}; use Uint8Array`);
      }
      if (!isPlainObject(value) && !Array.isArray(value)) {
        throw new CodecError(`non-canonical object (${value.constructor?.name ?? "?"}) at ${path}`);
      }
      if (seen.has(value)) {
        throw new CodecError(`cyclic reference at ${path}`);
      }
      seen.add(value);
      if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
          assertCanonical(value[i], `${path}[${i}]`, seen, depth + 1);
        }
      } else {
        for (const key of Object.keys(value)) {
          assertCanonical(value[key], `${path}.${key}`, seen, depth + 1);
        }
      }
      seen.delete(value);
      return;
    }
    default:
      throw new CodecError(`non-canonical value at ${path}`);
  }
}

/** Encode a canonical value to MessagePack bytes. */
export function encodeValue(value: unknown): Uint8Array {
  assertCanonical(value, "$", new WeakSet(), 0);
  return encode(value, { extensionCodec: codec, useBigInt64: true, ignoreUndefined: false });
}

/** Decode MessagePack bytes to a canonical value. */
export function decodeValue(bytes: Uint8Array): unknown {
  return decode(bytes, {
    extensionCodec: codec,
    useBigInt64: true,
    mapKeyConverter: (key) => String(key),
  });
}
