/**
 * Canonical value hash (Task 5).
 *
 * Computes a deterministic hash over the RGP/1 canonical msgpack encoding of a
 * value. Both languages hash the same canonical bytes, so pre-image hashes match
 * across the wire. Hash = SHA-256 over the canonical msgpack bytes (hex).
 *
 * `undefined` (missing path) is not a canonical value; it is hashed as a fixed
 * marker so pre-image checks for missing paths are stable in both languages.
 */

import { createHash } from "node:crypto";
import { encodeValue } from "@reactivegraph/protocol";

const MISSING_MARKER = "rgp:missing";

/** SHA-256 (hex) of the canonical msgpack encoding of `value`. */
export function canonicalHash(value: unknown): string {
  let bytes: Uint8Array;
  if (value === undefined) {
    bytes = encodeValue(MISSING_MARKER);
  } else {
    bytes = encodeValue(value);
  }
  return createHash("sha256").update(bytes).digest("hex");
}
