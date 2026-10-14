/**
 * Serializers (Task 9). JsonPlusSerializer semantics + EncryptedSerializer.
 *
 * Canonical values round-trip through the RGP/1 codec (bytes, int64, timestamps,
 * decimals) so checkpoints are byte-comparable across languages. Encryption
 * wraps the canonical payload; a decryption failure surfaces, never silently
 * returns bad data.
 */

import { CodecError, decodeValue, encodeValue } from "@reactivegraph/protocol";
import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  scryptSync,
  type CipherGCM,
  type DecipherGCM,
} from "node:crypto";
import type { EncryptingSerializer, Serializer } from "./types.js";

export class JsonPlusSerializer implements Serializer {
  dumps(value: unknown): Uint8Array {
    return encodeValue(value);
  }

  loads(data: Uint8Array): unknown {
    return decodeValue(data);
  }
}

export interface EncryptedSerializerOptions {
  readonly key: Uint8Array | string; // 32-byte AES-256 key (string = KDF-derived)
  readonly cipher?: string;
  /** Key derivation for string secrets. Default "scrypt" (slow, memory-hard,
   * resists offline brute force of low-entropy secrets); "legacy-sha256"
   * reads data encrypted by older releases. Ignored for explicit Uint8Array
   * keys. Production is recommended to pass a random 32-byte key. */
  readonly kdf?: "scrypt" | "legacy-sha256";
}

/** Fixed application salt for the scrypt derivation (see kdf option). */
const SCRYPT_SALT = "reactivegraph-encrypted-serializer-v1";
// OWASP-recommended minimum scrypt parameters (N=2^14, r=8, p=1).
const SCRYPT_N = 16384;
const SCRYPT_R = 8;
const SCRYPT_P = 1;

/**
 * AES-256-GCM encrypted serializer over the canonical msgpack payload. A fresh
 * 12-byte nonce per encryption; authenticated tag protects integrity. Failure to
 * decrypt raises, and is not retried (plan Task 2.7.3).
 */
export class EncryptedSerializer implements EncryptingSerializer {
  readonly encrypted = true;
  private readonly key: Buffer;
  private readonly cipher: string;

  constructor(opts: EncryptedSerializerOptions) {
    if (opts.key instanceof Uint8Array) {
      this.key = Buffer.from(opts.key);
    } else if (opts.kdf === "legacy-sha256") {
      this.key = legacyDeriveKey(opts.key);
    } else {
      this.key = deriveKeyScrypt(opts.key);
    }
    if (this.key.length !== 32) {
      throw new Error("encryption key must be 32 bytes (AES-256)");
    }
    this.cipher = opts.cipher ?? "aes-256-gcm";
  }

  dumps(value: unknown): Uint8Array {
    const payload = encodeValue(value);
    const nonce = randomBytes(12);
    const cipher = createCipheriv(this.cipher, this.key, nonce) as CipherGCM;
    const encrypted = Buffer.concat([cipher.update(payload), cipher.final()]);
    const tag = cipher.getAuthTag();
    // layout: nonce(12) | tag(16) | ciphertext
    const out = Buffer.alloc(nonce.length + tag.length + encrypted.length);
    nonce.copy(out, 0);
    tag.copy(out, nonce.length);
    encrypted.copy(out, nonce.length + tag.length);
    return new Uint8Array(out);
  }

  loads(data: Uint8Array): unknown {
    const buf = Buffer.from(data);
    if (buf.length < 28) throw new CodecError("encrypted payload too short");
    const nonce = buf.subarray(0, 12);
    const tag = buf.subarray(12, 28);
    const ciphertext = buf.subarray(28);
    const decipher = createDecipheriv(this.cipher, this.key, nonce) as DecipherGCM;
    decipher.setAuthTag(tag);
    const payload = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    return decodeValue(new Uint8Array(payload));
  }
}

/** scrypt key derivation: slow + memory-hard, resists offline brute force. */
function deriveKeyScrypt(secret: string): Buffer {
  return scryptSync(secret, SCRYPT_SALT, 32, {
    N: SCRYPT_N,
    r: SCRYPT_R,
    p: SCRYPT_P,
    maxmem: 64 * 1024 * 1024,
  });
}

/** Legacy fast derivation used before KDF support (kept for reading old data). */
function legacyDeriveKey(secret: string): Buffer {
  return createHash("sha256").update(secret).digest();
}

export const serializer = new JsonPlusSerializer();
