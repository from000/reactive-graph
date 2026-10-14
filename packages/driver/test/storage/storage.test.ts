import { describe, expect, it } from "vitest";
import { createRequire } from "node:module";
import {
  EncryptedSerializer,
  JsonPlusSerializer,
  MemoryCache,
  MemoryCheckpointSaver,
  MemoryStore,
  SqliteCache,
  SqliteCheckpointSaver,
  SqliteStore,
  type CheckpointRecord,
  type CheckpointWrite,
} from "../../src/storage/index.js";

const nodeRequire = createRequire(import.meta.url);

function record(threadId: string, checkpointId: string, n: number): CheckpointRecord {
  return {
    threadId,
    checkpointId,
    ts: `2026-01-01T00:00:${String(n).padStart(2, "0")}Z`,
    values: new Uint8Array([n]),
    metadata: new Uint8Array([n + 1]),
  };
}

describe.each([
  ["memory", () => new MemoryCheckpointSaver()],
  ["sqlite", () => new SqliteCheckpointSaver(":memory:")],
])("checkpoint saver (%s)", (_name, make) => {
  it("put + get round-trips a checkpoint", async () => {
    const saver = make();
    await saver.put(record("t1", "c1", 1));
    const got = await saver.get("t1", "c1");
    expect(got?.checkpointId).toBe("c1");
    expect(Array.from(got!.values)).toEqual([1]);
  });

  it("get returns the latest checkpoint when id omitted", async () => {
    const saver = make();
    await saver.put(record("t1", "c1", 1));
    await saver.put(record("t1", "c2", 2));
    expect((await saver.get("t1"))?.checkpointId).toBe("c2");
  });

  it("list returns history and honors limit", async () => {
    const saver = make();
    for (let i = 1; i <= 3; i++) saver.put(record("t1", `c${i}`, i));
    const all = await saver.list("t1");
    expect(all).toHaveLength(3);
    const limited = await saver.list("t1", { limit: 2 });
    expect(limited.map((r) => r.checkpointId)).toEqual(["c2", "c3"]);
    const before = await saver.list("t1", { before: "c3" });
    expect(before.map((r) => r.checkpointId)).toEqual(["c1", "c2"]);
  });

  it("putWrites accumulates version-stamped writes per checkpoint", async () => {
    const saver = make();
    await saver.put(record("t1", "c1", 1));
    await saver.putWrites("t1", "c1", [
      { taskId: "t", channel: "messages", value: new Uint8Array([9]) } satisfies CheckpointWrite,
    ]);
    const got = await saver.get("t1", "c1");
    expect(got?.writes).toHaveLength(1);
  });

  it("copy threads / delete for runs / prune / deleteThread", async () => {
    const saver = make();
    for (let i = 1; i <= 3; i++) saver.put(record("t1", `c${i}`, i));
    await saver.copy("t1", "t2");
    expect(await saver.list("t2")).toHaveLength(3);
    await saver.prune("t1", 2);
    expect(await (await saver.list("t1")).map((r) => r.checkpointId)).toEqual(["c2", "c3"]);
    await saver.deleteForRuns(["t1"]);
    expect(await saver.list("t1")).toHaveLength(0);
    await saver.deleteThread("t2");
    expect(await saver.list("t2")).toHaveLength(0);
  });
});

describe.each([
  ["memory", () => new MemoryStore()],
  ["sqlite", () => new SqliteStore(":memory:")],
])("store (%s)", (_name, make) => {
  it("get / put / delete / listNamespaces", async () => {
    const store = make();
    await store.put({ namespace: ["doc"], key: "a", value: new Uint8Array([1]) });
    await store.put({ namespace: ["doc", "x"], key: "b", value: new Uint8Array([2]) });
    expect((await store.get(["doc"], "a"))?.key).toBe("a");
    await store.delete(["doc"], "a");
    expect(await store.get(["doc"], "a")).toBeNull();
  });

  it("search honors filter and limit", async () => {
    const store = make();
    const ser = new JsonPlusSerializer();
    await store.put({ namespace: ["doc"], key: "a", value: ser.dumps({ kind: "note", n: 1 }) });
    await store.put({ namespace: ["doc"], key: "b", value: ser.dumps({ kind: "todo", n: 2 }) });
    const hits = await store.search(["doc"], { filter: new Map([["kind", "todo"]]), limit: 10 });
    expect(hits).toHaveLength(1);
    expect(ser.loads(hits[0]!.value)).toEqual({ kind: "todo", n: 2 });
  });

  it("search escapes LIKE metacharacters in namespaces", async () => {
    const store = make();
    await store.put({ namespace: ["a%b"], key: "k1", value: new Uint8Array([1]) });
    await store.put({ namespace: ["axb"], key: "k2", value: new Uint8Array([2]) });
    // `%` in a namespace segment must not act as a wildcard (parity with memory)
    const hits = await store.search(["a%b"]);
    expect(hits).toHaveLength(1);
    expect(hits[0]!.key).toBe("k1");
    const other = await store.search(["axb"]);
    expect(other).toHaveLength(1);
    expect(other[0]!.key).toBe("k2");
  });
});

describe("cache + TTL", () => {
  it("memory cache expires entries", async () => {
    const cache = new MemoryCache();
    await cache.set("k", { value: new Uint8Array([1]), expiresAt: Date.now() - 1000 });
    expect(await cache.get("k")).toBeNull();
    await cache.set("k2", { value: new Uint8Array([2]) });
    expect((await cache.get("k2"))?.value[0]).toBe(2);
    expect(await cache.has("k2")).toBe(true);
  });

  it("sqlite cache persists TTL", async () => {
    const cache = new SqliteCache(":memory:");
    await cache.set("k", { value: new Uint8Array([7]), expiresAt: Date.now() + 60_000 });
    expect((await cache.get("k"))?.value[0]).toBe(7);
    expect(await cache.has("k")).toBe(true);
    await cache.delete("k");
    expect(await cache.has("k")).toBe(false);
  });
});

describe("serializers (Task 2.7)", () => {
  it("JsonPlusSerializer round-trips canonical values", async () => {
    const ser = new JsonPlusSerializer();
    const value = {
      n: 9007199254740993n,
      bytes: new Uint8Array([1, 2]),
      at: new Date(1_700_000_000_000),
    };
    const decoded = ser.loads(ser.dumps(value)) as typeof value;
    expect(decoded.n).toBe(9007199254740993n);
    expect(decoded.bytes).toBeInstanceOf(Uint8Array);
    expect(decoded.at).toBeInstanceOf(Date);
  });

  it("EncryptedSerializer round-trips and resists tampering", async () => {
    const ser = new EncryptedSerializer({ key: "a".repeat(8) });
    const payload = { secret: "top", list: [1, 2, 3] };
    const encrypted = ser.dumps(payload);
    expect(ser.encrypted).toBe(true);
    expect(ser.loads(encrypted)).toEqual(payload);
    // tamper -> decrypt fails
    const tampered = new Uint8Array(encrypted);
    tampered[tampered.length - 1] ^= 0xff;
    expect(() => ser.loads(tampered)).toThrow();
  });
});

describe("sqlite schema guard (Task 9 §4)", () => {
  it("rejects a store stamped with a newer schema", async () => {
    const { mkdtempSync, rmSync } = nodeRequire("node:fs") as {
      mkdtempSync(p: string): string;
      rmSync(p: string, o: { recursive: boolean; force: boolean }): void;
    };
    const dir = mkdtempSync("/tmp/rgp-schema-");
    const file = `${dir}/s.db`;
    try {
      const s1 = new SqliteCheckpointSaver(file);
      expect(s1.schemaVersion).toBe(1);
      // bump the stored schema beyond what the driver supports
      (s1 as unknown as { db: { exec(sql: string): void } }).db.exec(
        "UPDATE rgp_meta SET value = '999' WHERE key = 'schema_version'",
      );
      s1.close();
      expect(() => new SqliteCheckpointSaver(file)).toThrow(/newer than driver/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
describe("sqlite schema guard (Task 9 §4)", () => {
  it("rejects a store stamped with a newer schema", async () => {
    const { mkdtempSync, rmSync } = nodeRequire("node:fs");
    const dir = mkdtempSync("/tmp/rgp-schema-");
    const file = `${dir}/s.db`;
    try {
      const s1 = new SqliteCheckpointSaver(file);
      expect(s1.schemaVersion).toBe(1);
      // bump the stored schema beyond what the driver supports
      (s1 as unknown as { db: { exec(sql: string): void } }).db.exec(
        "UPDATE rgp_meta SET value = '999' WHERE key = 'schema_version'",
      );
      s1.close();
      expect(() => new SqliteCheckpointSaver(file)).toThrow(/newer than driver/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("EncryptedSerializer key derivation (KDF hardening)", () => {
  it("default scrypt derivation round-trips across instances", () => {
    const a = new EncryptedSerializer({ key: "s3cret-password" });
    const b = new EncryptedSerializer({ key: "s3cret-password" });
    const blob = a.dumps({ ok: 1 });
    expect(b.loads(blob)).toEqual({ ok: 1 });
  });

  it("scrypt and legacy-sha256 derivations are incompatible (no silent cross-read)", () => {
    const scrypt = new EncryptedSerializer({ key: "same-secret" });
    const legacy = new EncryptedSerializer({ key: "same-secret", kdf: "legacy-sha256" });
    const blob = scrypt.dumps({ v: 1 });
    // Same secret, different derivation -> different key -> auth failure.
    expect(() => legacy.loads(blob)).toThrow();
  });

  it("legacy-sha256 mode reads data written by older releases", () => {
    // Simulate a pre-KDF record: sha256-derived key encryption.
    const legacy = new EncryptedSerializer({ key: "old-secret", kdf: "legacy-sha256" });
    const blob = legacy.dumps({ old: true });
    const reader = new EncryptedSerializer({ key: "old-secret", kdf: "legacy-sha256" });
    expect(reader.loads(blob)).toEqual({ old: true });
  });

  it("an explicit 32-byte key bypasses KDF entirely", () => {
    const key = new Uint8Array(32).fill(7);
    const a = new EncryptedSerializer({ key });
    const b = new EncryptedSerializer({ key });
    const blob = a.dumps({ raw: true });
    expect(b.loads(blob)).toEqual({ raw: true });
  });

  it("rejects a wrong secret with an auth failure (scrypt)", () => {
    const a = new EncryptedSerializer({ key: "correct-horse" });
    const b = new EncryptedSerializer({ key: "wrong-horse" });
    const blob = a.dumps({ secret: true });
    expect(() => b.loads(blob)).toThrow();
  });
});
