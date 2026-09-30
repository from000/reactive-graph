import { describe, expect, it } from "vitest";
import {
  PostgresCache,
  PostgresCheckpointSaver,
  PostgresStore,
  type CheckpointRecord,
  type CheckpointWrite,
} from "../../src/storage/index.js";

/**
 * M3-F8: Postgres backends. Run against a real database when reachable; skip
 * when not (set RGP_PG_DSN to point at one, e.g. in CI).
 */
const DSN = process.env["RGP_PG_DSN"] ?? "postgres://postgres:postgres@127.0.0.1:5432/rgp_test";

const pgAvailable = await (async () => {
  try {
    const saver = new PostgresCheckpointSaver(DSN);
    await saver.init();
    await saver.close();
    return true;
  } catch {
    return false;
  }
})();

const OR_DISABLED = pgAvailable ? "" : " (postgres unavailable; set RGP_PG_DSN)";

function record(threadId: string, checkpointId: string, n: number): CheckpointRecord {
  return {
    threadId,
    checkpointId,
    ts: `2026-01-01T00:00:${String(n).padStart(2, "0")}Z`,
    values: new Uint8Array([n]),
    metadata: new Uint8Array([n + 1]),
  };
}

describe.skipIf(!pgAvailable)(`PostgresCheckpointSaver${OR_DISABLED}`, () => {
  it("put + get + list round-trip and honor latest/limit", async () => {
    const saver = new PostgresCheckpointSaver(DSN);
    try {
      await saver.init();
      for (let i = 1; i <= 3; i++) await saver.put(record("t1", `c${i}`, i));
      expect((await saver.get("t1"))?.checkpointId).toBe("c3");
      expect((await saver.get("t1", "c1"))?.checkpointId).toBe("c1");
      expect(Array.from((await saver.get("t1", "c2"))!.values)).toEqual([2]);
      const all = await saver.list("t1");
      expect(all.map((r) => r.checkpointId)).toEqual(["c1", "c2", "c3"]);
      expect((await saver.list("t1", { limit: 1 })).map((r) => r.checkpointId)).toEqual(["c3"]);
      expect((await saver.list("t1", { before: "c2" })).map((r) => r.checkpointId)).toEqual(["c1"]);
    } finally {
      await saver.deleteThread("t1");
      await saver.close();
    }
  });

  it("putWrites attach incremental writes to a checkpoint", async () => {
    const saver = new PostgresCheckpointSaver(DSN);
    try {
      await saver.init();
      await saver.put(record("t1", "c1", 1));
      const writes: CheckpointWrite[] = [{ taskId: "a", channel: "x", value: new Uint8Array([9]) }];
      await saver.putWrites("t1", "c1", writes);
      expect((await saver.get("t1", "c1"))?.writes?.length).toBe(1);
    } finally {
      await saver.deleteThread("t1");
      await saver.close();
    }
  });

  it("copy and prune mirror the sqlite semantics", async () => {
    const saver = new PostgresCheckpointSaver(DSN);
    try {
      await saver.init();
      for (let i = 1; i <= 3; i++) await saver.put(record("src", `c${i}`, i));
      await saver.copy("src", "dst");
      expect((await saver.list("dst")).length).toBe(3);
      await saver.prune("src", 1);
      const kept = await saver.list("src");
      expect(kept.length).toBe(1);
      expect(kept[0]!.checkpointId).toBe("c3");
    } finally {
      await saver.deleteThread("src");
      await saver.deleteThread("dst");
      await saver.close();
    }
  });
});

describe.skipIf(!pgAvailable)(`PostgresStore${OR_DISABLED}`, () => {
  it("put/get/search/delete operate on namespaced items", async () => {
    const store = new PostgresStore(DSN);
    try {
      await store.init();
      await store.put({
        namespace: ["users", "alice"],
        key: "prefs",
        value: new Uint8Array([1, 2, 3]),
      });
      const got = await store.get(["users", "alice"], "prefs");
      expect(Array.from(got!.value)).toEqual([1, 2, 3]);
      const hits = await store.search(["users"]);
      expect(hits.some((h) => h.key === "prefs")).toBe(true);
      const nss = await store.listNamespaces();
      expect(nss.some((n) => n.join("/") === "users/alice")).toBe(true);
      await store.delete(["users", "alice"], "prefs");
      expect(await store.get(["users", "alice"], "prefs")).toBeNull();
    } finally {
      await store.close();
    }
  });
});

describe.skipIf(!pgAvailable)(`PostgresCache${OR_DISABLED}`, () => {
  it("set/get/has honors TTL and delete", async () => {
    const cache = new PostgresCache(DSN);
    try {
      await cache.init();
      await cache.set("k1", { value: new Uint8Array([42]) });
      expect(await cache.has("k1")).toBe(true);
      expect(Array.from((await cache.get("k1"))!.value)).toEqual([42]);
      await cache.set("kexp", { value: new Uint8Array([1]), expiresAt: Date.now() - 1 });
      expect(await cache.get("kexp")).toBeNull();
      await cache.delete("k1");
      expect(await cache.has("k1")).toBe(false);
    } finally {
      await cache.close();
    }
  });
});
