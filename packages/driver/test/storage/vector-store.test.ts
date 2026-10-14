import { afterEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  MemoryVectorStore,
  SqliteVectorStore,
  cosineSimilarity,
  type VectorStore,
} from "../../src/index.js";

const tmpDirs: string[] = [];
function sqliteStore(): VectorStore {
  const dir = mkdtempSync(join(tmpdir(), "rgp-vec-"));
  tmpDirs.push(dir);
  return new SqliteVectorStore(join(dir, "v.db"));
}

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/** Unit-ish vectors pointing in different directions. */
const DOC = [1, 0, 0];
const QUERY = [0.9, 0.1, 0];
const OTHER = [0, 1, 0];

describe.each([
  ["memory", () => new MemoryVectorStore()],
  ["sqlite", sqliteStore],
] as const)("vector store (%s)", (_name, make) => {
  it("ranks by cosine similarity and applies topK", async () => {
    const store = make();
    await store.upsert({ namespace: ["docs"], id: "a", vector: DOC, metadata: { title: "alpha" } });
    await store.upsert({
      namespace: ["docs"],
      id: "b",
      vector: OTHER,
      metadata: { title: "beta" },
    });
    const hits = await store.search(["docs"], QUERY, { limit: 1 });
    expect(hits).toHaveLength(1);
    expect(hits[0]!.id).toBe("a");
    expect(hits[0]!.score).toBeGreaterThan(0.9);
    expect(hits[0]!.metadata).toEqual({ title: "alpha" });
  });

  it("supports namespace isolation and minScore", async () => {
    const store = make();
    await store.upsert({ namespace: ["docs"], id: "a", vector: DOC });
    await store.upsert({ namespace: ["scratch"], id: "b", vector: DOC });
    expect(await store.search(["docs"], QUERY)).toHaveLength(1);
    expect(await store.search(["docs"], OTHER, { minScore: 0.99 })).toEqual([]);
  });

  it("upsert replaces and delete removes", async () => {
    const store = make();
    await store.upsert({ namespace: ["n"], id: "k", vector: DOC });
    await store.upsert({ namespace: ["n"], id: "k", vector: OTHER });
    expect((await store.search(["n"], OTHER))[0]!.id).toBe("k");
    await store.delete(["n"], "k");
    expect(await store.search(["n"], OTHER)).toEqual([]);
  });
});

describe("cosineSimilarity", () => {
  it("returns 1 for identical, 0 for orthogonal, 0 for length mismatch", () => {
    expect(cosineSimilarity([1, 0], [1, 0])).toBe(1);
    expect(cosineSimilarity([1, 0], [0, 1])).toBe(0);
    expect(cosineSimilarity([1], [1, 0])).toBe(0);
  });
});
