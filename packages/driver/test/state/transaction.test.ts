import { describe, expect, it } from "vitest";
import {
  applyPatch,
  canonicalHash,
  parsePath,
  pathToString,
  ReactiveStore,
  ReadonlyPathError,
  StaleVersionError,
  Transaction,
} from "../../src/state/index.js";

describe("path codec", () => {
  it("round-trips canonical paths with escaping and indexes", () => {
    const cases: [string, (string | number)[]][] = [
      ["", []],
      ["a", ["a"]],
      ["a.b", ["a", "b"]],
      ["a[0]", ["a", 0]],
      ["a.b[1].c", ["a", "b", 1, "c"]],
      ["a\\.b", ["a.b"]],
    ];
    for (const [canonical, segs] of cases) {
      expect(pathToString(segs)).toBe(canonical);
      expect(parsePath(canonical)).toEqual(segs);
    }
  });
});

describe("transactional reactive store", () => {
  it("commits nested writes as one version bump", () => {
    const store = new ReactiveStore({ a: { b: { c: 1 } }, list: [] });
    const tx = new Transaction(store, { taskId: "t1" });
    tx.set(["a", "b", "c"], 2);
    tx.set(["list"], [1]);
    const versionBefore = store.version;
    const patches = tx.commit();
    expect(patches).toHaveLength(2);
    expect(store.raw.a.b.c).toBe(2);
    expect(store.raw.list).toEqual([1]);
    expect(store.version).toBe(versionBefore + 1);
  });

  it("records read paths", () => {
    const store = new ReactiveStore({ a: { b: 1 } });
    const tx = new Transaction(store);
    tx.read(["a", "b"]);
    expect(tx.readPaths).toEqual(["a.b"]);
  });

  it("conditional dependency cleanup — reads that never happened are absent", () => {
    const store = new ReactiveStore({ flag: false, x: 1, y: 2 });
    const tx = new Transaction(store);
    tx.read(["flag"]);
    // branch not taken -> no read of y
    expect(tx.didRead(["y"])).toBe(false);
    expect(tx.didRead(["flag"])).toBe(true);
  });

  it("array append/splice dependency via set", () => {
    const store = new ReactiveStore({ list: [1, 2, 3] });
    const tx = new Transaction(store);
    tx.set(["list"], [1, 2, 3, 4]);
    tx.commit();
    expect(store.raw.list).toEqual([1, 2, 3, 4]);
    const tx2 = new Transaction(store);
    tx2.set(["list"], store.raw.list.slice(0, 2));
    tx2.commit();
    expect(store.raw.list).toEqual([1, 2]);
  });

  it("map and set mutation via canonical values", () => {
    const store = new ReactiveStore({ m: { k: "v" }, s: ["a"] });
    const tx = new Transaction(store);
    tx.set(["m", "k2"], "v2");
    tx.set(["s"], ["a", "b"]);
    tx.commit();
    expect(store.raw.m).toEqual({ k: "v", k2: "v2" });
    expect(store.raw.s).toEqual(["a", "b"]);
  });

  it("rejects writes to readonly paths", () => {
    const store = new ReactiveStore({ protected: { x: 1 }, free: 2 });
    const tx = new Transaction(store, { readonlyPaths: ["protected"] });
    expect(() => tx.set(["protected", "x"], 9)).toThrow(ReadonlyPathError);
    tx.set(["free"], 3);
    tx.commit();
    expect(store.raw.free).toBe(3);
  });

  it("rollback restores prior values without bumping version", () => {
    const store = new ReactiveStore({ a: 1 });
    const tx = new Transaction(store);
    tx.set(["a"], 100);
    expect(store.raw.a).toBe(100); // direct mutation visible in-transaction
    tx.rollback();
    expect(store.raw.a).toBe(1);
    expect(store.version).toBe(0);
  });

  it("stale expected version is rejected", () => {
    const store = new ReactiveStore({ a: 1 });
    const tx = new Transaction(store, { expectedVersion: 5 });
    expect(() => tx.set(["a"], 2)).toThrow(StaleVersionError);
  });

  it("requires a new transaction after commit", () => {
    const store = new ReactiveStore({ a: 1 });
    const tx = new Transaction(store);
    tx.set(["a"], 2);
    tx.commit();
    expect(() => tx.set(["a"], 3)).toThrow(/already closed/);
  });
});

describe("patches", () => {
  it("round-trips a patch against a fresh root", () => {
    const root = { a: { b: { c: 1 } } };
    const patch = {
      path: ["a", "b"] as (string | number)[],
      operation: "set" as const,
      beforeHash: canonicalHash({ c: 1 }),
      value: { c: 2 },
      taskId: "t1",
      transactionId: "tx1",
    };
    applyPatch(root, patch);
    expect(root.a.b).toEqual({ c: 2 });
    expect(pathToString(["a", "b"])).toBe("a.b");
  });

  // Shared patch corpus (Task 5 step 5): these exact SHA-256 hex digests must
  // match the Python side (python/reactivegraph/tests/test_tracked_state.py).
  const CORPUS: [unknown, string][] = [
    [1, "4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a"],
    ["hello", "2b57c5b79a3aee10237006d2fc64b7ecd13b761867f5992f43eda5777a0726d9"],
    [[1, 2, 3], "efd2ce5d1b243784f054828796128a9e3f85044cbfc21f7144a7a448ea3361e6"],
    [{ a: 1, b: [true, null] }, "781ba872c03932379df02707033108b09a372c8e65767985971101cde7d79c0a"],
    ["rgp:missing", "e6849aec7e0b007792f96dd2ed18e7388696b0d6eca1d01f2a4b1f28d6889351"],
  ];

  it("shared corpus hashes are byte-identical across languages", () => {
    for (const [value, expected] of CORPUS) {
      expect(canonicalHash(value)).toBe(expected);
    }
  });

  it("rejects a patch with mismatched pre-image", () => {
    const root = { a: 1 };
    const patch = {
      path: ["a"] as (string | number)[],
      operation: "set" as const,
      beforeHash: canonicalHash(999),
      value: 2,
      taskId: "t1",
      transactionId: "tx1",
    };
    expect(() => applyPatch(root, patch)).toThrow(/pre-image mismatch/);
  });

  it("append/insert/remove/set ops on arrays", () => {
    const root = { list: [1, 2, 3] };
    applyPatch(root, {
      path: ["list"],
      operation: "append",
      beforeHash: canonicalHash([1, 2, 3]),
      value: 4,
      taskId: "t",
      transactionId: "tx",
    });
    expect(root.list).toEqual([1, 2, 3, 4]);
    applyPatch(root, {
      path: ["list"],
      operation: "remove",
      beforeHash: canonicalHash([1, 2, 3, 4]),
      value: 1,
      taskId: "t",
      transactionId: "tx",
    });
    expect(root.list).toEqual([1, 3, 4]); // remove index 1
  });
});
