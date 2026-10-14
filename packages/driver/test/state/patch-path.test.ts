import { describe, expect, it } from "vitest";

import {
  canonicalHash,
  PatchError,
  applyPatch,
  invertPatch,
  patchEquals,
  type Patch,
} from "../../src/state/patch.js";
import {
  deleteAtPath,
  getAtPath,
  isPathUnder,
  parsePath,
  pathAppend,
  pathEquals,
  pathParent,
  pathToString,
  setAtPath,
  type Path,
} from "../../src/state/path.js";

function makePatch(overrides: Partial<Patch> = {}): Patch {
  return {
    path: ["a"],
    operation: "set",
    beforeHash: null,
    value: 1,
    taskId: "task",
    transactionId: "tx",
    ...overrides,
  };
}

describe("path encoding", () => {
  it("round-trips nested object and array segments", () => {
    const path: Path = ["a", "b", 0, "c"];
    expect(pathToString(path)).toBe("a.b[0].c");
    expect(parsePath("a.b[0].c")).toEqual(path);
  });

  it("escapes dots and brackets inside string keys", () => {
    const path: Path = ["weird.key", "brack[et]", "tail"];
    const encoded = pathToString(path);
    expect(encoded).toBe("weird\\.key.brack\\[et].tail");
    expect(parsePath(encoded)).toEqual(path);
  });

  it("handles empty paths and root-numeric paths", () => {
    expect(pathToString([])).toBe("");
    expect(parsePath("")).toEqual([]);
    expect(parsePath("[0]")).toEqual([0]);
    expect(pathToString([0])).toBe("[0]");
  });

  it("parses consecutive numeric segments and trailing brackets", () => {
    expect(parsePath("a[0][1]")).toEqual(["a", 0, 1]);
    expect(parsePath("a[")).toEqual(["a"]);
    expect(parsePath(".")).toEqual([]);
    expect(parsePath("a.")).toEqual(["a"]);
  });

  it("parses a trailing backslash without inventing a segment", () => {
    expect(parsePath("a\\")).toEqual(["a"]);
  });

  it("compares, appends, and parents paths", () => {
    expect(isPathUnder(["a"], ["a", "b"])).toBe(true);
    expect(isPathUnder(["a"], ["a"])).toBe(true);
    expect(isPathUnder(["a", "b"], ["a"])).toBe(false);
    expect(isPathUnder(["a"], ["b"])).toBe(false);

    expect(pathAppend(["a"], "b")).toEqual(["a", "b"]);
    expect(pathParent([])).toBeNull();
    expect(pathParent(["a", "b"])).toEqual(["a"]);
    expect(pathEquals([], [])).toBe(true);
    expect(pathEquals(["a"], ["a"])).toBe(true);
    expect(pathEquals(["a"], ["b"])).toBe(false);
    expect(pathEquals(["a"], ["a", "b"])).toBe(false);
  });
});

describe("path get/set/delete", () => {
  it("reads through objects and arrays, and returns undefined for misses", () => {
    const root = { a: { b: [10, { c: 3 }] } };
    expect(getAtPath(root, ["a", "b", 0])).toBe(10);
    expect(getAtPath(root, ["a", "b", 1, "c"])).toBe(3);
    expect(getAtPath(root, [])).toBe(root);
    expect(getAtPath(root, ["missing"])).toBeUndefined();
    expect(getAtPath(root, ["a", 0])).toBeUndefined();
    expect(getAtPath(root, ["a", "b", 5])).toBeUndefined();
    expect(getAtPath(null, ["a"])).toBeUndefined();
  });

  it("creates missing containers based on the next segment type", () => {
    const root: Record<string, unknown> = {};
    setAtPath(root, ["obj", "list", 0], 1);
    expect(root).toEqual({ obj: { list: [1] } });

    const arr: Record<string, unknown> = { list: [] };
    setAtPath(arr, ["list", 1, "deep"], 2);
    expect(arr).toEqual({ list: [undefined, { deep: 2 }] });
  });

  it("replaces non-container values while walking a set", () => {
    const root: Record<string, unknown> = { a: 5 };
    setAtPath(root, ["a", "b"], 1);
    expect(root).toEqual({ a: { b: 1 } });

    const asArray: Record<string, unknown> = { a: 1 };
    setAtPath(asArray, ["a", 0], "x");
    expect(asArray).toEqual({ a: ["x"] });
  });

  it("rejects setting the empty path", () => {
    expect(() => setAtPath({}, [], 1)).toThrow(/empty path/);
  });

  it("deletes array entries and object keys, and no-ops on misses", () => {
    const root = { a: { b: [1, 2, 3] }, c: 1 };
    deleteAtPath(root, ["a", "b", 1]);
    expect(root.a.b).toEqual([1, 3]);
    deleteAtPath(root, ["c"]);
    expect("c" in root).toBe(false);

    // no-ops that must not throw
    deleteAtPath(root, []);
    deleteAtPath(root, ["missing", "x"]);
    deleteAtPath(root, ["c", "x"]);
    deleteAtPath(root, ["a", "b", 0, "x"]);
    deleteAtPath(root, ["a", "b", 0]);
    expect(root.a.b).toEqual([3]);
  });

  it("walks array and object chains during delete", () => {
    const arr: Record<string, unknown> = { xs: [{ y: 1 }] };
    deleteAtPath(arr, ["xs", 0, "y"]);
    expect(arr).toEqual({ xs: [{}] });
  });
});

describe("applyPatch operations", () => {
  it("sets and deletes values", () => {
    const root: Record<string, unknown> = { a: 1 };
    expect(applyPatch(root, makePatch({ operation: "set", value: 2 }))).toBe(true);
    expect(root.a).toBe(2);
    expect(applyPatch(root, makePatch({ operation: "delete" }))).toBe(true);
    expect("a" in root).toBe(false);
  });

  it("appends, inserts, and removes array elements", () => {
    const root: Record<string, unknown> = { xs: [1, 2] };
    expect(applyPatch(root, makePatch({ path: ["xs"], operation: "append", value: 3 }))).toBe(true);
    expect(root.xs).toEqual([1, 2, 3]);

    expect(
      applyPatch(
        root,
        makePatch({ path: ["xs"], operation: "insert", value: { index: 1, value: 9 } }),
      ),
    ).toBe(true);
    expect(root.xs).toEqual([1, 9, 2, 3]);

    expect(
      applyPatch(root, makePatch({ path: ["xs"], operation: "insert", value: { value: 0 } })),
    ).toBe(true);
    expect(root.xs).toEqual([0, 1, 9, 2, 3]);

    expect(applyPatch(root, makePatch({ path: ["xs"], operation: "remove", value: 0 }))).toBe(true);
    expect(root.xs).toEqual([1, 9, 2, 3]);
  });

  it("rejects array operations on non-arrays", () => {
    const root: Record<string, unknown> = { a: 1 };
    for (const operation of ["append", "insert", "remove", "set_add", "set_delete"] as const) {
      expect(() => applyPatch(root, makePatch({ operation }))).toThrow(PatchError);
    }
  });

  it("maps and unmaps object entries", () => {
    const root: Record<string, unknown> = { m: { a: 1 } };
    expect(
      applyPatch(
        root,
        makePatch({ path: ["m"], operation: "map_set", value: { key: "b", value: 2 } }),
      ),
    ).toBe(true);
    expect(root.m).toEqual({ a: 1, b: 2 });

    expect(applyPatch(root, makePatch({ path: ["m"], operation: "map_delete", value: "a" }))).toBe(
      true,
    );
    expect(root.m).toEqual({ b: 2 });
  });

  it("rejects map operations on nullish values", () => {
    const root: Record<string, unknown> = {};
    expect(() => applyPatch(root, makePatch({ operation: "map_set" }))).toThrow(PatchError);
    expect(() => applyPatch(root, makePatch({ operation: "map_delete" }))).toThrow(PatchError);
  });

  it("adds and deletes set entries idempotently", () => {
    const root: Record<string, unknown> = { s: [1] };
    expect(applyPatch(root, makePatch({ path: ["s"], operation: "set_add", value: 2 }))).toBe(true);
    expect(applyPatch(root, makePatch({ path: ["s"], operation: "set_add", value: 2 }))).toBe(
      false,
    );

    expect(applyPatch(root, makePatch({ path: ["s"], operation: "set_delete", value: 2 }))).toBe(
      true,
    );
    expect(applyPatch(root, makePatch({ path: ["s"], operation: "set_delete", value: 99 }))).toBe(
      false,
    );
    expect(root.s).toEqual([1]);
  });

  it("rejects unknown operations", () => {
    const root: Record<string, unknown> = { a: [] };
    expect(() => applyPatch(root, makePatch({ operation: "nope" as Patch["operation"] }))).toThrow(
      /unknown operation/,
    );
  });

  it("enforces pre-image hashes unless recovery replay skips them", () => {
    const root: Record<string, unknown> = { a: 1 };
    const stale = makePatch({ beforeHash: canonicalHash(999) });
    expect(() => applyPatch(root, stale)).toThrow(PatchError);
    expect(applyPatch(root, stale, { skipPreImage: true })).toBe(true);
    expect(root.a).toBe(1);

    const fresh = makePatch({ beforeHash: canonicalHash(1), value: 5 });
    expect(applyPatch(root, fresh)).toBe(true);
    expect(root.a).toBe(5);
  });
});

describe("patch helpers", () => {
  it("inverts a patch into a restoring set", () => {
    const patch = makePatch({ operation: "append", beforeHash: "h", value: 9 });
    expect(invertPatch(patch, [1, 2])).toEqual({
      ...patch,
      operation: "set",
      value: [1, 2],
      beforeHash: "h",
    });
  });

  it("compares every structural field", () => {
    const base = makePatch();
    expect(patchEquals(base, makePatch())).toBe(true);
    expect(patchEquals(base, makePatch({ path: ["other"] }))).toBe(false);
    expect(patchEquals(base, makePatch({ operation: "delete" }))).toBe(false);
    expect(patchEquals(base, makePatch({ beforeHash: "h" }))).toBe(false);
    expect(patchEquals(base, makePatch({ value: 2 }))).toBe(false);
    expect(patchEquals(base, makePatch({ taskId: "other" }))).toBe(false);
    expect(patchEquals(base, makePatch({ transactionId: "other" }))).toBe(false);
  });
});
