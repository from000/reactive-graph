/**
 * State patches (Task 5 / docs/spec/state-and-transactions.md §C.2).
 *
 * A patch carries: path, operation, beforeHash (pre-image hash), value, taskId,
 * transactionId. Every operation expects a matching pre-image hash; the store
 * rejects patches with a mismatched pre-image unless a reducer owns the path.
 */

import type { Path } from "./path.js";
import { deleteAtPath, getAtPath, pathEquals, setAtPath } from "./path.js";
import { canonicalHash } from "./hash.js";

export type PatchOperation =
  | "set"
  | "delete"
  | "append"
  | "insert"
  | "remove"
  | "map_set"
  | "map_delete"
  | "set_add"
  | "set_delete";

export const PATCH_OPERATIONS: readonly PatchOperation[] = [
  "set",
  "delete",
  "append",
  "insert",
  "remove",
  "map_set",
  "map_delete",
  "set_add",
  "set_delete",
];

export interface Patch {
  readonly path: Path;
  readonly operation: PatchOperation;
  readonly beforeHash: string | null;
  readonly value?: unknown;
  readonly taskId: string;
  readonly transactionId: string;
}

export class PatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PatchError";
  }
}

export interface ApplyPatchOptions {
  /** Skip the pre-image hash check (durable recovery replay). */
  skipPreImage?: boolean;
}

/** Apply a patch to `root`. Throws PatchError on pre-image mismatch
 * (unless `opts.skipPreImage` — recovery replay treats the log as
 * authoritative and never rejects on pre-image). */
export function applyPatch(
  root: Record<string, unknown>,
  patch: Patch,
  opts: ApplyPatchOptions = {},
): boolean {
  const before = getAtPath(root, patch.path);
  if (
    !opts.skipPreImage &&
    patch.beforeHash !== null &&
    canonicalHash(before) !== patch.beforeHash
  ) {
    throw new PatchError(
      `pre-image mismatch at ${JSON.stringify(patch.path)}: expected ${patch.beforeHash}, got ${canonicalHash(before)}`,
    );
  }
  switch (patch.operation) {
    case "set": {
      setAtPath(root, patch.path, patch.value);
      return true;
    }
    case "delete": {
      deleteAtPath(root, patch.path);
      return true;
    }
    case "append": {
      if (!Array.isArray(before))
        throw new PatchError(`append requires array at ${JSON.stringify(patch.path)}`);
      before.push(patch.value);
      return true;
    }
    case "insert": {
      if (!Array.isArray(before))
        throw new PatchError(`insert requires array at ${JSON.stringify(patch.path)}`);
      const index = (patch.value as { index: number; value: unknown })["index"] ?? 0;
      before.splice(index, 0, (patch.value as { value: unknown })["value"]);
      return true;
    }
    case "remove": {
      if (!Array.isArray(before))
        throw new PatchError(`remove requires array at ${JSON.stringify(patch.path)}`);
      const index = patch.value as number;
      before.splice(index, 1);
      return true;
    }
    case "map_set": {
      if (before === undefined || before === null)
        throw new PatchError(`map_set requires object at ${JSON.stringify(patch.path)}`);
      const o = before as Record<string, unknown>;
      const entry = patch.value as { key: string; value: unknown };
      o[entry.key] = entry.value;
      return true;
    }
    case "map_delete": {
      if (before === undefined || before === null)
        throw new PatchError(`map_delete requires object at ${JSON.stringify(patch.path)}`);
      const o = before as Record<string, unknown>;
      delete o[patch.value as string];
      return true;
    }
    case "set_add": {
      if (!Array.isArray(before))
        throw new PatchError(`set_add requires array at ${JSON.stringify(patch.path)}`);
      if (!before.includes(patch.value)) {
        before.push(patch.value);
        return true;
      }
      return false;
    }
    case "set_delete": {
      if (!Array.isArray(before))
        throw new PatchError(`set_delete requires array at ${JSON.stringify(patch.path)}`);
      const idx = before.indexOf(patch.value);
      if (idx >= 0) {
        before.splice(idx, 1);
        return true;
      }
      return false;
    }
    default:
      throw new PatchError(`unknown operation ${String(patch.operation)}`);
  }
}

/** Compute the inverse of a patch (for rollback); works for value-carrying ops. */
export function invertPatch(patch: Patch, before: unknown): Patch {
  return { ...patch, operation: "set", value: before, beforeHash: patch.beforeHash };
}

export function patchEquals(a: Patch, b: Patch): boolean {
  return (
    pathEquals(a.path, b.path) &&
    a.operation === b.operation &&
    a.beforeHash === b.beforeHash &&
    JSON.stringify(a.value) === JSON.stringify(b.value) &&
    a.taskId === b.taskId &&
    a.transactionId === b.transactionId
  );
}

export { canonicalHash };
