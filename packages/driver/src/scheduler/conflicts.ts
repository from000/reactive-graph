/**
 * Read/write conflict detection (Task 7 / plan Appendix C.5).
 *
 * - Reads are optimistic and record a transaction version plus path hashes.
 * - Disjoint writes commit concurrently only when backend transaction semantics
 *   permit it.
 * - Same-path writes require a reducer, explicit priority, or serialized
 *   execution.
 * - A stale pure task may rerun with a new snapshot.
 * - A stale effect task cannot rerun until its previous receipt is inspected
 *   and idempotency policy approves the action.
 */

/** Canonical path conflict check. */
export interface WriteClaim {
  readonly taskId: string;
  readonly writes: readonly string[];
}

export interface ReadClaim {
  readonly taskId: string;
  readonly reads: readonly string[];
  readonly version: number;
}

export interface ConflictPolicy {
  /** Allow disjoint writes to commit concurrently. */
  allowConcurrentDisjointWrites?: boolean;
  /** Paths owned by a reducer (never conflict). */
  reducerOwned?: readonly string[];
  /** Explicit priority map: taskId -> rank (higher wins). */
  priority?: ReadonlyMap<string, number>;
}

export type ConflictVerdict =
  | { kind: "ok" }
  | {
      kind: "conflict";
      reason: string;
      otherTaskId: string;
      path: string;
    };

const DEFAULT_POLICY: Required<ConflictPolicy> = {
  allowConcurrentDisjointWrites: true,
  reducerOwned: [],
  priority: new Map(),
};

/**
 * Check a new write claim against already-committed write claims in the same
 * superstep. Returns "ok" or the first conflict found.
 */
export function detectWriteConflict(
  claim: WriteClaim,
  committed: readonly WriteClaim[],
  policy: ConflictPolicy = {},
): ConflictVerdict {
  const p: Required<ConflictPolicy> = { ...DEFAULT_POLICY, ...policy };
  for (const other of committed) {
    if (other.taskId === claim.taskId) continue;
    const shared = sharedPath(claim.writes, other.writes, p.reducerOwned);
    if (shared !== null) {
      // Explicit priority resolves: highest rank wins, lower yields.
      const mine = p.priority.get(claim.taskId) ?? 0;
      const theirs = p.priority.get(other.taskId) ?? 0;
      if (mine > theirs) continue;
      if (mine < theirs) {
        return {
          kind: "conflict",
          reason: "lower_priority_write",
          otherTaskId: other.taskId,
          path: shared,
        };
      }
      return {
        kind: "conflict",
        reason: "same_path_write",
        otherTaskId: other.taskId,
        path: shared,
      };
    }
    if (!p.allowConcurrentDisjointWrites) {
      // Any writes at all conflict when concurrency is disabled.
      return {
        kind: "conflict",
        reason: "concurrency_disabled",
        otherTaskId: other.taskId,
        path: claim.writes[0] ?? "$",
      };
    }
  }
  return { kind: "ok" };
}

/**
 * Detect a read/write conflict: a task's optimistic reads vs another task's
 * committed writes to the same path (stale read).
 */
export function detectReadWriteConflict(
  read: ReadClaim,
  committedWrites: readonly WriteClaim[],
): ConflictVerdict {
  for (const w of committedWrites) {
    if (w.taskId === read.taskId) continue;
    const shared = sharedPath(read.reads, w.writes, []);
    if (shared !== null) {
      return {
        kind: "conflict",
        reason: "stale_read",
        otherTaskId: w.taskId,
        path: shared,
      };
    }
  }
  return { kind: "ok" };
}

/** First shared canonical path between two path lists, skipping reducer-owned. */
function sharedPath(
  a: readonly string[],
  b: readonly string[],
  reducerOwned: readonly string[],
): string | null {
  for (const pa of a) {
    if (reducerOwned.some((r) => pa === r || pa.startsWith(`${r}.`))) continue;
    for (const pb of b) {
      if (sameOrNested(pa, pb) || sameOrNested(pb, pa)) return pa;
    }
  }
  return null;
}

/** True if `a` equals `b` or lies beneath it (canonical prefix match). */
function sameOrNested(a: string, b: string): boolean {
  if (a === b) return true;
  if (!a.startsWith(b)) return false;
  return a[b.length] === "." || a[b.length] === "[";
}
