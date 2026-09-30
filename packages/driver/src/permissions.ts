/**
 * Field-level state permissions (Task 12 / D.9.105).
 *
 * Enforced at dispatch (a task may only read/write fields it is allowed to)
 * and at patch validation (a returned patch that touches a denied path is
 * rejected). Nested paths are checked segment-wise so `a.b` under an allowed
 * prefix works while `credits.ledger` stays off-limits.
 */

import type { TracePath } from "./trace.js";

export type PermissionAction = "read" | "write";

export interface PermissionPolicy {
  /** Prefixes a principal may access for the given action. */
  allow: ReadonlyMap<PermissionAction, readonly TracePath[]>;
}

export function allowPaths(
  read: readonly TracePath[],
  write: readonly TracePath[],
): PermissionPolicy {
  return {
    allow: new Map([
      ["read", read],
      ["write", write],
    ]),
  };
}

/** Segment-wise prefix match: `["a"]` allows `["a","b"]`, denies `["ab"]`. */
export function isPathAllowed(
  policy: PermissionPolicy,
  action: PermissionAction,
  path: TracePath,
): boolean {
  const allowed = policy.allow.get(action) ?? [];
  for (const prefix of allowed) {
    if (prefix.length === 0) return true; // explicit root access
    // Compare segment-by-segment (not dotted strings) so a literal key that
    // contains "." (e.g. "a.b") is never wrongly treated as a nested path.
    if (prefix.length > path.length) continue;
    let match = true;
    for (let i = 0; i < prefix.length; i++) {
      if (String(prefix[i]) !== String(path[i])) {
        match = false;
        break;
      }
    }
    if (match) return true;
  }
  return false;
}

/** Validate a list of patches; throws on the first denied path. */
export function validatePatches(
  policy: PermissionPolicy,
  patches: ReadonlyArray<{ path: TracePath; value?: unknown }>,
): void {
  for (const patch of patches) {
    if (!isPathAllowed(policy, "write", patch.path)) {
      throw new PermissionDeniedError("write", patch.path);
    }
  }
}

export class PermissionDeniedError extends Error {
  readonly action: PermissionAction;
  readonly path: TracePath;
  constructor(action: PermissionAction, path: TracePath) {
    super(`permission denied: ${action} ${path.map(String).join(".")}`);
    this.name = "PermissionDeniedError";
    this.action = action;
    this.path = path;
  }
}
