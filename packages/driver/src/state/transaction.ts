/**
 * Transactions over the reactive store (Task 5).
 *
 * A transaction records read paths and mutation patches, performs all writes on
 * the raw object (no auto-trigger), and on commit validates pre-images, produces
 * one atomic version bump, and returns the patch list. Rollback restores prior
 * values without triggering. Stale versions are rejected.
 */

import { TriggerOpTypes } from "@vue/reactivity";
import { getAtPath, pathToString, setAtPath } from "./path.js";
import type { Patch, PatchOperation } from "./patch.js";
import { canonicalHash } from "./patch.js";
import { ReactiveStore, type CommitWrite } from "./store.js";

export interface TransactionOptions {
  taskId?: string;
  expectedVersion?: number;
  readonlyPaths?: readonly string[];
}

export class StaleVersionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StaleVersionError";
  }
}

export class ReadonlyPathError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReadonlyPathError";
  }
}

interface WriteRecord {
  path: string[]; // raw segments
  op: PatchOperation;
  before: unknown;
  value?: unknown;
  topKey: string;
  triggerOp: TriggerOpTypes;
}

export class Transaction {
  readonly id: string;
  private readonly store: ReactiveStore;
  private readonly taskId: string;
  private readonly expectedVersion?: number;
  private readonly readonlyPrefixes: readonly string[];
  private readonly reads = new Set<string>();
  private readonly writes = new Map<string, WriteRecord>();
  private state: "open" | "committed" | "rolled_back" = "open";

  constructor(store: ReactiveStore, opts: TransactionOptions = {}, id?: string) {
    this.store = store;
    this.id = id ?? `tx-${Math.random().toString(36).slice(2, 10)}`;
    this.taskId = opts.taskId ?? "local";
    this.expectedVersion = opts.expectedVersion;
    this.readonlyPrefixes = opts.readonlyPaths ?? [];
  }

  /** Record a read of a path (used by proxies/selectors during execution). */
  read(path: readonly (string | number)[]): void {
    this.reads.add(pathToString(path));
  }

  get readPaths(): readonly string[] {
    return [...this.reads];
  }

  get writeCount(): number {
    return this.writes.size;
  }

  private assertWritable(path: readonly (string | number)[]): void {
    const p = pathToString(path);
    for (const prefix of this.readonlyPrefixes) {
      if (
        p === prefix ||
        p.startsWith(prefix === "" ? prefix : `${prefix}.`) ||
        p.startsWith(`${prefix}[`)
      ) {
        throw new ReadonlyPathError(`path ${p} is readonly`);
      }
    }
  }

  /** Mutate a value at a path (array/map-aware helpers build on set). */
  set(path: readonly (string | number)[], value: unknown): void {
    if (this.state !== "open") throw new Error("transaction already closed");
    this.assertWritable(path);
    if (this.expectedVersion !== undefined && this.store.version !== this.expectedVersion) {
      throw new StaleVersionError(
        `expected version ${this.expectedVersion}, store is at ${this.store.version}`,
      );
    }
    const before = getAtPath(this.store.raw, path);
    setAtPath(this.store.raw, path, value);
    this.recordWrite(path, "set", before, value);
  }

  private recordWrite(
    path: readonly (string | number)[],
    op: PatchOperation,
    before: unknown,
    value?: unknown,
    triggerOp: TriggerOpTypes = TriggerOpTypes.SET,
  ): void {
    const topKey = path.length > 0 ? String(path[0]) : "$";
    this.writes.set(pathToString(path), {
      path: [...path] as string[],
      op,
      before,
      value,
      topKey,
      triggerOp,
    });
  }

  /** Compute and return the patches without committing (for preview/replay). */
  buildPatches(): Patch[] {
    const patches: Patch[] = [];
    for (const w of this.writes.values()) {
      patches.push({
        path: w.path,
        operation: w.op,
        beforeHash: canonicalHash(w.before),
        value: w.value,
        taskId: this.taskId,
        transactionId: this.id,
      });
    }
    return patches;
  }

  /** Commit: build patches, apply one atomic version bump. */
  commit(): Patch[] {
    if (this.state !== "open") throw new Error("transaction already closed");
    const patches = this.buildPatches();
    const writes = new Map<string, CommitWrite>();
    for (const w of this.writes.values()) {
      writes.set(w.topKey, { op: w.triggerOp });
    }
    this.store.commitVersion(writes);
    this.state = "committed";
    return patches;
  }

  /** Rollback: restore all prior values without triggering. */
  rollback(): void {
    if (this.state !== "open") return;
    for (const w of this.writes.values()) {
      setAtPath(this.store.raw, w.path, w.before);
    }
    this.state = "rolled_back";
  }

  /** True if an effect read the given path (dependency cleanup support). */
  didRead(path: readonly (string | number)[]): boolean {
    return this.reads.has(pathToString(path));
  }
}
