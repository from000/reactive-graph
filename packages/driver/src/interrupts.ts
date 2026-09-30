/**
 * Durable interrupts (Task 8 / docs/spec/streams-and-interrupts.md).
 *
 * An interrupt pauses a run at a checkpoint hint; resuming requires the expected
 * state version (stale resume is rejected). Human edits are NEW transactions
 * with permissions and audit data — never raw memory mutation.
 */

import type { DurableLog } from "./durability/log.js";
import { events } from "./durability/events.js";

export class StaleResumeError extends Error {
  constructor(expected: number, actual: number) {
    super(`stale resume: expected version ${expected}, current ${actual}`);
    this.name = "StaleResumeError";
  }
}

export class InterruptError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InterruptError";
  }
}

export interface Interrupt {
  readonly id: string;
  readonly value: unknown;
  readonly checkpointHint: string;
  readonly expectedVersion: number;
}

export interface HumanEdit {
  readonly actor: string;
  readonly permission: string;
  readonly at: number;
  readonly audit: unknown;
  readonly patches: unknown[];
}

export interface InterruptManagerOptions {
  readonly log?: DurableLog;
  readonly runId?: string;
  readonly threadId?: string;
  /** Allowed actors -> permissions map for human edits. */
  readonly allowedActors?: ReadonlyMap<string, string[]>;
}

/**
 * Tracks pending interrupts and validates human edits. Human edits are applied
 * as new transactions (caller commits them through the store); the manager only
 * enforces permissions and audit recording.
 */
export class InterruptManager {
  private readonly log?: DurableLog;
  private readonly runId: string;
  private readonly threadId: string;
  private readonly allowedActors: ReadonlyMap<string, string[]>;
  private readonly pending = new Map<string, Interrupt>();
  private readonly edits: HumanEdit[] = [];

  constructor(opts: InterruptManagerOptions = {}) {
    this.log = opts.log;
    this.runId = opts.runId ?? "run-1";
    this.threadId = opts.threadId ?? "thread-1";
    this.allowedActors = opts.allowedActors ?? new Map();
  }

  /** Record a pending interrupt. */
  interrupt(value: unknown, checkpointHint: string, expectedVersion: number): Interrupt {
    const inter: Interrupt = {
      id: `int-${this.pending.size + 1}`,
      value,
      checkpointHint,
      expectedVersion,
    };
    this.pending.set(inter.id, inter);
    this.log?.append(
      events.interrupt({
        runId: this.runId,
        threadId: this.threadId,
        value,
        checkpointHint,
      }),
    );
    return inter;
  }

  /** Validate a resume against the expected version (stale rejected). */
  resume(id: string, currentVersion: number): Interrupt {
    const inter = this.pending.get(id);
    if (!inter) throw new InterruptError(`unknown interrupt ${id}`);
    if (currentVersion !== inter.expectedVersion) {
      throw new StaleResumeError(inter.expectedVersion, currentVersion);
    }
    this.pending.delete(id);
    return inter;
  }

  /**
   * Record a human edit as a new audited transaction. Throws if the actor lacks
   * permission. Caller still commits the patches through the store.
   */
  recordHumanEdit(edit: Omit<HumanEdit, "at"> & { at?: number }): HumanEdit {
    const permission = edit.permission;
    const actorPerms = this.allowedActors.get(edit.actor) ?? [];
    if (!actorPerms.includes(permission)) {
      throw new InterruptError(`actor ${edit.actor} lacks permission ${permission}`);
    }
    const full: HumanEdit = { ...edit, at: edit.at ?? Date.now() };
    this.edits.push(full);
    return full;
  }

  get pendingInterrupts(): readonly Interrupt[] {
    return [...this.pending.values()];
  }

  get auditLog(): readonly HumanEdit[] {
    return [...this.edits];
  }
}
