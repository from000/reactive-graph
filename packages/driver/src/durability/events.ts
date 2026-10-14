/**
 * Durable execution events (Task 6 / docs/spec/durable-execution.md).
 *
 * Everything the runtime must survive a crash is recorded as an append-only,
 * ordered, typed event. Fields persist: graph hash, run/thread IDs, state
 * version, transaction boundaries, read/write sets, task input hash,
 * idempotency key, effect receipt, retry decision, interrupt, stream cursor.
 *
 * Event kinds mirror the upstream `checkpoint_writes` model (write-ahead intent
 * + receipt) but are intentionally protocol-agnostic and replayable.
 */

export type DurableEvent =
  | TransactionEvent
  | EffectIntentEvent
  | EffectReceiptEvent
  | RetryDecisionEvent
  | InterruptEvent
  | StreamCursorEvent
  | SnapshotEvent;

export interface EventBase {
  /** Monotonic sequence number assigned by the log (0 = none yet). */
  seq: number;
  kind: DurableEventKind;
  /** Wall-clock ms when appended. */
  at: number;
}

export type DurableEventKind =
  | "transaction"
  | "effect_intent"
  | "effect_receipt"
  | "retry_decision"
  | "interrupt"
  | "stream_cursor"
  | "snapshot";

/** One committed transaction: the atomic unit of state. */
export interface TransactionEvent extends EventBase {
  kind: "transaction";
  graphHash: string;
  runId: string;
  threadId: string;
  stateVersion: number;
  txId: string;
  taskId: string;
  /** Canonical read paths this transaction depended on. */
  readPaths: string[];
  /** Canonical write paths this transaction produced. */
  writePaths: string[];
  /** The applied patches (path, op, beforeHash, value...). */
  patches: unknown[];
}

/** Append BEFORE an external call: durable intent with an idempotency key. */
export interface EffectIntentEvent extends EventBase {
  kind: "effect_intent";
  idempotencyKey: string;
  taskId: string;
  effectId: string;
  /** SHA-256 of the serialized task input. */
  inputHash: string;
  stateVersion: number;
  runId: string;
}

/** Persist the effect receipt BEFORE committing the resulting patch. */
export interface EffectReceiptEvent extends EventBase {
  kind: "effect_receipt";
  idempotencyKey: string;
  taskId: string;
  receipt: string;
  outcome: "success" | "failure";
}

export interface RetryDecisionEvent extends EventBase {
  kind: "retry_decision";
  taskId: string;
  attempt: number;
  decision: "retry" | "give_up";
  backoffMs: number;
  reason?: string;
}

export interface InterruptEvent extends EventBase {
  kind: "interrupt";
  runId: string;
  threadId: string;
  value: unknown;
  checkpointHint: string;
}

export interface StreamCursorEvent extends EventBase {
  kind: "stream_cursor";
  runId: string;
  threadId: string;
  cursor: string;
  terminal: boolean;
}

/** Periodic snapshot enabling compaction (see recovery.ts). */
export interface SnapshotEvent extends EventBase {
  kind: "snapshot";
  runId: string;
  threadId: string;
  stateVersion: number;
  /** SHA-256 of the canonical state snapshot (compaction root). */
  stateHash: string;
  /** Deep snapshot of the canonical state. */
  state: unknown;
  /** seq of the first event this snapshot subsumes. */
  baseSeq: number;
  /** Confirmed effect receipts (idempotencyKey -> receipt) subsumed by this
   * snapshot. compaction truncates the receipt events behind it, so the
   * snapshot must carry them forward to preserve effect idempotency across
   * restarts (fail-open: absent receipts cause at most a re-run). */
  confirmedEffects?: Array<[string, string]>;
}

/** Builder helpers keep event creation explicit and typed. */
export const events = {
  transaction(input: Omit<TransactionEvent, keyof EventBase>): TransactionEvent {
    return { seq: 0, at: Date.now(), kind: "transaction", ...input };
  },
  effectIntent(input: Omit<EffectIntentEvent, keyof EventBase>): EffectIntentEvent {
    return { seq: 0, at: Date.now(), kind: "effect_intent", ...input };
  },
  effectReceipt(input: Omit<EffectReceiptEvent, keyof EventBase>): EffectReceiptEvent {
    return { seq: 0, at: Date.now(), kind: "effect_receipt", ...input };
  },
  retryDecision(input: Omit<RetryDecisionEvent, keyof EventBase>): RetryDecisionEvent {
    return { seq: 0, at: Date.now(), kind: "retry_decision", ...input };
  },
  interrupt(input: Omit<InterruptEvent, keyof EventBase>): InterruptEvent {
    return { seq: 0, at: Date.now(), kind: "interrupt", ...input };
  },
  streamCursor(input: Omit<StreamCursorEvent, keyof EventBase>): StreamCursorEvent {
    return { seq: 0, at: Date.now(), kind: "stream_cursor", ...input };
  },
  snapshot(input: Omit<SnapshotEvent, keyof EventBase>): SnapshotEvent {
    return { seq: 0, at: Date.now(), kind: "snapshot", ...input };
  },
};
