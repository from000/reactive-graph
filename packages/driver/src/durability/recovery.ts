/**
 * Durable recovery (Task 6): snapshots + replayable patches.
 *
 * Compaction: periodically persist a SnapshotEvent (canonical state + stateHash
 * + baseSeq), then `truncateBefore`. Recovery: load latest snapshot, replay
 * events after it, re-apply transaction patches in order, and resolve
 * `UNKNOWN_COMMIT` by inspecting the log (never blindly retried).
 *
 * Confirmed effects never repeat: an idempotency key is only "confirmed" after
 * both its EFFECT_INTENT and EFFECT_RECEIPT are durably present; recovery only
 * re-drives tasks whose receipt is absent.
 */

import { encodeValue, decodeValue } from "@reactivegraph/protocol";
import type { DurableEvent, EffectIntentEvent, SnapshotEvent } from "./events.js";
import type { DurableLog } from "./log.js";
import { applyPatch } from "../state/index.js";
import type { Patch } from "../state/index.js";

export interface RecoveryResult {
  /** Recovered canonical state. */
  state: Record<string, unknown>;
  /** State version after replay. */
  stateVersion: number;
  /** Highest seq consumed. */
  lastSeq: number;
  /** Idempotency keys whose receipts are durable (confirmed effects). */
  confirmedEffects: Map<string, string>;
}

export interface RecoveryOptions {
  runId?: string;
  /** Restrict replay to one thread's transactions (isolation across threads). */
  threadId?: string;
}

/**
 * Rebuild canonical state from the log: start from the newest snapshot (or an
 * empty state), then apply every transaction patch in seq order. When
 * `threadId` is given, only transactions for that thread are applied, so
 * multiple threads sharing one log stay isolated.
 */
export function recover(log: DurableLog, opts: RecoveryOptions = {}): RecoveryResult {
  const events = log.readFrom(0);
  let state: Record<string, unknown> = {};
  let stateVersion = 0;
  const confirmed = new Map<string, string>();

  for (const event of events) {
    switch (event.kind) {
      case "snapshot": {
        if (opts.threadId === undefined || event.threadId === opts.threadId) {
          state = deepCopyState(event.state);
          stateVersion = event.stateVersion;
          // The snapshot subsumes the receipt events behind it; carry the
          // confirmed effects forward so compacted logs stay idempotent.
          confirmed.clear();
          for (const [key, receipt] of event.confirmedEffects ?? []) {
            confirmed.set(key, receipt);
          }
        }
        break;
      }
      case "transaction": {
        if (opts.threadId !== undefined && event.threadId !== opts.threadId) break;
        stateVersion = event.stateVersion;
        for (const patch of event.patches as Patch[]) {
          // skipPreImage：恢复重放以日志为权威。运行时 beforeHash 反映的是
          // "当时 store 状态"（含 run 载荷并入的输入键——P3-1 语义），而
          // 恢复从空 store 按 seq 重放，首个写某路径的事务其 pre-image 必然
          // 与日志不符；strict 校验只会永远误报（运行时并发/冲突检测走
          // Transaction/runAll，不经 applyPatch，不受影响）。
          applyPatch(state, patch, { skipPreImage: true });
        }
        break;
      }
      case "effect_receipt": {
        // Idempotency keys are global (task-scoped) and must survive across
        // threads; confirmed effects are not thread-filtered.
        confirmed.set(event.idempotencyKey, event.receipt);
        break;
      }
      default:
        break;
    }
  }

  return { state, stateVersion, lastSeq: log.lastSeq, confirmedEffects: confirmed };
}

/** Find the newest snapshot (for compaction planning / receipt restore). */
export function latestSnapshot(log: DurableLog): { event: SnapshotEvent; seq: number } | null {
  const events = log.readFrom(0);
  let found: SnapshotEvent | null = null;
  for (const event of events) {
    if (event.kind === "snapshot") found = event as SnapshotEvent;
  }
  // Events are appended in seq order, so `found` is the newest snapshot; its
  // seq is the snapshot's own position in the log.
  return found ? { event: found, seq: found.seq } : null;
}

/**
 * Take a periodic snapshot and compact the log behind it.
 * @returns the seq the log was truncated to (events >= this seq remain).
 */
export function compact(log: DurableLog, snapshot: DurableEvent): number {
  if (snapshot.kind !== "snapshot") {
    throw new TypeError("compact() requires a snapshot event");
  }
  const stored = log.append(snapshot);
  // Truncate everything strictly before the snapshot's own seq: the snapshot
  // subsumes all prior events (its baseSeq marks the oldest event it covers).
  log.truncateBefore(stored.seq);
  return stored.seq;
}

function deepCopyState(value: unknown): Record<string, unknown> {
  // Canonical msgpack round-trip preserves the exact value semantics (bytes,
  // int64, timestamps) that JSON.stringify would corrupt.
  return decodeValue(encodeValue(value)) as Record<string, unknown>;
}

/** Check whether an effect intent already has its receipt (idempotency gate). */
export function hasConfirmedReceipt(
  confirmed: Map<string, string>,
  intent: EffectIntentEvent,
): boolean {
  return confirmed.has(intent.idempotencyKey);
}
