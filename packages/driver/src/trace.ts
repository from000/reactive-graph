/**
 * Causal trace normalization (Task 12 / D.9.105-106).
 *
 * The Scheduler already records a deterministic internal trace (event /
 * eligible / start / done / failed / retry / invalidated / computed / ...).
 * This module normalizes those records — plus explicit patch/checkpoint events
 * — into the stable `@reactivegraph/devtools-protocol` CausalTrace schema, with
 * configurable redaction of sensitive state paths before any export.
 */

import {
  REDACTED,
  type CausalTrace,
  type TraceEvent,
  type TraceEventKind,
  type TracePath,
} from "@reactivegraph/devtools-protocol";

export interface SchedulerTraceEntryLike {
  readonly seq: number;
  readonly kind: string;
  readonly taskId?: string;
  readonly computedId?: string;
  readonly reason?: string;
  readonly stateVersion?: number;
}

export interface TraceSource {
  getTrace(): readonly SchedulerTraceEntryLike[];
}

export interface NormalizeOptions {
  runId: string;
  /** Paths whose values must be redacted before export (e.g. ["credentials"]). */
  redact?: readonly TracePath[];
  /** Extra explicit events (patches, checkpoints) appended in caller order. */
  extra?: readonly TraceEvent[];
}

const KIND_MAP: Record<string, TraceEventKind> = {
  event: "trigger",
  eligible: "schedule",
  start: "span_start",
  done: "span_end",
  failed: "span_end",
  retry: "retry",
  invalidated: "invalidate",
  computed: "patch",
  computed_cache_hit: "cache_hit",
};

function isRedactedPath(
  path: readonly (string | number)[] | undefined,
  redact: readonly TracePath[],
): boolean {
  if (!path) return false;
  const dotted = path.map((s) => String(s)).join(".");
  for (const r of redact) {
    const rd = r.map((s) => String(s)).join(".");
    if (dotted === rd || dotted.startsWith(`${rd}.`) || rd.startsWith(`${dotted}.`)) return true;
  }
  return false;
}

export function normalizeCausalTrace(source: TraceSource, opts: NormalizeOptions): CausalTrace {
  const events: TraceEvent[] = [];
  for (const rec of source.getTrace()) {
    const kind = KIND_MAP[rec.kind];
    if (!kind) continue;
    events.push({
      seq: rec.seq,
      kind,
      taskId: rec.taskId,
      selector: rec.computedId,
      reason: rec.reason,
    });
  }
  for (const ev of opts.extra ?? []) {
    if (isRedactedPath(ev.path, opts.redact ?? [])) {
      events.push({ ...ev, value: REDACTED });
    } else {
      events.push(ev);
    }
  }
  return { protocol: "reactivegraph.causal-trace.v1", runId: opts.runId, events };
}

export { REDACTED };
export type { CausalTrace, TraceEvent, TraceEventKind, TracePath };
