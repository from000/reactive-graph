/**
 * ReactiveGraph devtools protocol (Task 12 / D.9.105-106).
 *
 * A stable, language-neutral trace schema emitted by the Driver's scheduler
 * and store. It never exposes Vue internals (no onTrack/onTrigger leak): the
 * Driver normalizes internal dependency records into these well-typed events.
 *
 * The `kind` set mirrors the causal chain a user needs to debug a reactive
 * graph: state mutation -> commit -> trigger -> selector invalidation ->
 * scheduled task -> callback -> span (model/tool) -> patch -> checkpoint.
 */

export const TRACE_EVENT_KINDS = [
  "state_set",
  "commit",
  "trigger",
  "invalidate",
  "schedule",
  "callback",
  "retry",
  "span_start",
  "span_end",
  "patch",
  "checkpoint",
  "cache_hit",
] as const;

export type TraceEventKind = (typeof TRACE_EVENT_KINDS)[number];

/** A field path like ["a", "b", 0] (dotted by convention in tools). */
export type TracePath = readonly (string | number)[];

/** A state value redacted before export (never the raw secret). */
export const REDACTED = Symbol("redacted");

export interface TraceEvent {
  readonly seq: number;
  readonly kind: TraceEventKind;
  readonly ts?: number;
  readonly path?: TracePath;
  readonly value?: unknown;
  readonly selector?: string;
  readonly taskId?: string;
  readonly callback?: string;
  readonly span?: SpanEvent;
  readonly retryOf?: string;
  readonly checkpointId?: string;
  readonly txId?: string;
  readonly reason?: string;
}

export interface SpanEvent {
  readonly name: string;
  readonly startNs: number;
  readonly endNs?: number;
  readonly status?: "ok" | "error";
  readonly attributes?: Record<string, unknown>;
  readonly tokenUsage?: { input?: number; output?: number; total?: number };
}

/** A normalized causal trace: all events for one run, oldest first. */
export interface CausalTrace {
  readonly protocol: "reactivegraph.causal-trace.v1";
  readonly runId: string;
  readonly events: readonly TraceEvent[];
}

/** Summary helper: path of a value through a nested object. */
export function pathToDotted(path: TracePath): string {
  let out = "";
  for (const seg of path) {
    out += typeof seg === "number" ? `[${seg}]` : out ? `.${seg}` : seg;
  }
  return out;
}

export function isTraceEventKind(kind: unknown): kind is TraceEventKind {
  return typeof kind === "string" && (TRACE_EVENT_KINDS as readonly string[]).includes(kind);
}
