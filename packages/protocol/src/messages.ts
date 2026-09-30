/**
 * RGP/1 envelope schema (docs/spec/rgp-1.md §2).
 * Cross-language: Python mirror is python/reactivegraph/reactivegraph/protocol.py.
 */

export const METHOD_LIST = [
  "DRIVER_HELLO",
  "COMPILE_GRAPH",
  "RELEASE_GRAPH",
  "RUN",
  "RESUME",
  "GET_STATE",
  "EXPORT_DOT",
  "TASK_INVOKE",
  "TASK_RESULT",
  "STREAM_EVENT",
  "CHECKPOINT_OP",
  "STORE_OP",
  "VECTOR_UPSERT",
  "VECTOR_SEARCH",
  "TRACE_QUERY",
  "CANCEL",
  "SHUTDOWN",
] as const;

export type Method = (typeof METHOD_LIST)[number];

export type EnvelopeKind = "request" | "response" | "event" | "cancel";

export interface TraceContext {
  readonly runId?: string;
  readonly transactionId?: string;
  readonly parentSpanId?: string;
}

export interface Envelope {
  readonly version: 1;
  readonly id: string;
  readonly kind: EnvelopeKind;
  readonly method: Method;
  readonly trace?: TraceContext;
  readonly payload: unknown;
}

const METHOD_SET = new Set<string>(METHOD_LIST);

export function isMethod(value: unknown): value is Method {
  return typeof value === "string" && METHOD_SET.has(value);
}

export function isEnvelope(value: unknown): value is Envelope {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  if (v["version"] !== 1) return false;
  if (typeof v["id"] !== "string" || v["id"].length === 0) return false;
  if (!isMethod(v["method"])) return false;
  if (
    v["kind"] !== "request" &&
    v["kind"] !== "response" &&
    v["kind"] !== "event" &&
    v["kind"] !== "cancel"
  ) {
    return false;
  }
  if (v["trace"] !== undefined) {
    if (typeof v["trace"] !== "object" || v["trace"] === null) return false;
  }
  return true;
}

export function makeId(rng?: () => string): string {
  if (rng) return rng();
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
