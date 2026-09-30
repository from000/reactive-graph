/**
 * OpenTelemetry span emission (Task 12 / D.9.106).
 *
 * Emits OTel-shaped spans for queue delay, model/tool calls, retries, cache
 * hits, and provider-supplied token usage. Spans are collected in-process via
 * `exportSpans` so tests (and the CLI `trace-export`) can inspect them without
 * requiring a live OTLP collector. Redaction of sensitive attributes happens at
 * emission time against a configured deny-list.
 */

import { REDACTED, pathToDotted, type TracePath } from "@reactivegraph/devtools-protocol";

export interface Span {
  readonly name: string;
  readonly kind: string; // e.g. "queue_delay" | "model_call" | "tool_call" | "retry" | "cache_hit"
  readonly startNs: number;
  readonly endNs: number;
  readonly attributes: Record<string, unknown>;
  readonly status: "ok" | "error";
}

export interface SpanEmitterOptions {
  redactAttributes?: readonly string[];
  nowNs?: () => number;
  /** Maximum completed spans retained for inspection/metrics (default 10,000).
   * Older spans are evicted first so a long-running process keeps a bounded
   * observability footprint. 0 disables retention. */
  maxSpans?: number;
  /** Optional real-time callback: invoked for every completed span
   * (span.end() or direct emit()) — used by P3-3 Task 3 to bridge span
   * events out over the stream custom channel. */
  onSpan?: (span: Span) => void;
}

export interface SpanKindMetrics {
  readonly kind: string;
  readonly count: number;
  readonly minMs: number;
  readonly meanMs: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
}

export interface SpanMetricsSnapshot {
  readonly protocol: "reactivegraph.span-metrics.v1";
  readonly maxSpans: number;
  readonly retainedSpans: number;
  readonly droppedSpans: number;
  readonly totalSpans: number;
  readonly errorSpans: number;
  readonly kinds: readonly SpanKindMetrics[];
}

export class SpanEmitter {
  private readonly spans: Span[] = [];
  private spanHead = 0;
  private droppedSpans = 0;
  private totalSpans = 0;
  private errorSpans = 0;
  private readonly redact: Set<string>;
  private readonly nowNs: () => number;
  private readonly onSpan?: (span: Span) => void;
  private readonly maxSpans: number;

  constructor(opts: SpanEmitterOptions = {}) {
    this.redact = new Set(opts.redactAttributes ?? []);
    this.nowNs = opts.nowNs ?? (() => process.hrtime.bigint() as unknown as number);
    this.onSpan = opts.onSpan;
    this.maxSpans = Math.max(0, opts.maxSpans ?? 10_000);
  }

  start(kind: string, name: string, attributes: Record<string, unknown> = {}): SpanHandle {
    return new SpanHandle(this, kind, name, attributes, this.nowNs());
  }

  emit(
    kind: string,
    name: string,
    attributes: Record<string, unknown>,
    status: "ok" | "error" = "ok",
  ): void {
    const t = this.nowNs();
    const span: Span = {
      name,
      kind,
      startNs: t,
      endNs: t,
      attributes: this.sanitize(attributes),
      status,
    };
    this.recordSpan(span);
  }

  /** Record an already-timed span (the SpanHandle path preserves startNs). */
  recordSpan(span: Span): void {
    this.totalSpans += 1;
    if (span.status === "error") this.errorSpans += 1;
    this.retain(span);
    this.onSpan?.(span);
  }

  exportSpans(): readonly Span[] {
    if (this.maxSpans === 0) return [];
    if (this.spans.length < this.maxSpans || this.spanHead === 0) return this.spans.slice();
    return [...this.spans.slice(this.spanHead), ...this.spans.slice(0, this.spanHead)];
  }

  /** Bounded p50/p95/p99 dashboard over the retained span window.
   *
   * Percentiles use the nearest-rank definition over completed spans grouped
   * by `kind`. The window is bounded by `maxSpans`; `droppedSpans` makes the
   * eviction visible so an operator never mistakes it for lifetime totals.
   */
  spanMetrics(): SpanMetricsSnapshot {
    const retained = this.exportSpans();
    const byKind = new Map<string, number[]>();
    for (const span of retained) {
      const durationMs = Math.max(0, span.endNs - span.startNs) / 1_000_000;
      const samples = byKind.get(span.kind);
      if (samples) samples.push(durationMs);
      else byKind.set(span.kind, [durationMs]);
    }
    const kinds = [...byKind.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([kind, samples]) => {
        samples.sort((a, b) => a - b);
        const sum = samples.reduce((acc, value) => acc + value, 0);
        return {
          kind,
          count: samples.length,
          minMs: samples[0]!,
          meanMs: sum / samples.length,
          p50Ms: percentile(samples, 0.5),
          p95Ms: percentile(samples, 0.95),
          p99Ms: percentile(samples, 0.99),
          maxMs: samples[samples.length - 1]!,
        };
      });
    return {
      protocol: "reactivegraph.span-metrics.v1",
      maxSpans: this.maxSpans,
      retainedSpans: retained.length,
      droppedSpans: this.droppedSpans,
      totalSpans: this.totalSpans,
      errorSpans: this.errorSpans,
      kinds,
    };
  }

  /** Export collected spans as an OTLP/JSON-compatible payload.
   *
   * This is the transport-neutral OTLP JSON shape (`resourceSpans`). A
   * collector adapter can POST it to `/v1/traces`; tests and local tools can
   * inspect it without a live collector.
   */
  exportOTLPJSON(serviceName = "reactivegraph-driver"): Record<string, unknown> {
    return {
      resourceSpans: [
        {
          resource: {
            attributes: [{ key: "service.name", value: { stringValue: serviceName } }],
          },
          scopeSpans: [
            {
              scope: { name: "reactivegraph.driver", version: "0.1.0" },
              spans: this.exportSpans().map((span) => ({
                traceId: "00000000000000000000000000000000",
                spanId: span.startNs.toString(16).padStart(16, "0").slice(0, 16),
                name: span.name,
                kind: 1,
                startTimeUnixNano: String(span.startNs),
                endTimeUnixNano: String(span.endNs),
                attributes: Object.entries(span.attributes).map(([key, value]) => ({
                  key,
                  value: { stringValue: value },
                })),
                status: {
                  code: span.status === "error" ? 2 : 1,
                },
              })),
            },
          ],
        },
      ],
    };
  }

  sanitize(attributes: Record<string, unknown>): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(attributes)) {
      if (this.redact.has(k)) {
        out[k] = REDACTED;
      } else if (
        typeof v === "string" &&
        [...this.redact].some((r) => k.toLowerCase().includes(r.toLowerCase()))
      ) {
        out[k] = REDACTED;
      } else {
        out[k] = v;
      }
    }
    return out;
  }

  /** Append one span to the bounded retention window. */
  private retain(span: Span): void {
    if (this.maxSpans === 0) {
      this.droppedSpans += 1;
      return;
    }
    if (this.spans.length < this.maxSpans) {
      this.spans.push(span);
      return;
    }
    this.spans[this.spanHead] = span;
    this.spanHead = (this.spanHead + 1) % this.maxSpans;
    this.droppedSpans += 1;
  }
}

/** Nearest-rank percentile over an ascending numeric sample. */
function percentile(samples: readonly number[], quantile: number): number {
  if (samples.length === 0) return 0;
  const rank = Math.max(1, Math.ceil(quantile * samples.length));
  return samples[Math.min(samples.length, rank) - 1]!;
}

export class SpanHandle {
  private readonly emitter: SpanEmitter;
  readonly kind: string;
  readonly name: string;
  readonly attributes: Record<string, unknown>;
  readonly startNs: number;
  private endNs?: number;
  private status: "ok" | "error" = "ok";

  constructor(
    emitter: SpanEmitter,
    kind: string,
    name: string,
    attributes: Record<string, unknown>,
    startNs: number,
  ) {
    this.emitter = emitter;
    this.kind = kind;
    this.name = name;
    this.attributes = attributes;
    this.startNs = startNs;
  }

  /** Record provider token usage (input/output/total). */
  tokens(usage: { input?: number; output?: number; total?: number }): void {
    this.attributes.tokenUsage = usage;
  }

  setError(err: unknown): void {
    this.status = "error";
    this.attributes.error = err instanceof Error ? err.message : String(err);
  }

  end(): void {
    if (this.endNs !== undefined) return;
    this.endNs = this.emitter["nowNs"]();
    this.emitter.recordSpan({
      name: this.name,
      kind: this.kind,
      startNs: this.startNs,
      endNs: this.endNs,
      attributes: this.emitter.sanitize(this.attributes),
      status: this.status,
    });
  }
}

export { pathToDotted };
export type { TracePath };
