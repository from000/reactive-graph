import { describe, expect, it } from "vitest";
import { REDACTED } from "@reactivegraph/devtools-protocol";
import {
  BudgetPolicy,
  CircuitBreakerPolicy,
  PriorityPolicy,
  RateLimitPolicy,
  SpanEmitter,
  allowPaths,
  checkAll,
  isPathAllowed,
  normalizeCausalTrace,
  validatePatches,
  type SchedulerTraceEntryLike,
} from "../src/index.js";

function fakeSource(entries: SchedulerTraceEntryLike[]) {
  return { getTrace: () => entries };
}

describe("causal trace normalization (Task 12 / D.9.106)", () => {
  it("orders a nested state change causal chain", () => {
    const source = fakeSource([
      { seq: 1, kind: "event", taskId: "ingest" },
      { seq: 2, kind: "start", taskId: "ingest" },
      { seq: 3, kind: "done", taskId: "ingest" },
      { seq: 4, kind: "invalidated", computedId: "total" },
      { seq: 5, kind: "computed", computedId: "total" },
      { seq: 6, kind: "retry", taskId: "tool_call" },
      { seq: 7, kind: "done", taskId: "tool_call" },
    ]);
    const trace = normalizeCausalTrace(source, { runId: "r1" });
    expect(trace.protocol).toBe("reactivegraph.causal-trace.v1");
    expect(trace.events.map((e) => e.kind)).toEqual([
      "trigger",
      "span_start",
      "span_end",
      "invalidate",
      "patch",
      "retry",
      "span_end",
    ]);
    expect(trace.events[3].selector).toBe("total");
  });

  it("redacts configured state paths before export", () => {
    const trace = normalizeCausalTrace(fakeSource([]), {
      runId: "r1",
      redact: [["credentials"]],
      extra: [
        { seq: 1, kind: "patch", path: ["credentials", "token"], value: "secret" },
        { seq: 2, kind: "patch", path: ["profile", "name"], value: "ok" },
      ],
    });
    const [cred, prof] = trace.events;
    expect(cred!.value).toBe(REDACTED);
    expect(prof!.value).toBe("ok");
  });
});

describe("OpenTelemetry spans (Task 12 / D.9.106)", () => {
  it("emits queue-delay / model / retry / cache spans and redacts attributes", () => {
    const emitter = new SpanEmitter({ redactAttributes: ["api_key"] });
    emitter.emit("queue_delay", "queue", { ms: 12 });
    const model = emitter.start("model_call", "llm.chat", { model: "gpt", api_key: "sk-secret" });
    model.tokens({ input: 10, output: 20, total: 30 });
    model.end();
    emitter.emit("retry", "retry", { attempt: 2 });
    emitter.emit("cache_hit", "cache", { computedId: "total" });

    const spans = emitter.exportSpans();
    expect(spans.map((s) => s.kind)).toEqual(["queue_delay", "model_call", "retry", "cache_hit"]);
    const modelSpan = spans[1]!;
    expect(modelSpan.attributes.api_key).toBe(REDACTED);
    expect(modelSpan.attributes.tokenUsage).toEqual({ input: 10, output: 20, total: 30 });
  });

  it("marks error status", () => {
    const emitter = new SpanEmitter();
    const handle = emitter.start("tool_call", "tool.search", {});
    handle.setError(new Error("boom"));
    handle.end();
    expect(emitter.exportSpans()[0]!.status).toBe("error");
  });
});

describe("field-level permissions (Task 12 / D.9.105)", () => {
  const policy = allowPaths([["a"], ["b", "c"]], [["a"]]);

  it("allows nested reads under an allowed prefix", () => {
    expect(isPathAllowed(policy, "read", ["a", "b", 0])).toBe(true);
    expect(isPathAllowed(policy, "read", ["b", "c", "deep"])).toBe(true);
    expect(isPathAllowed(policy, "read", ["b", "secret"])).toBe(false);
  });

  it("rejects patches on denied write paths", () => {
    expect(() =>
      validatePatches(policy, [
        { path: ["a", "x"], value: 1 },
        { path: ["b"], value: 2 },
      ]),
    ).toThrow(/permission denied/);
  });

  it("accepts allowed writes", () => {
    expect(() => validatePatches(policy, [{ path: ["a", "x"], value: 1 }])).not.toThrow();
  });
});

describe("task policies (Task 12 / D.9.105)", () => {
  it("budget policy caps cumulative cost", () => {
    const budget = new BudgetPolicy(5);
    expect(budget.allow({ taskId: "t" }).allowed).toBe(true);
    expect(budget.allow({ taskId: "t" }).allowed).toBe(true);
    expect(budget.allow({ taskId: "t", cost: 4 }).allowed).toBe(false);
  });

  it("rate-limit policy enforces a sliding window", () => {
    let t = 0;
    const rl = new RateLimitPolicy(2, 100, () => t);
    expect(rl.allow({ taskId: "t", at: t }).allowed).toBe(true);
    t += 1;
    expect(rl.allow({ taskId: "t", at: t }).allowed).toBe(true);
    t += 1;
    const denied = rl.allow({ taskId: "t", at: t });
    expect(denied.allowed).toBe(false);
    expect(denied.delayMs).toBeGreaterThan(0);
  });

  it("circuit-breaker opens after failures and half-opens after cooldown", () => {
    let t = 0;
    const cb = new CircuitBreakerPolicy(2, 100, () => t);
    cb.onError();
    cb.onError();
    expect(cb.allow({ taskId: "t", at: t }).allowed).toBe(false); // open
    t += 200;
    expect(cb.allow({ taskId: "t", at: t }).allowed).toBe(true); // half-open probe
    cb.onSuccess();
  });

  it("composes policies via checkAll and supports priority hints", () => {
    const priority = new PriorityPolicy(10);
    const composed = checkAll([new BudgetPolicy(1), new RateLimitPolicy(1, 100), priority], {
      taskId: "t",
    });
    expect(composed.allowed).toBe(true);
    expect(checkAll([new BudgetPolicy(0)], { taskId: "t" }).allowed).toBe(false);
    expect(priority.priority).toBe(10);
  });
});

describe("field-level permissions: literal dots (review hardening)", () => {
  it("a key containing a dot is not treated as a nested path", () => {
    // allow ["a"] must NOT authorize a literal key "a.b".
    const policy = allowPaths([["a"]], [["a"]]);
    expect(isPathAllowed(policy, "write", ["a", "b"])).toBe(true); // nested under a
    expect(isPathAllowed(policy, "write", ["a.b"])).toBe(false); // literal dotted key
    expect(isPathAllowed(policy, "write", ["ab"])).toBe(false);
  });
});

describe("OTLP JSON export", () => {
  it("exports spans as OTLP JSON resourceSpans without leaking redacted values", () => {
    const emitter = new SpanEmitter({ redactAttributes: ["api_key"] });
    emitter.emit("task", "task:t", { taskId: "t", api_key: "secret" });
    const payload = emitter.exportOTLPJSON("service-a");
    const resourceSpans = payload.resourceSpans as Array<{
      resource: { attributes: Array<{ key: string; value: { stringValue: string } }> };
      scopeSpans: Array<{
        spans: Array<{
          name: string;
          kind: number;
          startTimeUnixNano: string;
          endTimeUnixNano: string;
          attributes: Array<{ key: string; value: { stringValue: unknown } }>;
        }>;
      }>;
    }>;
    expect(resourceSpans[0]!.resource.attributes[0]).toEqual({
      key: "service.name",
      value: { stringValue: "service-a" },
    });
    const span = resourceSpans[0]!.scopeSpans[0]!.spans[0]!;
    expect(span.name).toBe("task:t");
    expect(span.kind).toBe(1);
    expect(span.startTimeUnixNano).toMatch(/^\d+$/);
    expect(span.endTimeUnixNano).toMatch(/^\d+$/);
    expect(span.attributes).toContainEqual({
      key: "api_key",
      value: { stringValue: REDACTED },
    });
    expect(JSON.stringify(payload)).not.toContain("secret");
  });
});

describe("span metrics dashboard (P6.2)", () => {
  function clockedEmitter(maxSpans?: number) {
    let now = 0;
    const emitter = new SpanEmitter({ nowNs: () => now, maxSpans });
    const record = (kind: string, name: string, durationMs: number): void => {
      const handle = emitter.start(kind, name, {});
      now += durationMs * 1_000_000;
      handle.end();
    };
    return { emitter, record };
  }

  it("reports p50/p95/p99 per kind over the retained window", () => {
    const { emitter, record } = clockedEmitter();
    for (const ms of [1, 2, 3, 4]) record("tool_call", "tool.run", ms);
    record("model_call", "llm.chat", 10);
    const errorSpan = emitter.start("run", "run:r1", {});
    errorSpan.setError(new Error("boom"));
    errorSpan.end();

    const snapshot = emitter.spanMetrics();
    expect(snapshot.protocol).toBe("reactivegraph.span-metrics.v1");
    expect(snapshot.kinds.map((k) => k.kind)).toEqual(["model_call", "run", "tool_call"]);
    const tool = snapshot.kinds.find((k) => k.kind === "tool_call")!;
    expect(tool.count).toBe(4);
    expect(tool.minMs).toBe(1);
    expect(tool.meanMs).toBe(2.5);
    expect(tool.p50Ms).toBe(2);
    expect(tool.p95Ms).toBe(4);
    expect(tool.p99Ms).toBe(4);
    expect(tool.maxMs).toBe(4);
    expect(snapshot.errorSpans).toBe(1);
  });

  it("bounds retention and surfaces evictions instead of growing forever", () => {
    const emitter = new SpanEmitter({ maxSpans: 3, nowNs: () => 0 });
    for (let i = 0; i < 5; i++) emitter.emit("task", `task-${i}`, {});
    expect(emitter.exportSpans().map((s) => s.name)).toEqual(["task-2", "task-3", "task-4"]);
    const snapshot = emitter.spanMetrics();
    expect(snapshot.retainedSpans).toBe(3);
    expect(snapshot.droppedSpans).toBe(2);
    expect(snapshot.totalSpans).toBe(5);
    expect(snapshot.maxSpans).toBe(3);

    const disabled = new SpanEmitter({ maxSpans: 0, nowNs: () => 0 });
    disabled.emit("task", "dropped", {});
    expect(disabled.exportSpans()).toEqual([]);
    expect(disabled.spanMetrics().droppedSpans).toBe(1);
  });

  it("keeps the OTLP export within the bounded retention window", () => {
    const emitter = new SpanEmitter({ maxSpans: 2, nowNs: () => 0 });
    emitter.emit("task", "t1", {});
    emitter.emit("task", "t2", {});
    emitter.emit("task", "t3", {});
    const payload = emitter.exportOTLPJSON() as {
      resourceSpans: Array<{ scopeSpans: Array<{ spans: Array<{ name: string }> }> }>;
    };
    expect(payload.resourceSpans[0]!.scopeSpans[0]!.spans.map((s) => s.name)).toEqual(["t2", "t3"]);
  });
});
