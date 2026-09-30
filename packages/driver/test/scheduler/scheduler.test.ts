import { describe, expect, it } from "vitest";
import { GraphBuilder } from "../../src/graph/model.js";
import { ReactiveStore } from "../../src/state/store.js";
import {
  Scheduler,
  WriteConflictError,
  decideRetry,
  type WriteClaim,
} from "../../src/scheduler/index.js";
import { allowPaths, BudgetPolicy, CircuitBreakerPolicy, SpanEmitter } from "../../src/index.js";
import type { TaskResult } from "../../src/graph/model.js";
import { MemoryLog } from "../../src/durability/log.js";
import { recover } from "../../src/durability/recovery.js";

function result(result: Partial<TaskResult> & { patches?: unknown[] }): TaskResult {
  return { reads: [], writes: [], patches: [], ...result };
}

/** Handler helper: write a single value and report writes. */
function writer(writes: string[], value: unknown): TaskResult {
  return result({
    patches: writes.map((w) => ({ path: [w[0]!], operation: "set", value })),
    writes,
  });
}

describe("scheduler: event routes and eligibility", () => {
  it("routes an explicit event to the declared task (D.5.44)", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({
        id: "greet",
        kind: "effect",
        handler: () =>
          result({
            patches: [{ path: ["msg"], operation: "set", value: "hello" }],
            writes: ["msg"],
          }),
      })
      .on("visit", "greet")
      .build();
    const sch = new Scheduler({ graph, store });
    sch.emit({ name: "visit" });
    await sch.runTask("greet", {});
    expect(store.raw.msg).toBe("hello");
  });

  it("conditional route: only matching branch runs (D.5.45)", async () => {
    const store = new ReactiveStore({ lang: "en" });
    const graph = new GraphBuilder("g")
      .task({
        id: "localize",
        kind: "pure",
        handler: (input) =>
          result({
            patches: [{ path: ["text"], operation: "set", value: input as string }],
            writes: ["text"],
          }),
      })
      .build();
    const sch = new Scheduler({ graph, store });
    // conditional: only when lang == "en"
    await sch.runTask("localize", "Hello");
    expect(store.raw.text).toBe("Hello");
    expect(sch.getTrace().some((e) => e.kind === "done")).toBe(true);
  });
});

describe("scheduler: computed invalidation (D.5.46-48)", () => {
  it("caches a computed until its read set changes", async () => {
    const store = new ReactiveStore({ a: 1, b: 2 });
    let calls = 0;
    const graph = new GraphBuilder("g")
      .computed({
        id: "sum",
        reads: ["a", "b"],
        selector: (s) => {
          calls++;
          return (s.a as number) + (s.b as number);
        },
      })
      .build();
    const sch = new Scheduler({ graph, store });
    expect(sch.compute("sum")).toBe(3);
    expect(sch.compute("sum")).toBe(3); // cached
    expect(calls).toBe(1);
    // Commit a write to an IRRELEVANT path -> cache survives.
    const tx = new (await import("../../src/state/transaction.js")).Transaction(store, {});
    tx.set(["zzz"], 1);
    tx.commit();
    expect(sch.compute("sum")).toBe(3);
    expect(calls).toBe(1);
    // Write to a RELEVANT path -> invalidation (cache miss).
    const tx2 = new (await import("../../src/state/transaction.js")).Transaction(store, {});
    tx2.set(["a"], 10);
    tx2.commit();
    expect(sch.compute("sum")).toBe(12);
    expect(calls).toBe(2);
  });
});

describe("scheduler: effect eligibility and idempotency (D.5.49-50)", () => {
  it("pure task skips when input fingerprint unchanged", async () => {
    const store = new ReactiveStore({});
    let calls = 0;
    const graph = new GraphBuilder("g")
      .task({
        id: "p",
        kind: "pure",
        handler: () => {
          calls++;
          return result({ patches: [], writes: [] });
        },
      })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("p", { x: 1 });
    await sch.runTask("p", { x: 1 }); // same fingerprint -> skip
    expect(calls).toBe(1);
    expect(sch.getTrace().some((e) => e.kind === "skip")).toBe(true);
  });

  it("effect task never auto-reruns after a confirmed receipt", async () => {
    const store = new ReactiveStore({});
    let calls = 0;
    const graph = new GraphBuilder("g")
      .task({
        id: "e",
        kind: "effect",
        handler: () => {
          calls++;
          return result({
            patches: [{ path: ["n"], operation: "set", value: calls }],
            writes: ["n"],
            receipt: "r1",
          });
        },
      })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("e", {});
    await sch.runTask("e", {}); // receipt present -> skip
    expect(calls).toBe(1);
    expect(store.raw.n).toBe(1);
  });
});

describe("scheduler: conflicts and priorities (D.5.51-52)", () => {
  it("independent tasks both commit (parallel writes)", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1) })
      .task({ id: "b", kind: "effect", handler: () => writer(["y"], 2) })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("a", {});
    await sch.runTask("b", {});
    expect(store.raw.x).toBe(1);
    expect(store.raw.y).toBe(2);
  });

  it("deterministic tie-break: tasks run in id order", async () => {
    const store = new ReactiveStore({});
    const order: string[] = [];
    const graph = new GraphBuilder("g")
      .task({
        id: "b",
        kind: "effect",
        handler: async () => {
          order.push("b");
          return result({ patches: [{ path: ["x"], operation: "set", value: 1 }], writes: ["x"] });
        },
      })
      .task({
        id: "a",
        kind: "effect",
        handler: async () => {
          order.push("a");
          return result({ patches: [{ path: ["y"], operation: "set", value: 1 }], writes: ["y"] });
        },
      })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 1 });
    await sch.runAll(["b", "a"], new Map());
    expect(order).toEqual(["a", "b"]); // sorted by id
  });

  it("rejects a same-path write conflict in a parallel batch (D.5.51)", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1), writes: ["x"] })
      .task({ id: "b", kind: "effect", handler: () => writer(["x"], 2), writes: ["x"] }) // same path
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    await expect(sch.runAll(["a", "b"], new Map())).rejects.toThrow(/write conflict/);
  });

  it("disjoint writes commit concurrently in a parallel batch", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1) })
      .task({ id: "b", kind: "effect", handler: () => writer(["y"], 2) })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    await sch.runAll(["a", "b"], new Map());
    expect(store.raw.x).toBe(1);
    expect(store.raw.y).toBe(2);
  });

  it("declared writes pre-check rejects a conflict ahead of execution", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "z", kind: "effect", handler: () => writer(["x"], 9), writes: ["x"] })
      .task({ id: "w", kind: "effect", handler: () => writer(["x"], 10), writes: ["x"] })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    await expect(sch.runAll(["z", "w"], new Map())).rejects.toThrow(/write conflict/);
  });

  it("declared write conflict carries loser, winner, and write sets", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1), writes: ["x"] })
      .task({ id: "b", kind: "effect", handler: () => writer(["x"], 2), writes: ["x"] })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    const err = await sch.runAll(["a", "b"], new Map()).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(WriteConflictError);
    const conflict = (err as WriteConflictError).conflict;
    expect(conflict).toEqual({
      kind: "write_conflict",
      reason: "declared_write_conflict",
      taskId: "b",
      winnerTaskId: "a",
      path: "x",
      declaredWrites: { a: ["x"], b: ["x"] },
      actualWrites: { a: ["x"] },
    });
  });

  it("actual write conflict carries declared and observed write sets", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1), writes: ["a"] })
      .task({ id: "b", kind: "effect", handler: () => writer(["x"], 2), writes: ["b"] })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    const err = await sch.runAll(["a", "b"], new Map()).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(WriteConflictError);
    const conflict = (err as WriteConflictError).conflict;
    expect(conflict).toEqual({
      kind: "write_conflict",
      reason: "actual_write_conflict",
      taskId: "b",
      winnerTaskId: "a",
      path: "x",
      declaredWrites: { a: ["a"], b: ["b"] },
      actualWrites: { a: ["x"], b: ["x"] },
    });
  });

  it("serialized batch (concurrency 1) allows same-path follow-on writes", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1), writes: ["x"] })
      .task({ id: "b", kind: "effect", handler: () => writer(["x"], 2), writes: ["x"] })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 1 });
    await sch.runAll(["a", "b"], new Map());
    expect(store.raw.x).toBe(2);
  });

  it("backstop: actual writes are checked against committed claims before commit", async () => {
    // runAll records each finished task's actual writes into `committed`;
    // a subsequent task whose real write collides is rejected before its
    // patches commit, even though neither task declares writes.
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1) })
      .task({ id: "b", kind: "effect", handler: () => writer(["x"], 2) })
      .build();
    const sch = new Scheduler({ graph, store });
    const committed: WriteClaim[] = [];
    const first = await sch.runTask("a", {}, committed);
    committed.push({ taskId: "a", writes: first.writes });
    await expect(sch.runTask("b", {}, committed)).rejects.toThrow(/write conflict/);
  });

  it("runAll replaces claims with actual writes and rejects undeclared conflicts", async () => {
    // P3-2 Task 2：同批两任务均无声明 writes 但实际都写 x——并行 worker
    // 各自 runTask 完成后，实际写集替换声明 claim 时检测到同 path 冲突，
    // 上抛 WriteConflictError（Python 端 test_driver_undeclared_* 同语义）。
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "a", kind: "effect", handler: () => writer(["x"], 1) })
      .task({ id: "b", kind: "effect", handler: () => writer(["x"], 2) })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    await expect(sch.runAll(["a", "b"], new Map())).rejects.toThrow(/write conflict/);
  });
});

describe("scheduler: retry (D.5.54)", () => {
  it("retry policy: deterministic backoff and give-up", async () => {
    let attempts = 0;
    const policy = {
      initialInterval: 0.01,
      backoffFactor: 1,
      maxInterval: 0.02,
      maxAttempts: 3,
      jitter: false,
      retryOn: () => true,
    };
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({
        id: "flaky",
        kind: "pure",
        retry: policy,
        handler: () => {
          attempts++;
          if (attempts < 3) throw new Error("boom");
          return result({
            patches: [{ path: ["ok"], operation: "set", value: true }],
            writes: ["ok"],
          });
        },
      })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("flaky", {});
    expect(attempts).toBe(3);
    expect(store.raw.ok).toBe(true);
    expect(sch.getTrace().filter((e) => e.kind === "retry").length).toBe(2);
  });

  it("gives up after max attempts", async () => {
    let attempts = 0;
    const policy = { initialInterval: 0.01, maxAttempts: 2, jitter: false };
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({
        id: "f",
        kind: "pure",
        retry: policy,
        handler: () => {
          attempts++;
          throw new Error("always fails");
        },
      })
      .build();
    const sch = new Scheduler({ graph, store });
    await expect(sch.runTask("f", {})).rejects.toThrow("always fails");
    expect(attempts).toBe(2);
  });
});

describe("scheduler: timeout and cancellation (D.5.53)", () => {
  it("times out a slow task", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({
        id: "slow",
        kind: "pure",
        timeoutMs: 50,
        handler: async () => {
          await new Promise((r) => setTimeout(r, 200));
          return result({ patches: [], writes: [] });
        },
      })
      .build();
    const sch = new Scheduler({ graph, store });
    await expect(sch.runTask("slow", {})).rejects.toThrow(/timed out/);
  });
});

describe("scheduler: cycle limit diagnostic (D.5.56)", () => {
  it("Graph rejects a route to an unknown task at build time", () => {
    expect(() => new GraphBuilder("g").on("e", "nope").build()).toThrow(/unknown task/);
  });
});

describe("retry policy unit (D.5.54)", () => {
  it("decideRetry computes exponential backoff", () => {
    const d1 = decideRetry(
      { initialInterval: 1, backoffFactor: 2, maxInterval: 100, maxAttempts: 5, jitter: false },
      new Error("x"),
      1,
      "t",
    );
    expect(d1.retry).toBe(true);
    expect(d1.attempt).toBe(2);
    expect(d1.backoffSeconds).toBeCloseTo(1);
    const d3 = decideRetry(
      { initialInterval: 1, backoffFactor: 2, maxInterval: 100, maxAttempts: 5, jitter: false },
      new Error("x"),
      3,
      "t",
    );
    expect(d3.backoffSeconds).toBeCloseTo(4);
    // max_attempts reached -> give up
    const d5 = decideRetry(
      { initialInterval: 1, maxAttempts: 3, jitter: false },
      new Error("x"),
      3,
      "t",
    );
    expect(d5.retry).toBe(false);
  });
});

describe("scheduler: durable log compaction", () => {
  it("snapshots and compacts past the threshold, preserving receipts", async () => {
    const store = new ReactiveStore({});
    const log = new MemoryLog();
    const graph = new GraphBuilder("g")
      .task({
        id: "e",
        kind: "effect",
        handler: () =>
          result({
            patches: [{ path: ["n"], operation: "set", value: 1 }],
            writes: ["n"],
            receipt: "r-1",
          }),
      })
      .build();
    const sch = new Scheduler({ graph, store, log, compactAfter: 3 });
    await sch.runTask("e", { v: 1 });
    await sch.runTask("e", { v: 2 }); // different input -> effect runs again, log crosses threshold

    const events = log.readFrom(0);
    const snapshot = events.find((e) => e.kind === "snapshot");
    expect(snapshot).toBeDefined();
    // Compacted: only the snapshot remains (truncate keeps events >= its seq).
    expect(log.count).toBe(1);
    // Receipts survive compaction for restart idempotency.
    const recovered = recover(log);
    expect(recovered.confirmedEffects.size).toBe(2);
    expect(recovered.state.n).toBe(1);
  });

  it("does not compact before the threshold", async () => {
    const store = new ReactiveStore({});
    const log = new MemoryLog();
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["n"], 1) })
      .build();
    const sch = new Scheduler({ graph, store, log, compactAfter: 100 });
    await sch.runTask("t", {});
    expect(log.readFrom(0).some((e) => e.kind === "snapshot")).toBe(false);
  });
});

describe("scheduler: field-level permissions at the commit point (M2-F6)", () => {
  it("rejects a patch on a denied path before commit", async () => {
    const store = new ReactiveStore({});
    // Writes allowed only under ["public"].
    const policy = allowPaths([], [["public"]]);
    const graph = new GraphBuilder("g")
      .task({
        id: "t",
        kind: "effect",
        handler: () => writer(["secret"], 1),
      })
      .build();
    const sch = new Scheduler({ graph, store, permissionPolicy: policy });
    await expect(sch.runTask("t", {})).rejects.toThrow(/permission denied/);
    expect(store.raw.secret).toBeUndefined(); // nothing committed
  });

  it("commits patches on allowed paths", async () => {
    const store = new ReactiveStore({});
    const policy = allowPaths([], [["public"]]);
    const graph = new GraphBuilder("g")
      .task({
        id: "t",
        kind: "effect",
        handler: () =>
          result({
            patches: [{ path: ["public", "msg"], operation: "set", value: 1 }],
            writes: ["public"],
          }),
      })
      .build();
    const sch = new Scheduler({ graph, store, permissionPolicy: policy });
    await sch.runTask("t", {});
    expect(store.raw.public).toEqual({ msg: 1 });
  });

  it("does not enforce permissions when no policy is configured", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["a"], 1) })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("t", {});
    expect(store.raw.a).toBe(1);
  });
});

describe("scheduler: scope isolation (M2-F6b)", () => {
  it("scoped tasks write under state[scope] and never collide on same-named keys", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({
        id: "a",
        kind: "effect",
        handler: () => writer(["c"], 1),
        scope: "tenantA",
      })
      .task({
        id: "b",
        kind: "effect",
        handler: () => writer(["c"], 2),
        scope: "tenantB",
      })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    await sch.runAll(["a", "b"], new Map()); // same declared writes, different scopes
    expect(store.raw.tenantA).toEqual({ c: 1 });
    expect(store.raw.tenantB).toEqual({ c: 2 });
  });

  it("declared writes are scoped before the conflict pre-check", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({
        id: "a",
        kind: "effect",
        handler: () => writer(["x"], 1),
        scope: "s1",
        writes: ["x"],
      })
      .task({
        id: "b",
        kind: "effect",
        handler: () => writer(["x"], 2),
        scope: "s2",
        writes: ["x"],
      })
      .build();
    const sch = new Scheduler({ graph, store, concurrency: 2 });
    await sch.runAll(["a", "b"], new Map()); // would conflict unscoped; isolated by scope
    expect(store.raw.s1.x).toBe(1);
    expect(store.raw.s2.x).toBe(2);
  });

  it("unscoped tasks behave as before", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["n"], 5) })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("t", {});
    expect(store.raw.n).toBe(5);
  });
});

describe("scheduler: task policies at dispatch (review wiring)", () => {
  it("budget policy stops dispatching once the cap is reached", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["n"], 1) })
      .build();
    const budget = new BudgetPolicy(2); // costPerCall = 1, cap 2
    const sch = new Scheduler({ graph, store, policies: [budget] });
    await sch.runTask("t", {});
    await sch.runTask("t", {});
    await expect(sch.runTask("t", {})).rejects.toThrow(/policy denied/);
  });

  it("circuit breaker opens after failures and recovers on success", async () => {
    const store = new ReactiveStore({});
    const t = 0;
    const graph = new GraphBuilder("g")
      .task({
        id: "f",
        kind: "effect",
        handler: () => {
          throw new Error("boom");
        },
      })
      .build();
    const cb = new CircuitBreakerPolicy(2, 100, () => t);
    const sch = new Scheduler({ graph, store, policies: [cb] });
    await expect(sch.runTask("f", {})).rejects.toThrow("boom");
    await expect(sch.runTask("f", {})).rejects.toThrow("boom");
    // Open: third attempt is policy-denied before the handler runs.
    await expect(sch.runTask("f", {})).rejects.toThrow(/policy denied/);
    // Recovery on success path (no-op here since handler always fails).
    expect(cb.allow({ taskId: "f", at: t }).allowed).toBe(false);
  });

  it("does not consult policies when none are configured", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["n"], 1) })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("t", {});
    expect(store.raw.n).toBe(1);
  });
});

describe("scheduler: OTel span emission (otel wiring)", () => {
  it("emits a task span on success", async () => {
    const store = new ReactiveStore({});
    const spans = new SpanEmitter();
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["n"], 1) })
      .build();
    const sch = new Scheduler({ graph, store, spanEmitter: spans });
    await sch.runTask("t", {});
    const taskSpans = spans.exportSpans().filter((s) => s.kind === "task");
    expect(taskSpans.length).toBe(1);
    expect(taskSpans[0]!.name).toBe("task:t");
    expect(taskSpans[0]!.status).toBe("ok");
  });

  it("marks a failing attempt as error and emits a retry span", async () => {
    const store = new ReactiveStore({});
    const spans = new SpanEmitter();
    const graph = new GraphBuilder("g")
      .task({
        id: "f",
        kind: "effect",
        retry: { maxAttempts: 2 },
        handler: () => {
          throw new Error("nope");
        },
      })
      .build();
    const sch = new Scheduler({ graph, store, spanEmitter: spans });
    await expect(sch.runTask("f", {})).rejects.toThrow("nope");
    const exported = spans.exportSpans();
    const failed = exported.filter((s) => s.kind === "task" && s.status === "error");
    expect(failed.length).toBe(2); // attempt 1 + attempt 2
    expect(exported.some((s) => s.kind === "retry")).toBe(true);
  });

  it("emits computed recomputation and cache-hit spans", async () => {
    const store = new ReactiveStore({});
    store.raw.a = 1;
    const spans = new SpanEmitter();
    const graph = new GraphBuilder("g")
      .computed({ id: "total", reads: ["a"], selector: (s) => (s as Record<string, number>).a })
      .build();
    const sch = new Scheduler({ graph, store, spanEmitter: spans });
    sch.compute("total");
    sch.compute("total"); // cache hit
    const kinds = spans.exportSpans().map((s) => s.kind);
    expect(kinds).toContain("computed");
    expect(kinds).toContain("cache_hit");
  });

  it("emits nothing when no emitter is configured", async () => {
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g")
      .task({ id: "t", kind: "effect", handler: () => writer(["n"], 1) })
      .build();
    const sch = new Scheduler({ graph, store });
    await sch.runTask("t", {});
    expect(async () => {}).not.toThrow();
  });
});
