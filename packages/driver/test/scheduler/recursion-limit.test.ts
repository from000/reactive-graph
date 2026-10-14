import { describe, expect, it } from "vitest";
import { GraphBuilder, ReactiveStore, RecursionLimitError, Scheduler } from "../../src/index.js";
import type { TaskResult } from "../../src/index.js";

function r(patches: { path: string[]; operation: string; value: unknown }[]): TaskResult {
  return { reads: [], writes: [], patches };
}

describe("recursion limit (scheduler)", () => {
  it("raises RecursionLimitError once handler executions exceed the limit", async () => {
    const graph = new GraphBuilder("g")
      .task({
        id: "t",
        kind: "effect",
        handler: () => r([{ path: ["n"], operation: "set", value: 1 }]),
      })
      .on("run", "t")
      .build();
    const scheduler = new Scheduler({
      graph,
      store: new ReactiveStore(),
      recursionLimit: 2,
      concurrency: 1,
    });
    // first run: 1 handler execution (limit 2, ok)
    await scheduler.runAll(["t"], new Map([["t", {}]]));
    // second run: another execution -> total 2 (== limit, still ok)
    await scheduler.runAll(["t"], new Map([["t", {}]]));
    // third run: total 3 > 2 -> RecursionLimitError, never retried
    await expect(scheduler.runAll(["t"], new Map([["t", {}]]))).rejects.toThrow(
      RecursionLimitError,
    );
  });

  it("does not count skipped pure tasks toward the limit", async () => {
    const graph = new GraphBuilder("g")
      .task({
        id: "t",
        kind: "pure",
        handler: (input: unknown) =>
          r([{ path: ["n"], operation: "set", value: (input as { v: number }).v }]),
      })
      .on("run", "t")
      .build();
    const scheduler = new Scheduler({
      graph,
      store: new ReactiveStore(),
      recursionLimit: 1,
      concurrency: 1,
    });
    // executed once
    await scheduler.runAll(["t"], new Map([["t", { v: 1 }]]));
    // identical input -> fingerprint skip, limit NOT consumed
    await scheduler.runAll(["t"], new Map([["t", { v: 1 }]]));
    // different input -> executes, total 2 > 1 -> raise
    await expect(scheduler.runAll(["t"], new Map([["t", { v: 2 }]]))).rejects.toThrow(
      RecursionLimitError,
    );
  });

  it("is unlimited by default (backward compatible)", async () => {
    const graph = new GraphBuilder("g")
      .task({
        id: "t",
        kind: "effect",
        handler: () => r([{ path: ["n"], operation: "set", value: 1 }]),
      })
      .on("run", "t")
      .build();
    const scheduler = new Scheduler({ graph, store: new ReactiveStore(), concurrency: 1 });
    for (let i = 0; i < 100; i++) {
      await scheduler.runAll(["t"], new Map([["t", {}]]));
    }
    // no throw
  });
});
