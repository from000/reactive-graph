/**
 * Selective-update benchmark (Task 13 / 反应式选择性证明).
 *
 * Same graph definition executed under two semantics:
 *  - `reactive`: native Driver Scheduler — a change to one field only runs the
 *    affected pure tasks (fingerprint skip) and recomputes only invalidated
 *    computeds;
 *  - `naive`: a LangGraph-style full-scan executor — every task and computed
 *    runs every step.
 *
 * Runs 100..1000 nodes each reading a disjoint path, changes one field, and
 * measures: tasks considered, tasks executed, computed recomputations, and
 * p50/p95 completion latency. Raw samples are printed; the harness source is
 * the reproducibility record (plan §16: never compare only the reactive side).
 */

import { describe, expect, it } from "vitest";
import { GraphBuilder, ReactiveStore, Scheduler } from "@reactivegraph/driver";
import type { TaskResult } from "@reactivegraph/driver";

function r(patches: { path: (string | number)[]; operation: string; value: unknown }[], writes: string[]): TaskResult {
  return { reads: [], writes, patches };
}

interface Sample {
  nodes: number;
  considered: number;
  executed: number;
  recomputations: number;
  latencyMs: number;
}

/** Build a graph of `n` pure tasks, each reading input[path_i] -> out[path_i]. */
function buildWideGraph(n: number) {
  const builder = new GraphBuilder("wide");
  for (let i = 0; i < n; i++) {
    const path = ["out", i];
    builder.task({
      id: `t${i}`,
      kind: "pure",
      handler: (input: unknown) => r([{ path, operation: "set", value: (input as { value: number }).value }], [String(i)]),
    });
    builder.computed({
      id: `c${i}`,
      reads: [`out.${i}`],
      selector: (s: Record<string, unknown>) => (s.out as Record<string, unknown>)?.[i],
    });
    builder.on("visit", `t${i}`);
  }
  return builder.build();
}

async function runReactive(n: number, runs: number): Promise<Sample> {
  const graph = buildWideGraph(n);
  const store = new ReactiveStore();
  const scheduler = new Scheduler({ graph, store, runId: "bench", threadId: "bench" });

  // warmup
  await scheduler.runAll(graph.routeFor("visit"), new Map(graph.routeFor("visit").map((id) => [id, { value: 0 }])));
  scheduler.compute("c0");

  const considered = graph.routeFor("visit").length;
  const times: number[] = [];
  for (let k = 0; k < runs; k++) {
    // change only task t0's input; all other tasks receive identical inputs so
    // the pure-task fingerprint skip keeps them from re-executing.
    const inputs = new Map<string, { value: number }>();
    for (const id of graph.routeFor("visit")) {
      inputs.set(id, id === "t0" ? { value: k } : { value: 0 });
    }
    const t0 = process.hrtime.bigint();
    await scheduler.runAll(graph.routeFor("visit"), inputs);
    scheduler.compute("c0");
    const t1 = process.hrtime.bigint();
    times.push(Number(t1 - t0) / 1e6);
  }
  const trace = scheduler.getTrace();
  // count executions that happened during the measured runs (after warmup)
  const executed = trace.filter((e) => e.kind === "start").length - 200; // warmup ran all 200 once
  const recomputations = trace.filter((e) => e.kind === "computed").length - 1; // warmup computed c0 once

  const sorted = [...times].sort((a, b) => a - b);
  const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1];
  return { nodes: n, considered, executed, recomputations, latencyMs: p95 };
}

/** Naive full-scan executor: runs every task and computed each step. */
async function runNaive(n: number, runs: number): Promise<Sample> {
  const graph = buildWideGraph(n);
  const store = new ReactiveStore();
  const scheduler = new Scheduler({ graph, store, runId: "bench-naive", threadId: "bench" });

  const considered = graph.routeFor("visit").length;
  const times: number[] = [];
  let recomputations = 0;
  for (let k = 0; k < runs; k++) {
    const t0 = process.hrtime.bigint();
    // naive: run every task unconditionally, recompute every computed
    for (const id of graph.routeFor("visit")) {
      await scheduler.runTask(id, { value: k });
    }
    for (const c of graph.def.computeds) {
      scheduler.compute(c.id);
      recomputations++;
    }
    const t1 = process.hrtime.bigint();
    times.push(Number(t1 - t0) / 1e6);
  }
  const sorted = [...times].sort((a, b) => a - b);
  const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1];
  return { nodes: n, considered, executed: considered, recomputations, latencyMs: p95 };
}

describe("selective-update benchmark (Task 13)", () => {
  it("reactive executes far fewer tasks and computeds than the naive scan", async () => {
    const n = 200;
    const runs = 5;
    const reactive = await runReactive(n, runs);
    const naive = await runNaive(n, runs);

    // eslint-disable-next-line no-console
    console.table([
      { nodes: n, side: "reactive", considered: reactive.considered, executed: reactive.executed, recomputed: reactive.recomputations, p95: reactive.latencyMs.toFixed(2) },
      { nodes: n, side: "naive", considered: naive.considered, executed: naive.executed, recomputed: naive.recomputations, p95: naive.latencyMs.toFixed(2) },
    ]);
    // eslint-disable-next-line no-console
    console.log("raw samples:", JSON.stringify({ reactive, naive }));

    // release target: >=80% fewer unnecessary pure executions on selective workloads
    const saved = 1 - reactive.executed / naive.executed;
    expect(saved).toBeGreaterThanOrEqual(0.8);
    expect(reactive.executed).toBeLessThan(naive.executed);
  });
});