/**
 * ReactiveGraph side of the differential benchmark (plan §16 reproducibility).
 *
 * Runs the SAME deterministic graphs as `langgraph_side.py` under the native
 * Driver Scheduler and writes `.results/reactive.json`:
 *   { runs, nodes, <workload>: { median_ms, output, executedPerRun } }
 *
 * Fingerprint-skip semantics are identical to benchmarks/harness/selective.test.ts:
 * per-run inputs change only the affected task(s), so pure tasks receiving
 * identical inputs are skipped without invoking their handler. Executed counts
 * come from the scheduler trace ("start" events), exactly like selective.test.ts.
 *
 * `run_differential.py` runs both sides, asserts identical outputs, and prints
 * the comparison table (median of runs, same machine, no-gain published).
 */

import { describe, expect, it } from "vitest";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { GraphBuilder, ReactiveStore, Scheduler } from "@reactivegraph/driver";
import type { TaskResult } from "@reactivegraph/driver";

const NODES = 1000;
const RUNS = 3;
const RESULTS_DIR = join(dirname(fileURLToPath(import.meta.url)), ".results");

function r(patches: { path: (string | number)[]; operation: string; value: unknown }[], writes: string[]): TaskResult {
  return { reads: [], writes, patches };
}

function median(ms: number[]): number {
  const s = [...ms].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

/** Graph of N pure tasks: t_i reads input.value, writes out.i = value + 1. */
function buildPassThrough(n: number) {
  const builder = new GraphBuilder("differential");
  for (let i = 0; i < n; i++) {
    const path = ["out", i];
    builder.task({
      id: `t${i}`,
      kind: "pure",
      handler: (input: unknown) =>
        r([{ path, operation: "set", value: ((input as { value: number }).value ?? 0) + 1 }], [`out.${i}`]),
    });
    builder.on("run", `t${i}`);
  }
  return builder.build();
}

interface Sample {
  median_ms: number;
  output: Record<string, unknown>;
  executedPerRun: number;
}

/** Run the graph `runs` times; per-run inputs come from `inputsFor(k)` (k=1..runs). */
async function runReactive(
  graph: ReturnType<typeof buildPassThrough>,
  inputsFor: (k: number) => Map<string, { value: number }>,
): Promise<Sample> {
  const store = new ReactiveStore();
  const scheduler = new Scheduler({ graph, store, runId: "bench-diff", threadId: "bench" });
  const ids = graph.routeFor("run");
  const times: number[] = [];
  const executedPerRun: number[] = [];
  for (let k = 1; k <= RUNS; k++) {
    const before = scheduler.getTrace().filter((e) => e.kind === "start").length;
    const t0 = process.hrtime.bigint();
    await scheduler.runAll(ids, inputsFor(k));
    times.push(Number(process.hrtime.bigint() - t0) / 1e6);
    const after = scheduler.getTrace().filter((e) => e.kind === "start").length;
    executedPerRun.push(after - before);
  }
  return {
    median_ms: median(times),
    output: store.raw as Record<string, unknown>,
    executedPerRun: median(executedPerRun),
  };
}

describe("differential benchmark: reactive side", () => {
  it("runs chat/wide/selective/nogain and writes .results/reactive.json", async () => {
    const report: Record<string, unknown> = { runs: RUNS, nodes: NODES };

    // chat: 2 constant nodes (x=1, y=2); input churn forces both to run.
    {
      const builder = new GraphBuilder("chat");
      builder.task({
        id: "t0",
        kind: "pure",
        handler: () => r([{ path: ["out", 0], operation: "set", value: 1 }], ["out.0"]),
      });
      builder.task({
        id: "t1",
        kind: "pure",
        handler: () => r([{ path: ["out", 1], operation: "set", value: 2 }], ["out.1"]),
      });
      builder.on("run", "t0");
      builder.on("run", "t1");
      const chat = await runReactive(builder.build(), (k) => new Map([["t0", { value: k }], ["t1", { value: k }]]));
      report.chat = chat;
      expect(chat.executedPerRun).toBe(2); // both tasks ran every run
      expect(chat.output).toEqual({ out: [1, 2] }); // constant graph output
    }

    // wide: one affected field per run (t0), the other 999 get identical inputs.
    {
      const graph = buildPassThrough(NODES);
      const wide = await runReactive(
        graph,
        (k) =>
          new Map([
            ["t0", { value: k }],
            ...Array.from({ length: NODES - 1 }, (_, i) => [`t${i + 1}`, { value: 0 }] as const),
          ]),
      );
      report.wide = wide;
      expect(wide.executedPerRun).toBe(1); // only t0 ran each run
    }

    // selective: 10 affected fields per run.
    {
      const graph = buildPassThrough(NODES);
      const sel = await runReactive(
        graph,
        (k) =>
          new Map([
            ...Array.from({ length: 10 }, (_, i) => [`t${i}`, { value: k }] as const),
            ...Array.from({ length: NODES - 10 }, (_, i) => [`t${i + 10}`, { value: 0 }] as const),
          ]),
      );
      report.selective = sel;
      expect(sel.executedPerRun).toBe(10);
    }

    // nogain: full dependency chain — every input differs, nothing is skipped.
    {
      const graph = buildPassThrough(NODES);
      const nogain = await runReactive(
        graph,
        (k) => new Map(Array.from({ length: NODES }, (_, i) => [`t${i}`, { value: k + i }] as const)),
      );
      report.nogain = nogain;
      expect(nogain.executedPerRun).toBe(NODES);
    }

    mkdirSync(RESULTS_DIR, { recursive: true });
    writeFileSync(join(RESULTS_DIR, "reactive.json"), JSON.stringify(report, null, 2));
    // eslint-disable-next-line no-console
    console.table(
      Object.entries(report)
        .filter(([w]) => w !== "runs" && w !== "nodes")
        .map(([w, s]) => ({
          workload: w,
          median_ms: (s as Sample).median_ms.toFixed(3),
          executedPerRun: (s as Sample).executedPerRun,
        })),
    );
  });
});
