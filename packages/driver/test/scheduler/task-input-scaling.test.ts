import { describe, expect, it } from "vitest";
import { GraphBuilder } from "../../src/graph/model.js";
import { DriverRuntime, type TaskExecutor } from "../../src/runtime.js";

/**
 * Scaling contract: a run whose tasks are all skipped must not pay a
 * per-task full-state copy.
 *
 * `run()` used to build the merged task input (`{...store.raw, ...baseInput}`)
 * once *per task* inside the dispatch loop. With a 1000-node graph the store
 * holds ~2000 keys (inputs + committed writes), and V8's spread on a
 * 2000-key dictionary object costs ~4.6 ms, so a run that executed **zero**
 * tasks still took ~3.1 s of pure CPU. The measured executor call count was
 * 0, which is what proved the cost was the copy, not the work.
 */
const N = 1000;

function buildWide(n: number) {
  const b = new GraphBuilder("scaling");
  for (let i = 0; i < n; i++) {
    b.task({
      id: `t${i}`,
      kind: "pure",
      reads: [`k${i}`],
      writes: [`v${i}`],
      handler: () => ({ patches: [], writes: [] }),
    });
    b.on("run", `t${i}`);
  }
  return b.build();
}

function specOf(graph: ReturnType<typeof buildWide>) {
  return {
    id: "scaling",
    tasks: graph.tasks().map((t) => ({
      id: t.id,
      kind: t.kind,
      on: ["run"],
      reads: [...t.reads],
      writes: [...t.writes],
      callbackId: t.id,
      maxRuns: 1,
    })),
    routes: graph.tasks().map((t) => ({ event: "run", taskId: t.id })),
    scopes: [],
    computeds: [],
  };
}

function input() {
  const o: Record<string, number> = {};
  for (let i = 0; i < N; i++) o[`k${i}`] = 0;
  return o;
}

describe("task-input scaling", () => {
  it("a fully-skipped 1000-node run stays well under a second", async () => {
    const graph = buildWide(N);
    let calls = 0;
    const executor: TaskExecutor = {
      invokeTask: async () => {
        calls += 1;
        return { patches: [], writes: [] };
      },
    };
    const rt = new DriverRuntime(executor);
    rt.compileGraph(specOf(graph) as never);

    await rt.run("scaling", input(), { threadId: "scaling" });
    calls = 0;

    const t0 = process.hrtime.bigint();
    await rt.run("scaling", input(), { threadId: "scaling" });
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;

    expect(calls).toBe(0); // nothing to execute: selective skip still works
    expect(ms).toBeLessThan(1000); // was ~3100 ms with the per-task copy
  });
});
