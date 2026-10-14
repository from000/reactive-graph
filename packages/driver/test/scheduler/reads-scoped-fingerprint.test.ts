import { describe, expect, it } from "vitest";
import { GraphBuilder } from "../../src/graph/model.js";
import { ReactiveStore } from "../../src/state/store.js";
import { Scheduler } from "../../src/scheduler/scheduler.js";
import { DriverRuntime } from "../../src/runtime.js";

describe("reads-scoped pure fingerprint", () => {
  it("reruns only the task whose declared read changed (in-batch)", async () => {
    const N = 50;
    const calls: Record<string, number> = {};
    const b = new GraphBuilder("granular");
    for (let i = 0; i < N; i++) {
      calls[`t${i}`] = 0;
      b.task({
        id: `t${i}`,
        kind: "pure",
        reads: [`k${i}`],
        writes: [`v${i}`],
        handler: () => {
          calls[`t${i}`] = (calls[`t${i}`] ?? 0) + 1;
          return { reads: [], patches: [], writes: [], return_value: null, external_receipts: [] };
        },
      });
      b.on("run", `t${i}`);
    }
    const graph = b.build();
    const store = new ReactiveStore({});
    const persistent = { fingerprints: new Map<string, string>() };
    const s1 = new Scheduler({
      graph,
      store,
      runId: "g1",
      threadId: "t",
      persistent,
      baseInput: { k0: 1 },
    });
    const ids = graph.routeFor("run");
    const inputsFor = (k0: number) => {
      const m = new Map<string, unknown>();
      for (let i = 0; i < N; i++) m.set(`t${i}`, { [`k${i}`]: i === 0 ? k0 : 0 });
      return m;
    };
    await s1.runAll(ids, inputsFor(1) as never);
    const before = { ...calls };
    const s2 = new Scheduler({
      graph,
      store,
      runId: "g2",
      threadId: "t",
      persistent,
      baseInput: { k0: 2 },
    });
    await s2.runAll(ids, inputsFor(2) as never);
    const reran = Object.keys(calls).filter((k) => calls[k]! > (before[k] ?? 0));
    expect(reran).toEqual(["t0"]);
  });

  it("still skips an unscoped task across runs with identical input", async () => {
    let n = 0;
    const executor = {
      invokeTask: async () => {
        n++;
        return { reads: [], patches: [], writes: [], return_value: null, external_receipts: [] };
      },
    };
    const rt = new DriverRuntime(executor as never);
    rt.compileGraph({
      id: "g-unscoped",
      tasks: [{ id: "p", kind: "pure", callbackId: "p", on: ["run"] }],
      routes: [{ event: "run", taskId: "p" }],
    });
    await rt.run("g-unscoped", { x: 1 }, { event: "run", threadId: "t" });
    await rt.run("g-unscoped", { x: 1 }, { event: "run", threadId: "t" });
    expect(n).toBe(1);
    await rt.run("g-unscoped", { x: 2 }, { event: "run", threadId: "t" });
    expect(n).toBe(2);
  });
});
