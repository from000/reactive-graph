import { describe, expect, it } from "vitest";
import { StreamMux, StreamTerminatedError } from "../src/stream/index.js";
import { InterruptManager, StaleResumeError } from "../src/interrupts.js";
import { MemoryLog } from "../src/durability/index.js";
import { GraphBuilder, ReactiveStore } from "../src/index.js";
import { SubgraphRunner } from "../src/subgraph.js";
import { buildFunctionGraph, entrypoint, functask } from "../src/functional.js";

describe("stream mux (Task 8)", () => {
  it("keeps committed chunks ordered and separate from transient", () => {
    const mux = new StreamMux();
    mux.push({ type: "custom", payload: { event: "e1", payload: 1 } });
    mux.push({ type: "committed", payload: { patches: [], stateVersion: 1 } });
    mux.push({ type: "messages", payload: { message: "tok" }, coalescible: true });
    mux.push({ type: "values", payload: { state: { a: 1 } } });
    expect(mux.lastCommittedSeq).toBe(4);
    const all = mux.readFrom(0);
    // global order by seq
    expect(all.map((c) => c.type)).toEqual(["custom", "committed", "messages", "values"]);
  });

  it("resume cursor: reads only chunks after the given seq", () => {
    const mux = new StreamMux();
    const c1 = mux.push({ type: "values", payload: { state: { a: 1 } } });
    mux.push({ type: "custom", payload: { event: "e", payload: "x" } });
    const rest = mux.readFrom(c1.seq);
    expect(rest).toHaveLength(1);
    expect(rest[0]!.type).toBe("custom");
  });

  it("terminated stream rejects further pushes", () => {
    const mux = new StreamMux();
    mux.terminate();
    expect(() => mux.push({ type: "values", payload: {} })).toThrow(StreamTerminatedError);
  });
});

describe("interrupts (Task 8)", () => {
  it("resumes only at the expected version (stale rejected)", () => {
    const mgr = new InterruptManager();
    const inter = mgr.interrupt({ ask: "approve?" }, "ckpt-1", 5);
    expect(() => mgr.resume(inter.id, 4)).toThrow(StaleResumeError);
    const resumed = mgr.resume(inter.id, 5);
    expect(resumed.checkpointHint).toBe("ckpt-1");
  });

  it("human edits are audited transactions with permission checks", () => {
    const mgr = new InterruptManager({ allowedActors: new Map([["admin", ["edit_state"]]]) });
    expect(() =>
      mgr.recordHumanEdit({ actor: "bob", permission: "edit_state", patches: [] }),
    ).toThrow(/lacks permission/);
    const edit = mgr.recordHumanEdit({
      actor: "admin",
      permission: "edit_state",
      patches: [{ path: ["a"], value: 1 }],
    });
    expect(edit.at).toBeGreaterThan(0);
    expect(mgr.auditLog).toHaveLength(1);
  });

  it("interrupts are durable", () => {
    const log = new MemoryLog();
    const mgr = new InterruptManager({ log });
    mgr.interrupt({ v: 1 }, "ckpt", 1);
    expect(log.count).toBe(1);
  });
});

describe("subgraphs (Task 8)", () => {
  it("isolated scope with typed input/output mapping", async () => {
    const parent = new ReactiveStore({ x: 10 });
    const childGraph = new GraphBuilder("child")
      .task({
        id: "double",
        kind: "pure",
        handler: () => ({
          reads: [],
          writes: ["y"],
          patches: [{ path: ["y"], operation: "set", value: 20 }],
        }),
      })
      .on("__enter__", "double")
      .build();
    const runner = new SubgraphRunner();
    runner.register({
      id: "child",
      graph: childGraph,
      io: { input: new Map([["x", "x"]]), output: new Map([["y", "result"]]) },
    });
    const out = await runner.run(parent, "child", new InterruptManager());
    expect(out["result"]).toBe(20);
  });
});

describe("functional workflows (Task 8)", () => {
  it("functask + entrypoint + build produce a runnable graph", async () => {
    const add = functask(async (input: unknown) => ({ n: (input as { n: number }).n + 1 }), {
      id: "add",
    });
    const plan = entrypoint("g", [add], [{ event: "run", taskId: "add" }]);
    expect(plan.taskIds).toEqual(["add"]);
    const builder = buildFunctionGraph("g", [add], [{ event: "run", taskId: "add" }]);
    const graph = builder.build();
    expect(graph.id).toBe("g");
    expect(graph.routeFor("run")).toEqual(["add"]);
  });
});
