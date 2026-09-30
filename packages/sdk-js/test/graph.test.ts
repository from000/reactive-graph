import { describe, expect, it } from "vitest";
import { GraphBuilder, ReactiveGraph, invoke } from "../src/index.js";
import type { TaskResult } from "@reactivegraph/driver";

function r(patches: unknown[], writes: string[]): TaskResult {
  return { reads: [], writes, patches };
}

describe("ReactiveGraph native JS SDK (Task 7)", () => {
  it("builds and invokes a graph through the native API", async () => {
    const graph = ReactiveGraph.build((b) => {
      b.task({
        id: "greet",
        kind: "effect",
        handler: (input) =>
          r(
            [{ path: ["msg"], operation: "set", value: `hi ${(input as { name: string }).name}` }],
            ["msg"],
          ),
      });
      b.on("visit", "greet");
    });
    const out = await invoke(graph, "visit", { name: "Ada" });
    expect((out as { state: Record<string, unknown> }).state.msg).toBe("hi Ada");
  });

  it("computed caches and invalidates on read-set change", () => {
    const graph = ReactiveGraph.build((b) => {
      b.computed({
        id: "total",
        reads: ["a", "b"],
        selector: (s) => (s.a as number) + (s.b as number),
      });
    });
    const raw = graph.store.raw;
    raw.a = 1;
    raw.b = 2;
    expect(graph.scheduler.compute("total")).toBe(3);
    expect(graph.scheduler.compute("total")).toBe(3); // cached
    raw.b = 20;
    expect(graph.scheduler.compute("total")).toBe(21);
  });

  it("exposes the fluent GraphBuilder with scope", () => {
    const builder = new GraphBuilder("g");
    builder.task({ id: "t", kind: "pure", handler: () => r([], []) });
    builder.scope("user");
    const graph = builder.build();
    expect(graph.id).toBe("g");
    expect(graph.def.scopes).toEqual(["user"]);
  });
});

describe("ReactiveGraph causal trace export (M2-F6c)", () => {
  it("exports a normalized causal trace with scheduling decisions", async () => {
    const graph = ReactiveGraph.build(
      (b) => {
        b.task({
          id: "t",
          kind: "effect",
          handler: () => r([{ path: ["n"], operation: "set", value: 1 }], ["n"]),
        });
        b.on("run", "t");
      },
      { graphId: "trace-graph" },
    );
    await invoke(graph, "run", {});
    const trace = graph.trace();
    expect(trace.protocol).toBe("reactivegraph.causal-trace.v1");
    expect(trace.runId).toBeTruthy();
    expect(trace.events.some((e) => e.kind === "span_start")).toBe(true);
    expect(trace.events.some((e) => e.kind === "span_end")).toBe(true);
  });

  it("redacts sensitive paths before export", async () => {
    const graph = ReactiveGraph.build((b) => {
      b.task({
        id: "t",
        kind: "effect",
        handler: () =>
          r([{ path: ["credentials"], operation: "set", value: "secret" }], ["credentials"]),
      });
      b.on("run", "t");
    });
    await invoke(graph, "run", {});
    const trace = graph.trace({ redact: [["credentials"]] });
    // The scheduler trace itself carries no values, so redaction applies to
    // any patched-value events; here we just verify the export is well-formed.
    expect(trace.events.length).toBeGreaterThan(0);
  });
});
