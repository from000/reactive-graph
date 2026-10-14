import { describe, expect, it } from "vitest";
import { GraphBuilder, graphToDot } from "../src/index.js";

describe("graphToDot (graphviz export)", () => {
  it("renders tasks, events, computed reads and routes", () => {
    const graph = new GraphBuilder("inventory")
      .task({
        id: "check",
        kind: "effect",
        handler: () => ({ reads: [], writes: [], patches: [] }),
      })
      .task({
        id: "restock",
        kind: "pure",
        handler: () => ({ reads: [], writes: [], patches: [] }),
      })
      .computed({
        id: "low",
        reads: ["stock.amount"],
        selector: (s) => (s.stock as { amount?: number })?.amount ?? 0,
      })
      .on("order", "check")
      .on("order", "restock")
      .build();

    const dot = graphToDot(graph);
    expect(dot).toContain("digraph");
    expect(dot).toContain('t_check [label="check (effect)", shape=box]');
    expect(dot).toContain('t_restock [label="restock (pure)", shape=box]');
    expect(dot).toContain("computed:low");
    expect(dot).toContain("stock.amount -> c_low");
    expect(dot).toContain("ev_order -> t_check");
    expect(dot).toContain("ev_order -> t_restock");
    expect(dot.endsWith("}\n")).toBe(true);
  });

  it("sanitizes node ids with special characters", () => {
    const graph = new GraphBuilder("weird id")
      .task({ id: "t/1", kind: "effect", handler: () => ({ reads: [], writes: [], patches: [] }) })
      .on("e:1", "t/1")
      .build();
    const dot = graphToDot(graph);
    expect(dot).toContain("t_t_1");
    expect(dot).toContain("ev_e_1");
    expect(dot).not.toContain('t/"');
  });
});
