import { describe, expect, it } from "vitest";
import { MessageAssembler } from "../src/message-assembler.js";

describe("MessageAssembler (Task 11 / sdk-js-message-assembler)", () => {
  it("assembles content deltas into a complete message by id", () => {
    const a = new MessageAssembler();
    const start = a.consume({
      namespace: ["thread", "1"],
      node: "agent",
      data: { event: "message-start", id: "m1", metadata: { step: 0 } },
    });
    expect(start.kind).toBe("message-start");
    const mid = a.consume({
      namespace: ["thread", "1"],
      node: "agent",
      data: { id: "m1", content: "Hel" },
    });
    expect(mid.kind).toBe("message-delta");
    a.consume({ namespace: ["thread", "1"], node: "agent", data: { id: "m1", content: "lo" } });
    const end = a.consume({
      namespace: ["thread", "1"],
      node: "agent",
      data: { event: "message-end", id: "m1", usage: { input: 5, output: 2, total: 7 } },
    });
    expect(end.kind).toBe("message-end");
    const final = (end as { final: { content: unknown; complete: boolean; usage?: unknown } })
      .final;
    expect(final.content).toBe("Hello");
    expect(final.complete).toBe(true);
    expect(final.usage).toEqual({ input: 5, output: 2, total: 7 });
  });

  it("folds tool-call blocks by id", () => {
    const a = new MessageAssembler();
    a.consume({ namespace: [], data: { event: "message-start", id: "t" } });
    a.consume({
      namespace: [],
      data: { id: "t", tool_calls: [{ id: "c1", name: "search", args: {} }] },
    });
    a.consume({ namespace: [], data: { id: "t", tool_calls: [{ id: "c1", args: { q: "rgp" } }] } });
    const end = a.consume({ namespace: [], data: { event: "message-end", id: "t" } }) as {
      final: { tool_calls: Array<{ id: string; name?: string; args: unknown }> };
    };
    expect(end.final.tool_calls).toEqual([{ id: "c1", name: "search", args: { q: "rgp" } }]);
  });

  it("delta without a start synthesizes a shell", () => {
    const a = new MessageAssembler();
    const first = a.consume({ namespace: [], node: "x", data: { id: "orphan", content: "hi" } });
    expect(first.kind).toBe("message-delta");
  });

  it("reset clears active messages", () => {
    const a = new MessageAssembler();
    a.consume({ namespace: [], data: { event: "message-start", id: "a" } });
    a.reset();
    const again = a.consume({ namespace: [], data: { event: "message-start", id: "a" } });
    expect(again.kind).toBe("message-start");
  });
});
