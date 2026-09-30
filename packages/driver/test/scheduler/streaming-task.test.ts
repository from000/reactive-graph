import { describe, expect, it } from "vitest";
import { GraphBuilder, ReactiveStore, Scheduler } from "../../src/index.js";
import type { TaskResult, TaskStreamChunk } from "../../src/index.js";

describe("streaming tasks (async-generator handlers)", () => {
  it("emits each yielded chunk and commits the generator's return value", async () => {
    const graph = new GraphBuilder("g")
      .task({
        id: "llm",
        kind: "effect",
        handler: async function* (): AsyncGenerator<TaskStreamChunk, TaskResult, unknown> {
          yield { type: "messages", payload: { role: "assistant", content: "Hel" } };
          yield { type: "messages", payload: { role: "assistant", content: "lo" } };
          yield { type: "custom", payload: { tag: "log" } };
          return {
            reads: [],
            writes: ["n"],
            patches: [{ path: ["n"], operation: "set", value: 2 }],
          };
        },
      })
      .on("run", "llm")
      .build();

    const chunks: { taskId: string; chunk: TaskStreamChunk }[] = [];
    const store = new ReactiveStore();
    const scheduler = new Scheduler({
      graph,
      store,
      concurrency: 1,
      onStreamChunk: (taskId, chunk) => chunks.push({ taskId, chunk }),
    });

    await scheduler.runTask("llm", {});
    expect(chunks.map((c) => c.chunk.type)).toEqual(["messages", "messages", "custom"]);
    expect(chunks[0].taskId).toBe("llm");
    expect((chunks[0].chunk as { payload: { content: string } }).payload.content).toBe("Hel");
    // the generator's return value was committed as the task result
    expect(store.raw.n).toBe(2);
  });

  it("still supports plain handlers (no chunk sink called)", async () => {
    const graph = new GraphBuilder("g")
      .task({
        id: "plain",
        kind: "effect",
        handler: () => ({
          reads: [],
          writes: ["n"],
          patches: [{ path: ["n"], operation: "set", value: 9 }],
        }),
      })
      .on("run", "plain")
      .build();
    const chunks: unknown[] = [];
    const scheduler = new Scheduler({
      graph,
      store: new ReactiveStore(),
      concurrency: 1,
      onStreamChunk: (taskId, chunk) => chunks.push({ taskId, chunk }),
    });
    await scheduler.runTask("plain", {});
    expect(chunks).toEqual([]);
  });
});

// local type mirror to avoid a circular import in the test
interface TaskResult {
  readonly reads: readonly string[];
  readonly writes: readonly string[];
  readonly patches: unknown[];
}
