import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  DriverRuntime,
  type TaskExecutor,
  type TaskSuccess,
  type WirePatch,
} from "../src/runtime.js";
import { SqliteLog } from "../src/durability/index.js";
import { SqliteCheckpointSaver } from "../src/storage/index.js";

function setPatch(path: string[], value: unknown): WirePatch {
  return { path, operation: "set", value };
}

const TASK_SUCCESS: TaskSuccess = {
  reads: [],
  patches: [],
  writes: [],
  return_value: undefined,
  external_receipts: [],
};

describe("time travel: checkpoint restore (replay/fork semantics)", () => {
  function incRt(dir: string): DriverRuntime {
    const executor: TaskExecutor = {
      invokeTask: async (callbackId, input) => {
        if (callbackId !== "inc")
          return { error: { type: "UnknownCallback", message: `no handler ${callbackId}` } };
        return {
          ...TASK_SUCCESS,
          patches: [setPatch(["n"], ((input as { n: number }).n ?? 0) + 1)],
          writes: ["n"],
        };
      },
    };
    const log = new SqliteLog({ location: `${dir}/rgp.db`, name: "run" });
    const saver = new SqliteCheckpointSaver(`${dir}/rgp.db`);
    const rt = new DriverRuntime(executor, { log, checkpointSaver: saver, recoverThreads: true });
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "inc", kind: "effect", callbackId: "inc", on: ["run"] }],
      routes: [{ event: "run", taskId: "inc" }],
    });
    return rt;
  }

  it("rolls a thread back to a historical checkpoint and continues from it", async () => {
    const dir = mkdtempSync(join(tmpdir(), "rgp-tt-"));
    try {
      const rt = incRt(dir);
      await rt.run("g", { n: 0 }, { event: "run", threadId: "t" }); // n -> 1   (cp-1)
      await rt.run("g", { n: 2 }, { event: "run", threadId: "t" }); // n -> 3   (cp-2)
      expect((await rt.getState("t")).n).toBe(3);

      const list = (await rt.checkpointOp({ threadId: "t", op: "list" })) as {
        checkpointId: string;
      }[];
      expect(list.length).toBeGreaterThanOrEqual(2);

      const restored = (await rt.checkpointOp({
        threadId: "t",
        op: "restore",
        checkpointId: list[0].checkpointId, // earliest: n == 1
      })) as { state: Record<string, unknown> };
      expect(restored.state.n).toBe(1);
      expect((await rt.getState("t")).n).toBe(1);

      // continuing from the restored snapshot
      await rt.run("g", { n: 5 }, { event: "run", threadId: "t" });
      expect((await rt.getState("t")).n).toBe(6);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects restore of an unknown checkpoint with a hint", async () => {
    const dir = mkdtempSync(join(tmpdir(), "rgp-tt-"));
    try {
      const rt = incRt(dir);
      await rt.run("g", { n: 0 }, { event: "run", threadId: "t" });
      await expect(
        rt.checkpointOp({ threadId: "t", op: "restore", checkpointId: "cp-999" }),
      ).rejects.toThrow(/Hint:/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
