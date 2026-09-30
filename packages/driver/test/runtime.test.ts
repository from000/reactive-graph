import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import {
  DriverRuntime,
  type TaskExecutor,
  type TaskSuccess,
  type WirePatch,
} from "../src/runtime.js";
import { MemoryLog, SqliteLog, compact, recover } from "../src/durability/index.js";
import { events } from "../src/durability/events.js";
import { SqliteCheckpointSaver, SqliteStore } from "../src/storage/index.js";
/** Deterministic executor: maps callbackId -> patches, records calls. */
function makeExecutor(handlers: Record<string, (input: unknown) => TaskSuccess>): {
  executor: TaskExecutor;
  calls: { callbackId: string; input: unknown; version: number }[];
} {
  const calls: { callbackId: string; input: unknown; version: number }[] = [];
  const executor: TaskExecutor = {
    invokeTask: async (callbackId, input, version) => {
      calls.push({ callbackId, input, version });
      const handler = handlers[callbackId];
      if (!handler)
        return { error: { type: "UnknownCallback", message: `no handler ${callbackId}` } };
      return handler(input);
    },
  };
  return { executor, calls };
}

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

describe("DriverRuntime protocol layer (RGP/1), Task 4/7 wiring", () => {
  it("compiles a graph and runs a callback-backed task (end-to-end)", async () => {
    const { executor, calls } = makeExecutor({
      greet: (input) => ({
        ...TASK_SUCCESS,
        patches: [setPatch(["msg"], `hi ${(input as { name: string }).name}`)],
        writes: ["msg"],
      }),
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "greet", kind: "effect", callbackId: "greet", on: ["visit"] }],
      routes: [{ event: "visit", taskId: "greet" }],
    });

    const out = await rt.run("g", { name: "Ada" }, { event: "visit", threadId: "t1" });
    expect(out.state.msg).toBe("hi Ada");
    expect(out.threadId).toBe("t1");
    expect(calls).toHaveLength(1);
    expect(calls[0]!.callbackId).toBe("greet");
  });

  it("rejects duplicate graph ids and unknown callbacks at compile time", async () => {
    const rt = new DriverRuntime(makeExecutor({}).executor);
    rt.compileGraph({ id: "g", tasks: [{ id: "t", kind: "effect", callbackId: "cb" }] });
    expect(() =>
      rt.compileGraph({ id: "g", tasks: [{ id: "t2", kind: "effect", callbackId: "cb2" }] }),
    ).toThrow(/duplicate graph id/);
    expect(() =>
      rt.compileGraph({
        id: "g2",
        tasks: [{ id: "t", kind: "effect", callbackId: "nope" }],
      }),
    ).not.toThrow(); // no validator by default
    const strict = new DriverRuntime(makeExecutor({}).executor);
    expect(() =>
      strict.compileGraph(
        { id: "g3", tasks: [{ id: "t", kind: "effect", callbackId: "nope" }] },
        { validateCallback: (cb) => cb === "registered" },
      ),
    ).toThrow(/unregistered callback: nope/);
  });

  it("propagates a callback failure as a run error", async () => {
    const { executor } = makeExecutor({
      boom: () => ({ error: { type: "TaskError", message: "task exploded" } }),
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "boom", kind: "effect", callbackId: "boom" }],
      routes: [{ event: "run", taskId: "boom" }],
    });
    await expect(rt.run("g", {})).rejects.toThrow(/task exploded/);
  });

  it("keeps each concurrent run's id on its own callbacks", async () => {
    const observed: { callbackId: string; runId?: string }[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const executor: TaskExecutor = {
      invokeTask: async (callbackId, _input, _version, runId) => {
        observed.push({ callbackId, runId });
        if (callbackId === "a1") await gate;
        return {
          ...TASK_SUCCESS,
          patches: [setPatch([callbackId], runId)],
          writes: [callbackId],
        };
      },
    };
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g-concurrent",
      tasks: [
        { id: "a1", kind: "effect", callbackId: "a1" },
        { id: "a2", kind: "effect", callbackId: "a2" },
        { id: "b", kind: "effect", callbackId: "b" },
      ],
      routes: [
        { event: "a", taskId: "a1" },
        { event: "a1:written", taskId: "a2" },
        { event: "b", taskId: "b" },
      ],
    });

    const first = rt.run("g-concurrent", {}, { event: "a", threadId: "a", runId: "run-A" });
    await new Promise((resolve) => setTimeout(resolve, 0));
    const second = rt.run("g-concurrent", {}, { event: "b", threadId: "b", runId: "run-B" });
    await second;
    release();
    const a = await first;

    expect(observed).toEqual([
      { callbackId: "a1", runId: "run-A" },
      { callbackId: "b", runId: "run-B" },
      { callbackId: "a2", runId: "run-A" },
    ]);
    expect(a.state.a2).toBe("run-A");
  });

  it("GET_STATE returns per-thread committed state", async () => {
    const { executor } = makeExecutor({
      w: () => ({ ...TASK_SUCCESS, patches: [setPatch(["n"], 1)], writes: ["n"] }),
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
      routes: [{ event: "run", taskId: "w" }],
    });
    await rt.run("g", {}, { threadId: "a" });
    await rt.run("g", {}, { threadId: "b" });
    expect(await rt.getState("a")).toEqual({ n: 1 });
    expect(await rt.getState("b")).toEqual({ n: 1 });
    expect(await rt.getState("unseen")).toEqual({});
  });

  it("RELEASE_GRAPH forgets the compiled graph", async () => {
    const rt = new DriverRuntime(makeExecutor({}).executor);
    rt.compileGraph({ id: "g", tasks: [{ id: "t", kind: "effect", callbackId: "cb" }] });
    expect(rt.hasGraph("g")).toBe(true);
    rt.releaseGraph("g");
    expect(rt.hasGraph("g")).toBe(false);
    await expect(rt.run("g", {})).rejects.toThrow(/graph not found/);
  });

  it("runs with no route raise a clear error", async () => {
    const rt = new DriverRuntime(makeExecutor({}).executor);
    rt.compileGraph({ id: "g", tasks: [{ id: "t", kind: "effect", callbackId: "cb" }] });
    await expect(rt.run("g", {})).rejects.toThrow(/no task routes for event run/);
  });

  it("resume continues a run on a known runId", async () => {
    const { executor, calls } = makeExecutor({
      step: (input) => ({
        ...TASK_SUCCESS,
        patches: [setPatch(["done"], (input as { done?: boolean })?.done ?? false)],
        writes: ["done"],
      }),
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "step", kind: "effect", callbackId: "step" }],
      routes: [{ event: "run", taskId: "step" }],
    });
    const first = await rt.run("g", {}, { threadId: "t" });
    const resumed = await rt.resume(first.runId, { done: true }, { threadId: "t" });
    expect(resumed.state.done).toBe(true);
    expect(calls).toHaveLength(2);
  });

  it("accepts interrupts and reports interrupted runs", async () => {
    const { executor } = makeExecutor({
      ask: () =>
        ({
          error: { type: "Interrupt", message: "ask the user" },
        }) as unknown as TaskSuccess,
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "ask", kind: "effect", callbackId: "ask" }],
      routes: [{ event: "run", taskId: "ask" }],
    });
    const out = await rt.run("g", {});
    expect(out.interrupted).toBe(true);
    expect(out.state).toEqual({});
  });

  it("re-interrupts during resume (multi-step HITL)", async () => {
    const { executor } = makeExecutor({
      ask1: () => ({ error: { type: "Interrupt", message: "first" } }) as unknown as TaskSuccess,
      ask2: () => ({ error: { type: "Interrupt", message: "second" } }) as unknown as TaskSuccess,
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [
        { id: "ask1", kind: "effect", callbackId: "ask1" },
        { id: "ask2", kind: "effect", callbackId: "ask2" },
      ],
      routes: [
        { event: "run", taskId: "ask1" },
        { event: "resume", taskId: "ask2" },
      ],
    });
    const first = await rt.run("g", {});
    expect(first.interrupted).toBe(true);
    // 第二次挂起：resume 也应报告 interrupted（而非抛 TaskInterruptedError）
    const resumed = await rt.resume(first.runId, { ok: true }, { threadId: "t" });
    expect(resumed.interrupted).toBe(true);
    // resume 载荷并入线程 store（HITL 响应进入 state 的既有语义）
    expect(resumed.state.ok).toBe(true);
  });

  it("surfaces GraphNotFoundError for unknown graphs", async () => {
    const rt = new DriverRuntime(makeExecutor({}).executor);
    await expect(rt.run("missing", {})).rejects.toThrow(/graph not found: missing/);
    await expect(rt.run("missing", {})).rejects.toMatchObject({ name: "GraphNotFoundError" });
  });
});

describe("DriverRuntime persistence (Task 9 wiring)", () => {
  function tmpDb(): { dir: string; file: string; cleanup: () => void } {
    const dir = mkdtempSync("/tmp/rgp-runtime-");
    return {
      dir,
      file: `${dir}/rgp.db`,
      cleanup: () => rmSync(dir, { recursive: true, force: true }),
    };
  }

  it("recovers a thread store and long-term store across Driver instances", async () => {
    const { file, cleanup } = tmpDb();
    try {
      const writer = makeExecutor({
        w: () => ({ ...TASK_SUCCESS, patches: [setPatch(["n"], 1)], writes: ["n"] }),
      });
      // Instance A: sqlite-backed log + checkpoint saver + long-term store.
      const aLog = new SqliteLog({ location: file, name: "run" });
      const aSaver = new SqliteCheckpointSaver(file);
      const aStore = new SqliteStore(file);
      const aRt = new DriverRuntime(writer.executor, {
        log: aLog,
        checkpointSaver: aSaver,
        longTermStore: aStore,
        recoverThreads: true,
      });
      aRt.compileGraph({
        id: "g",
        tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
        routes: [{ event: "run", taskId: "w" }],
      });
      await aRt.run("g", {}, { threadId: "persist" });
      await aRt.storeOp({ namespace: ["docs"], key: "k", op: "put", value: { v: 1 } });
      aLog.close();

      // Instance B: fresh runtime over the same file must see the state + store.
      const bLog = new SqliteLog({ location: file, name: "run" });
      const bSaver = new SqliteCheckpointSaver(file);
      const bStore = new SqliteStore(file);
      const bRt = new DriverRuntime(writer.executor, {
        log: bLog,
        checkpointSaver: bSaver,
        longTermStore: bStore,
        recoverThreads: true,
      });
      expect(await bRt.getState("persist")).toEqual({ n: 1 });
      const item = bStore.get(["docs"], "k");
      expect(item).not.toBeNull();
      bLog.close();
    } finally {
      cleanup();
    }
  });

  it("keeps threads isolated per process when no persistence is configured", async () => {
    const writer = makeExecutor({
      w: () => ({ ...TASK_SUCCESS, patches: [setPatch(["n"], 1)], writes: ["n"] }),
    });
    const rt = new DriverRuntime(writer.executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
      routes: [{ event: "run", taskId: "w" }],
    });
    await rt.run("g", {}, { threadId: "a" });
    await rt.run("g", {}, { threadId: "b" });
    expect(await rt.getState("a")).toEqual({ n: 1 });
    expect(await rt.getState("b")).toEqual({ n: 1 });
  });

  it("isolates threads when recovering from a shared log", async () => {
    const { file, cleanup } = tmpDb();
    try {
      const writer = makeExecutor({
        w: (input) => ({
          ...TASK_SUCCESS,
          patches: [setPatch(["n"], (input as { n?: number })?.n ?? 1)],
          writes: ["n"],
        }),
      });
      const aLog = new SqliteLog({ location: file, name: "run" });
      const aSaver = new SqliteCheckpointSaver(file);
      const aRt = new DriverRuntime(writer.executor, {
        log: aLog,
        checkpointSaver: aSaver,
        recoverThreads: true,
      });
      aRt.compileGraph({
        id: "g",
        tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
        routes: [{ event: "run", taskId: "w" }],
      });
      await aRt.run("g", { n: 10 }, { threadId: "ten" });
      await aRt.run("g", { n: 20 }, { threadId: "twenty" });
      aLog.close();

      // Recover: each thread must see ONLY its own transaction.
      const bLog = new SqliteLog({ location: file, name: "run" });
      const bSaver = new SqliteCheckpointSaver(file);
      const bRt = new DriverRuntime(writer.executor, {
        log: bLog,
        checkpointSaver: bSaver,
        recoverThreads: true,
      });
      expect(await bRt.getState("ten")).toEqual({ n: 10 });
      expect(await bRt.getState("twenty")).toEqual({ n: 20 });
      expect(await bRt.getState("missing")).toEqual({});
      bLog.close();
    } finally {
      cleanup();
    }
  });

  it("restores effect receipts across restarts so effects never re-run", async () => {
    const { file, cleanup } = tmpDb();
    try {
      let invocations = 0;
      const effect = makeExecutor({
        send: () => {
          invocations += 1;
          return {
            ...TASK_SUCCESS,
            patches: [setPatch(["sent"], true)],
            writes: ["sent"],
            external_receipts: [{ receipt: "rcpt-1" }],
          };
        },
      });
      const aLog = new SqliteLog({ location: file, name: "run" });
      const aSaver = new SqliteCheckpointSaver(file);
      const aRt = new DriverRuntime(effect.executor, {
        log: aLog,
        checkpointSaver: aSaver,
        recoverThreads: true,
      });
      aRt.compileGraph({
        id: "g",
        tasks: [{ id: "send", kind: "effect", callbackId: "send" }],
        routes: [{ event: "run", taskId: "send" }],
      });
      await aRt.run("g", {}, { threadId: "t" });
      expect(invocations).toBe(1);
      aLog.close();

      // Re-start over the same log: the restored receipt must gate the effect
      // FOR THE SAME INPUT only.
      const bLog = new SqliteLog({ location: file, name: "run" });
      const bSaver = new SqliteCheckpointSaver(file);
      const bRt = new DriverRuntime(effect.executor, {
        log: bLog,
        checkpointSaver: bSaver,
        recoverThreads: true,
      });
      bRt.compileGraph({
        id: "g",
        tasks: [{ id: "send", kind: "effect", callbackId: "send" }],
        routes: [{ event: "run", taskId: "send" }],
      });
      const out = await bRt.run("g", {}, { threadId: "t" });
      expect(invocations).toBe(1); // effect NOT re-run: receipt restored for same input
      expect(out.state.sent).toBe(true);

      // A DIFFERENT input must still execute the effect (input-scoped gate).
      await bRt.run("g", { other: true }, { threadId: "t" });
      expect(invocations).toBe(2);

      // Back to the CONFIRMED input: the gate persists and skips again.
      await bRt.run("g", {}, { threadId: "t" });
      expect(invocations).toBe(2);
      bLog.close();
    } finally {
      cleanup();
    }
  });

  it("restart-storm: compacted log preserves seq, state and confirmed receipts", async () => {
    const { file, cleanup } = tmpDb();
    try {
      let invocations = 0;
      const effect = makeExecutor({
        send: () => {
          invocations += 1;
          return {
            ...TASK_SUCCESS,
            patches: [setPatch(["sent"], true)],
            writes: ["sent"],
            external_receipts: [{ receipt: "rcpt-1" }],
          };
        },
      });
      let lastSeq = 0;
      for (let cycle = 0; cycle < 4; cycle++) {
        const log = new SqliteLog({ location: file, name: "storm" });
        // Reopening after a compaction must not lose the snapshot position.
        expect(log.lastSeq).toBe(lastSeq);
        expect(log.readFrom(0).map((e) => e.seq)).toEqual(lastSeq === 0 ? [] : [lastSeq]);

        const saver = new SqliteCheckpointSaver(file);
        const rt = new DriverRuntime(effect.executor, {
          log,
          checkpointSaver: saver,
          recoverThreads: true,
        });
        rt.compileGraph({
          id: "g",
          tasks: [{ id: "send", kind: "effect", callbackId: "send" }],
          routes: [{ event: "run", taskId: "send" }],
        });
        if (cycle > 0) {
          // The snapshot is the only compaction root; its carried receipt must
          // gate the effect instead of losing seq=0 and re-running it.
          expect(rt.restoredReceipts().size).toBe(1);
        }
        const out = await rt.run("g", {}, { threadId: "t", runId: `storm-${cycle}` });
        expect(out.state.sent).toBe(true);
        expect(invocations).toBe(1);

        // Compact exactly like the scheduler does in production.
        const recovered = recover(log, { threadId: "t" });
        const seq = compact(
          log,
          events.snapshot({
            runId: `storm-${cycle}`,
            threadId: "t",
            stateVersion: recovered.stateVersion,
            stateHash: `h-${cycle}`,
            state: recovered.state,
            baseSeq: log.lastSeq,
            confirmedEffects: [...recovered.confirmedEffects.entries()],
          }),
        );
        expect(seq).toBeGreaterThan(lastSeq);
        lastSeq = seq;
        log.close();
      }
    } finally {
      cleanup();
    }
  });

  it("bounds completed-run retention during a run storm (memory ceiling)", async () => {
    const writer = makeExecutor({
      w: () => ({ ...TASK_SUCCESS, patches: [setPatch(["n"], 1)], writes: ["n"] }),
    });
    const rt = new DriverRuntime(writer.executor, {}, { maxRuns: 8 });
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
      routes: [{ event: "run", taskId: "w" }],
    });
    for (let i = 0; i < 200; i++) {
      await rt.run("g", {}, { threadId: `t-${i}`, runId: `run-${i}` });
    }
    expect(rt.stats.runs).toBeLessThanOrEqual(8);
    // The oldest completed run is evicted; the newest is still resumable.
    await expect(rt.resume("run-0", {}, { threadId: "t-0" })).rejects.toThrow(/unknown run/);
    await expect(rt.resume("run-199", {}, { threadId: "t-199" })).resolves.toBeDefined();
  });

  it("bounds the thread-store cache and rehydrates evicted durable threads", async () => {
    const writer = makeExecutor({
      w: (input) => ({
        ...TASK_SUCCESS,
        patches: [setPatch(["n"], (input as { n?: number })?.n ?? 0)],
        writes: ["n"],
      }),
    });
    const log = new MemoryLog();
    const rt = new DriverRuntime(writer.executor, { log, recoverThreads: true }, { maxThreads: 3 });
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
      routes: [{ event: "run", taskId: "w" }],
    });
    for (let i = 0; i < 25; i++) {
      await rt.run("g", { n: i }, { threadId: `t-${i}` });
    }
    expect(rt.stats.threads).toBeLessThanOrEqual(3);
    // Evicted store is rehydrated from the durable log on next access.
    expect(await rt.getState("t-0")).toEqual({ n: 0 });
    expect(rt.stats.threads).toBeLessThanOrEqual(3);
    log.close();
  });

  it("delete_thread clears runtime caches for the deleted thread", async () => {
    const writer = makeExecutor({
      w: () => ({ ...TASK_SUCCESS, patches: [setPatch(["n"], 1)], writes: ["n"] }),
    });
    const rt = new DriverRuntime(writer.executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "w", kind: "effect", callbackId: "w" }],
      routes: [{ event: "run", taskId: "w" }],
    });
    await rt.run("g", {}, { threadId: "gone", runId: "gone-run" });
    expect(rt.stats.threads).toBe(1);
    expect(rt.stats.runs).toBe(1);
    await rt.checkpointOp({ threadId: "gone", op: "delete_thread" });
    expect(rt.stats.threads).toBe(0);
    expect(rt.stats.runs).toBe(0);
    await expect(rt.resume("gone-run", {})).rejects.toThrow(/unknown run/);
    expect(await rt.getState("gone")).toEqual({});
  });

  it("does NOT trust a lone receipt: effect re-runs when intent is missing", async () => {
    const { file, cleanup } = tmpDb();
    try {
      let invocations = 0;
      const effect = makeExecutor({
        send: () => {
          invocations += 1;
          return {
            ...TASK_SUCCESS,
            patches: [setPatch(["sent"], true)],
            writes: ["sent"],
            external_receipts: [{ receipt: "rcpt-1" }],
          };
        },
      });
      // A separate log partition carrying ONLY a receipt (no matching intent)
      // simulates a crash between effect_intent and effect_receipt.
      const loneLog = new SqliteLog({ location: file, name: "lone" });
      const { events } = await import("../src/durability/events.js");
      loneLog.append(
        events.effectReceipt({
          idempotencyKey: "send:whatever",
          taskId: "send",
          receipt: "rcpt-1",
          outcome: "success",
        }),
      );
      const rt = new DriverRuntime(effect.executor, {
        log: loneLog,
        recoverThreads: true,
      });
      rt.compileGraph({
        id: "g",
        tasks: [{ id: "send", kind: "effect", callbackId: "send" }],
        routes: [{ event: "run", taskId: "send" }],
      });
      // Lone receipt not paired with an intent: not trusted, so no gate.
      expect([...rt.restoredReceipts().keys()]).toEqual([]);
      await rt.run("g", {}, { threadId: "t" });
      // Ran once: the stale lone receipt did NOT gate the effect (a wrong
      // gate would have skipped it and left invocations at 0).
      expect(invocations).toBe(1);
      loneLog.close();
    } finally {
      cleanup();
    }
  });
});

describe("DriverRuntime streaming + cancellation (gap 4)", () => {
  it("emits a values snapshot after each task when stream is enabled", async () => {
    const writer = makeExecutor({
      first: () => ({ ...TASK_SUCCESS, patches: [setPatch(["a"], 1)], writes: ["a"] }),
      second: () => ({ ...TASK_SUCCESS, patches: [setPatch(["b"], 2)], writes: ["b"] }),
    });
    const rt = new DriverRuntime(writer.executor);
    rt.compileGraph({
      id: "g",
      tasks: [
        { id: "first", kind: "effect", callbackId: "first" },
        { id: "second", kind: "effect", callbackId: "second" },
      ],
      routes: [
        { event: "run", taskId: "first" },
        { event: "run", taskId: "second" },
      ],
    });
    const values: { state: Record<string, unknown> }[] = [];
    const out = await rt.run(
      "g",
      {},
      {
        threadId: "t",
        stream: true,
        onStream: (ev) => values.push(ev.payload),
        // P3-2：concurrency=1 串行兜底——每任务真实帧（批内逐任务）
        config: { concurrency: 1 },
      },
    );
    // initial + after first + after second
    expect(values).toHaveLength(3);
    expect(values[0]!.state).toEqual({});
    expect(values[1]!.state).toEqual({ a: 1 });
    expect(values[2]!.state).toEqual({ a: 1, b: 2 });
    expect(out.state).toEqual({ a: 1, b: 2 });
  });

  it("emits batch-aggregated values frames for parallel rounds", async () => {
    // P3-2：written 派生批（扇出 root → first/second）默认并行——批内
    // 任务并发执行，每任务一帧但内容为批后聚合 state（无中间帧）。
    const writer = makeExecutor({
      root: () => ({ ...TASK_SUCCESS, patches: [setPatch(["r"], 1)], writes: ["r"] }),
      first: () => ({ ...TASK_SUCCESS, patches: [setPatch(["a"], 1)], writes: ["a"] }),
      second: () => ({ ...TASK_SUCCESS, patches: [setPatch(["b"], 2)], writes: ["b"] }),
    });
    const rt = new DriverRuntime(writer.executor);
    rt.compileGraph({
      id: "g",
      tasks: [
        { id: "root", kind: "effect", callbackId: "root" },
        { id: "first", kind: "effect", callbackId: "first" },
        { id: "second", kind: "effect", callbackId: "second" },
      ],
      routes: [
        { event: "run", taskId: "root" },
        { event: "root:written", taskId: "first" },
        { event: "root:written", taskId: "second" },
      ],
    });
    const values: { state: Record<string, unknown> }[] = [];
    await rt.run(
      "g",
      {},
      { threadId: "t", stream: true, onStream: (ev) => values.push(ev.payload) },
    );
    // initial + root 后 + first（批后聚合）+ second（批后聚合）
    expect(values).toHaveLength(4);
    expect(values[0]!.state).toEqual({});
    expect(values[1]!.state).toEqual({ r: 1 });
    expect(values[2]!.state).toEqual({ r: 1, a: 1, b: 2 });
    expect(values[3]!.state).toEqual({ r: 1, a: 1, b: 2 });
  });

  it("stops when the run is cancelled", async () => {
    const writer = makeExecutor({
      first: () => ({ ...TASK_SUCCESS, patches: [setPatch(["a"], 1)], writes: ["a"] }),
      second: () => ({ ...TASK_SUCCESS, patches: [setPatch(["b"], 2)], writes: ["b"] }),
    });
    const rt = new DriverRuntime(writer.executor);
    rt.compileGraph({
      id: "g",
      tasks: [
        { id: "first", kind: "effect", callbackId: "first" },
        { id: "second", kind: "effect", callbackId: "second" },
      ],
      routes: [
        { event: "run", taskId: "first" },
        { event: "run", taskId: "second" },
      ],
    });
    // Pre-aborted signal: the run must refuse to start and surface the reason.
    const controller = new AbortController();
    controller.abort("test-cancel");
    await expect(rt.run("g", {}, { threadId: "t", signal: controller.signal })).rejects.toThrow(
      /run cancelled \(test-cancel\)/,
    );
  });
});

describe("DriverRuntime store semantics", () => {
  it("store get round-trips values and search spans shared memory store", async () => {
    const runtime = new DriverRuntime(makeExecutor({}).executor, {});
    await runtime.storeOp({
      namespace: ["users", "alice"],
      key: "prefs",
      op: "put",
      value: { theme: "dark" },
    });
    // get returns the deserialized value
    const got = (await runtime.storeOp({
      namespace: ["users", "alice"],
      key: "prefs",
      op: "get",
    })) as {
      value: { theme: string };
    };
    expect(got.value).toEqual({ theme: "dark" });
    // search uses prefix semantics across namespaces in the shared memory store
    const hits = (await runtime.storeOp({
      namespace: ["users"],
      key: "",
      op: "search",
      filter: { theme: "dark" },
    })) as Array<{ key: string }>;
    expect(hits.some((h) => h.key === "prefs")).toBe(true);
  });

  it("list_namespaces and delete work", async () => {
    const runtime = new DriverRuntime(makeExecutor({}).executor, {});
    await runtime.storeOp({ namespace: ["a", "b"], key: "x", op: "put", value: 1 });
    const nss = (await runtime.storeOp({
      namespace: [],
      key: "",
      op: "list_namespaces",
    })) as string[][];
    expect(nss.some((n) => n.join("/") === "a/b")).toBe(true);
    await runtime.storeOp({ namespace: ["a", "b"], key: "x", op: "delete" });
    expect(await runtime.storeOp({ namespace: ["a", "b"], key: "x", op: "get" })).toBeNull();
  });
});

describe("executor streaming (executor-level token flow)", () => {
  it("forwards async-generator chunks to onStream and commits the return value", async () => {
    const executor: TaskExecutor = {
      invokeTask: async function* () {
        yield { type: "messages", payload: { role: "assistant", content: "Hel" } };
        yield { type: "messages", payload: { role: "assistant", content: "lo" } };
        yield { type: "custom", payload: { tag: "log" } };
        return { ...TASK_SUCCESS, patches: [setPatch(["n"], 2)], writes: ["n"] };
      },
    };
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "llm", kind: "effect", callbackId: "llm" }],
      routes: [{ event: "run", taskId: "llm" }],
    });
    const events: { eventType: string; payload: unknown }[] = [];
    const out = await rt.run(
      "g",
      {},
      {
        threadId: "t",
        stream: true,
        onStream: (ev) => events.push(ev),
      },
    );
    const types = events.map((e) => e.eventType);
    expect(types).toContain("messages");
    expect(types).toContain("custom");
    const msgs = events.filter((e) => e.eventType === "messages");
    expect(msgs).toHaveLength(2);
    expect((msgs[0]!.payload as { message: { content: string } }).message.content).toBe("Hel");
    // token churn is transient; the committed state comes from the generator return value
    expect(out.state).toEqual({ n: 2 });
  });

  it("keeps plain promise executors working untouched", async () => {
    const writer = makeExecutor({
      plain: () => ({ ...TASK_SUCCESS, patches: [setPatch(["a"], 1)], writes: ["a"] }),
    });
    const rt = new DriverRuntime(writer.executor);
    rt.compileGraph({
      id: "g",
      tasks: [{ id: "plain", kind: "effect", callbackId: "plain" }],
      routes: [{ event: "run", taskId: "plain" }],
    });
    const events: { eventType: string }[] = [];
    const out = await rt.run(
      "g",
      {},
      {
        threadId: "t",
        stream: true,
        onStream: (ev) => events.push(ev),
      },
    );
    expect(events.map((e) => e.eventType)).toEqual(["values", "values"]);
    expect(out.state).toEqual({ a: 1 });
  });
});

describe("RUN config: recursionLimit", () => {
  it("raises RecursionLimitError when a multi-task run exceeds the configured limit", async () => {
    const handlers: Record<string, () => TaskSuccess> = {};
    const tasks: { id: string; kind: "effect"; callbackId: string }[] = [];
    const routes: { event: string; taskId: string }[] = [];
    for (let i = 0; i < 10; i++) {
      handlers[`t${i}`] = () => ({ ...TASK_SUCCESS });
      tasks.push({ id: `t${i}`, kind: "effect", callbackId: `t${i}` });
      routes.push({ event: "run", taskId: `t${i}` });
    }
    const rt = new DriverRuntime(makeExecutor(handlers).executor);
    rt.compileGraph({ id: "g", tasks, routes });
    await expect(rt.run("g", {}, { threadId: "t", config: { recursionLimit: 3 } })).rejects.toThrow(
      /recursion/i,
    );
    // no config → unlimited (backwards compatible)
    const ok = await rt.run("g", {}, { threadId: "t2" });
    expect(ok.state).toBeDefined();
  });
});

describe("selective execution across runs (fingerprint persists)", () => {
  it("pure task skips on identical input across separate runs", async () => {
    const { executor, calls } = makeExecutor({
      p: (input) => ({
        ...TASK_SUCCESS,
        patches: [setPatch(["seen"], (input as { x: number }).x)],
        writes: ["seen"],
      }),
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g-pure-x",
      tasks: [{ id: "p", kind: "pure", callbackId: "p", on: ["run"] }],
      routes: [{ event: "run", taskId: "p" }],
    });

    await rt.run("g-pure-x", { x: 1 }, { event: "run", threadId: "t1" });
    await rt.run("g-pure-x", { x: 1 }, { event: "run", threadId: "t1" });
    expect(calls).toHaveLength(1); // 同输入跨 run：指纹跳过
    await rt.run("g-pure-x", { x: 2 }, { event: "run", threadId: "t1" });
    expect(calls).toHaveLength(2); // 新输入：重跑
  });
});

describe("historical run explanation", () => {
  it("reads committed transactions from the durable log by runId", async () => {
    const { executor } = makeExecutor({
      p: () => ({
        ...TASK_SUCCESS,
        patches: [setPatch(["x"], 1)],
        writes: ["x"],
        reads: ["n"],
      }),
    });
    const log = new MemoryLog();
    const rt = new DriverRuntime(executor, { log });
    rt.compileGraph({
      id: "g-history",
      tasks: [{ id: "p", kind: "effect", callbackId: "p", on: ["run"] }],
      routes: [{ event: "run", taskId: "p" }],
    });
    await rt.run("g-history", { n: 1 }, { runId: "run-hist", threadId: "t" });

    const explanation = rt.explainHistoricalRun("run-hist");
    expect(explanation).toMatchObject({
      runId: "run-hist",
      executed: 1,
      transactions: 1,
      decisionsPersisted: false,
    });
    const events = explanation.events as Array<Record<string, unknown>>;
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      event: "task:p",
      task: "p",
      reads: ["n"],
      writes: ["x"],
    });
  });

  it("rejects an unknown historical run without inventing events", () => {
    const rt = new DriverRuntime(makeExecutor({}).executor, { log: new MemoryLog() });
    expect(() => rt.explainHistoricalRun("missing")).toThrow(/no historical run found/);
  });
});

describe("scale and stability smoke", () => {
  it("runs 1000 isolated threads without state bleed", async () => {
    const { executor, calls } = makeExecutor({
      write: (input) => ({
        ...TASK_SUCCESS,
        patches: [setPatch(["n"], (input as { n: number }).n)],
        writes: ["n"],
      }),
    });
    const rt = new DriverRuntime(executor);
    rt.compileGraph({
      id: "g-scale-threads",
      tasks: [{ id: "write", kind: "effect", callbackId: "write", on: ["run"] }],
      routes: [{ event: "run", taskId: "write" }],
    });
    for (let i = 0; i < 1000; i++) {
      await rt.run("g-scale-threads", { n: i }, { threadId: `t-${i}` });
    }
    expect(calls).toHaveLength(1000);
    for (let i = 0; i < 1000; i += 100) {
      expect(await rt.getState(`t-${i}`)).toEqual({ n: i });
    }
  });

  it("builds and executes a 10000-node graph within the test budget", async () => {
    const handlers: Record<string, () => TaskSuccess> = {};
    const tasks: { id: string; kind: "effect"; callbackId: string; on: string[] }[] = [];
    const routes: { event: string; taskId: string }[] = [];
    for (let i = 0; i < 10_000; i++) {
      const id = `t${i}`;
      handlers[id] = () => TASK_SUCCESS;
      tasks.push({ id, kind: "effect", callbackId: id, on: ["run"] });
      routes.push({ event: "run", taskId: id });
    }
    const rt = new DriverRuntime(makeExecutor(handlers).executor);
    rt.compileGraph({ id: "g-scale-10000", tasks, routes });
    const out = await rt.run("g-scale-10000", {}, { threadId: "scale" });
    expect(out.state).toEqual({});
    // Scale smoke: the budget is explicit so slower or loaded machines do not
    // fail on vitest's 5s default for a genuinely large graph.
  }, 60_000);
});

describe("event-routed re-entry via maxRuns", () => {
  it("defaults to at-most-once per run (a cycle terminates instead of looping)", async () => {
    const calls: string[] = [];
    const rt = new DriverRuntime(
      makeExecutor({
        a: () => {
          calls.push("a");
          return { ...TASK_SUCCESS, patches: [setPatch(["a"], 1)], writes: ["a"] };
        },
        b: () => {
          calls.push("b");
          return { ...TASK_SUCCESS, patches: [setPatch(["b"], 1)], writes: ["b"] };
        },
      }).executor,
    );
    rt.compileGraph({
      id: "g-cycle-default",
      tasks: [
        { id: "a", kind: "effect", callbackId: "a" },
        { id: "b", kind: "effect", callbackId: "b" },
      ],
      routes: [
        { event: "run", taskId: "a" },
        { event: "a:written", taskId: "b" },
        { event: "b:written", taskId: "a" },
      ],
    });
    const out = await rt.run("g-cycle-default", {}, { threadId: "t" });
    expect(calls).toEqual(["a", "b"]);
    expect(out.state).toEqual({ a: 1, b: 1 });
  });

  it("re-enters a task up to maxRuns so a real model/tools loop converges", async () => {
    const calls: string[] = [];
    const rt = new DriverRuntime(
      makeExecutor({
        model: (input) => {
          const n = (input as { n?: number }).n ?? 0;
          calls.push(`model:${n}`);
          return { ...TASK_SUCCESS, patches: [setPatch(["n"], n + 1)], writes: ["n"] };
        },
        tools: (input) => {
          calls.push(`tools:${(input as { n?: number }).n ?? 0}`);
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
      }).executor,
    );
    rt.compileGraph({
      id: "g-cycle-maxruns",
      tasks: [
        { id: "model", kind: "effect", callbackId: "model", maxRuns: 3 },
        { id: "tools", kind: "effect", callbackId: "tools", maxRuns: 3 },
      ],
      routes: [
        { event: "run", taskId: "model" },
        { event: "model:written", taskId: "tools" },
        { event: "tools:written", taskId: "model" },
      ],
    });
    const out = await rt.run("g-cycle-maxruns", {}, { threadId: "t" });
    expect(calls.filter((c) => c.startsWith("model:"))).toHaveLength(3);
    expect(out.state).toEqual({ n: 3 });
    expect(calls.filter((c) => c.startsWith("tools:")).length).toBeLessThanOrEqual(3);
  });

  it("re-entry sees its own committed writes; the run payload must not shadow them", async () => {
    const seen: number[] = [];
    const rt = new DriverRuntime(
      makeExecutor({
        model: (input) => {
          const n = (input as { n?: number }).n ?? 0;
          seen.push(n);
          return { ...TASK_SUCCESS, patches: [setPatch(["n"], n + 1)], writes: ["n"] };
        },
        tools: () => ({ ...TASK_SUCCESS, patches: [], writes: [] }),
      }).executor,
    );
    rt.compileGraph({
      id: "g-cycle-payload-shadow",
      tasks: [
        { id: "model", kind: "effect", callbackId: "model", maxRuns: 3 },
        { id: "tools", kind: "effect", callbackId: "tools", maxRuns: 3 },
      ],
      routes: [
        { event: "run", taskId: "model" },
        { event: "model:written", taskId: "tools" },
        { event: "tools:written", taskId: "model" },
      ],
    });
    const out = await rt.run("g-cycle-payload-shadow", { n: 0 }, { threadId: "t" });
    // Each model turn must observe the previous turn's committed write, even
    // though the original run payload also carries `n`.
    expect(seen).toEqual([0, 1, 2]);
    expect(out.state).toEqual({ n: 3 });
  });

  it("does not schedule the same task twice within one round", async () => {
    let downstream = 0;
    const rt = new DriverRuntime(
      makeExecutor({
        root: () => ({ ...TASK_SUCCESS, patches: [], writes: [] }),
        left: () => ({ ...TASK_SUCCESS, patches: [], writes: [] }),
        right: () => ({ ...TASK_SUCCESS, patches: [], writes: [] }),
        join: () => {
          downstream += 1;
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
      }).executor,
    );
    rt.compileGraph({
      id: "g-round-dedupe",
      tasks: [
        { id: "root", kind: "effect", callbackId: "root" },
        { id: "left", kind: "effect", callbackId: "left" },
        { id: "right", kind: "effect", callbackId: "right" },
        { id: "join", kind: "effect", callbackId: "join", maxRuns: 5 },
      ],
      routes: [
        { event: "run", taskId: "root" },
        { event: "root:written", taskId: "left" },
        { event: "root:written", taskId: "right" },
        { event: "left:written", taskId: "join" },
        { event: "right:written", taskId: "join" },
      ],
    });
    await rt.run("g-round-dedupe", {}, { threadId: "t" });
    expect(downstream).toBe(1);
  });

  it("recursionLimit still bounds total handler executions across re-entry", async () => {
    const rt = new DriverRuntime(
      makeExecutor({
        model: () => ({ ...TASK_SUCCESS, patches: [], writes: [] }),
        tools: () => ({ ...TASK_SUCCESS, patches: [], writes: [] }),
      }).executor,
    );
    rt.compileGraph({
      id: "g-cycle-limit",
      tasks: [
        { id: "model", kind: "effect", callbackId: "model", maxRuns: 1000 },
        { id: "tools", kind: "effect", callbackId: "tools", maxRuns: 1000 },
      ],
      routes: [
        { event: "run", taskId: "model" },
        { event: "model:written", taskId: "tools" },
        { event: "tools:written", taskId: "model" },
      ],
    });
    await expect(
      rt.run("g-cycle-limit", {}, { threadId: "t", config: { recursionLimit: 7 } }),
    ).rejects.toThrow(/recursion/i);
  });

  it("routes follow-up events from the task's own emits list", async () => {
    const calls: string[] = [];
    const rt = new DriverRuntime(
      makeExecutor({
        classify: (input) => {
          calls.push("classify");
          const kind = (input as { kind?: string }).kind;
          return {
            ...TASK_SUCCESS,
            patches: [setPatch(["kind"], kind)],
            writes: ["kind"],
            emits: [kind === "spam" ? "spam:detected" : "spam:clear"],
          };
        },
        "handle-spam": () => {
          calls.push("handle-spam");
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
        "handle-clear": () => {
          calls.push("handle-clear");
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
      }).executor,
    );
    rt.compileGraph({
      id: "g-emits",
      tasks: [
        { id: "classify", kind: "effect", callbackId: "classify" },
        { id: "handle-spam", kind: "effect", callbackId: "handle-spam" },
        { id: "handle-clear", kind: "effect", callbackId: "handle-clear" },
      ],
      routes: [
        { event: "run", taskId: "classify" },
        { event: "spam:detected", taskId: "handle-spam" },
        { event: "spam:clear", taskId: "handle-clear" },
      ],
    });
    await rt.run("g-emits", { kind: "spam" }, { threadId: "t" });
    expect(calls).toEqual(["classify", "handle-spam"]);

    calls.length = 0;
    await rt.run("g-emits", { kind: "ham" }, { threadId: "t2" });
    expect(calls).toEqual(["classify", "handle-clear"]);
  });

  it("an empty emits list suppresses the implicit <taskId>:written event (loop stop)", async () => {
    const calls: string[] = [];
    const rt = new DriverRuntime(
      makeExecutor({
        model: (input) => {
          const n = (input as { n?: number }).n ?? 0;
          calls.push(`model:${n}`);
          const next = n + 1;
          return {
            ...TASK_SUCCESS,
            patches: [setPatch(["n"], next)],
            writes: ["n"],
            // 收敛判定在任务侧：写满 2 轮后不再发出下游事件。
            emits: next >= 2 ? [] : ["model:written"],
          };
        },
        tools: () => {
          calls.push("tools");
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
      }).executor,
    );
    rt.compileGraph({
      id: "g-emits-stop",
      tasks: [
        { id: "model", kind: "effect", callbackId: "model", maxRuns: 10 },
        { id: "tools", kind: "effect", callbackId: "tools", maxRuns: 10 },
      ],
      routes: [
        { event: "run", taskId: "model" },
        { event: "model:written", taskId: "tools" },
        { event: "tools:written", taskId: "model" },
      ],
    });
    const out = await rt.run("g-emits-stop", {}, { threadId: "t" });
    expect(calls).toEqual(["model:0", "tools", "model:1"]);
    expect(out.state).toEqual({ n: 2 });
  });

  it("re-checks the budget at dispatch so a pre-queued task never runs twice", async () => {
    // Regression: a task already dispatched in this round can be queued again
    // by a sibling that runs later in the same (serial) batch. The budget must
    // be re-checked when the next round is dispatched, not only when queued.
    const calls: string[] = [];
    const rt = new DriverRuntime(
      makeExecutor({
        a: () => {
          calls.push("a");
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
        b: () => {
          calls.push("b");
          return { ...TASK_SUCCESS, patches: [], writes: [] };
        },
      }).executor,
    );
    rt.compileGraph({
      id: "g-dispatch-recheck",
      tasks: [
        { id: "a", kind: "effect", callbackId: "a" },
        { id: "b", kind: "effect", callbackId: "b" },
      ],
      routes: [
        { event: "run", taskId: "a" },
        { event: "run", taskId: "b" },
        { event: "a:written", taskId: "b" },
      ],
    });
    await rt.run("g-dispatch-recheck", {}, { threadId: "t" });
    expect(calls).toEqual(["a", "b"]);
  });
});
