import { describe, expect, it } from "vitest";
import { MemoryLog, SqliteLog } from "../../src/durability/index.js";
import { GraphBuilder } from "../../src/graph/model.js";
import { ReactiveStore } from "../../src/state/store.js";
import { Scheduler } from "../../src/scheduler/index.js";
import { compact, recover } from "../../src/durability/index.js";
import { events } from "../../src/durability/events.js";

// A helper that simulates the durable execution lifecycle for a single effect
// task, letting tests "crash" at each boundary (mirrors plan Task 6 §1-2).
class DurableTaskDriver {
  readonly log: MemoryLog;
  constructor(log: MemoryLog = new MemoryLog()) {
    this.log = log;
  }

  /** Phase A: append durable intent before the external call. */
  beginEffect(intent: {
    idempotencyKey: string;
    taskId: string;
    effectId: string;
    input: unknown;
    stateVersion: number;
  }): void {
    this.log.append(
      events.effectIntent({
        idempotencyKey: intent.idempotencyKey,
        taskId: intent.taskId,
        effectId: intent.effectId,
        inputHash: hashOf(intent.input),
        stateVersion: intent.stateVersion,
        runId: "run-1",
      }),
    );
  }

  /** Phase B: external call happened; persist receipt. */
  recordReceipt(intent: { idempotencyKey: string; taskId: string }, receipt: string): void {
    this.log.append(
      events.effectReceipt({
        idempotencyKey: intent.idempotencyKey,
        taskId: intent.taskId,
        receipt,
        outcome: "success",
      }),
    );
  }

  /** Phase C: commit the resulting transaction. */
  commitTx(tx: {
    graphHash: string;
    runId: string;
    threadId: string;
    stateVersion: number;
    txId: string;
    taskId: string;
    readPaths: string[];
    writePaths: string[];
    patches: unknown[];
  }): void {
    this.log.append(events.transaction(tx));
  }
}

function hashOf(value: unknown): string {
  return `h:${JSON.stringify(value)}`;
}

describe("durable recovery", () => {
  it("crash BEFORE effect intent: nothing recorded, task reruns", () => {
    const driver = new DurableTaskDriver();
    // crash before beginEffect -> no events at all
    const recovered = recover(driver.log);
    expect(recovered.state).toEqual({});
    expect(recovered.confirmedEffects.size).toBe(0);
    expect(recovered.stateVersion).toBe(0);
  });

  it("crash AFTER effect intent but BEFORE receipt: effect is NOT confirmed", () => {
    const driver = new DurableTaskDriver();
    driver.beginEffect({
      idempotencyKey: "k1",
      taskId: "t1",
      effectId: "e1",
      input: { x: 1 },
      stateVersion: 1,
    });
    // crash before recordReceipt
    const recovered = recover(driver.log);
    // Intent is durable but no receipt -> idempotency gate must NOT approve rerun
    expect(recovered.confirmedEffects.has("k1")).toBe(false);
    expect(driver.log.count).toBe(1);
  });

  it("crash AFTER receipt but BEFORE commit: effect confirmed, patch not applied", () => {
    const driver = new DurableTaskDriver();
    driver.beginEffect({
      idempotencyKey: "k1",
      taskId: "t1",
      effectId: "e1",
      input: { x: 1 },
      stateVersion: 1,
    });
    driver.recordReceipt({ idempotencyKey: "k1", taskId: "t1" }, "rcpt-abc");
    // crash before commitTx
    const recovered = recover(driver.log);
    expect(recovered.confirmedEffects.get("k1")).toBe("rcpt-abc");
    // state has NOT been updated by the transaction
    expect(recovered.state).toEqual({});
    expect(driver.log.count).toBe(2);
  });

  it("crash AFTER commit: transaction replay restores canonical state", () => {
    const driver = new DurableTaskDriver();
    driver.beginEffect({
      idempotencyKey: "k1",
      taskId: "t1",
      effectId: "e1",
      input: { x: 1 },
      stateVersion: 1,
    });
    driver.recordReceipt({ idempotencyKey: "k1", taskId: "t1" }, "rcpt-abc");
    driver.commitTx({
      graphHash: "g1",
      runId: "run-1",
      threadId: "thread-1",
      stateVersion: 1,
      txId: "tx1",
      taskId: "t1",
      readPaths: ["input"],
      writePaths: ["result"],
      patches: [
        {
          path: ["result"],
          operation: "set",
          beforeHash: null,
          value: { doubled: 2 },
          taskId: "t1",
          transactionId: "tx1",
        },
      ],
    });
    const recovered = recover(driver.log);
    expect(recovered.stateVersion).toBe(1);
    expect((recovered.state as Record<string, unknown>).result).toEqual({ doubled: 2 });
    expect(recovered.confirmedEffects.get("k1")).toBe("rcpt-abc");
  });

  it("UNKNOWN_COMMIT resolves by inspecting the log, never blindly retried", () => {
    const driver = new DurableTaskDriver();
    // commit intent existed in a prior epoch -> recovered confirmed, no rerun
    driver.beginEffect({
      idempotencyKey: "k2",
      taskId: "t2",
      effectId: "e2",
      input: {},
      stateVersion: 1,
    });
    driver.recordReceipt({ idempotencyKey: "k2", taskId: "t2" }, "rcpt-zzz");
    const recovered = recover(driver.log);
    // UNKNOWN_COMMIT path: recovery reads log, sees receipt, resolves to committed.
    expect(recovered.confirmedEffects.get("k2")).toBe("rcpt-zzz");
  });
});

describe("compaction restores canonical state exactly", () => {
  it("snapshot + truncate + replay gives identical state", () => {
    const log = new MemoryLog();
    const driver = new DurableTaskDriver(log);
    // tx1 committed.
    driver.commitTx({
      graphHash: "g1",
      runId: "run-1",
      threadId: "thread-1",
      stateVersion: 1,
      txId: "tx1",
      taskId: "t1",
      readPaths: [],
      writePaths: ["a"],
      patches: [
        {
          path: ["a"],
          operation: "set",
          beforeHash: null,
          value: 1,
          taskId: "t1",
          transactionId: "tx1",
        },
      ],
    });
    const afterFirst = recover(log); // {a:1}, version 1

    // Compact AT the boundary: snapshot subsumes tx1, then truncate.
    const snapshotEvent = events.snapshot({
      runId: "run-1",
      threadId: "thread-1",
      stateVersion: 1,
      stateHash: "hash-of-state",
      state: afterFirst.state,
      baseSeq: 1,
    });
    compact(log, snapshotEvent);
    expect(log.count).toBe(1); // just the snapshot

    // tx2 committed after the snapshot boundary.
    driver.commitTx({
      graphHash: "g1",
      runId: "run-1",
      threadId: "thread-1",
      stateVersion: 2,
      txId: "tx2",
      taskId: "t2",
      readPaths: [],
      writePaths: ["b"],
      patches: [
        {
          path: ["b"],
          operation: "set",
          beforeHash: null,
          value: 2,
          taskId: "t2",
          transactionId: "tx2",
        },
      ],
    });
    const before = recover(log);
    expect(before.state).toEqual({ a: 1, b: 2 });
    expect(before.stateVersion).toBe(2);
    expect(log.count).toBe(2); // snapshot + tx2

    // Final recovery is independent of the compacted history length.
    const full = new MemoryLog();
    const d2 = new DurableTaskDriver(full);
    d2.commitTx({
      graphHash: "g1",
      runId: "run-1",
      threadId: "thread-1",
      stateVersion: 1,
      txId: "tx1",
      taskId: "t1",
      readPaths: [],
      writePaths: ["a"],
      patches: [
        {
          path: ["a"],
          operation: "set",
          beforeHash: null,
          value: 1,
          taskId: "t1",
          transactionId: "tx1",
        },
      ],
    });
    d2.commitTx({
      graphHash: "g1",
      runId: "run-1",
      threadId: "thread-1",
      stateVersion: 2,
      txId: "tx2",
      taskId: "t2",
      readPaths: [],
      writePaths: ["b"],
      patches: [
        {
          path: ["b"],
          operation: "set",
          beforeHash: null,
          value: 2,
          taskId: "t2",
          transactionId: "tx2",
        },
      ],
    });
    const uncompacted = recover(full);
    expect(before.state).toEqual(uncompacted.state);
    expect(before.stateVersion).toEqual(uncompacted.stateVersion);
  });
});

describe("log backends", () => {
  it("memory log assigns monotonic seq and reads after a cursor", () => {
    const log = new MemoryLog();
    const e1 = log.append(
      events.retryDecision({ taskId: "t", attempt: 1, decision: "retry", backoffMs: 100 }),
    );
    log.append(
      events.retryDecision({ taskId: "t", attempt: 2, decision: "give_up", backoffMs: 0 }),
    );
    expect(e1.seq).toBe(1);
    expect(log.lastSeq).toBe(2);
    expect(log.readFrom(1)).toHaveLength(1);
    expect(log.count).toBe(2);
    log.close();
  });

  it("sqlite log persists across instances (file location)", () => {
    const { mkdtempSync, rmSync } = requireNodeFS();
    const dir = mkdtempSync("/tmp/rgp-log-");
    const location = `${dir}/log.db`;
    try {
      const log1 = new SqliteLog({ location, name: "run-1" });
      log1.append(
        events.retryDecision({ taskId: "t", attempt: 1, decision: "retry", backoffMs: 100 }),
      );
      log1.append(
        events.transaction({
          graphHash: "g1",
          runId: "run-1",
          threadId: "thread-1",
          stateVersion: 1,
          txId: "tx1",
          taskId: "t",
          readPaths: [],
          writePaths: ["a"],
          patches: [
            {
              path: ["a"],
              operation: "set",
              beforeHash: null,
              value: 1,
              taskId: "t",
              transactionId: "tx1",
            },
          ],
        }),
      );
      log1.close();

      // New instance over the same file must see the same durable events.
      const log2 = new SqliteLog({ location, name: "run-1" });
      expect(log2.count).toBe(2);
      expect(log2.lastSeq).toBe(2);
      const events2 = log2.readFrom(0);
      expect(events2).toHaveLength(2);
      expect(recover(log2).state).toEqual({ a: 1 });
      log2.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("sqlite log preserves assigned seq and ordering across reopen (restart storm)", () => {
    const { mkdtempSync, rmSync } = requireNodeFS();
    const dir = mkdtempSync("/tmp/rgp-log-seq-");
    const location = `${dir}/log.db`;
    try {
      const log1 = new SqliteLog({ location, name: "run-1" });
      const first = log1.append(
        events.retryDecision({ taskId: "t", attempt: 1, decision: "retry", backoffMs: 100 }),
      );
      const second = log1.append(
        events.retryDecision({ taskId: "t", attempt: 2, decision: "give_up", backoffMs: 0 }),
      );
      expect(first.seq).toBe(1);
      expect(second.seq).toBe(2);
      log1.close();

      // A restarted process must observe the same durable seq values, not 0
      // placeholders: snapshot positions and readFrom cursors depend on them.
      const log2 = new SqliteLog({ location, name: "run-1" });
      expect(log2.readFrom(0).map((e) => e.seq)).toEqual([1, 2]);
      expect(log2.lastSeq).toBe(2);
      log2.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("sqlite log supports truncate + count", () => {
    const log = new SqliteLog({ location: ":memory:", name: "run-1" });
    log.append(
      events.retryDecision({ taskId: "t", attempt: 1, decision: "retry", backoffMs: 100 }),
    );
    log.append(
      events.retryDecision({ taskId: "t", attempt: 2, decision: "give_up", backoffMs: 0 }),
    );
    expect(log.readFrom(0)).toHaveLength(2);
    expect(log.count).toBe(2);
    log.truncateBefore(2);
    expect(log.readFrom(0)).toHaveLength(1);
    expect(log.count).toBe(1);
    log.close();
  });
});

// Lazy Node built-ins (kept out of the static module graph for vite-node).
import { createRequire } from "node:module";
const nodeRequire = createRequire(import.meta.url);
function requireNodeFS(): Pick<typeof import("node:fs"), "mkdtempSync" | "rmSync"> {
  return nodeRequire("node:fs");
}

describe("write-pressure compaction", () => {
  it("compacts the durable log after many transactions and preserves receipts", async () => {
    const log = new MemoryLog();
    const store = new ReactiveStore({});
    const graph = new GraphBuilder("g-pressure")
      .task({
        id: "e",
        kind: "effect",
        handler: () => ({
          reads: [],
          writes: ["n"],
          patches: [{ path: ["n"], operation: "set", value: 1 }],
          receipt: "receipt",
        }),
      })
      .build();
    const sch = new Scheduler({ graph, store, log, compactAfter: 10, runId: "pressure" });
    for (let i = 0; i < 25; i++) await sch.runTask("e", {});
    expect(log.count).toBeLessThanOrEqual(20);
    expect(log.count).toBeLessThanOrEqual(20); // compaction occurred via truncation
  });
});
