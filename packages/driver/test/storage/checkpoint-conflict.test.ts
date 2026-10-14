import { afterEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CheckpointConflictError,
  MemoryCheckpointSaver,
  SqliteCheckpointSaver,
  type CheckpointRecord,
  type CheckpointSaver,
} from "../../src/index.js";

const tmpDirs: string[] = [];
function sqliteSaver(): CheckpointSaver {
  const dir = mkdtempSync(join(tmpdir(), "rgp-cp-"));
  tmpDirs.push(dir);
  return new SqliteCheckpointSaver(join(dir, "cp.db"));
}

afterEach(() => {
  for (const dir of tmpDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function cp(threadId: string, checkpointId: string, parent?: string): CheckpointRecord {
  return {
    threadId,
    checkpointId,
    parentCheckpointId: parent,
    ts: new Date().toISOString(),
    values: new Uint8Array([1]),
    metadata: new Uint8Array(),
  };
}

describe.each([
  ["memory", () => new MemoryCheckpointSaver()],
  ["sqlite", sqliteSaver],
] as const)("checkpoint optimistic concurrency (%s)", (_name, make) => {
  it("accepts a chain derived from the latest checkpoint", async () => {
    const saver = make();
    await saver.put(cp("t", "cp0"));
    await saver.put({ ...cp("t", "cp1"), expectedParentCheckpointId: "cp0" });
    await saver.put({ ...cp("t", "cp2"), expectedParentCheckpointId: "cp1" });
    const latest = await saver.get("t");
    expect(latest?.checkpointId).toBe("cp2");
    expect(latest?.parentCheckpointId).toBe("cp1");
  });

  it("rejects a write based on a stale parent (multi-Driver lost-update guard)", async () => {
    // Two Driver instances share one durable saver; both derived from cp0.
    const saver = make();
    await saver.put(cp("t", "cp0"));
    // Instance A advances the thread.
    await saver.put({ ...cp("t", "cp1"), expectedParentCheckpointId: "cp0" });
    // Instance B still thinks it is based on cp0 → its write must be rejected.
    await expect(
      saver.put({ ...cp("t", "cp1b"), expectedParentCheckpointId: "cp0" }),
    ).rejects.toThrow(CheckpointConflictError);
    const latest = await saver.get("t");
    expect(latest?.checkpointId).toBe("cp1");
  });

  it("updates the parent link when only expectedParent is provided", async () => {
    const saver = make();
    await saver.put(cp("t", "cp0"));
    await saver.put({ ...cp("t", "cp1"), expectedParentCheckpointId: "cp0" });
    expect((await saver.get("t"))?.parentCheckpointId).toBe("cp0");
  });

  it("keeps unconditional writes backwards compatible", async () => {
    const saver = make();
    await saver.put(cp("t", "cp0"));
    await saver.put(cp("t", "cp1"));
    // no condition → overwrite allowed (old behaviour); give it a later ts so
    // the sqlite backend's "latest by ts" ordering is deterministic.
    await saver.put({ ...cp("t", "cp0b"), ts: new Date(Date.now() + 1000).toISOString() });
    expect((await saver.get("t"))?.checkpointId).toBe("cp0b");
  });
});
