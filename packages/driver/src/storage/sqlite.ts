/**
 * SQLite storage backends (Task 9). Schema-versioned with protected migrations;
 * durable backends provide atomic checkpoint + write behavior (single txn).
 */

import type { DatabaseSync as DatabaseSyncT } from "node:sqlite";
import type {
  Cache,
  CacheEntry,
  CheckpointRecord,
  CheckpointSaver,
  CheckpointWrite,
  ListCheckpointsOptions,
  LongTermStore,
  StoreItem,
  StoreSearchOptions,
  VectorPoint,
  VectorSearchOptions,
  VectorSearchResult,
  VectorStore,
  VersionedStorage,
} from "./types.js";
import { CURRENT_SCHEMA_VERSION, assertCheckpointParent } from "./types.js";
import { serializer } from "./serializer.js";
import { isPrefix, nsKey } from "./ns.js";
import { cosineSimilarity } from "./vector.js";
import { loadDatabaseSync } from "../node-sqlite.js";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS rgp_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
  thread_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  parent_checkpoint_id TEXT,
  ts TEXT NOT NULL,
  state_values BLOB NOT NULL,
  metadata BLOB NOT NULL,
  PRIMARY KEY (thread_id, checkpoint_id)
);
CREATE TABLE IF NOT EXISTS checkpoint_writes (
  thread_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY (thread_id, checkpoint_id, task_id, channel)
);
CREATE TABLE IF NOT EXISTS store_items (
  namespace TEXT NOT NULL,
  key TEXT NOT NULL,
  value BLOB NOT NULL,
  created_at TEXT,
  updated_at TEXT,
  PRIMARY KEY (namespace, key)
);
CREATE TABLE IF NOT EXISTS cache_entries (
  cache_key TEXT PRIMARY KEY,
  value BLOB NOT NULL,
  expires_at INTEGER
);
CREATE TABLE IF NOT EXISTS vector_points (
  namespace TEXT NOT NULL,
  id TEXT NOT NULL,
  vector BLOB NOT NULL,
  metadata BLOB,
  PRIMARY KEY (namespace, id)
);
`;

/** Base class providing schema bootstrap + version checks. */
export class SqliteBase implements VersionedStorage {
  protected readonly db: DatabaseSyncT;
  readonly schemaVersion: number;

  constructor(db: DatabaseSyncT) {
    this.db = db;
    db.exec("PRAGMA journal_mode=WAL;");
    db.exec(SCHEMA);
    const row = db.prepare("SELECT value FROM rgp_meta WHERE key = 'schema_version'").get() as
      { value: string } | undefined;
    this.schemaVersion = row ? Number(row.value) : CURRENT_SCHEMA_VERSION;
    if (this.schemaVersion > CURRENT_SCHEMA_VERSION) {
      throw new Error(
        `storage schema ${this.schemaVersion} is newer than driver supports ${CURRENT_SCHEMA_VERSION}`,
      );
    }
    if (!row) {
      db.prepare("INSERT INTO rgp_meta (key, value) VALUES ('schema_version', ?)").run(
        String(CURRENT_SCHEMA_VERSION),
      );
    }
  }

  migrate(targetVersion: number): void {
    if (targetVersion < this.schemaVersion) throw new Error("cannot migrate backwards");
    this.db.exec("BEGIN");
    try {
      // Future migrations extend this. v1 is the initial schema.
      this.db
        .prepare("INSERT OR REPLACE INTO rgp_meta (key, value) VALUES ('schema_version', ?)")
        .run(String(targetVersion));
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }
}

export class SqliteCheckpointSaver extends SqliteBase implements CheckpointSaver {
  constructor(location: string | DatabaseSyncT) {
    super(typeof location === "string" ? new (loadDatabaseSync())(location) : location);
  }

  async get(threadId: string, checkpointId?: string): Promise<CheckpointRecord | null> {
    let row: Record<string, unknown> | undefined;
    if (checkpointId === undefined) {
      row = this.db
        .prepare(
          "SELECT * FROM checkpoints WHERE thread_id = ? ORDER BY ts DESC, checkpoint_id DESC LIMIT 1",
        )
        .get(threadId) as Record<string, unknown> | undefined;
    } else {
      row = this.db
        .prepare("SELECT * FROM checkpoints WHERE thread_id = ? AND checkpoint_id = ?")
        .get(threadId, checkpointId) as Record<string, unknown> | undefined;
    }
    if (!row) return null;
    const rec = rowToRecord(row);
    const writes = this.db
      .prepare(
        "SELECT task_id, channel, value FROM checkpoint_writes WHERE thread_id = ? AND checkpoint_id = ?",
      )
      .all(threadId, rec.checkpointId) as { task_id: string; channel: string; value: Uint8Array }[];
    if (writes.length > 0) {
      return {
        ...rec,
        writes: writes.map((w) => ({
          taskId: w.task_id,
          channel: w.channel,
          value: new Uint8Array(w.value),
        })),
      };
    }
    return rec;
  }

  async list(threadId: string, opts: ListCheckpointsOptions = {}): Promise<CheckpointRecord[]> {
    const rows = this.db
      .prepare("SELECT * FROM checkpoints WHERE thread_id = ? ORDER BY ts ASC, checkpoint_id ASC")
      .all(threadId) as Record<string, unknown>[];
    let records = rows.map(rowToRecord);
    if (opts.before) {
      const idx = records.findIndex((r) => r.checkpointId === opts.before);
      if (idx >= 0) records = records.slice(0, idx);
    }
    if (opts.limit !== undefined) records = records.slice(-(opts.limit as number));
    return records;
  }

  async put(record: CheckpointRecord): Promise<void> {
    // 并发安全：BEGIN 前完成所有 await（realworld Task 5 并发线程测试暴露：
    // 原实现在 BEGIN 后 await get → 其他 run 的 put 可交错进入 → node:sqlite
    // 报 "cannot start a transaction within a transaction"）。事务体内保持
    // 纯同步段，保证 BEGIN…COMMIT 原子不可打断。
    if (record.expectedParentCheckpointId !== undefined) {
      assertCheckpointParent(await this.get(record.threadId), record);
    }
    this.db.exec("BEGIN");
    try {
      this.db
        .prepare(
          `INSERT OR REPLACE INTO checkpoints
           (thread_id, checkpoint_id, parent_checkpoint_id, ts, state_values, metadata)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(
          record.threadId,
          record.checkpointId,
          record.parentCheckpointId ?? record.expectedParentCheckpointId ?? null,
          record.ts,
          Buffer.from(record.values),
          Buffer.from(record.metadata),
        );
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  async putWrites(
    threadId: string,
    checkpointId: string,
    writes: readonly CheckpointWrite[],
  ): Promise<void> {
    this.db.exec("BEGIN");
    try {
      for (const w of writes) {
        this.db
          .prepare(
            `INSERT OR REPLACE INTO checkpoint_writes (thread_id, checkpoint_id, task_id, channel, value)
             VALUES (?, ?, ?, ?, ?)`,
          )
          .run(threadId, checkpointId, w.taskId, w.channel, Buffer.from(w.value));
      }
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  async deleteThread(threadId: string): Promise<void> {
    this.db.exec("BEGIN");
    try {
      this.db.prepare("DELETE FROM checkpoints WHERE thread_id = ?").run(threadId);
      this.db.prepare("DELETE FROM checkpoint_writes WHERE thread_id = ?").run(threadId);
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  async deleteForRuns(threadIds: readonly string[]): Promise<void> {
    this.db.exec("BEGIN");
    try {
      const stmt = this.db.prepare("DELETE FROM checkpoints WHERE thread_id = ?");
      const stmtW = this.db.prepare("DELETE FROM checkpoint_writes WHERE thread_id = ?");
      for (const id of threadIds) {
        stmt.run(id);
        stmtW.run(id);
      }
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  async copy(fromThreadId: string, toThreadId: string): Promise<void> {
    this.db.exec("BEGIN");
    try {
      const rows = this.db
        .prepare("SELECT * FROM checkpoints WHERE thread_id = ?")
        .all(fromThreadId) as Record<string, unknown>[];
      for (const row of rows) {
        this.db
          .prepare(
            `INSERT OR REPLACE INTO checkpoints
             (thread_id, checkpoint_id, parent_checkpoint_id, ts, state_values, metadata)
             VALUES (?, ?, ?, ?, ?, ?)`,
          )
          .run(
            toThreadId,
            String(row.checkpoint_id),
            row.parent_checkpoint_id == null ? null : String(row.parent_checkpoint_id),
            String(row.ts),
            row.state_values as Uint8Array,
            row.metadata as Uint8Array,
          );
      }
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  async prune(threadId: string, keep: number): Promise<void> {
    this.db.exec("BEGIN");
    try {
      const all = await this.list(threadId);
      const toDelete = all.slice(0, Math.max(0, all.length - keep));
      const stmt = this.db.prepare(
        "DELETE FROM checkpoints WHERE thread_id = ? AND checkpoint_id = ?",
      );
      for (const r of toDelete) stmt.run(threadId, r.checkpointId);
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  close(): void {
    this.db.close();
  }
}

export class SqliteStore extends SqliteBase implements LongTermStore {
  constructor(location: string | DatabaseSyncT) {
    super(typeof location === "string" ? new (loadDatabaseSync())(location) : location);
  }

  async get(namespace: readonly string[], key: string): Promise<StoreItem | null> {
    const row = this.db
      .prepare("SELECT * FROM store_items WHERE namespace = ? AND key = ?")
      .get(nsKey(namespace), key) as Record<string, unknown> | undefined;
    return row ? rowToItem(row) : null;
  }

  async put(item: StoreItem): Promise<void> {
    this.db
      .prepare(
        `INSERT OR REPLACE INTO store_items (namespace, key, value, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?)`,
      )
      .run(
        nsKey(item.namespace),
        item.key,
        Buffer.from(item.value),
        item.createdAt ?? null,
        item.updatedAt ?? new Date().toISOString(),
      );
  }

  async search(namespace: readonly string[], opts: StoreSearchOptions = {}): Promise<StoreItem[]> {
    // nsKey may contain LIKE metacharacters (%, _, /); escape them so the
    // prefix match stays exact, matching MemoryStore's segment-wise isPrefix.
    const key = nsKey(namespace);
    const escaped = key.replace(/[\\%_]/g, (m) => `\\${m}`);
    const rows = this.db
      .prepare("SELECT * FROM store_items WHERE namespace = ? OR namespace LIKE ? ESCAPE '\\'")
      .all(key, `${escaped}/%`) as Record<string, unknown>[];
    let items = rows.map(rowToItem);
    if (opts.filter) {
      for (const [k, v] of opts.filter) {
        items = items.filter((i) => {
          const decoded = serializer.loads(i.value) as Record<string, unknown>;
          return decoded[k] === v;
        });
      }
    }
    const offset = opts.offset ?? 0;
    const limit = opts.limit ?? items.length;
    return items.slice(offset, offset + limit);
  }

  async delete(namespace: readonly string[], key: string): Promise<void> {
    this.db
      .prepare("DELETE FROM store_items WHERE namespace = ? AND key = ?")
      .run(nsKey(namespace), key);
  }

  async listNamespaces(prefix?: readonly string[]): Promise<readonly (readonly string[])[]> {
    const rows = this.db.prepare("SELECT DISTINCT namespace FROM store_items").all() as {
      namespace: string;
    }[];
    const seen = new Set<string>();
    for (const row of rows) {
      const ns = row.namespace.split("/");
      if (prefix && !isPrefix(prefix, ns)) continue;
      seen.add(row.namespace);
    }
    return [...seen].map((s) => s.split("/"));
  }

  close(): void {
    this.db.close();
  }
}

export class SqliteCache extends SqliteBase implements Cache {
  constructor(location: string | DatabaseSyncT) {
    super(typeof location === "string" ? new (loadDatabaseSync())(location) : location);
  }

  async get(key: string): Promise<CacheEntry | null> {
    const row = this.db.prepare("SELECT * FROM cache_entries WHERE cache_key = ?").get(key) as
      Record<string, unknown> | undefined;
    if (!row) return null;
    const expiresAt = row.expires_at === null ? undefined : Number(row.expires_at);
    if (expiresAt !== undefined && expiresAt !== 0 && expiresAt <= Date.now()) {
      this.delete(key);
      return null;
    }
    return { value: new Uint8Array(row.value as Uint8Array), expiresAt };
  }

  async set(key: string, entry: CacheEntry): Promise<void> {
    this.db
      .prepare(
        "INSERT OR REPLACE INTO cache_entries (cache_key, value, expires_at) VALUES (?, ?, ?)",
      )
      .run(key, Buffer.from(entry.value), entry.expiresAt ?? null);
  }

  async delete(key: string): Promise<void> {
    this.db.prepare("DELETE FROM cache_entries WHERE cache_key = ?").run(key);
  }

  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== null;
  }

  close(): void {
    this.db.close();
  }
}

function rowToRecord(row: Record<string, unknown>): CheckpointRecord {
  return {
    threadId: row.thread_id as string,
    checkpointId: row.checkpoint_id as string,
    parentCheckpointId: (row.parent_checkpoint_id as string | null) ?? undefined,
    ts: row.ts as string,
    values: new Uint8Array(row.state_values as Uint8Array),
    metadata: new Uint8Array(row.metadata as Uint8Array),
  };
}

function rowToItem(row: Record<string, unknown>): StoreItem {
  return {
    namespace: (row.namespace as string).split("/"),
    key: row.key as string,
    value: new Uint8Array(row.value as Uint8Array),
    createdAt: (row.created_at as string | null) ?? undefined,
    updatedAt: (row.updated_at as string | null) ?? undefined,
  };
}

/** SQLite-backed vector store: linear-scan cosine search over vector_points. */
export class SqliteVectorStore extends SqliteBase implements VectorStore {
  constructor(location: string | DatabaseSyncT) {
    super(typeof location === "string" ? new (loadDatabaseSync())(location) : location);
  }

  async upsert(point: VectorPoint): Promise<void> {
    this.db
      .prepare(
        `INSERT INTO vector_points (namespace, id, vector, metadata)
         VALUES (?, ?, ?, ?)
         ON CONFLICT (namespace, id) DO UPDATE SET vector = excluded.vector, metadata = excluded.metadata`,
      )
      .run(
        nsKey(point.namespace),
        point.id,
        serializer.dumps(point.vector),
        point.metadata === undefined ? null : serializer.dumps(point.metadata),
      );
  }

  async search(
    namespace: readonly string[],
    query: number[],
    options: VectorSearchOptions = {},
  ): Promise<VectorSearchResult[]> {
    const prefix = nsKey(namespace);
    const rows = this.db
      .prepare(
        "SELECT id, vector, metadata FROM vector_points WHERE namespace = ? OR namespace LIKE ?",
      )
      .all(prefix, `${prefix}/%`) as Record<string, unknown>[];
    const scored = rows.map((row) => {
      const vector = serializer.loads(row.vector as Uint8Array) as number[];
      return {
        id: row.id as string,
        score: cosineSimilarity(vector, query),
        metadata: row.metadata == null ? undefined : serializer.loads(row.metadata as Uint8Array),
      };
    });
    return scored
      .filter((r) => r.score >= (options.minScore ?? 0))
      .sort((a, b) => b.score - a.score)
      .slice(0, options.limit ?? scored.length)
      .map((r) => ({ id: r.id, score: r.score, metadata: r.metadata }));
  }

  async delete(namespace: readonly string[], id: string): Promise<void> {
    this.db
      .prepare("DELETE FROM vector_points WHERE namespace = ? AND id = ?")
      .run(nsKey(namespace), id);
  }
}
