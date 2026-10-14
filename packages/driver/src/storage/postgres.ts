/**
 * PostgreSQL durable backends (M3-F8).
 *
 * Checkpoint saver / long-term store / cache over a shared Postgres, mirroring
 * the sqlite schema (rgp_checkpoints + rgp_checkpoint_writes for atomic lineage,
 * rgp_store_items with namespace prefix search, rgp_cache_entries with TTL). A shared
 * Postgres gives multi-Driver durability: any Driver instance pointed at the
 * same database sees the same threads and rgp_checkpoints.
 *
 * Values travel as BYTEA (cross-language serializer payloads are opaque).
 */

import pg from "pg";
import { isPrefix, nsKey } from "./ns.js";
import { serializer } from "./serializer.js";
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
} from "./types.js";
import { CheckpointConflictError } from "./types.js";

const { Pool } = pg;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS rgp_checkpoints (
  thread_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  parent_checkpoint_id TEXT,
  ts TEXT NOT NULL,
  state_values BYTEA NOT NULL,
  metadata BYTEA NOT NULL,
  PRIMARY KEY (thread_id, checkpoint_id)
);
CREATE TABLE IF NOT EXISTS rgp_checkpoint_writes (
  thread_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  value BYTEA NOT NULL,
  PRIMARY KEY (thread_id, checkpoint_id, task_id, channel)
);
CREATE TABLE IF NOT EXISTS rgp_store_items (
  namespace TEXT NOT NULL,
  key TEXT NOT NULL,
  value BYTEA NOT NULL,
  created_at TEXT,
  updated_at TEXT,
  PRIMARY KEY (namespace, key)
);
CREATE TABLE IF NOT EXISTS rgp_cache_entries (
  cache_key TEXT PRIMARY KEY,
  value BYTEA NOT NULL,
  expires_at BIGINT
);
`;

function toBytes(value: Uint8Array | Buffer | null | undefined): Buffer {
  return Buffer.from(value ?? new Uint8Array());
}

function toWrites(rows: { task_id: string; channel: string; value: Buffer }[]): CheckpointWrite[] {
  return rows.map((w) => ({
    taskId: w.task_id,
    channel: w.channel,
    value: new Uint8Array(w.value),
  }));
}

async function withSchema(client: pg.Pool): Promise<void> {
  await client.query(SCHEMA);
}

export class PostgresCheckpointSaver implements CheckpointSaver {
  private readonly pool: pg.Pool;
  #ready: Promise<void> | null = null;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async init(): Promise<void> {
    await withSchema(this.pool);
  }

  #ensure(): Promise<void> {
    this.#ready ??= withSchema(this.pool);
    return this.#ready;
  }

  async get(threadId: string, checkpointId?: string): Promise<CheckpointRecord | null> {
    await this.#ensure();
    const res = checkpointId
      ? await this.pool.query(
          "SELECT * FROM rgp_checkpoints WHERE thread_id = $1 AND checkpoint_id = $2",
          [threadId, checkpointId],
        )
      : await this.pool.query(
          "SELECT * FROM rgp_checkpoints WHERE thread_id = $1 ORDER BY ts DESC, checkpoint_id DESC LIMIT 1",
          [threadId],
        );
    const row = res.rows[0] as
      | {
          thread_id: string;
          checkpoint_id: string;
          parent_checkpoint_id: string | null;
          ts: string;
          state_values: Buffer;
          metadata: Buffer;
        }
      | undefined;
    if (!row) return null;
    const writesRes = await this.pool.query(
      "SELECT task_id, channel, value FROM rgp_checkpoint_writes WHERE thread_id = $1 AND checkpoint_id = $2",
      [threadId, row.checkpoint_id],
    );
    const record: CheckpointRecord = {
      threadId: row.thread_id,
      checkpointId: row.checkpoint_id,
      parentCheckpointId: row.parent_checkpoint_id ?? undefined,
      ts: row.ts,
      values: new Uint8Array(row.state_values),
      metadata: new Uint8Array(row.metadata),
    };
    const writes = toWrites(
      writesRes.rows as { task_id: string; channel: string; value: Buffer }[],
    );
    if (writes.length > 0) {
      return { ...record, writes };
    }
    return record;
  }

  async list(threadId: string, opts: ListCheckpointsOptions = {}): Promise<CheckpointRecord[]> {
    await this.#ensure();
    const res = await this.pool.query(
      "SELECT * FROM rgp_checkpoints WHERE thread_id = $1 ORDER BY ts ASC, checkpoint_id ASC",
      [threadId],
    );
    let records = res.rows.map(
      (r) =>
        ({
          threadId: r.thread_id,
          checkpointId: r.checkpoint_id,
          parentCheckpointId: r.parent_checkpoint_id ?? undefined,
          ts: r.ts,
          values: new Uint8Array(r.state_values),
          metadata: new Uint8Array(r.metadata),
        }) as CheckpointRecord,
    );
    if (opts.before) {
      const idx = records.findIndex((r) => r.checkpointId === opts.before);
      if (idx >= 0) records = records.slice(0, idx);
    }
    if (opts.limit !== undefined) records = records.slice(-opts.limit);
    return records;
  }

  async put(record: CheckpointRecord): Promise<void> {
    await this.#ensure();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      if (record.expectedParentCheckpointId !== undefined) {
        const { rows } = await client.query(
          `SELECT checkpoint_id FROM rgp_checkpoints
           WHERE thread_id = $1
           ORDER BY ts DESC, checkpoint_id DESC
           LIMIT 1
           FOR UPDATE`,
          [record.threadId],
        );
        const latest = rows[0]?.checkpoint_id as string | undefined;
        if (latest !== undefined && latest !== record.expectedParentCheckpointId) {
          await client.query("ROLLBACK");
          throw new CheckpointConflictError(
            record.threadId,
            latest,
            record.expectedParentCheckpointId,
          );
        }
      }
      await client.query(
        `INSERT INTO rgp_checkpoints (thread_id, checkpoint_id, parent_checkpoint_id, ts, state_values, metadata)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (thread_id, checkpoint_id) DO UPDATE SET
           parent_checkpoint_id = EXCLUDED.parent_checkpoint_id,
           ts = EXCLUDED.ts,
           state_values = EXCLUDED.state_values,
           metadata = EXCLUDED.metadata`,
        [
          record.threadId,
          record.checkpointId,
          record.parentCheckpointId ?? record.expectedParentCheckpointId ?? null,
          record.ts,
          toBytes(record.values),
          toBytes(record.metadata),
        ],
      );
      await client.query("COMMIT");
    } catch (err) {
      if (err instanceof CheckpointConflictError) {
        throw err;
      }
      await client.query("ROLLBACK").catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  async putWrites(
    threadId: string,
    checkpointId: string,
    writes: readonly CheckpointWrite[],
  ): Promise<void> {
    await this.#ensure();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      for (const w of writes) {
        await client.query(
          `INSERT INTO rgp_checkpoint_writes (thread_id, checkpoint_id, task_id, channel, value)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (thread_id, checkpoint_id, task_id, channel) DO UPDATE SET value = EXCLUDED.value`,
          [threadId, checkpointId, w.taskId, w.channel, toBytes(w.value)],
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async deleteThread(threadId: string): Promise<void> {
    await this.#ensure();
    await this.pool.query("DELETE FROM rgp_checkpoints WHERE thread_id = $1", [threadId]);
    await this.pool.query("DELETE FROM rgp_checkpoint_writes WHERE thread_id = $1", [threadId]);
  }

  async deleteForRuns(threadIds: readonly string[]): Promise<void> {
    await this.#ensure();
    for (const id of threadIds) await this.deleteThread(id);
  }

  async copy(fromThreadId: string, toThreadId: string): Promise<void> {
    await this.#ensure();
    await this.pool.query(
      `INSERT INTO rgp_checkpoints (thread_id, checkpoint_id, parent_checkpoint_id, ts, state_values, metadata)
       SELECT $1, checkpoint_id, parent_checkpoint_id, ts, state_values, metadata FROM rgp_checkpoints
       WHERE thread_id = $2
       ON CONFLICT (thread_id, checkpoint_id) DO UPDATE SET state_values = EXCLUDED.state_values`,
      [toThreadId, fromThreadId],
    );
  }

  async prune(threadId: string, keep: number): Promise<void> {
    await this.#ensure();
    const all = await this.list(threadId);
    const toDelete = all.slice(0, Math.max(0, all.length - keep));
    for (const r of toDelete) {
      await this.pool.query(
        "DELETE FROM rgp_checkpoints WHERE thread_id = $1 AND checkpoint_id = $2",
        [threadId, r.checkpointId],
      );
      await this.pool.query(
        "DELETE FROM rgp_checkpoint_writes WHERE thread_id = $1 AND checkpoint_id = $2",
        [threadId, r.checkpointId],
      );
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

export class PostgresStore implements LongTermStore {
  private readonly pool: pg.Pool;
  #ready: Promise<void> | null = null;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async init(): Promise<void> {
    await withSchema(this.pool);
  }

  #ensure(): Promise<void> {
    this.#ready ??= withSchema(this.pool);
    return this.#ready;
  }

  async get(namespace: readonly string[], key: string): Promise<StoreItem | null> {
    await this.#ensure();
    const res = await this.pool.query(
      "SELECT * FROM rgp_store_items WHERE namespace = $1 AND key = $2",
      [nsKey(namespace), key],
    );
    const row = res.rows[0] as Record<string, unknown> | undefined;
    if (!row) return null;
    return this.#rowToItem(row);
  }

  async put(item: StoreItem): Promise<void> {
    await this.#ensure();
    await this.pool.query(
      `INSERT INTO rgp_store_items (namespace, key, value, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (namespace, key) DO UPDATE SET
         value = EXCLUDED.value, updated_at = EXCLUDED.updated_at`,
      [
        nsKey(item.namespace),
        item.key,
        toBytes(item.value),
        item.createdAt ?? new Date().toISOString(),
        item.updatedAt ?? new Date().toISOString(),
      ],
    );
  }

  async search(namespace: readonly string[], opts: StoreSearchOptions = {}): Promise<StoreItem[]> {
    await this.#ensure();
    const rows = await this.pool.query("SELECT * FROM rgp_store_items ORDER BY namespace, key");
    let items = (rows.rows as Record<string, unknown>[]).map((r) => this.#rowToItem(r));
    items = items.filter((i) => isPrefix(namespace, i.namespace));
    if (opts.filter) {
      for (const [k, v] of opts.filter) {
        items = items.filter((i) => this.#matches(i, k, v));
      }
    }
    const offset = opts.offset ?? 0;
    const limit = opts.limit ?? items.length;
    return items.slice(offset, offset + limit);
  }

  async delete(namespace: readonly string[], key: string): Promise<void> {
    await this.#ensure();
    await this.pool.query("DELETE FROM rgp_store_items WHERE namespace = $1 AND key = $2", [
      nsKey(namespace),
      key,
    ]);
  }

  async listNamespaces(prefix?: readonly string[]): Promise<readonly (readonly string[])[]> {
    const res = await this.pool.query("SELECT DISTINCT namespace FROM rgp_store_items");
    const seen = new Set<string>();
    const out: string[][] = [];
    for (const row of res.rows as { namespace: string }[]) {
      const segs = row.namespace.split("/").filter((s) => s.length > 0);
      if (prefix && !isPrefix(prefix, segs)) continue;
      const key = segs.join("/");
      if (!seen.has(key)) {
        seen.add(key);
        out.push(segs);
      }
    }
    return out;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  #rowToItem(row: Record<string, unknown>): StoreItem {
    const namespace = typeof row["namespace"] === "string" ? row["namespace"] : "";
    return {
      namespace: namespace.split("/").filter((s) => s.length > 0),
      key: row["key"] as string,
      value: new Uint8Array(row["value"] as Buffer),
      createdAt: (row["created_at"] as string | null | undefined) ?? undefined,
      updatedAt: (row["updated_at"] as string | null | undefined) ?? undefined,
    };
  }

  #matches(item: StoreItem, key: string, expected: unknown): boolean {
    // Values are opaque serialized bytes; decode with the canonical
    // serializer (same semantics as the memory store) and compare the field.
    const value = serializer.loads(item.value) as Record<string, unknown>;
    return value[key] === expected;
  }
}

export class PostgresCache implements Cache {
  private readonly pool: pg.Pool;
  #ready: Promise<void> | null = null;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async init(): Promise<void> {
    await withSchema(this.pool);
  }

  #ensure(): Promise<void> {
    this.#ready ??= withSchema(this.pool);
    return this.#ready;
  }

  async get(key: string): Promise<CacheEntry | null> {
    await this.#ensure();
    const res = await this.pool.query(
      "SELECT value, expires_at FROM rgp_cache_entries WHERE cache_key = $1",
      [key],
    );
    const row = res.rows[0] as { value: Buffer; expires_at: string | null } | undefined;
    if (!row) return null;
    const expiresAt = row.expires_at ? Number(row.expires_at) : undefined;
    if (expiresAt !== undefined && expiresAt <= Date.now()) {
      await this.pool.query("DELETE FROM rgp_cache_entries WHERE cache_key = $1", [key]);
      return null;
    }
    return { value: new Uint8Array(row.value), expiresAt };
  }

  async set(key: string, entry: CacheEntry): Promise<void> {
    await this.#ensure();
    await this.pool.query(
      `INSERT INTO rgp_cache_entries (cache_key, value, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (cache_key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at`,
      [key, toBytes(entry.value), entry.expiresAt ?? null],
    );
  }

  async delete(key: string): Promise<void> {
    await this.#ensure();
    await this.pool.query("DELETE FROM rgp_cache_entries WHERE cache_key = $1", [key]);
  }

  async has(key: string): Promise<boolean> {
    await this.#ensure();
    return (await this.get(key)) !== null;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
