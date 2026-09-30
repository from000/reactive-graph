/**
 * Memory storage backends (Task 9). Atomic within a single synchronous
 * operation; for durable semantics use SqliteCheckpointSaver.
 */

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
} from "./types.js";
import { assertCheckpointParent } from "./types.js";
import { serializer } from "./serializer.js";
import { isPrefix, nsKey } from "./ns.js";
import { cosineSimilarity } from "./vector.js";

export class MemoryCheckpointSaver implements CheckpointSaver {
  private readonly byThread = new Map<string, CheckpointRecord[]>();

  /** Number of threads with at least one retained checkpoint. */
  get threadCount(): number {
    return this.byThread.size;
  }

  /** Whether this saver already holds checkpoints for `threadId`. */
  hasThread(threadId: string): boolean {
    return this.byThread.has(threadId);
  }

  async get(threadId: string, checkpointId?: string): Promise<CheckpointRecord | null> {
    const list = this.byThread.get(threadId) ?? [];
    if (checkpointId === undefined) return list[list.length - 1] ?? null;
    return list.find((r) => r.checkpointId === checkpointId) ?? null;
  }

  async list(threadId: string, opts: ListCheckpointsOptions = {}): Promise<CheckpointRecord[]> {
    let list = [...(this.byThread.get(threadId) ?? [])];
    if (opts.before) {
      const idx = list.findIndex((r) => r.checkpointId === opts.before);
      if (idx >= 0) list = list.slice(0, idx);
    }
    if (opts.limit !== undefined) list = list.slice(-(opts.limit as number));
    return list;
  }

  async put(record: CheckpointRecord): Promise<void> {
    if (record.expectedParentCheckpointId !== undefined) {
      assertCheckpointParent(await this.get(record.threadId), record);
    }
    const { expectedParentCheckpointId: _expected, parentCheckpointId: _parent, ...rest } = record;
    const stored: CheckpointRecord = {
      ...rest,
      parentCheckpointId: record.parentCheckpointId ?? record.expectedParentCheckpointId,
    };
    const list = this.byThread.get(record.threadId) ?? [];
    const idx = list.findIndex((r) => r.checkpointId === record.checkpointId);
    if (idx >= 0) {
      list[idx] = stored;
    } else {
      list.push(stored);
    }
    this.byThread.set(record.threadId, list);
  }

  async putWrites(
    threadId: string,
    checkpointId: string,
    writes: readonly CheckpointWrite[],
  ): Promise<void> {
    const rec = await this.get(threadId, checkpointId);
    if (!rec) return;
    const merged = { ...rec, writes: [...(rec.writes ?? []), ...writes] };
    await this.put(merged);
  }

  async deleteThread(threadId: string): Promise<void> {
    this.byThread.delete(threadId);
  }

  async deleteForRuns(threadIds: readonly string[]): Promise<void> {
    for (const id of threadIds) this.byThread.delete(id);
  }

  async copy(fromThreadId: string, toThreadId: string): Promise<void> {
    const src = this.byThread.get(fromThreadId) ?? [];
    if (src.length === 0) return;
    const copied: CheckpointRecord[] = src.map((r) => ({ ...r, threadId: toThreadId }));
    this.byThread.set(toThreadId, copied);
  }

  async prune(threadId: string, keep: number): Promise<void> {
    const list = this.byThread.get(threadId) ?? [];
    if (list.length <= keep) return;
    this.byThread.set(threadId, list.slice(-keep));
  }
}

export class MemoryStore implements LongTermStore {
  private readonly items = new Map<string, StoreItem>();

  private key(namespace: readonly string[], key: string): string {
    return `${nsKey(namespace)}/${key}`;
  }

  async get(namespace: readonly string[], key: string): Promise<StoreItem | null> {
    return this.items.get(this.key(namespace, key)) ?? null;
  }

  async put(item: StoreItem): Promise<void> {
    this.items.set(this.key(item.namespace, item.key), { ...item });
  }

  async search(namespace: readonly string[], opts: StoreSearchOptions = {}): Promise<StoreItem[]> {
    let results = [...this.items.values()].filter((i) => isPrefix(namespace, i.namespace));
    if (opts.filter) {
      for (const [k, v] of opts.filter) {
        results = results.filter((i) => matchesFilter(i, k, v));
      }
    }
    const offset = opts.offset ?? 0;
    const limit = opts.limit ?? results.length;
    return results.slice(offset, offset + limit);
  }

  async delete(namespace: readonly string[], key: string): Promise<void> {
    this.items.delete(this.key(namespace, key));
  }

  async listNamespaces(prefix?: readonly string[]): Promise<readonly (readonly string[])[]> {
    const seen = new Set<string>();
    for (const i of this.items.values()) {
      const ns = i.namespace;
      if (prefix && !isPrefix(prefix, ns)) continue;
      seen.add(nsKey(ns));
    }
    return [...seen].map((s) => s.split("/")).filter((s) => s.length > 1 || s[0] !== "");
  }
}

export class MemoryCache implements Cache {
  private readonly entries = new Map<string, CacheEntry>();

  async get(key: string): Promise<CacheEntry | null> {
    const e = this.entries.get(key);
    if (!e) return null;
    if (e.expiresAt !== undefined && e.expiresAt !== 0 && e.expiresAt <= Date.now()) {
      this.entries.delete(key);
      return null;
    }
    return e;
  }

  async set(key: string, entry: CacheEntry): Promise<void> {
    this.entries.set(key, entry);
  }

  async delete(key: string): Promise<void> {
    this.entries.delete(key);
  }

  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== null;
  }
}

function matchesFilter(item: StoreItem, key: string, expected: unknown): boolean {
  const value = serializer.loads(item.value) as Record<string, unknown>;
  return value[key] === expected;
}

/** Cosine similarity between two same-length vectors (0 when length differs). */
export { cosineSimilarity } from "./vector.js";

/** In-memory vector store: linear-scan cosine search, namespace-scoped. */
export class MemoryVectorStore implements VectorStore {
  private readonly points = new Map<string, VectorPoint>();

  private key(namespace: readonly string[], id: string): string {
    return `${nsKey(namespace)}/${id}`;
  }

  async upsert(point: VectorPoint): Promise<void> {
    this.points.set(this.key(point.namespace, point.id), { ...point });
  }

  async search(
    namespace: readonly string[],
    query: number[],
    options: VectorSearchOptions = {},
  ): Promise<VectorSearchResult[]> {
    const rows = [...this.points.values()]
      .filter((p) => isPrefix(namespace, p.namespace))
      .map((p) => ({ score: cosineSimilarity(p.vector, query), point: p }))
      .filter((r) => r.score >= (options.minScore ?? 0))
      .sort((a, b) => b.score - a.score);
    const limit = options.limit ?? rows.length;
    return rows
      .slice(0, limit)
      .map((r) => ({ id: r.point.id, score: r.score, metadata: r.point.metadata }));
  }

  async delete(namespace: readonly string[], id: string): Promise<void> {
    this.points.delete(this.key(namespace, id));
  }
}
