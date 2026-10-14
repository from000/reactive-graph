/**
 * Storage interfaces (Task 9).
 *
 * Independent interfaces for: event log (already in ../durability/log.ts),
 * checkpoint saver, long-term store, cache, and serializer. Durable backends
 * must implement atomic checkpoint+write behavior (plan Task 9 §3).
 */

/** A serialized checkpoint record (opaque to the storage layer). */
export interface CheckpointRecord {
  readonly threadId: string;
  readonly checkpointId: string;
  /** Parent checkpoint id (lineage/fork). */
  readonly parentCheckpointId?: string;
  /**
   * Optimistic-concurrency condition for `put`: the checkpoint is only
   * written if the thread's LATEST checkpoint is still this id (i.e. the
   * writer derived from it). Undefined = no check (backwards compatible).
   * Protects multi-Driver setups sharing one durable backend from silently
   * overwriting each other's progress on the same thread.
   */
  readonly expectedParentCheckpointId?: string;
  readonly ts: string;
  /** Opaque serialized channel values (see Serializer). */
  readonly values: Uint8Array;
  readonly metadata: Uint8Array;
  /** version-stamped writes for incremental replay. */
  readonly writes?: readonly { taskId: string; channel: string; value: Uint8Array }[];
}

/** Thrown by `CheckpointSaver.put` when `expectedParentCheckpointId` no
 * longer matches the thread's latest checkpoint (lost-update guard). */
export class CheckpointConflictError extends Error {
  constructor(
    readonly threadId: string,
    readonly latestCheckpointId: string,
    readonly expectedParentCheckpointId: string,
  ) {
    super(
      `checkpoint conflict on thread ${threadId}: expected parent ` +
        `${expectedParentCheckpointId} but latest is ${latestCheckpointId} ` +
        "(another writer advanced this thread) — retry from the latest checkpoint, or fork a new thread",
    );
    this.name = "CheckpointConflictError";
  }
}

/** Shared optimistic-concurrency guard used by checkpoint backends: rejects a
 * `put` when `expectedParentCheckpointId` no longer matches the thread's
 * latest checkpoint (no-op when the condition is absent). */
export function assertCheckpointParent(
  latest: CheckpointRecord | null,
  record: CheckpointRecord,
): void {
  if (record.expectedParentCheckpointId === undefined) return;
  if (latest && latest.checkpointId !== record.expectedParentCheckpointId) {
    throw new CheckpointConflictError(
      record.threadId,
      latest.checkpointId,
      record.expectedParentCheckpointId,
    );
  }
}

export interface CheckpointWrite {
  readonly taskId: string;
  readonly channel: string;
  readonly value: Uint8Array;
}

export interface ListCheckpointsOptions {
  readonly before?: string; // checkpointId cursor (exclusive)
  readonly limit?: number;
}

export interface CheckpointSaver {
  get(threadId: string, checkpointId?: string): Promise<CheckpointRecord | null>;
  list(threadId: string, opts?: ListCheckpointsOptions): Promise<CheckpointRecord[]>;
  put(record: CheckpointRecord): Promise<void>;
  putWrites(
    threadId: string,
    checkpointId: string,
    writes: readonly CheckpointWrite[],
  ): Promise<void>;
  deleteThread(threadId: string): Promise<void>;
  deleteForRuns(threadIds: readonly string[]): Promise<void>;
  copy(fromThreadId: string, toThreadId: string): Promise<void>;
  prune(threadId: string, keep: number): Promise<void>;
}

export interface StoreItem {
  readonly namespace: readonly string[];
  readonly key: string;
  readonly value: Uint8Array;
  readonly createdAt?: string;
  readonly updatedAt?: string;
}

export interface StoreSearchOptions {
  readonly filter?: ReadonlyMap<string, unknown>;
  readonly limit?: number;
  readonly offset?: number;
}

/** A vector point in a namespace: an embedding plus opaque metadata. */
export interface VectorPoint {
  readonly namespace: readonly string[];
  readonly id: string;
  readonly vector: number[];
  readonly metadata?: unknown;
}

export interface VectorSearchOptions {
  readonly limit?: number;
  /** Minimum cosine similarity (0..1) to include. */
  readonly minScore?: number;
}

export interface VectorSearchResult {
  readonly id: string;
  /** Cosine similarity, 1.0 = identical direction. */
  readonly score: number;
  readonly metadata?: unknown;
}

/**
 * Semantic (vector) store. Embeddings are produced by the caller — the store
 * only persists vectors and answers nearest-neighbour queries by cosine
 * similarity. Backends use a linear scan (no ANN index): fine for
 * thousands of points, not for millions.
 */
export interface VectorStore {
  upsert(point: VectorPoint): Promise<void>;
  search(
    namespace: readonly string[],
    query: number[],
    options?: VectorSearchOptions,
  ): Promise<VectorSearchResult[]>;
  delete(namespace: readonly string[], id: string): Promise<void>;
}

export interface LongTermStore {
  get(namespace: readonly string[], key: string): Promise<StoreItem | null>;
  put(item: StoreItem): Promise<void>;
  search(namespace: readonly string[], opts?: StoreSearchOptions): Promise<StoreItem[]>;
  delete(namespace: readonly string[], key: string): Promise<void>;
  listNamespaces(prefix?: readonly string[]): Promise<readonly (readonly string[])[]>;
}

export interface CacheEntry {
  readonly value: Uint8Array;
  readonly expiresAt?: number; // epoch ms; 0/undefined = no TTL
}

export interface Cache {
  get(key: string): Promise<CacheEntry | null>;
  set(key: string, entry: CacheEntry): Promise<void>;
  delete(key: string): Promise<void>;
  /** True if key exists and not expired (for TTL tests). */
  has(key: string): Promise<boolean>;
}

/** Cross-language serializer (mirrors JsonPlusSerializer semantics). */
export interface Serializer {
  dumps(value: unknown): Uint8Array;
  loads(data: Uint8Array): unknown;
}

/** A serializer that can encrypt payloads (EncryptedSerializer). */
export interface EncryptingSerializer extends Serializer {
  readonly encrypted: boolean;
}

/** Schema versioning + protected migrations for durable backends. */
export interface VersionedStorage {
  readonly schemaVersion: number;
  migrate(targetVersion: number): void;
}

export const CURRENT_SCHEMA_VERSION = 1;
