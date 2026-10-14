/**
 * Append-only durable event log (Task 6).
 *
 * Two backends behind one interface: `MemoryLog` (in-process) and `SqliteLog`
 * (node:sqlite). Events are assigned a monotonically increasing `seq` at append
 * time; recovery replays `readFrom(seq)`. `truncateBefore` supports compaction
 * once a snapshot is durable.
 */

// Type-only import: erased at compile time, so vite-node never tries to resolve
// the runtime module. The runtime value is loaded lazily via node-sqlite.ts.
import type { DatabaseSync as DatabaseSyncT } from "node:sqlite";
import type { DurableEvent, EventBase } from "./events.js";
import { loadDatabaseSync } from "../node-sqlite.js";

export interface DurableLog {
  /** Append an event, returning it with its assigned `seq`. */
  append(event: DurableEvent): DurableEvent;
  /** Read events with seq > `after` in ascending order. */
  readFrom(after: number): DurableEvent[];
  /** Highest seq appended so far (0 if empty). */
  readonly lastSeq: number;
  /** Count of events currently stored. */
  readonly count: number;
  /** Drop everything before `beforeSeq` (compaction; keeps >= beforeSeq). */
  truncateBefore(beforeSeq: number): void;
  close(): void;
}

/** JSON round-trip is lossless for our event shapes (all msgpack-canonical). */
function deserialize(raw: string): DurableEvent {
  return JSON.parse(raw) as DurableEvent;
}

export class MemoryLog implements DurableLog {
  private readonly items: DurableEvent[] = [];
  private seq = 0;

  append(event: DurableEvent): DurableEvent {
    this.seq += 1;
    const stored: DurableEvent = { ...event, seq: this.seq };
    this.items.push(stored);
    return stored;
  }

  readFrom(after: number): DurableEvent[] {
    return this.items.filter((e) => e.seq > after);
  }

  get lastSeq(): number {
    return this.seq;
  }

  get count(): number {
    return this.items.length;
  }

  truncateBefore(beforeSeq: number): void {
    // keep events with seq >= beforeSeq
    const keep = this.items.filter((e) => e.seq >= beforeSeq);
    this.items.length = 0;
    this.items.push(...keep);
  }

  close(): void {
    this.items.length = 0;
  }
}

export interface SqliteLogOptions {
  /** ':memory:' or a file path. */
  location: string;
  /** Log name/partition (e.g. runId) to namespace events. */
  name: string;
}

export class SqliteLog implements DurableLog {
  private readonly db: DatabaseSyncT;
  private readonly name: string;
  private _lastSeq: number;
  private _count: number;

  constructor(opts: SqliteLogOptions) {
    this.db = new (loadDatabaseSync())(opts.location);
    this.name = opts.name;
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS durable_events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        payload TEXT NOT NULL
      );
    `);
    const stmt = this.db.prepare(
      "SELECT MAX(seq) AS lastSeq, COUNT(*) AS n FROM durable_events WHERE name = ?",
    );
    const row = stmt.get(this.name) as { lastSeq: number | null; n: number };
    this._lastSeq = row.lastSeq ?? 0;
    this._count = row.n;
  }

  append(event: DurableEvent): DurableEvent {
    // The SQLite AUTOINCREMENT column is the authoritative sequence. Using
    // `lastInsertRowid` (rather than a local counter) keeps seq monotonic when
    // another process appended to the same database and makes the value
    // recoverable after a restart.
    const result = this.db
      .prepare("INSERT INTO durable_events (name, payload) VALUES (?, ?)")
      .run(this.name, JSON.stringify(event));
    const seq = Number(result.lastInsertRowid);
    this._lastSeq = Math.max(this._lastSeq, seq);
    this._count += 1;
    return { ...event, seq };
  }

  readFrom(after: number): DurableEvent[] {
    // The payload deliberately stores the pre-append event (seq 0). Merge the
    // durable seq column back in so a restarted process replays the exact
    // sequence positions that cursors, snapshots and compaction rely on.
    const rows = this.db
      .prepare("SELECT seq, payload FROM durable_events WHERE name = ? AND seq > ? ORDER BY seq")
      .all(this.name, after) as { seq: number; payload: string }[];
    return rows.map((r) => ({ ...deserialize(r.payload), seq: r.seq }));
  }

  get lastSeq(): number {
    return this._lastSeq;
  }

  get count(): number {
    return this._count;
  }

  truncateBefore(beforeSeq: number): void {
    this.db
      .prepare("DELETE FROM durable_events WHERE name = ? AND seq < ?")
      .run(this.name, beforeSeq);
    const row = this.db
      .prepare("SELECT COUNT(*) AS n FROM durable_events WHERE name = ?")
      .get(this.name) as { n: number };
    this._count = row.n;
  }

  close(): void {
    this.db.close();
  }
}

export type { EventBase };
