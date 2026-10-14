/**
 * Typed stream multiplexer (Task 8 / docs/spec/streams-and-interrupts.md).
 *
 * Separates *transient* chunks (token churn, custom events, debug) from
 * *committed* state patches. Under backpressure, transient token churn is
 * coalesced; committed state is never dropped or reordered. A resume cursor
 * allows a disconnected consumer to continue from the last delivered chunk.
 *
 * Chunk ordering: committed chunks are strictly ordered by sequence; transient
 * chunks may be coalesced but never reorder a committed state boundary.
 */

export type StreamChunkType =
  | "values" // channel snapshot after a superstep (committed)
  | "updates" // per-node update dict (committed)
  | "custom" // user event payload (transient)
  | "messages" // model/tool message or token stream (transient)
  | "debug" // debug/trace event (transient)
  | "committed"; // raw patch batch + state version (committed)

export interface StreamChunk {
  readonly seq: number;
  readonly type: StreamChunkType;
  /** For transient chunks: whether this chunk is coalescible token churn. */
  readonly coalescible?: boolean;
  /**
   * Payload shape by type:
   *  - values:   { state: unknown }
   *  - updates:  { updates: Record<string, unknown> }
   *  - custom:   { event: string; payload: unknown }
   *  - messages: { message: unknown }
   *  - debug:    { payload: unknown }
   *  - committed:{ patches: unknown[]; stateVersion: number }
   */
  readonly payload: unknown;
}

const TRANSIENT: ReadonlySet<StreamChunkType> = new Set(["custom", "messages", "debug"]);
const COALESCIBLE: ReadonlySet<StreamChunkType> = new Set(["messages", "debug"]);

export class StreamTerminatedError extends Error {
  constructor() {
    super("stream terminated");
    this.name = "StreamTerminatedError";
  }
}

export interface StreamMuxOptions {
  /** Max buffered committed chunks (0 = unbounded). */
  maxBuffered?: number;
  /** Max coalesced transient bytes before flushing (token churn cap). */
  coalesceBytes?: number;
}

export class StreamMux {
  private readonly committed: StreamChunk[] = [];
  private readonly transient: StreamChunk[] = [];
  private seq = 0;
  private terminated = false;
  private readonly maxBuffered: number;
  private readonly coalesceBytes: number;

  constructor(opts: StreamMuxOptions = {}) {
    this.maxBuffered = opts.maxBuffered ?? 0;
    this.coalesceBytes = opts.coalesceBytes ?? 64 * 1024;
  }

  /** Push a chunk. Ordering guarantees: committed chunks never reorder. */
  push(chunk: Omit<StreamChunk, "seq">): StreamChunk {
    if (this.terminated) throw new StreamTerminatedError();
    this.seq += 1;
    const stored: StreamChunk = { ...chunk, seq: this.seq };
    if (TRANSIENT.has(chunk.type)) {
      this.transient.push(stored);
      this.coalesceIfNeeded();
    } else {
      // Committed boundary: flush pending transient coalescing first so a
      // consumer sees coalesced tokens before the committed state.
      this.committed.push(stored);
      this.enforceBufferLimit();
    }
    return stored;
  }

  /** Terminal marker: no further chunks accepted. */
  terminate(): void {
    this.terminated = true;
  }

  /**
   * Read all buffered chunks from `afterSeq` onward, in global order
   * (transient first within a superstep, then committed boundary; a consumer
   * awaiting committed state must read to the latest `committed` chunk).
   */
  readFrom(afterSeq: number): StreamChunk[] {
    const all = [...this.transient, ...this.committed].sort((a, b) => a.seq - b.seq);
    return all.filter((c) => c.seq > afterSeq);
  }

  /** Latest committed sequence number (0 if none). */
  get lastCommittedSeq(): number {
    let last = 0;
    for (const c of this.committed) {
      if (c.seq > last) last = c.seq;
    }
    return last;
  }

  get bufferedCount(): number {
    return this.transient.length + this.committed.length;
  }

  get isTerminated(): boolean {
    return this.terminated;
  }

  /** Coalesce trailing coalescible transient chunks into one (token churn). */
  private coalesceIfNeeded(): void {
    const totalBytes = this.transient.reduce((acc, c) => acc + sizeOf(c.payload), 0);
    if (totalBytes <= this.coalesceBytes) return;
    // Keep the first chunk as the coalescing anchor; fold the rest's payloads.
    const head = this.transient[0];
    if (!head || !COALESCIBLE.has(head.type)) return;
    const folded: unknown[] = [];
    for (const c of this.transient) {
      const payload = c.payload as { message?: unknown; payload?: unknown };
      folded.push(payload.message ?? payload.payload);
    }
    this.transient.length = 0;
    this.seq += 1;
    this.transient.push({
      seq: this.seq,
      type: head.type,
      coalescible: true,
      payload: head.type === "messages" ? { messages: folded } : { payload: folded },
    });
  }

  private enforceBufferLimit(): void {
    if (this.maxBuffered <= 0) return;
    while (this.committed.length > this.maxBuffered) {
      this.committed.shift();
    }
  }
}

function sizeOf(value: unknown): number {
  try {
    return JSON.stringify(value)?.length ?? 0;
  } catch {
    return 0;
  }
}
