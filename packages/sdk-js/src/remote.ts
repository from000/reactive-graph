/**
 * Remote SDK client (Task 11 / D.9.99-101).
 *
 * Speaks RGP/1 over a Duplex-shaped transport (WebSocket / HTTP2 adapter, or
 * the gateway's InMemoryDuplex in tests). Mirrors the Python SDK surface:
 * compile a graph on the server, run it, stream events, cancel, and inspect
 * state — with backpressure and cancellation propagation handled by the
 * gateway, not by this client.
 */

import { RgpSession } from "@reactivegraph/driver";
import type { Method } from "@reactivegraph/protocol";

/** Byte-level transport a remote client connects over. */
export interface RemoteTransport {
  send(bytes: Uint8Array): void;
  onData(cb: (bytes: Uint8Array) => void): void;
  onClose(cb: (err?: Error) => void): void;
  close(): void;
}

export interface RunOptions {
  readonly input?: unknown;
  readonly stream?: boolean;
  readonly config?: Record<string, unknown>;
}

export interface RunResult {
  readonly runId: string;
  readonly output: unknown;
  readonly state: Record<string, unknown>;
}

/**
 * Client bound to one authenticated gateway session. All methods are
 * request/response over RGP/1; streaming consumes STREAM_EVENT events.
 */
export class RemoteClient {
  private readonly session: RgpSession;

  constructor(transport: RemoteTransport, opts?: { timeoutMs?: number }) {
    this.session = new RgpSession({
      send: (bytes) => transport.send(bytes),
      onClose: () => transport.close(),
      timeoutMs: opts?.timeoutMs,
    });
    transport.onData((chunk) => this.session.ingest(chunk));
  }

  /** Compile a graph definition server-side; returns a graph id. */
  async compileGraph(def: { id?: string; tasks?: unknown[]; routes?: unknown[] }): Promise<string> {
    const res = (await this.request("COMPILE_GRAPH", def)) as { graphId: string };
    return res.graphId;
  }

  /** Run a compiled graph with an input; returns the final output + state. */
  async run(graphId: string, input: unknown, opts: RunOptions = {}): Promise<RunResult> {
    const res = (await this.request("RUN", {
      graphId,
      input,
      stream: opts.stream ?? false,
      config: opts.config ?? {},
    })) as { runId: string; output: unknown; state?: Record<string, unknown> };
    return {
      runId: res.runId,
      output: res.output,
      state: (res.state as Record<string, unknown>) ?? {},
    };
  }

  /** Read the latest state of a thread. */
  async getState(config: { threadId: string }): Promise<Record<string, unknown>> {
    const res = (await this.request("GET_STATE", { config })) as {
      values: Record<string, unknown>;
    };
    return res.values;
  }

  /**
   * Resume a previously interrupted run with an interrupt response.
   * Returns the run's updated state after the interrupted node re-enters.
   */
  async resume(
    runId: string,
    interruptResponse: unknown,
    opts: { threadId?: string } = {},
  ): Promise<{ runId: string; state: Record<string, unknown> }> {
    const res = (await this.request("RESUME", {
      runId,
      threadId: opts.threadId,
      interruptResponse,
    })) as { runId: string; state?: Record<string, unknown> };
    return {
      runId: res.runId,
      state: (res.state as Record<string, unknown>) ?? {},
    };
  }

  /** Cancel an in-flight run (propagates a CANCEL frame through the gateway). */
  async cancel(runId: string): Promise<void> {
    this.session.emit("cancel", "CANCEL", { targetId: runId, reason: "client cancel" });
  }

  /**
   * Stream events for a run. STREAM_EVENT frames arriving while the RUN
   * request is in flight are collected (keyed by the client-chosen runId, so
   * concurrent stream() calls never mix) and replayed oldest-first once the
   * request resolves. The event listener is removed when the run completes.
   */
  async *stream(
    graphId: string,
    input: unknown,
    config: Record<string, unknown> = {},
    opts: { runId?: string } = {},
  ): AsyncGenerator<unknown> {
    const runId = opts.runId ?? `run-${Math.random().toString(36).slice(2)}`;
    const events: unknown[] = [];
    const listener = (env: { payload?: unknown }): void => {
      const p = env.payload as { runId?: string } | undefined;
      if (p?.runId === runId) events.push(env.payload);
    };
    this.session.on("STREAM_EVENT", listener);
    try {
      await this.request("RUN", { graphId, input, runId, stream: true, config });
    } finally {
      this.session.off("STREAM_EVENT", listener);
    }
    for (const ev of events) yield ev;
  }

  close(): void {
    this.session.close();
  }

  private request(method: Method, payload: unknown): Promise<unknown> {
    return this.session.request(method, payload);
  }
}
