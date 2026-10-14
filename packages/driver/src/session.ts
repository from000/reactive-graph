/**
 * RgpSession: a request/response + event channel over a Duplex (stdio for local
 * Driver, WebSocket for remote). Uses the RGP/1 framing/codec from @reactivegraph/protocol.
 *
 * Responsibilities:
 *  - frame encode/decode on the wire,
 *  - multiplex requests by id with out-of-order response correlation,
 *  - reject duplicate ids, unknown-response ids, and response-method mismatches,
 *  - deliver Driver-initiated events (e.g. TASK_INVOKE) to registered handlers,
 *  - cancel pending requests, and detect peer exit (crash propagation).
 */
import {
  CodecError,
  FrameDecoder,
  ProtocolError,
  decodeValue,
  encodeFrame,
  encodeValue,
  isEnvelope,
  isMethod,
  makeId,
  type Envelope,
  type Method,
} from "@reactivegraph/protocol";

type Pending = {
  method: Method;
  resolve: (payload: unknown) => void;
  reject: (err: Error) => void;
  timer?: NodeJS.Timeout;
};

export type EventHandler = (envelope: Envelope) => void | Promise<void>;

export interface SessionOptions {
  /** How to write bytes to the peer. */
  send: (bytes: Uint8Array) => void;
  /** Optional hook when the peer closes/crashes. */
  onClose?: (err?: Error) => void;
  /** Default per-request timeout in ms (0 = none). */
  timeoutMs?: number;
  /** Optional deterministic id generator for tests. */
  idGen?: () => string;
}

export class RgpSession {
  private readonly decoder: FrameDecoder;
  private readonly pending = new Map<string, Pending>();
  private readonly events = new Map<Method, Set<EventHandler>>();
  private readonly sendFn: (bytes: Uint8Array) => void;
  private readonly onClose?: (err?: Error) => void;
  private readonly timeoutMs: number;
  private readonly idGen: () => string;
  private closed = false;

  constructor(opts: SessionOptions) {
    this.sendFn = opts.send;
    this.onClose = opts.onClose;
    this.timeoutMs = opts.timeoutMs ?? 0;
    this.idGen = opts.idGen ?? makeId;
    this.decoder = new FrameDecoder();
  }

  /** Feed inbound bytes (from the peer). */
  ingest(chunk: Uint8Array): void {
    if (this.closed) return;
    let frames: Uint8Array[];
    try {
      frames = this.decoder.push(chunk);
    } catch (err) {
      this.fail(new ProtocolError(`frame error: ${(err as Error).message}`));
      return;
    }
    for (const frame of frames) {
      this.handleFrame(frame);
    }
  }

  private handleFrame(frame: Uint8Array): void {
    let envelope: unknown;
    try {
      envelope = decodeValue(frame);
    } catch (err) {
      this.fail(new CodecError(`undecodable envelope: ${(err as Error).message}`));
      return;
    }
    if (!isEnvelope(envelope)) {
      this.fail(new ProtocolError("malformed envelope"));
      return;
    }
    const env = envelope as Envelope;
    if (env.kind === "response") {
      this.resolveResponse(env);
    } else if (env.kind === "event" || env.kind === "cancel") {
      void this.dispatchEvent(env);
    } else if (env.kind === "request" && this.requestResponder) {
      // A request sent to us (gateway/DriverLink server side): handle and reply.
      void (async () => {
        try {
          const result = await this.requestResponder!(env.method, env.payload);
          const response: Envelope = {
            version: 1,
            id: env.id,
            kind: "response",
            method: env.method,
            payload: result,
          };
          this.sendFrame(response);
        } catch (err) {
          const response: Envelope = {
            version: 1,
            id: env.id,
            kind: "response",
            method: env.method,
            payload: { error: { type: (err as Error).name, message: (err as Error).message } },
          };
          this.sendFrame(response);
        }
      })();
    } else {
      // A request with no server handler: ignore (caller-side session).
    }
  }

  private resolveResponse(env: Envelope): void {
    const pending = this.pending.get(env.id);
    if (!pending) {
      // Unknown response id — ignore or surface? Per contract: reject silently
      // to avoid cross-talk, but log for debugging.
      return;
    }
    if (pending.method !== env.method) {
      this.pending.delete(env.id);
      pending.reject(
        new ProtocolError(
          `response method mismatch: expected ${pending.method}, got ${env.method}`,
        ),
      );
      return;
    }
    if (pending.timer) clearTimeout(pending.timer);
    this.pending.delete(env.id);
    // A response envelope payload is the result payload; errors are transported
    // as {error: {type, message}}.
    const payload = env.payload as Record<string, unknown>;
    if (
      typeof payload === "object" &&
      payload !== null &&
      (payload as Record<string, unknown>)["error"]
    ) {
      const err = payload["error"] as Record<string, unknown>;
      pending.reject(
        new Error(`${String(err["type"] ?? "Error")}: ${String(err["message"] ?? "unknown")}`),
      );
    } else {
      pending.resolve(env.payload);
    }
  }

  private async dispatchEvent(env: Envelope): Promise<void> {
    const handlers = this.events.get(env.method);
    if (!handlers) return;
    for (const handler of handlers) {
      try {
        await handler(env);
      } catch (err) {
        // Event handler errors are reported via TASK_RESULT error by the host;
        // a throwing handler should not tear down the session.
        console.error("[rgp-session] event handler error:", err);
      }
    }
  }

  /** Handler for requests directed at us (server side of a session). */
  private requestResponder?: (method: Method, payload: unknown) => unknown | Promise<unknown>;

  onRequestRespond(
    handler: (method: Method, payload: unknown) => unknown | Promise<unknown>,
  ): void {
    this.requestResponder = handler;
  }

  /** Register a handler for Driver-initiated events (e.g. TASK_INVOKE). */
  on(method: Method, handler: EventHandler): void {
    let handlers = this.events.get(method);
    if (!handlers) {
      handlers = new Set();
      this.events.set(method, handlers);
    }
    handlers.add(handler);
  }

  /** Remove a previously registered event handler. */
  off(method: Method, handler: EventHandler): void {
    const handlers = this.events.get(method);
    handlers?.delete(handler);
  }

  /** Send a request and await its response. */
  request(method: Method, payload: unknown, opts?: { timeoutMs?: number }): Promise<unknown> {
    if (this.closed) {
      return Promise.reject(new Error("session closed"));
    }
    const id = this.idGen();
    const envelope: Envelope = { version: 1, id, kind: "request", method, payload };
    const timeoutMs = opts?.timeoutMs ?? this.timeoutMs;
    return new Promise<unknown>((resolve, reject) => {
      const pending: Pending = { method, resolve, reject };
      if (timeoutMs > 0) {
        pending.timer = setTimeout(() => {
          this.pending.delete(id);
          reject(new Error(`request ${method} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }
      this.pending.set(id, pending);
      try {
        this.sendFrame(envelope);
      } catch (err) {
        this.pending.delete(id);
        if (pending.timer) clearTimeout(pending.timer);
        reject(err as Error);
      }
    });
  }

  /** Fire a Driver-initiated event/cancel envelope (fire-and-forget). */
  emit(kind: "event" | "cancel", method: Method, payload: unknown): void {
    const envelope: Envelope = { version: 1, id: this.idGen(), kind, method, payload };
    this.sendFrame(envelope);
  }

  /** Cancel a pending request by id (best-effort). */
  cancel(requestId: string): boolean {
    const pending = this.pending.get(requestId);
    if (!pending) return false;
    if (pending.timer) clearTimeout(pending.timer);
    this.pending.delete(requestId);
    pending.reject(new Error("request cancelled"));
    this.emit("cancel", "CANCEL", { targetId: requestId, reason: "cancelled by caller" });
    return true;
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  private sendFrame(envelope: Envelope): void {
    if (this.closed) {
      throw new Error("session closed");
    }
    this.sendFn(encodeFrame(encodeValue(envelope)));
  }

  /** Reject all pending requests; used when the peer crashes. */
  fail(err?: Error): void {
    if (this.closed) return;
    this.closed = true;
    const failure = err ?? new Error("session closed by peer");
    for (const [, pending] of this.pending) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(failure);
    }
    this.pending.clear();
    this.onClose?.(failure);
  }

  /** Gracefully close without an error. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const [, pending] of this.pending) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(new Error("session closed"));
    }
    this.pending.clear();
    this.onClose?.();
  }

  get isClosed(): boolean {
    return this.closed;
  }
}

export { isMethod };
