/**
 * RGP/1 gateway transport (Task 11 / D.9.98-101).
 *
 * A Duplex is the abstract transport used by both the local Driver stdio link
 * and a remote WebSocket/HTTP2 link. `RgpGateway` authenticates an accepted
 * duplex, wraps it in a server-side `RgpSession` (the same request/response +
 * event abstraction the stdio Driver uses), and exposes the session as a
 * `SessionPeer`. Concrete behaviors (compile/run/get_state) are provided by
 * registering a request handler via `onRequest` — typically a `DriverLink`.
 */

import { encodeFrame, encodeValue, makeId, type Method } from "@reactivegraph/protocol";
import { RgpSession } from "../session.js";

/** Byte-level transport: push inbound bytes, send outbound bytes. */
export interface Duplex {
  /** Feed bytes received from the peer. */
  ingest(chunk: Uint8Array): void;
  /** Write bytes to the peer. `false` signals backpressure until `onDrain`. */
  send(bytes: Uint8Array): boolean | void;
  /** Notify the peer has gone away. */
  close(err?: Error): void;
  onData(cb: (bytes: Uint8Array) => void): void;
  onClose(cb: (err?: Error) => void): void;
  /** Optional: transport-level drain notification for `send() === false`. */
  onDrain?(cb: () => void): void;
}

export interface GatewayConfig {
  /** Bearer tokens -> tenant id. Empty map = allow any. */
  tenants?: Record<string, string>;
  /** Maximum outstanding un-read outbound bytes before backpressure stalls writes. */
  highWaterMark?: number;
  /** Maximum in-flight requests per tenant. 0/unset = unlimited. */
  perTenantConcurrency?: number;
}

export interface GatewayEvent {
  kind: "connected" | "disconnected" | "cancelled";
  sessionId: string;
  tenant: string;
  runId?: string;
}

export type RequestHandler = (
  method: Method,
  payload: unknown,
  tenant: string,
) => unknown | Promise<unknown>;

/**
 * In-memory duplex pair (both ends wired together). Used by local integration
 * and SDK tests; identical contract to a WebSocket adapter.
 */
export class InMemoryDuplex implements Duplex {
  private dataCb?: (bytes: Uint8Array) => void;
  private closeCb?: (err?: Error) => void;
  private drainCb?: () => void;
  private peer?: Duplex;
  private pending = 0;
  private closed = false;
  private readonly highWaterMark: number;

  constructor(opts?: { highWaterMark?: number }) {
    this.highWaterMark = opts?.highWaterMark ?? 1024 * 1024;
  }

  /** Wire the peer end into this one. */
  connect(peer: Duplex): void {
    this.peer = peer;
  }

  onData(cb: (bytes: Uint8Array) => void): void {
    this.dataCb = cb;
  }
  onClose(cb: (err?: Error) => void): void {
    this.closeCb = cb;
  }
  onDrain(cb: () => void): void {
    this.drainCb = cb;
  }

  ingest(chunk: Uint8Array): void {
    if (this.dataCb) this.dataCb(chunk);
  }

  send(bytes: Uint8Array): boolean {
    this.pending += bytes.length;
    if (this.pending > this.highWaterMark) {
      // backpressure: only propagate when the reader drains (stall indicator)
      setImmediate(() => this.peer?.ingest(bytes));
      return false;
    }
    // write straight to the peer, which feeds its own onData handler
    this.peer?.ingest(bytes);
    return true;
  }

  close(err?: Error): void {
    if (this.closed) return;
    this.closed = true;
    if (this.closeCb) this.closeCb(err);
    this.peer?.close(err);
  }

  /** Drain signal: reduces outstanding pending so a stalled write resumes. */
  ack(bytes: number): void {
    const wasBlocked = this.pending > this.highWaterMark;
    this.pending = Math.max(0, this.pending - bytes);
    if (wasBlocked && this.pending <= this.highWaterMark) this.drainCb?.();
  }
}

/** Raised when a peer cannot drain its bounded outbound queue in time. */
export class SlowConsumerError extends Error {
  constructor(limitBytes: number) {
    super(`slow consumer exceeded ${limitBytes} bytes of outbound backpressure`);
    this.name = "SlowConsumerError";
  }
}

export interface GatewayMetrics {
  readonly sessions: number;
  readonly blockedSessions: number;
  readonly queuedBytes: number;
  readonly evictedSlowConsumers: number;
}

interface SessionRuntime {
  blocked: boolean;
  /** Bytes retained because the transport has not reported drain yet. */
  blockedBytes: number;
  /** Frames not yet handed to the transport (bounded by highWaterMark). */
  queue: Uint8Array[];
  queuedBytes: number;
  closed: boolean;
}

export interface SessionPeer {
  /** Unique session id. */
  readonly id: string;
  /** Authenticated tenant. */
  readonly tenant: string;
  /** Send an RGP/1 request and await the response. */
  request(method: Method, payload: unknown): Promise<unknown>;
  /** Deliver an event to the peer. */
  emit(kind: "event" | "cancel", method: Method, payload: unknown): void;
  /** Cancel an in-flight request id. */
  cancel(requestId: string): void;
  /** Close the session. */
  close(): void;
}

/**
 * RgpGateway: accepts a duplex, authenticates it, and hosts a server-side
 * RGP/1 session. Behavior is supplied through `onRequest` (a DriverLink).
 * Emits lifecycle events (connected/disconnected/cancelled) for observability.
 */
export class RgpGateway {
  private readonly tenants: Record<string, string>;
  private readonly highWaterMark: number;
  private readonly perTenantConcurrency: number;
  private readonly inFlight = new Map<string, number>();
  private readonly sessions = new Map<string, SessionRuntime>();
  private evictedSlowConsumers = 0;
  private readonly eventCbs = new Set<(event: GatewayEvent) => void>();
  private requestHandler?: RequestHandler;

  constructor(config: GatewayConfig = {}) {
    // Null prototype prevents __proto__/constructor tokens from mutating the
    // auth table while rotateToken is adding new credentials.
    this.tenants = Object.assign(
      Object.create(null) as Record<string, string>,
      config.tenants ?? {},
    );
    this.highWaterMark = config.highWaterMark ?? 1024 * 1024;
    this.perTenantConcurrency = config.perTenantConcurrency ?? 0;
  }

  /** Provide the request implementation (compile/run/get_state/...). */
  onRequest(handler: RequestHandler): void {
    this.requestHandler = handler;
  }

  /** Accept a duplex (e.g. an accepted WebSocket) and authenticate it. */
  accept(duplex: Duplex, authToken: string): SessionPeer | null {
    const tenant = this.authenticate(authToken);
    if (tenant === null) {
      // reject: send a short error frame then close
      duplex.send(this.errorFrame("authentication failed"));
      duplex.close(new Error("authentication failed"));
      return null;
    }
    const sessionId = makeId();
    const runtime: SessionRuntime = {
      blocked: false,
      blockedBytes: 0,
      queue: [],
      queuedBytes: 0,
      closed: false,
    };
    this.sessions.set(sessionId, runtime);

    const session = new RgpSession({
      send: (bytes) => this.writeOrQueue(sessionId, session, duplex, bytes),
      onClose: (err) => {
        this.releaseSession(sessionId);
        this.emitEvent({ kind: "disconnected", sessionId, tenant });
        duplex.close(err);
      },
    });
    if (this.requestHandler) {
      session.onRequestRespond((method, payload) =>
        this.dispatchRequest(this.requestHandler!, method, payload, tenant),
      );
    }
    duplex.onData((chunk) => session.ingest(chunk));
    duplex.onClose((err) => session.fail(err ?? new Error("transport closed")));
    duplex.onDrain?.(() => this.flushQueue(sessionId, session, duplex));

    const peer: SessionPeer = {
      id: sessionId,
      tenant,
      request: (method, payload) => session.request(method, payload),
      emit: (kind, method, payload) => session.emit(kind, method, payload),
      cancel: (requestId: string) => {
        session.cancel(requestId);
        this.emitEvent({ kind: "cancelled", sessionId, tenant, runId: requestId });
      },
      close: () => {
        session.close();
      },
    };

    this.emitEvent({ kind: "connected", sessionId, tenant });
    return peer;
  }

  onEvent(cb: (event: GatewayEvent) => void): void {
    this.eventCbs.add(cb);
  }

  /** Operational counters for slow-consumer and session monitoring. */
  metrics(): GatewayMetrics {
    let blockedSessions = 0;
    let queuedBytes = 0;
    for (const state of this.sessions.values()) {
      if (state.blocked) blockedSessions += 1;
      queuedBytes += state.queuedBytes;
    }
    return {
      sessions: this.sessions.size,
      blockedSessions,
      queuedBytes,
      evictedSlowConsumers: this.evictedSlowConsumers,
    };
  }

  private async dispatchRequest(
    handler: RequestHandler,
    method: Method,
    payload: unknown,
    tenant: string,
  ): Promise<unknown> {
    if (this.perTenantConcurrency > 0) {
      const active = this.inFlight.get(tenant) ?? 0;
      if (active >= this.perTenantConcurrency) {
        throw new Error(`tenant ${tenant} concurrency limit exceeded`);
      }
      this.inFlight.set(tenant, active + 1);
    }
    try {
      return await handler(method, payload, tenant);
    } finally {
      if (this.perTenantConcurrency > 0) {
        this.inFlight.set(tenant, Math.max(0, (this.inFlight.get(tenant) ?? 1) - 1));
      }
    }
  }

  /** Write now, or queue behind transport backpressure without unbounded growth. */
  private writeOrQueue(
    sessionId: string,
    session: RgpSession,
    duplex: Duplex,
    bytes: Uint8Array,
  ): void {
    const state = this.sessions.get(sessionId);
    if (!state || state.closed) return;
    if (state.blocked) {
      state.queue.push(bytes);
      state.queuedBytes += bytes.length;
      state.blockedBytes += bytes.length;
      if (state.blockedBytes > this.highWaterMark) {
        state.closed = true;
        this.evictedSlowConsumers += 1;
        session.fail(new SlowConsumerError(this.highWaterMark));
      }
      return;
    }
    if (duplex.send(bytes) === false) {
      state.blocked = true;
      state.blockedBytes = bytes.length;
    }
  }

  private flushQueue(sessionId: string, session: RgpSession, duplex: Duplex): void {
    const state = this.sessions.get(sessionId);
    if (!state || state.closed || session.isClosed) return;
    state.blocked = false;
    state.blockedBytes = 0;
    while (state.queue.length > 0) {
      const next = state.queue.shift()!;
      state.queuedBytes -= next.length;
      if (duplex.send(next) === false) {
        state.blocked = true;
        state.blockedBytes = next.length + state.queuedBytes;
        return;
      }
    }
  }

  private releaseSession(sessionId: string): void {
    const state = this.sessions.get(sessionId);
    if (!state) return;
    state.closed = true;
    state.queue.length = 0;
    state.queuedBytes = 0;
    state.blockedBytes = 0;
    this.sessions.delete(sessionId);
  }

  authenticate(token: string): string | null {
    if (Object.keys(this.tenants).length === 0) return "default";
    // Object.hasOwn guards against prototype-chain pollution ("__proto__",
    // "constructor", ...) letting a client impersonate any tenant.
    return Object.hasOwn(this.tenants, token) ? this.tenants[token]! : null;
  }

  /** Atomically replace one tenant token without touching active sessions.

   * Already-authenticated sessions retain their resolved tenant; only new
   * handshakes must present `newToken`. The old token is removed first so a
   * concurrent authentication can never see both tokens valid.
   */
  rotateToken(oldToken: string, newToken: string, tenant: string): void {
    const current = this.authenticate(oldToken);
    if (current !== tenant) throw new Error(`token does not map to tenant ${tenant}`);
    if (oldToken === newToken) return;
    if (Object.hasOwn(this.tenants, newToken)) {
      throw new Error(`new token ${newToken} already maps to tenant ${this.tenants[newToken]}`);
    }
    delete this.tenants[oldToken];
    this.tenants[newToken] = tenant;
  }

  private errorFrame(message: string): Uint8Array {
    return encodeFrame(
      encodeValue({
        version: 1,
        id: makeId(),
        kind: "response",
        method: "DRIVER_HELLO",
        payload: { error: message },
      }),
    );
  }

  private emitEvent(event: GatewayEvent): void {
    for (const cb of this.eventCbs) cb(event);
  }
}
