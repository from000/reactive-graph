import { describe, expect, it } from "vitest";
import { RgpSession } from "../src/session.js";
import { InMemoryDuplex, RgpGateway, type Duplex } from "../src/gateway/index.js";
import { DriverLink } from "../src/gateway/link.js";

function wirePair(): { a: InMemoryDuplex; b: InMemoryDuplex } {
  const a = new InMemoryDuplex();
  const b = new InMemoryDuplex();
  a.connect(b);
  b.connect(a);
  return { a, b };
}

describe("RgpGateway (Task 11 / D.9.98-99)", () => {
  it("authenticates a token to a tenant and emits connected events", () => {
    const gateway = new RgpGateway({ tenants: { "tok-a": "tenant-a" } });
    const events: string[] = [];
    gateway.onEvent((e) => events.push(e.kind));

    const { a, b } = wirePair();
    const peer = gateway.accept(a, "tok-a");
    expect(peer).not.toBeNull();
    expect(peer!.tenant).toBe("tenant-a");
    expect(events).toEqual(["connected"]);
    peer!.close();
    void b;
  });

  it("rejects an unknown token", () => {
    const gateway = new RgpGateway({ tenants: { "tok-a": "tenant-a" } });
    const { a, b } = wirePair();
    const peer = gateway.accept(a, "bad-token");
    expect(peer).toBeNull();
    void b;
  });

  it("defaults to a single tenant when no tenant map is given", () => {
    const gateway = new RgpGateway();
    const { a } = wirePair();
    const peer = gateway.accept(a, "anything");
    expect(peer!.tenant).toBe("default");
    peer!.close();
  });

  it("propagates cancellation as a cancel event", () => {
    const gateway = new RgpGateway();
    const events: string[] = [];
    gateway.onEvent((e) => events.push(e.kind));
    const { a } = wirePair();
    const peer = gateway.accept(a, "t")!;
    peer.cancel("run-1");
    expect(events).toContain("cancelled");
    peer.close();
  });

  it("backpressures writes above the high-water mark without crashing", () => {
    const gateway = new RgpGateway({ highWaterMark: 64 });
    const { a } = wirePair();
    const peer = gateway.accept(a, "t")!;
    // emit more bytes than the HWM; the duplex stalls via setImmediate
    for (let i = 0; i < 20; i++) peer.emit("event", "STREAM_EVENT", { n: i });
    expect(peer).toBeTruthy();
    peer.close();
  });

  it("round-trips a request through the gateway + DriverLink handler", async () => {
    const gateway = new RgpGateway();
    new DriverLink(gateway, {
      run: (p) => ({ runId: "r1", output: (p as { input: number }).input * 2, state: {} }),
    });
    const { a, b } = wirePair();
    const peer = gateway.accept(a, "t")!;

    // b is the client side session
    const client = new RgpSession({ send: (bytes) => b.send(bytes), onClose: () => undefined });
    b.onData((chunk) => client.ingest(chunk));

    const result = (await client.request("RUN", { input: 21 })) as { output: number };
    expect(result.output).toBe(42);
    client.close();
    peer.close();
  });

  it("surfaces server handler errors to the caller", async () => {
    const gateway = new RgpGateway();
    new DriverLink(gateway, {
      run: () => {
        throw new Error("boom");
      },
    });
    const { a, b } = wirePair();
    const peer = gateway.accept(a, "t")!;
    const client = new RgpSession({ send: (bytes) => b.send(bytes) });
    b.onData((chunk) => client.ingest(chunk));
    await expect(client.request("RUN", {})).rejects.toThrow(/boom/);
    client.close();
    peer.close();
  });
});

describe("gateway auth token rotation", () => {
  it("rotates tokens without disconnecting already-authenticated sessions", async () => {
    const gateway = new RgpGateway({ tenants: { "old-token": "tenant-a" } });
    const runTenants: string[] = [];
    gateway.onRequest((method, payload, tenant) => {
      void method;
      void payload;
      runTenants.push(tenant);
      return { ok: true };
    });
    const { a, b } = wirePair();
    const peer = gateway.accept(a, "old-token")!;
    const client = new RgpSession({ send: (bytes) => b.send(bytes), onClose: () => undefined });
    b.onData((chunk) => client.ingest(chunk));
    gateway.rotateToken("old-token", "new-token", "tenant-a");
    expect(gateway.authenticate("old-token")).toBeNull();
    expect(gateway.authenticate("new-token")).toBe("tenant-a");
    const result = await client.request("GET_STATE", {});
    expect(result).toEqual({ ok: true });
    expect(runTenants).toEqual(["tenant-a"]);
    client.close();
    peer.close();
  });

  it("refuses to steal a token already owned by another tenant", () => {
    const gateway = new RgpGateway({
      tenants: { "old-token": "tenant-a", shared: "tenant-b" },
    });
    expect(() => gateway.rotateToken("old-token", "shared", "tenant-a")).toThrow(
      /already maps to tenant tenant-b/,
    );
    expect(gateway.authenticate("old-token")).toBe("tenant-a");
    expect(gateway.authenticate("shared")).toBe("tenant-b");
  });

  it("never leaves zero or two valid tokens across an interleaved rotation storm", () => {
    const gateway = new RgpGateway({
      tenants: { "tok-a-0": "tenant-a", "tok-b": "tenant-b" },
    });
    let current = "tok-a-0";
    for (let i = 1; i <= 200; i++) {
      const next = `tok-a-${i}`;
      // Authentication interleaved with each atomic rotation must always
      // resolve to tenant-a for exactly one of the two adjacent tokens.
      const oldBefore = gateway.authenticate(current);
      const newBefore = gateway.authenticate(next);
      gateway.rotateToken(current, next, "tenant-a");
      const oldAfter = gateway.authenticate(current);
      const newAfter = gateway.authenticate(next);
      expect([oldBefore, newBefore]).toEqual(["tenant-a", null]);
      expect([oldAfter, newAfter]).toEqual([null, "tenant-a"]);
      expect(gateway.authenticate("tok-b")).toBe("tenant-b");
      current = next;
    }
  });

  it("keeps prototype-shaped tokens as ordinary credentials", () => {
    const gateway = new RgpGateway({ tenants: { "old-token": "tenant-a" } });
    gateway.rotateToken("old-token", "__proto__", "tenant-a");
    expect(gateway.authenticate("__proto__")).toBe("tenant-a");
    expect(Object.getPrototypeOf({})).toBe(Object.prototype);
  });
});

/** Scripted transport that reports backpressure and exposes its drain hook. */
class ScriptedDuplex implements Duplex {
  readonly sent: Uint8Array[] = [];
  accepting = true;
  closedWith?: Error;
  private dataCb?: (bytes: Uint8Array) => void;
  private closeCb?: (err?: Error) => void;
  private drainCb?: () => void;

  ingest(chunk: Uint8Array): void {
    this.dataCb?.(chunk);
  }
  send(bytes: Uint8Array): boolean {
    this.sent.push(bytes);
    return this.accepting;
  }
  close(err?: Error): void {
    this.closedWith = err;
    this.closeCb?.(err);
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
  drain(): void {
    this.accepting = true;
    this.drainCb?.();
  }
}

describe("gateway slow-consumer backpressure (P6.1)", () => {
  it("stalls queued writes while the transport is backpressured and flushes on drain", () => {
    const gateway = new RgpGateway({ highWaterMark: 512 });
    const duplex = new ScriptedDuplex();
    const peer = gateway.accept(duplex, "t")!;

    peer.emit("event", "STREAM_EVENT", { n: 1 });
    expect(duplex.sent).toHaveLength(1);
    duplex.accepting = false; // the next socket write reports backpressure
    peer.emit("event", "STREAM_EVENT", { n: 2 });
    peer.emit("event", "STREAM_EVENT", { n: 3 });

    expect(duplex.sent).toHaveLength(2); // chunk 2 accepted into the socket buffer
    expect(gateway.metrics()).toMatchObject({
      sessions: 1,
      blockedSessions: 1,
      evictedSlowConsumers: 0,
    });
    expect(gateway.metrics().queuedBytes).toBeGreaterThan(0);

    duplex.drain(); // the reader caught up
    expect(duplex.sent).toHaveLength(3); // chunk 3 flushed, nothing dropped/reordered
    expect(gateway.metrics()).toMatchObject({ blockedSessions: 0, queuedBytes: 0 });
    peer.close();
  });

  it("evicts a slow consumer once its bounded outbound queue would overflow", () => {
    const gateway = new RgpGateway({ highWaterMark: 64 });
    const duplex = new ScriptedDuplex();
    duplex.accepting = false;
    const peer = gateway.accept(duplex, "t")!;

    for (let i = 0; i < 10 && !duplex.closedWith; i++) {
      peer.emit("event", "STREAM_EVENT", { payload: "x".repeat(64) });
    }

    expect(duplex.closedWith?.name).toBe("SlowConsumerError");
    expect(gateway.metrics()).toMatchObject({
      sessions: 0,
      queuedBytes: 0,
      evictedSlowConsumers: 1,
    });
    expect(duplex.sent).toHaveLength(1); // the first write was already given to the transport
  });

  it("removes sessions from metrics when the transport disconnects", () => {
    const gateway = new RgpGateway();
    const duplex = new ScriptedDuplex();
    gateway.accept(duplex, "t");
    expect(gateway.metrics().sessions).toBe(1);
    duplex.close(new Error("peer vanished"));
    expect(gateway.metrics().sessions).toBe(0);
  });
});

describe("gateway per-tenant rate limits", () => {
  it("limits concurrent requests per tenant independently", async () => {
    const gateway = new RgpGateway({
      tenants: { "tok-a": "tenant-a", "tok-b": "tenant-b" },
      perTenantConcurrency: 1,
    });
    const release = new Map<string, (() => void)[]>();
    gateway.onRequest((_method, _payload, tenant) => {
      return new Promise((resolve) => {
        release.set(tenant, [...(release.get(tenant) ?? []), () => resolve({ tenant })]);
      });
    });
    const { a, b } = wirePair();
    const peerA = gateway.accept(a, "tok-a")!;
    const clientA = new RgpSession({ send: (bytes) => b.send(bytes) });
    b.onData((chunk) => clientA.ingest(chunk));
    const first = clientA.request("GET_STATE", {});
    await new Promise((resolve) => setTimeout(resolve, 10));
    await expect(clientA.request("GET_STATE", {})).rejects.toThrow(
      /tenant tenant-a concurrency limit exceeded/,
    );
    (release.get("tenant-a") ?? [])[0]?.();
    const result = (await first) as { tenant: string };
    expect(result.tenant).toBe("tenant-a");
    clientA.close();
    peerA.close();
  });

  it("counts concurrent in-flight requests atomically under a burst", async () => {
    const gateway = new RgpGateway({
      tenants: { "tok-a": "tenant-a" },
      perTenantConcurrency: 3,
    });
    let started = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    gateway.onRequest(async () => {
      started += 1;
      await gate;
      return { ok: true };
    });
    const { a, b } = wirePair();
    const peerA = gateway.accept(a, "tok-a")!;
    const clientA = new RgpSession({ send: (bytes) => b.send(bytes) });
    b.onData((chunk) => clientA.ingest(chunk));
    const burst = Array.from({ length: 12 }, () => clientA.request("GET_STATE", {}));
    // The rejection path is asserted below; attach handlers immediately so a
    // fast response cannot surface as an unhandled rejection first.
    const settledPromise = Promise.allSettled(burst);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(started).toBe(3);
    release();
    const settled = await settledPromise;
    expect(settled.filter((r) => r.status === "fulfilled")).toHaveLength(3);
    expect(settled.filter((r) => r.status === "rejected")).toHaveLength(9);
    clientA.close();
    peerA.close();
  });
});
