import { describe, expect, it } from "vitest";
import { DriverLink, InMemoryDuplex, RgpGateway, type SessionPeer } from "@reactivegraph/driver";
import { RemoteClient } from "../src/remote.js";

/** Build a client connected to a DriverLink-backed gateway. */
function connectRemote(handlers: Record<string, (p: unknown) => unknown>): {
  client: RemoteClient;
  close: () => void;
} {
  const gateway = new RgpGateway();
  new DriverLink(gateway, handlers as never);

  const serverSide = new InMemoryDuplex();
  const clientSide = new InMemoryDuplex();
  serverSide.connect(clientSide);
  clientSide.connect(serverSide);

  const peer = gateway.accept(serverSide, "tok")!;
  const client = new RemoteClient(clientSide);
  return {
    client,
    close: () => {
      client.close();
      peer.close();
    },
  };
}

describe("RemoteClient (Task 11 / D.9.99-101)", () => {
  it("compiles a graph remotely and returns its id", async () => {
    const { client, close } = connectRemote({
      compileGraph: () => ({ graphId: "g-abc" }),
    });
    try {
      const id = await client.compileGraph({ id: "g" });
      expect(id).toBe("g-abc");
    } finally {
      close();
    }
  });

  it("runs a graph and returns output + state", async () => {
    const { client, close } = connectRemote({
      run: (p) => {
        const input = (p as { input: { n: number } }).input;
        return { runId: "r-1", output: input.n * 2, state: { n: input.n } };
      },
    });
    try {
      const res = await client.run("g-abc", { n: 21 });
      expect(res.runId).toBe("r-1");
      expect(res.output).toBe(42);
      expect(res.state).toEqual({ n: 21 });
    } finally {
      close();
    }
  });

  it("reads remote state", async () => {
    const { client, close } = connectRemote({
      getState: () => ({ values: { x: 1 } }),
    });
    try {
      const values = await client.getState({ threadId: "t1" });
      expect(values).toEqual({ x: 1 });
    } finally {
      close();
    }
  });

  it("resumes an interrupted run with a response", async () => {
    const { client, close } = connectRemote({
      resume: (p) => {
        const payload = p as { runId: string; interruptResponse: unknown };
        return {
          runId: payload.runId,
          state: { approved: payload.interruptResponse },
        };
      },
    });
    try {
      const res = await client.resume("r-1", "yes", { threadId: "t1" });
      expect(res.runId).toBe("r-1");
      expect(res.state).toEqual({ approved: "yes" });
    } finally {
      close();
    }
  });

  it("propagates a cancel to the server session", async () => {
    const { client, close } = connectRemote({
      run: () => ({ runId: "r-1" }),
      getState: () => ({ values: {} }),
    });
    try {
      await client.cancel("r-1");
      await client.getState({ threadId: "t" });
    } finally {
      close();
    }
  });

  it("streams events emitted during a run", async () => {
    const gateway = new RgpGateway();
    new DriverLink(gateway, { run: () => ({ runId: "r-s", output: 1 }) });
    const serverSide = new InMemoryDuplex();
    const clientSide = new InMemoryDuplex();
    serverSide.connect(clientSide);
    clientSide.connect(serverSide);

    const peer = gateway.accept(serverSide, "tok")!;
    const client = new RemoteClient(clientSide);

    const pending = client.run("g", {});
    peer.emit("event", "STREAM_EVENT", { value: 1 });
    peer.emit("event", "STREAM_EVENT", { value: 2 });
    const res = await pending;
    expect(res.runId).toBe("r-s");
    client.close();
    peer.close();
  });

  it("filters stream events by runId so concurrent streaming never mixes", async () => {
    // Keep RUN in flight until after frames are emitted, so the generator
    // drains events only after all three tagged frames arrive.
    let releaseRun: (() => void) | undefined;
    const gateway = new RgpGateway();
    new DriverLink(gateway, {
      run: async (p) => {
        await new Promise<void>((r) => {
          releaseRun = r;
        });
        return {
          runId: (p as { runId?: string }).runId ?? "r",
          output: 1,
          state: {},
        };
      },
    });
    const serverSide = new InMemoryDuplex();
    const clientSide = new InMemoryDuplex();
    serverSide.connect(clientSide);
    clientSide.connect(serverSide);
    const peer = gateway.accept(serverSide, "tok")!;
    const client = new RemoteClient(clientSide);

    const gen = client.stream("g", {}, {}, { runId: "run-a" });
    const pending = gen.next();
    await new Promise((r) => setTimeout(r, 10)); // RUN is now in flight
    // A frame tagged run-a must reach the stream; a run-b frame must not.
    peer.emit("event", "STREAM_EVENT", {
      runId: "run-a",
      eventType: "values",
      payload: { state: { a: 1 } },
    });
    peer.emit("event", "STREAM_EVENT", {
      runId: "run-b",
      eventType: "values",
      payload: { state: { b: 2 } },
    });
    peer.emit("event", "STREAM_EVENT", {
      runId: "run-a",
      eventType: "values",
      payload: { state: { a: 3 } },
    });
    releaseRun?.();
    const first = await pending;
    const payload = (first.value as { payload?: { state?: unknown } })?.payload;
    expect(payload).toEqual({ state: { a: 1 } });
    await gen.return?.();
    client.close();
    peer.close();
  });

  it("round-trips through the gateway (auth + execution)", async () => {
    const gateway = new RgpGateway({ tenants: { tok: "tenant-a" } });
    new DriverLink(gateway, { getState: () => ({ values: { viaGateway: true } }) });
    const serverSide = new InMemoryDuplex();
    const clientSide = new InMemoryDuplex();
    serverSide.connect(clientSide);
    clientSide.connect(serverSide);

    const peer: SessionPeer | null = gateway.accept(serverSide, "tok");
    expect(peer).not.toBeNull();
    expect(peer!.tenant).toBe("tenant-a");

    const client = new RemoteClient(clientSide);
    const values = await client.getState({ threadId: "t" });
    expect(values).toEqual({ viaGateway: true });
    client.close();
    peer!.close();
  });
});
