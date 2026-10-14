import { describe, expect, it } from "vitest";
import net from "node:net";
import { RgpTcpServer, connectTcp } from "../../src/gateway/index.js";
import { RgpSession } from "../../src/session.js";
import {
  FrameDecoder,
  decodeValue,
  encodeFrame,
  encodeValue,
  makeId,
} from "@reactivegraph/protocol";

/**
 * M3-F9: real TCP transport + tenant token authentication, end to end.
 * A live socket connects, authenticates via the DRIVER_HELLO token, then
 * executes RGP/1 requests through the gateway session.
 */
describe("RGP/1 TCP gateway (M3-F9)", () => {
  it("authenticates a valid token and routes requests to the handler", async () => {
    const server = new RgpTcpServer(0, { tenants: { "token-a": "tenantA" } });
    const seen: Array<[string, string]> = [];
    server.onRequest(((method, payload, tenant) => {
      seen.push([method as string, tenant]);
      return { values: { who: tenant } };
    }) as never);
    await server.listen();
    try {
      const transport = await connectTcp(server.port, "token-a");
      const frames: unknown[] = [];
      transport.onData((bytes) => {
        for (const frame of new FrameDecoder().push(bytes)) {
          frames.push(decodeValue(frame));
        }
      });
      transport.send(
        encodeFrame(
          encodeValue({
            version: 1,
            id: makeId(),
            kind: "request",
            method: "GET_STATE",
            payload: {},
          }),
        ),
      );
      await waitFor(() => frames.length >= 1);
      expect(seen.some(([m]) => m === "GET_STATE")).toBe(true);
      expect(seen.find(([m]) => m === "GET_STATE")?.[1]).toBe("tenantA");
      transport.close();
    } finally {
      await server.close();
    }
  });

  it("rejects an unknown token with an error frame and closes", async () => {
    const server = new RgpTcpServer(0, { tenants: { "token-a": "tenantA" } });
    let connected = false;
    server.onEvent((e) => {
      if (e.kind === "connected") connected = true;
    });
    await server.listen();
    try {
      const socket = net.connect({ port: server.port, host: "127.0.0.1" });
      const decoder = new FrameDecoder();
      const responses: unknown[] = [];
      socket.on("data", (chunk) => {
        for (const frame of decoder.push(new Uint8Array(chunk))) {
          responses.push(decodeValue(frame));
        }
      });
      await new Promise<void>((resolve) => socket.once("connect", resolve));
      socket.write(
        Buffer.from(
          encodeFrame(
            encodeValue({
              version: 1,
              id: makeId(),
              kind: "request",
              method: "DRIVER_HELLO",
              payload: { token: "bad" },
            }),
          ),
        ),
      );
      await waitFor(() => responses.length >= 1);
      expect(connected).toBe(false);
      const err = responses[0] as { payload?: { error?: string } };
      expect(err.payload?.error).toBe("authentication failed");
      socket.destroy();
    } finally {
      await server.close();
    }
  });

  it("serves compile + run over a session-shaped client (RemoteClient parity)", async () => {
    const server = new RgpTcpServer(0, { tenants: { "token-a": "tenantA" } });
    server.onRequest(((method, payload, tenant) => {
      if (method === "COMPILE_GRAPH") return { graphId: "g1" };
      if (method === "RUN") return { runId: "r1", output: payload, state: { tenant } };
      return { ok: true };
    }) as never);
    await server.listen();
    try {
      const transport = await connectTcp(server.port, "token-a");
      const client = new RgpSession({
        send: (bytes) => transport.send(bytes),
        onClose: () => transport.close(),
      });
      transport.onData((chunk) => client.ingest(chunk));
      const compile = (await client.request("COMPILE_GRAPH", { id: "g" })) as { graphId: string };
      expect(compile.graphId).toBe("g1");
      const run = (await client.request("RUN", { graphId: "g1", input: { x: 1 } })) as {
        runId: string;
        state: Record<string, unknown>;
      };
      expect(run.runId).toBe("r1");
      expect(run.state).toEqual({ tenant: "tenantA" });
      client.close();
    } finally {
      await server.close();
    }
  });
});

function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = (): void => {
      if (cond()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error("timeout waiting for condition"));
      setTimeout(tick, 10);
    };
    tick();
  });
}

describe("RGP/1 TCP gateway hardening", () => {
  it("rejects oversized handshake frames without crashing the server", async () => {
    const server = new RgpTcpServer(0, { tenants: { token: "tenant" } });
    await server.listen();
    try {
      const socket = net.connect({ port: server.port, host: "127.0.0.1" });
      await new Promise<void>((resolve) => socket.once("connect", resolve));
      socket.write(Buffer.alloc(8));
      socket.on("error", () => undefined);
      socket.write(Buffer.alloc(80 * 1024 * 1024));
      await new Promise<void>((resolve) => socket.once("close", resolve));
      expect(server.port).toBeGreaterThan(0);
    } finally {
      await server.close();
    }
  });

  it("rejects malformed handshake frames without crashing the server", async () => {
    const server = new RgpTcpServer(0, { tenants: { token: "tenant" } });
    await server.listen();
    try {
      const socket = net.connect({ port: server.port, host: "127.0.0.1" });
      await new Promise<void>((resolve) => socket.once("connect", resolve));
      socket.write(Buffer.from([0, 0, 0, 2, 0xff, 0xfe]));
      await new Promise<void>((resolve) => socket.once("close", resolve));
      expect(server.port).toBeGreaterThan(0);
    } finally {
      await server.close();
    }
  });
});
