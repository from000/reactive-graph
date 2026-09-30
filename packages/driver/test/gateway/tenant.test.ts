import { describe, expect, it } from "vitest";
import { DriverRuntime, DriverLink, InMemoryDuplex, RgpGateway } from "../../src/index.js";
import { RgpSession } from "../../src/session.js";
import type { TaskExecutor, TaskSuccess } from "../../src/runtime.js";

/**
 * Gateway tenant isolation: with a per-tenant factory, each authenticated
 * tenant gets its own DriverRuntime — graph registry, thread state,
 * checkpoints and store are fully partitioned even for the same graphId and
 * threadId.
 */

function makeExecutor(): TaskExecutor {
  return {
    invokeTask: async (_cb, input) => {
      const value = (input as { n?: number }) ?? {};
      const n = value.n ?? 0;
      return {
        reads: [],
        writes: ["count"],
        patches: [{ path: ["count"], operation: "set", value: n }],
        return_value: { count: n },
        external_receipts: [],
      } satisfies TaskSuccess;
    },
  };
}

/** Runtime -> gateway handler set (thin adapter of the Driver surfaces). */
function runtimeHandlers(rt: DriverRuntime) {
  return {
    compileGraph: (def: { id?: string }) => {
      const id = def?.id ?? "g";
      rt.compileGraph({
        id,
        tasks: [{ id: "t", kind: "effect", callbackId: "t" }],
        routes: [{ event: "run", taskId: "t" }],
      });
      return { graphId: id };
    },
    run: (p: { graphId: string; input: unknown; threadId?: string }) =>
      rt.run(p.graphId, p.input ?? {}, { threadId: p.threadId ?? "default" }) as never,
    getState: async (p: { config: { threadId: string } }) => ({
      values: await rt.getState(p.config.threadId),
    }),
    resume: () => ({ runId: "r", state: {} }),
  };
}

function connect(gateway: RgpGateway, token: string): RgpSession {
  const serverSide = new InMemoryDuplex();
  const clientSide = new InMemoryDuplex();
  serverSide.connect(clientSide);
  clientSide.connect(serverSide);
  const peer = gateway.accept(serverSide, token);
  if (!peer) throw new Error("accept failed");
  const client = new RgpSession({
    send: (bytes) => clientSide.send(bytes), // send() delivers to the peer (server)
    onClose: () => clientSide.close(),
  });
  // Bytes arriving from the server are pushed into the client session.
  clientSide.onData((bytes) => client.ingest(bytes));
  return client;
}

describe("gateway tenant isolation (M3)", () => {
  it("partitions DriverRuntime per tenant: same graphId and threadId stay invisible across tenants", async () => {
    const gateway = new RgpGateway({ tenants: { "tok-a": "tenantA", "tok-b": "tenantB" } });
    // Each tenant gets its OWN DriverRuntime (factory called per tenant).
    new DriverLink(gateway, (_tenant) => {
      const rt = new DriverRuntime(makeExecutor(), {});
      return runtimeHandlers(rt);
    });

    const clientA = connect(gateway, "tok-a");
    const clientB = connect(gateway, "tok-b");

    // Both tenants wear the same graphId/threadId — the isolation point.
    const graphId = "shared-graph";
    await clientA.request("COMPILE_GRAPH", { id: graphId });
    await clientB.request("COMPILE_GRAPH", { id: graphId }); // no duplicate-graph clash

    await clientA.request("RUN", { graphId, input: { n: 1 }, threadId: "default" });
    await clientB.request("RUN", { graphId, input: { n: 99 }, threadId: "default" });

    const stateA = (await clientA.request("GET_STATE", { config: { threadId: "default" } })) as {
      values: { count: number };
    };
    const stateB = (await clientB.request("GET_STATE", { config: { threadId: "default" } })) as {
      values: { count: number };
    };
    expect(stateA.values.count).toBe(1);
    expect(stateB.values.count).toBe(99);
    // Cross-tenant leak would show both as 99 (or 1).
    expect(stateA.values).not.toEqual(stateB.values);
  });

  it("lazily creates one runtime per tenant and reuses it", async () => {
    const gateway = new RgpGateway({ tenants: { a: "tenantA" } });
    let factoryCalls = 0;
    new DriverLink(gateway, () => {
      factoryCalls += 1;
      return runtimeHandlers(new DriverRuntime(makeExecutor(), {}));
    });
    const client = connect(gateway, "a");
    await client.request("COMPILE_GRAPH", { id: "g" });
    await client.request("COMPILE_GRAPH", { id: "g2" });
    expect(factoryCalls).toBe(1); // cached after first request
  });
});
