import { describe, expect, it } from "vitest";
import { createPair } from "./helpers.js";

describe("RgpSession lifecycle", () => {
  it("correlates out-of-order responses to the right callers", async () => {
    const { server, client } = createPair();
    const p1 = client.request("RUN", { n: 1 });
    const p2 = client.request("RUN", { n: 2 });
    await new Promise((r) => setTimeout(r, 10));
    const requests = server.drainRequests();
    expect(requests).toHaveLength(2);
    // Respond out of order: second request first.
    server.respond(requests[1]!, { result: 2 });
    server.respond(requests[0]!, { result: 1 });
    // Each caller must resolve with ITS OWN payload, regardless of arrival order.
    const [r1, r2] = await Promise.all([p1, p2]);
    expect((r1 as { result: number }).result).toBe(1);
    expect((r2 as { result: number }).result).toBe(2);
    server.close();
  });

  it("rejects a response whose method does not match the request", async () => {
    const { server, client } = createPair();
    const p = client.request("RUN", {});
    await new Promise((r) => setTimeout(r, 10));
    const [req] = server.drainRequests();
    // Respond with wrong method but same id.
    server.respondRaw({
      id: req!.id,
      method: "GET_STATE",
      kind: "response",
      version: 1,
      payload: {},
    });
    await expect(p).rejects.toThrow(/method mismatch/);
    server.close();
  });

  it("propagates a peer crash to all pending requests", async () => {
    const { server, client } = createPair();
    const p1 = client.request("RUN", { a: 1 });
    const p2 = client.request("RUN", { b: 2 });
    await new Promise((r) => setTimeout(r, 10));
    server.crash(new Error("driver crashed"));
    await expect(p1).rejects.toThrow("driver crashed");
    await expect(p2).rejects.toThrow("driver crashed");
    expect(client.isClosed).toBe(true);
  });

  it("supports cancelling a pending request", async () => {
    const { server, client } = createPair();
    const p = client.request("RUN", {});
    await new Promise((r) => setTimeout(r, 10));
    const pendingIds = [...(client as unknown as { pending: Map<string, unknown> }).pending.keys()];
    expect(pendingIds).toHaveLength(1);
    const cancelled = client.cancel(pendingIds[0]!);
    expect(cancelled).toBe(true);
    await expect(p).rejects.toThrow("cancelled");
    server.close();
  });

  it("rejects requests after close", async () => {
    const { server, client } = createPair();
    server.close();
    await expect(client.request("RUN", {})).rejects.toThrow("session closed");
  });
});
