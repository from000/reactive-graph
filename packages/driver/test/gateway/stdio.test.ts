import { describe, expect, it } from "vitest";
import { PassThrough } from "node:stream";
import { startStdioGateway } from "../../src/gateway/index.js";
import {
  FrameDecoder,
  decodeValue,
  encodeFrame,
  encodeValue,
  makeId,
} from "@reactivegraph/protocol";

/**
 * M3-F9 stdio transport: gateway over injectable stdin/stdout streams, with
 * the same tenant-token authentication and session semantics as TCP.
 */
function wire(tenants?: Record<string, string>) {
  // server stdin  <-- client writes; server stdout --> client reads
  const serverIn = new PassThrough();
  const serverOut = new PassThrough();
  const promise = startStdioGateway({ tenants, stdin: serverIn, stdout: serverOut });
  const send = (bytes: Uint8Array): void => serverIn.write(Buffer.from(bytes));
  const clientFrames: unknown[] = [];
  const decoder = new FrameDecoder();
  serverOut.on("data", (chunk) => {
    for (const frame of decoder.push(new Uint8Array(chunk))) {
      clientFrames.push(decodeValue(frame));
    }
  });
  return { promise, send, clientFrames };
}

function hello(token: string): Uint8Array {
  return encodeFrame(
    encodeValue({
      version: 1,
      id: makeId(),
      kind: "request",
      method: "DRIVER_HELLO",
      payload: { token },
    }),
  );
}

describe("RGP/1 stdio gateway (M3-F9)", () => {
  it("authenticates a valid token and opens a session", async () => {
    const { promise, send } = wire({ "token-a": "tenantA" });
    send(hello("token-a"));
    const session = await promise;
    expect(session).not.toBeNull();
    expect(session!.tenant).toBe("tenantA");
    session!.close();
  });

  it("rejects an unknown token with an error frame and no session", async () => {
    const { promise, send, clientFrames } = wire({ "token-a": "tenantA" });
    send(hello("bad"));
    const session = await promise;
    expect(session).toBeNull();
    await waitFor(() => clientFrames.length >= 1);
    const err = clientFrames[0] as { payload?: { error?: string } };
    expect(err.payload?.error).toBe("authentication failed");
  });

  it("pins the token from configuration (no frame token needed)", async () => {
    const serverIn = new PassThrough();
    const serverOut = new PassThrough();
    const promise = startStdioGateway({
      pinnedToken: "token-a",
      tenants: { "token-a": "tenantA" },
      stdin: serverIn,
      stdout: serverOut,
    });
    const session = await promise;
    expect(session!.tenant).toBe("tenantA");
    session!.close();
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
