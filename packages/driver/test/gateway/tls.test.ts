import { describe, expect, it } from "vitest";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RgpTcpServer, connectTcp } from "../../src/gateway/index.js";
import { RgpSession } from "../../src/session.js";

/**
 * M3-F9 TLS: the gateway can serve over a real TLS socket; the client trusts
 * the self-signed certificate via `ca` (rejectUnauthorized:false is only used
 * because the cert is self-signed — production uses a CA-signed cert).
 */

const opensslAvailable = ((): boolean => {
  try {
    execFileSync("openssl", ["version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

function makeSelfSignedCert(dir: string): { key: string; cert: string } {
  execFileSync("openssl", [
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-keyout",
    join(dir, "key.pem"),
    "-out",
    join(dir, "cert.pem"),
    "-days",
    "1",
    "-nodes",
    "-subj",
    "/CN=localhost",
  ]);
  return {
    key: readFileSync(join(dir, "key.pem"), "utf8"),
    cert: readFileSync(join(dir, "cert.pem"), "utf8"),
  };
}

describe("RGP/1 TLS gateway (M3-F9)", () => {
  it.skipIf(!opensslAvailable)("serves requests over TLS with a self-signed cert", async () => {
    const dir = mkdtempSync(join(tmpdir(), "rgp-tls-"));
    try {
      const { key, cert } = makeSelfSignedCert(dir);
      const server = new RgpTcpServer(0, {
        tenants: { "tok-a": "tenantA" },
        tls: { key, cert },
      });
      server.onRequest(((method, payload, tenant) => {
        if (method === "GET_STATE") return { values: { who: tenant } };
        return { ok: true };
      }) as never);
      await server.listen();

      const transport = await connectTcp(server.port, "tok-a", "127.0.0.1", {
        tls: { ca: cert, rejectUnauthorized: false },
      });
      const client = new RgpSession({
        send: (bytes) => transport.send(bytes),
        onClose: () => transport.close(),
      });
      transport.onData((bytes) => client.ingest(bytes));
      const state = (await client.request("GET_STATE", {})) as { values: { who: string } };
      expect(state.values.who).toBe("tenantA");
      client.close();
      await server.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
