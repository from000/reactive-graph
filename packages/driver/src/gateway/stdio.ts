/**
 * stdio transport for the RGP/1 gateway (M3-F9).
 *
 * The Driver's canonical process transport is stdio (a host child process);
 * this server adapts stdin/stdout into the same gateway Duplex + session the
 * TCP server uses, so a Driver can be hosted as a gateway child process with
 * identical tenant-token authentication and request handling.
 *
 * The stdio streams are injectable for tests (PassThrough), defaulting to
 * process.stdin/stdout. Authentication follows the TCP convention: the first
 * frame must be DRIVER_HELLO carrying a bearer token (or the token may be
 * pinned via the REACTIVEGRAPH_TOKEN env var for process-scoped deployments).
 */

import { Readable, Writable } from "node:stream";
import {
  decodeValue,
  encodeFrame,
  encodeValue,
  isEnvelope,
  makeId,
  type Envelope,
} from "@reactivegraph/protocol";
import { RgpGateway, type Duplex } from "./transport.js";
import { extractToken } from "./tcp.js";

const MAX_FRAME_BYTES = 64 * 1024 * 1024;
const HANDSHAKE_TIMEOUT_MS = 15_000;

class StreamDuplex implements Duplex {
  private dataCb?: (bytes: Uint8Array) => void;
  private closeCb?: (err?: Error) => void;

  constructor(
    private readonly input: Readable,
    private readonly output: Writable,
  ) {}

  ingest(chunk: Uint8Array): void {
    this.dataCb?.(chunk);
  }

  send(bytes: Uint8Array): boolean {
    if (this.output.destroyed) return true;
    return this.output.write(Buffer.from(bytes));
  }

  close(err?: Error): void {
    this.closeCb?.(err);
  }

  onData(cb: (bytes: Uint8Array) => void): void {
    this.dataCb = cb;
  }

  onClose(cb: (err?: Error) => void): void {
    this.closeCb = cb;
  }
  onDrain(cb: () => void): void {
    this.output.on("drain", cb);
  }
}

export interface StdioGatewayOptions {
  /** Bearer tokens -> tenant id. Empty map = any token → "default". */
  tenants?: Record<string, string>;
  /** Pin the token for process-scoped stdio (skips frame token check). */
  pinnedToken?: string;
  /** Maximum bytes queued behind a backpressured stdout before eviction. */
  highWaterMark?: number;
  /** Test hooks: defaults to process.stdin / process.stdout. */
  stdin?: Readable;
  stdout?: Writable;
}

/**
 * Host an RGP/1 gateway session over stdio. Resolves once the handshake frame
 * (DRIVER_HELLO) authenticates; returns the authenticated tenant (or null on
 * auth failure, after writing the error frame). Frames after the handshake are
 * fed to the gateway session.
 */
export async function startStdioGateway(
  opts: StdioGatewayOptions = {},
): Promise<{ tenant: string; close: () => void } | null> {
  const gateway = new RgpGateway({
    tenants: opts.tenants ?? {},
    highWaterMark: opts.highWaterMark,
  });
  const input = opts.stdin ?? process.stdin;
  const output = opts.stdout ?? process.stdout;
  const duplex = new StreamDuplex(input, output);

  const token =
    opts.pinnedToken ??
    (await readHelloToken(input, output, MAX_FRAME_BYTES, HANDSHAKE_TIMEOUT_MS));
  if (token === null) {
    // Handshake failed (timeout / malformed / non-hello first frame): write
    // an auth failure frame and refuse the session.
    output.write(
      Buffer.from(
        encodeFrame(
          encodeValue({
            version: 1,
            id: makeId(),
            kind: "response",
            method: "DRIVER_HELLO",
            payload: { error: "authentication failed" },
          }),
        ),
      ),
    );
    return null;
  }
  const peer = gateway.accept(duplex, token);
  if (peer === null) {
    output.write(
      Buffer.from(
        encodeFrame(
          encodeValue({
            version: 1,
            id: makeId(),
            kind: "response",
            method: "DRIVER_HELLO",
            payload: { error: "authentication failed" },
          }),
        ),
      ),
    );
    return null;
  }
  // Feed all subsequent bytes to the session through the duplex.
  input.on("data", (chunk) => duplex.ingest(new Uint8Array(chunk)));
  return { tenant: peer.tenant, close: () => peer.close() };
}

export { RgpGateway };

/** Read exactly one DRIVER_HELLO frame from a stream and return its token. */
async function readHelloToken(
  input: Readable,
  output: Writable,
  maxFrameBytes: number,
  timeoutMs: number,
): Promise<string | null> {
  return await new Promise<string | null>((resolve, _reject) => {
    let pre = Buffer.alloc(0);
    let bufferBytes = 0;
    const timer = setTimeout(() => {
      cleanup();
      resolve(null);
    }, timeoutMs);
    const onData = (chunk: Buffer | string): void => {
      try {
        pre = Buffer.concat([pre, Buffer.from(chunk)]);
        bufferBytes += chunk.length;
        if (bufferBytes > maxFrameBytes) {
          cleanup();
          resolve(null);
          return;
        }
        while (pre.length >= 4) {
          const len = pre.readUInt32BE(0);
          if (len > maxFrameBytes) {
            cleanup();
            resolve(null);
            return;
          }
          if (pre.length < 4 + len) return;
          const frame = pre.subarray(4, 4 + len);
          const env = decodeValue(frame) as Envelope;
          if (!isEnvelope(env) || env.method !== "DRIVER_HELLO") {
            cleanup();
            resolve(null);
            return;
          }
          cleanup();
          resolve(extractToken(env.payload));
          return;
        }
      } catch {
        // Malformed handshake frame must not crash the process.
        cleanup();
        resolve(null);
      }
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      input.off("data", onData);
    };
    timer.unref?.();
    input.on("data", onData);
  });
}
