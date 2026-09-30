/**
 * TCP transport for the RGP/1 gateway (M3-F9).
 *
 * Production-grade real transport over `node:net`: a server that accepts TCP
 * sockets, authenticates the first frame (DRIVER_HELLO carrying a bearer
 * token), and hands the connection to `RgpGateway`; plus a client transport
 * for `RemoteClient`. Frames use the standard RGP/1 framing, so any RGP/1
 * peer interoperates.
 */

import net from "node:net";
import tls from "node:tls";
import {
  decodeValue,
  encodeFrame,
  encodeValue,
  isEnvelope,
  makeId,
  type Envelope,
  type Method,
} from "@reactivegraph/protocol";
import { RgpGateway, type Duplex } from "./transport.js";

/** Socket adapter implementing the gateway Duplex. */
class SocketDuplex implements Duplex {
  private dataCb?: (bytes: Uint8Array) => void;
  private closeCb?: (err?: Error) => void;

  constructor(private readonly socket: net.Socket) {
    socket.on("error", (err) => this.closeCb?.(err));
    socket.on("close", () => this.closeCb?.());
    socket.on("end", () => this.closeCb?.());
  }

  ingest(chunk: Uint8Array): void {
    this.dataCb?.(chunk);
  }

  send(bytes: Uint8Array): boolean {
    if (this.socket.destroyed) return true;
    return this.socket.write(Buffer.from(bytes));
  }

  close(err?: Error): void {
    if (err) this.socket.destroy(err);
    else this.socket.end();
  }

  onData(cb: (bytes: Uint8Array) => void): void {
    this.dataCb = cb;
  }

  onClose(cb: (err?: Error) => void): void {
    this.closeCb = cb;
  }
  onDrain(cb: () => void): void {
    this.socket.on("drain", cb);
  }
}

/** Extract the bearer token from a DRIVER_HELLO payload. */
export function extractToken(payload: unknown): string {
  if (typeof payload !== "object" || payload === null) return "";
  const p = payload as Record<string, unknown>;
  const auth = p["auth"];
  if (typeof auth === "string" && auth.startsWith("Bearer ")) {
    return auth.slice("Bearer ".length);
  }
  if (typeof auth === "object" && auth !== null) {
    const t = (auth as Record<string, unknown>)["token"];
    if (typeof t === "string") return t;
  }
  return typeof p["token"] === "string" ? p["token"] : "";
}

/**
 * RGP/1 TCP server: accepts connections, authenticates the first frame
 * (DRIVER_HELLO with a token), then hosts each session in the gateway.
 * Unauthenticated peers get an error frame and the socket is closed.
 */
export interface RgpTcpServerOptions {
  readonly tenants?: Record<string, string>;
  /** Maximum bytes queued behind a backpressured socket before eviction. */
  readonly highWaterMark?: number;
  /** TLS server options (cert/key/...). When set, the listener accepts TLS
   * connections (a TLSSocket is still a net.Socket, so the same handshake
   * and session logic applies). */
  readonly tls?: Parameters<typeof tls.createServer>[0];
}

export class RgpTcpServer {
  private readonly gateway: RgpGateway;
  private readonly server: net.Server;
  private readonly sockets = new Set<net.Socket>();
  private boundPort = 0;

  constructor(port: number, opts: RgpTcpServerOptions = {}) {
    this.gateway = new RgpGateway({
      tenants: opts.tenants,
      highWaterMark: opts.highWaterMark,
    });
    const onConnection = (socket: net.Socket): void => this.acceptSocket(socket);
    this.server = opts.tls
      ? (tls.createServer(opts.tls, (socket) => onConnection(socket)) as unknown as net.Server)
      : net.createServer(onConnection);
    this.server.on("error", () => {
      /* peer resets are expected; the per-socket handler owns errors */
    });
    this.boundPort = port;
  }

  /** The bound port (useful with port 0 = ephemeral). */
  get port(): number {
    return this.boundPort;
  }

  onRequest(handler: (method: Method, payload: unknown, tenant: string) => unknown): void {
    this.gateway.onRequest(handler as never);
  }

  onEvent(
    cb: (event: {
      kind: "connected" | "disconnected" | "cancelled";
      sessionId: string;
      tenant: string;
      runId?: string;
    }) => void,
  ): void {
    this.gateway.onEvent(cb);
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.boundPort, "127.0.0.1", () => {
        const addr = this.server.address() as net.AddressInfo;
        this.boundPort = addr.port;
        resolve();
      });
    });
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    this.sockets.clear();
    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
      // server.close stalls if a socket lingers; destroy as a fallback.
      setTimeout(() => resolve(), 100).unref?.();
    });
  }

  private acceptSocket(socket: net.Socket): void {
    this.sockets.add(socket);
    socket.on("close", () => this.sockets.delete(socket));
    const duplex = new SocketDuplex(socket);
    // Manual frame scan over raw bytes: handshake frames and follow-up
    // requests may share one TCP segment, so we keep the unconsumed raw
    // bytes and feed them to the session once authenticated. Frame length is
    // bounded (FrameDecoder enforces the 32MB cap) and handshake completes
    // within HANDSHAKE_TIMEOUT_MS.
    let pre = Buffer.alloc(0);
    let handshaken = false;
    let bufferBytes = 0;
    const MAX_HANDSHAKE_BYTES = 64 * 1024 * 1024; // far above a real hello
    const handshakeTimer = setTimeout(() => socket.destroy(), 15_000);
    socket.on("data", (chunk) => {
      try {
        if (!handshaken) {
          pre = Buffer.concat([pre, Buffer.from(chunk)]);
          bufferBytes += chunk.length;
          if (bufferBytes > MAX_HANDSHAKE_BYTES) {
            socket.destroy();
            return;
          }
          while (pre.length >= 4) {
            const len = pre.readUInt32BE(0);
            if (len > MAX_HANDSHAKE_BYTES) {
              socket.destroy();
              return;
            }
            if (pre.length < 4 + len) return;
            const frame = pre.subarray(4, 4 + len);
            pre = pre.subarray(4 + len);
            bufferBytes -= 4 + len;
            const env = decodeValue(frame) as Envelope;
            if (!isEnvelope(env) || env.method !== "DRIVER_HELLO") {
              socket.destroy();
              return;
            }
            clearTimeout(handshakeTimer);
            const token = extractToken(env.payload);
            if (this.gateway.accept(duplex, token) === null) {
              // gateway already sent the auth error frame.
              socket.destroy();
              return;
            }
            handshaken = true;
            // Feed any request frames that arrived in the same segment.
            if (pre.length > 0) duplex.ingest(new Uint8Array(pre));
            return;
          }
          return;
        }
        duplex.ingest(new Uint8Array(chunk));
      } catch {
        // Malformed frames during handshake must not crash the process.
        socket.destroy();
      }
    });
    socket.on("error", () => socket.destroy());
  }
}

export { SocketDuplex };

/** Byte transport for a TCP client (usable by RemoteClient). */
export interface RemoteTcpTransport {
  send(bytes: Uint8Array): void;
  onData(cb: (bytes: Uint8Array) => void): void;
  onClose(cb: (err?: Error) => void): void;
  close(): void;
}

export interface TcpClientOptions {
  /** TLS options for the client connection (e.g. ca for the self-signed
   * certificate; set rejectUnauthorized:false only for tests). */
  readonly tls?: { ca?: string; rejectUnauthorized?: boolean };
}

/**
 * Connect to an RgpTcpServer as a client, perform the token handshake, and
 * return a RemoteTcpTransport whose frames flow over the live socket.
 */
export async function connectTcp(
  port: number,
  token: string,
  host = "127.0.0.1",
  opts: TcpClientOptions = {},
): Promise<RemoteTcpTransport> {
  const tlsOpts = opts.tls;
  const socket: net.Socket = tlsOpts
    ? tls.connect({
        port,
        host,
        ca: tlsOpts.ca,
        rejectUnauthorized: tlsOpts.rejectUnauthorized ?? true,
      })
    : net.connect({ port, host });
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  const transport: RemoteTcpTransport = {
    send: (bytes) => socket.write(Buffer.from(bytes)),
    onData: (cb) => socket.on("data", (chunk) => cb(new Uint8Array(chunk))),
    onClose: (cb) => {
      socket.on("close", () => cb?.());
      socket.on("error", (err) => cb?.(err));
    },
    close: () => socket.end(),
  };
  socket.write(
    Buffer.from(
      encodeFrame(
        encodeValue({
          version: 1,
          id: makeId(),
          kind: "request",
          method: "DRIVER_HELLO",
          payload: { token },
        }),
      ),
    ),
  );
  return transport;
}
