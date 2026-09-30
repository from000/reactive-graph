/**
 * Test helpers: an in-memory pair of (TestServer, RgpSession client) over a
 * loopback byte channel, so session tests need no real child process.
 */
import {
  FrameDecoder,
  decodeValue,
  encodeFrame,
  encodeValue,
  isEnvelope,
  makeId,
  type Envelope,
} from "@reactivegraph/protocol";
import { RgpSession } from "../src/session.js";

export interface TestServer {
  /** Requests received from the client (oldest first). */
  drainRequests(): Envelope[];
  /** Respond to the request with the given result payload. */
  respond(req: Envelope, payload: unknown): void;
  /** Respond with an arbitrary envelope (e.g. wrong method). */
  respondRaw(env: Envelope): void;
  /** Simulate a peer crash: fail the client, reject its pending requests. */
  crash(err: Error): void;
  close(): void;
}

export function createPair(): { server: TestServer; client: RgpSession } {
  // server side: raw decoder, records requests, lets test respond manually.
  const serverDecoder = new FrameDecoder();
  const requests: Envelope[] = [];
  let serverCrashed = false;

  const server: TestServer = {
    drainRequests(): Envelope[] {
      const out = [...requests];
      requests.length = 0;
      return out;
    },
    respond(req: Envelope, payload: unknown): void {
      if (serverCrashed) return;
      // encode the response and feed it into the client's session
      const response: Envelope = {
        version: 1,
        id: req.id,
        kind: "response",
        method: req.method,
        payload,
      };
      client.ingest(encodeFrame(encodeValue(response)));
    },
    respondRaw(env: Envelope): void {
      if (serverCrashed) return;
      client.ingest(encodeFrame(encodeValue(env)));
    },
    crash(err: Error): void {
      serverCrashed = true;
      client.fail(err);
    },
    close(): void {
      serverCrashed = true;
      client.close();
    },
  };

  let counter = 0;
  const client = new RgpSession({
    send: (bytes) => {
      // feed into server decoder; collect requests
      const frames = serverDecoder.push(bytes);
      for (const frame of frames) {
        const value = decodeValue(frame);
        if (!isEnvelope(value)) continue;
        const env = value as Envelope;
        if (env.kind === "request") {
          requests.push(env);
        }
      }
    },
    idGen: () => `test-${counter++}`,
  });

  return { server, client };
}

export { makeId };
