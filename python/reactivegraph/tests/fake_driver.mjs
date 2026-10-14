#!/usr/bin/env node
/**
 * Fake Driver for lifecycle tests (python/reactivegraph/tests).
 *
 * Modes:
 *  - default: answers DRIVER_HELLO with a WRONG protocol version (handshake
 *    mismatch), then exits.
 *  - with env FAKE_DRIVER_CRASH=1: reads a frame then exits(1) immediately
 *    (crash propagation test).
 *  - with env FAKE_DRIVER_ECHO=1: echoes RUN input back as state (callback-free).
 */
import process from "node:process";
import path from "node:path";
import { fileURLToPath } from "node:url";
// Relative to packages/protocol/dist (built by pnpm --filter @reactivegraph/protocol build).
const here = path.dirname(fileURLToPath(import.meta.url));
const PROTOCOL = path.resolve(here, "../../../packages/protocol/dist/index.js");
const {
  FrameDecoder,
  PROTOCOL_VERSION,
  decodeValue,
  encodeFrame,
  encodeValue,
  isEnvelope,
  makeId,
} = await import(PROTOCOL);

const decoder = new FrameDecoder();
const crash = process.env.FAKE_DRIVER_CRASH === "1";
const echo = process.env.FAKE_DRIVER_ECHO === "1";
const mismatch = process.env.FAKE_DRIVER_MISMATCH !== "0";

function write(env) {
  process.stdout.write(Buffer.from(encodeFrame(encodeValue(env))));
}

function respond(id, method, payload) {
  write({ version: 1, id, kind: "response", method, payload });
}

process.stdin.setEncoding("binary");
process.stdin.on("data", (chunk) => {
  if (crash) {
    process.exit(1);
  }
  const bytes = typeof chunk === "string" ? Buffer.from(chunk, "binary") : chunk;
  let frames = [];
  try {
    frames = decoder.push(new Uint8Array(bytes));
  } catch (e) {
    process.exit(1);
    return;
  }
  for (const frame of frames) {
    const env = decodeValue(frame);
    if (!isEnvelope(env)) continue;
    if (env.kind !== "request") continue;
    if (env.method === "DRIVER_HELLO") {
      if (mismatch) {
        respond(env.id, env.method, {
          error: {
            type: "ProtocolVersionError",
            message: `no common protocol version: driver supports 999, sdk offers [${PROTOCOL_VERSION}]`,
          },
        });
        setTimeout(() => process.exit(0), 30);
      } else {
        respond(env.id, env.method, {
          driverVersion: "0.0.0",
          protocolVersions: [PROTOCOL_VERSION],
          featureFlags: [],
        });
      }
    } else if (env.method === "RUN" && echo) {
      const p = env.payload || {};
      respond(env.id, env.method, { graphId: "g", threadId: "t", runId: makeId(), state: p.input });
    } else if (env.method === "SHUTDOWN") {
      respond(env.id, env.method, { ok: true });
      setTimeout(() => process.exit(0), 20);
    } else {
      respond(env.id, env.method, { error: { type: "NotImplemented", message: `unhandled ${env.method}` } });
    }
  }
});

process.stdin.on("end", () => process.exit(0));