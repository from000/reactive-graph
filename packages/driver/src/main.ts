#!/usr/bin/env node
/**
 * ReactiveGraph Driver process entry (RGP/1 server over stdio).
 *
 * Reads frames from stdin, answers requests (DRIVER_HELLO / COMPILE_GRAPH /
 * RELEASE_GRAPH / RUN / RESUME / GET_STATE / CHECKPOINT_OP / STORE_OP /
 * SHUTDOWN), and initiates callbacks to the host (TASK_INVOKE) during run
 * execution. A callback is correlated to exactly one invocation;
 * results/errors flow back as TASK_RESULT payloads.
 *
 * Run semantics: a compiled graph's tasks are scheduled by the real reactive
 * scheduler; task bodies are executed by the host through TASK_INVOKE, and the
 * returned patches are committed through a transaction (RGP/1 §6 — a callback
 * never decides whether its own state changes commit).
 */
import process from "node:process";
import {
  FrameDecoder,
  ProtocolError,
  PROTOCOL_VERSION,
  decodeValue,
  encodeFrame,
  encodeValue,
  isEnvelope,
  makeId,
  type Envelope,
  type Method,
} from "@reactivegraph/protocol";
import { DriverRuntime, GraphError, MemoryVectorStore, type GraphSpec } from "./index.js";
import type { TaskStreamChunk } from "./graph/model.js";
import { SqliteLog } from "./durability/index.js";
import {
  PostgresCheckpointSaver,
  PostgresStore,
  RedisCache,
  SqliteCheckpointSaver,
  SqliteStore,
  SqliteVectorStore,
} from "./storage/index.js";

const decoder = new FrameDecoder();
const inflight = new Map<string, { method: Method; resolve: (payload: unknown) => void }>();
/** In-flight RUN requests keyed by `run:<requestId>`, for CANCEL propagation. */
const runControllers = new Map<string, AbortController>();

/**
 * Optional durability backend, enabled with `REACTIVEGRAPH_DB=/path/to/db`.
 * When set, thread state, checkpoints, and long-term store items survive
 * across Driver restarts over the same file (remote recovery / durable runs).
 */
function buildPersistence(): {
  log?: SqliteLog;
  checkpointSaver?: SqliteCheckpointSaver | PostgresCheckpointSaver;
  longTermStore?: SqliteStore | PostgresStore;
  cache?: RedisCache;
} {
  const db = process.env["REACTIVEGRAPH_DB"];
  const pgDsn = process.env["REACTIVEGRAPH_PG_DSN"];
  const redisUrl = process.env["REACTIVEGRAPH_REDIS_URL"];
  const out: {
    log?: SqliteLog;
    checkpointSaver?: SqliteCheckpointSaver | PostgresCheckpointSaver;
    longTermStore?: SqliteStore | PostgresStore;
    cache?: RedisCache;
  } = {};
  if (db) {
    out.log = new SqliteLog({ location: db, name: "driver" });
    out.checkpointSaver = new SqliteCheckpointSaver(db);
    out.longTermStore = new SqliteStore(db);
  }
  // A shared Postgres gives multi-Driver durability (threads + checkpoints
  // visible to every instance pointing at the same database).
  if (pgDsn) {
    out.checkpointSaver = new PostgresCheckpointSaver(pgDsn);
    out.longTermStore = new PostgresStore(pgDsn);
  }
  if (redisUrl) {
    out.cache = new RedisCache(redisUrl);
  }
  return out;
}

const persistence = buildPersistence();

/** Process-wide vector store (VECTOR_UPSERT / VECTOR_SEARCH). Persisted to
 * sqlite when REACTIVEGRAPH_DB is configured (survives driver restarts),
 * else in-memory (driver restart clears it). */
const vectorStore = process.env["REACTIVEGRAPH_DB"]
  ? new SqliteVectorStore(process.env["REACTIVEGRAPH_DB"])
  : new MemoryVectorStore();

/** Runtime receives task bodies via TASK_INVOKE round trips. */
const runtime = new DriverRuntime(
  {
    invokeTask: (callbackId, input, snapshotVersion, runId) => {
      const id = makeId();
      const pending = new Promise<unknown>((resolve, reject) => {
        inflight.set(id, { method: "TASK_RESULT", resolve });
        writeFrame({
          version: 1,
          id,
          kind: "request",
          method: "TASK_INVOKE",
          payload: { invocationId: id, callbackId, snapshotVersion, allowedPaths: ["$"], input },
          // The host keys run-scoped context (thread id, runtime, trace) off
          // this: task bodies run on callback worker threads, so the ambient
          // context of the caller does not travel with them.
          trace: { runId: runId ?? "run-pending" },
        });
        // Safety net so a never-completing host does not hang the Driver forever.
        setTimeout(() => {
          if (inflight.delete(id)) reject(new Error(`task callback ${id} timed out`));
        }, 30000);
      });
      // Always an async generator: a host may attach `stream_chunks` to its
      // TASK_INVOKE response (LLM token flow); they are forwarded as transient
      // chunks before the committed result.
      return (async function* (): AsyncGenerator<
        TaskStreamChunk,
        Record<string, unknown>,
        unknown
      > {
        const reply = (await pending) as Record<string, unknown>;
        const chunks = Array.isArray(reply.stream_chunks) ? reply.stream_chunks : [];
        for (const chunk of chunks) {
          yield chunk as TaskStreamChunk;
        }
        const { stream_chunks: _omitted, ...rest } = reply;
        return rest;
      })();
    },
  },
  { ...persistence, recoverThreads: persistence.log !== undefined },
);

function writeFrame(envelope: Envelope): void {
  process.stdout.write(Buffer.from(encodeFrame(encodeValue(envelope))));
}

function respond(id: string, method: Method, payload: unknown): void {
  writeFrame({ version: 1, id, kind: "response", method, payload });
}

function respondError(id: string, method: Method, type: string, message: string): void {
  respond(id, method, { error: { type, message } });
}

function log(msg: string): void {
  process.stderr.write(`[driver] ${msg}\n`);
}

async function handleRequest(env: Envelope): Promise<void> {
  const { id, method, payload } = env;
  try {
    switch (method) {
      case "DRIVER_HELLO": {
        const p = payload as {
          sdkVersion?: string;
          protocolVersions?: number[];
          featureFlags?: string[];
        };
        const sdkVersions = p.protocolVersions ?? [];
        if (!sdkVersions.includes(PROTOCOL_VERSION)) {
          respondError(
            id,
            method,
            "ProtocolVersionError",
            `no common protocol version: driver supports ${PROTOCOL_VERSION}, sdk offers [${sdkVersions.join(", ")}]`,
          );
          return;
        }
        respond(id, method, {
          driverVersion: "0.1.0",
          protocolVersions: [PROTOCOL_VERSION],
          featureFlags: [],
        });
        return;
      }
      case "COMPILE_GRAPH": {
        const spec = payload as GraphSpec;
        if (!spec || typeof spec !== "object" || typeof spec.id !== "string") {
          respondError(
            id,
            method,
            "ProtocolError",
            "COMPILE_GRAPH requires a graph spec with an id",
          );
          return;
        }
        const opts = (payload as { schemaHash?: string; callbackIds?: string[] }) ?? {};
        if (Array.isArray(opts.callbackIds)) {
          runtime.compileGraph(spec, { validateCallback: (cb) => opts.callbackIds!.includes(cb) });
        } else {
          runtime.compileGraph(spec, { schemaHash: opts.schemaHash });
        }
        respond(id, method, { graphId: spec.id, ok: true });
        return;
      }
      case "RELEASE_GRAPH": {
        const graphId = (payload as { graphId?: string })?.graphId;
        if (!graphId) throw new GraphError("RELEASE_GRAPH requires graphId");
        runtime.releaseGraph(graphId);
        respond(id, method, { ok: true });
        return;
      }
      case "RUN": {
        const p = payload as {
          graphId?: string;
          input?: unknown;
          event?: string;
          config?: unknown;
          threadId?: string;
          runId?: string;
          stream?: boolean;
        };
        const graphId = p.graphId ?? "graph";
        const runId = p.runId ?? makeId();
        const controller = new AbortController();
        runControllers.set(runId, controller);
        runControllers.set(`run:${id}`, controller);
        try {
          const result = await runtime.run(graphId, p.input ?? {}, {
            threadId: p.threadId ?? "default",
            event: p.event,
            config: (() => {
              const cfg = (p.config as Record<string, unknown>) ?? {};
              // P3-2 Task 3：env 默认并发上限（REACTIVEGRAPH_CONCURRENCY，
              // =1 即串行兜底），config 显式给 concurrency 时优先。
              // 非法值（NaN/0/非整数）回退默认并发，避免破坏 worker 计数。
              const envConc = process.env["REACTIVEGRAPH_CONCURRENCY"];
              const parsed = envConc === undefined ? NaN : Number(envConc);
              if (
                envConc !== undefined &&
                cfg.concurrency === undefined &&
                Number.isInteger(parsed) &&
                parsed > 0
              ) {
                return { ...cfg, concurrency: parsed };
              }
              return cfg;
            })(),
            runId,
            stream: p.stream === true,
            signal: controller.signal,
            onStream: (ev) => {
              writeFrame({
                version: 1,
                id: makeId(),
                kind: "event",
                method: "STREAM_EVENT",
                payload: {
                  runId,
                  cursor: `${ev.seq}`,
                  eventType: ev.eventType,
                  payload: ev.payload,
                  terminal: false,
                },
              });
            },
          });
          respond(id, method, {
            graphId,
            threadId: result.threadId,
            runId: result.runId,
            state: result.state,
            interrupted: result.interrupted ?? false,
          });
        } catch (err) {
          // A cancellation surfaces as an error response; the client sees the
          // state chunks that were already streamed.
          const conflictMeta = (err as { conflict?: Record<string, unknown> }).conflict;
          if (conflictMeta) {
            respond(id, method, {
              error: {
                type: (err as Error).name ?? "Error",
                message: (err as Error).message,
                meta: { conflict: conflictMeta },
              },
            });
          } else {
            respondError(id, method, (err as Error).name ?? "Error", (err as Error).message);
          }
        } finally {
          runControllers.delete(runId);
          runControllers.delete(`run:${id}`);
        }
        return;
      }
      case "RESUME": {
        const p = payload as { runId?: string; threadId?: string; interruptResponse?: unknown };
        if (!p.runId) throw new GraphError("RESUME requires runId");
        const result = await runtime.resume(p.runId, p.interruptResponse ?? {}, {
          threadId: p.threadId,
        });
        respond(id, method, {
          runId: p.runId,
          threadId: result.threadId,
          state: result.state,
          // 多段 HITL：resume 期间任务可再次挂起——与 RUN 对齐透传 interrupted
          interrupted: result.interrupted ?? false,
        });
        return;
      }
      case "GET_STATE": {
        const p = payload as { config?: { threadId?: string } };
        const threadId = p?.config?.threadId ?? "default";
        const values = await runtime.getState(threadId);
        respond(id, method, { values, config: { threadId } });
        return;
      }
      case "EXPORT_DOT": {
        const p = payload as { graphId?: string };
        const dot = runtime.exportDot(p.graphId ?? "graph");
        respond(id, method, { dot });
        return;
      }
      case "CHECKPOINT_OP": {
        const p = payload as {
          threadId?: string;
          op?: string;
          checkpointId?: string;
          record?: unknown;
        };
        if (!p.threadId || !p.op) throw new GraphError("CHECKPOINT_OP requires threadId and op");
        const result = await runtime.checkpointOp({
          threadId: p.threadId,
          op: p.op as "get" | "list" | "put" | "delete_thread",
          checkpointId: p.checkpointId,
          record: p.record,
        });
        respond(id, method, { ok: true, result });
        return;
      }
      case "STORE_OP": {
        const p = payload as {
          namespace?: string[];
          key?: string;
          op?: string;
          value?: unknown;
          filter?: Record<string, unknown>;
          limit?: number;
          offset?: number;
        };
        if (!p.op) throw new GraphError("STORE_OP requires op");
        if ((p.op === "get" || p.op === "put" || p.op === "delete") && !p.key) {
          throw new GraphError(`STORE_OP ${p.op} requires key`);
        }
        const result = await runtime.storeOp({
          namespace: p.namespace ?? [],
          key: p.key ?? "",
          op: p.op as "get" | "put" | "search" | "delete" | "list_namespaces",
          value: p.value,
          filter: p.filter,
          limit: p.limit,
          offset: p.offset,
        });
        respond(id, method, { ok: true, result });
        return;
      }
      case "VECTOR_UPSERT": {
        const p = payload as {
          namespace?: string[];
          id?: string;
          vector?: number[];
          metadata?: unknown;
        };
        if (!p.id || !Array.isArray(p.vector)) {
          throw new GraphError("VECTOR_UPSERT requires id and vector");
        }
        await vectorStore.upsert({
          namespace: p.namespace ?? [],
          id: p.id,
          vector: p.vector,
          metadata: p.metadata,
        });
        respond(id, method, { ok: true });
        return;
      }
      case "VECTOR_SEARCH": {
        const p = payload as {
          namespace?: string[];
          query?: number[];
          limit?: number;
          minScore?: number;
        };
        if (!Array.isArray(p.query)) {
          throw new GraphError("VECTOR_SEARCH requires query");
        }
        const results = await vectorStore.search(p.namespace ?? [], p.query, {
          limit: p.limit,
          minScore: p.minScore,
        });
        respond(id, method, { results });
        return;
      }
      case "TRACE_QUERY": {
        const p = payload as { runId?: string };
        if (!p.runId) throw new GraphError("TRACE_QUERY requires runId");
        respond(id, method, { result: runtime.explainHistoricalRun(p.runId) });
        return;
      }
      case "SHUTDOWN": {
        respond(id, method, { ok: true });
        setTimeout(() => process.exit(0), 50);
        return;
      }
      default:
        respondError(id, method, "NotImplemented", `driver does not implement ${method} yet`);
    }
  } catch (err) {
    respondError(id, method, (err as Error).name ?? "Error", (err as Error).message);
  }
}

process.stdin.setEncoding("binary");
process.stdin.on("data", (chunk: string | Buffer) => {
  const bytes = typeof chunk === "string" ? Buffer.from(chunk, "binary") : chunk;
  let frames: Uint8Array[];
  try {
    frames = decoder.push(new Uint8Array(bytes));
  } catch (err) {
    const message = err instanceof ProtocolError ? err.message : String(err);
    log(`protocol error: ${message}`);
    writeFrame({
      version: 1,
      id: makeId(),
      kind: "event",
      method: "STREAM_EVENT",
      payload: {
        eventType: "driver_error",
        payload: { error: "protocol_error", message },
        terminal: true,
      },
    });
    setTimeout(() => process.exit(1), 20);
    return;
  }
  for (const frame of frames) {
    try {
      const envelope = decodeValue(frame);
      if (!isEnvelope(envelope)) {
        log("malformed envelope ignored");
        continue;
      }
      const env = envelope as Envelope;
      if (env.kind === "response") {
        const pending = inflight.get(env.id);
        if (pending) {
          inflight.delete(env.id);
          pending.resolve(env.payload);
        }
      } else if (env.kind === "cancel" && env.method === "CANCEL") {
        const p = env.payload as { targetId?: string; reason?: string } | undefined;
        const target = p?.targetId;
        if (target) {
          const controller = runControllers.get(target) ?? runControllers.get(`run:${target}`);
          if (controller) {
            log(`cancelling ${target} (${p?.reason ?? "client cancel"})`);
            controller.abort(p?.reason ?? "client cancel");
          } else {
            log(`cancel ignored: unknown target ${target}`);
          }
        }
      } else if (env.kind === "request") {
        void handleRequest(env);
      }
    } catch (err) {
      log(`envelope error: ${(err as Error).message}`);
    }
  }
});

process.stdin.on("end", () => {
  log("stdin closed, exiting");
  process.exit(0);
});

// Crash/watchdog: if anything unexpected happens, exit(1) so the host can detect
// and propagate the failure.
process.on("uncaughtException", (err) => {
  log(`uncaught exception: ${err.stack ?? String(err)}`);
  process.exit(1);
});
