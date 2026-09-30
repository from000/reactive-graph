/**
 * Driver runtime: the stateful side of the protocol layer (RGP/1).
 *
 * Owns the compiled-graph registry and per-thread stores, and wires RUN/RESUME
 * to the real Scheduler. Task bodies live in the host (Python/JS) and are
 * executed through the injected `TaskExecutor` (TASK_INVOKE round trip); the
 * runtime validates and commits the returned patches through the store — a
 * callback never decides whether its own state changes commit (RGP/1 §6).
 *
 * Single long-lived Driver process / single RgpSession: RUN and RESUME are
 * handled serially per request, so the runtime keeps one active executor
 * session at a time.
 */

import { Graph, GraphBuilder, GraphNotFoundError, graphToDot } from "./graph/model.js";
import type { GraphDef, TaskDef, TaskHandler, TaskResult, TaskStreamChunk } from "./graph/model.js";
import { ReactiveStore } from "./state/store.js";
import { Scheduler, SchedulerPersistent, TaskExecutionError } from "./scheduler/scheduler.js";
import type { RetryPolicy } from "./scheduler/retry.js";
import { InterruptManager } from "./interrupts.js";
import { SpanEmitter } from "./otel.js";
import {
  MemoryCheckpointSaver,
  MemoryStore,
  type Cache,
  type CheckpointSaver,
  type LongTermStore,
} from "./storage/index.js";
import { serializer } from "./storage/serializer.js";
import type { DurableLog } from "./durability/log.js";
import type { TransactionEvent } from "./durability/events.js";
import { recover, latestSnapshot } from "./durability/recovery.js";
import { TriggerOpTypes } from "@vue/reactivity";
import { Transaction } from "./state/transaction.js";
import { parsePath } from "./state/path.js";
import { allowPaths, type PermissionPolicy } from "./permissions.js";
import { BudgetPolicy, type Policy } from "./policies.js";

/** Wire shape of a compiled task (cross-language; mirrors python graph.py). */
export interface WireTask {
  readonly id: string;
  readonly kind: "pure" | "effect" | "opaque";
  readonly on?: readonly string[];
  readonly dependsOn?: readonly string[];
  readonly writes?: readonly string[];
  /** Re-entry budget within a single run (default 1) — mirrors graph.py. */
  readonly maxRuns?: number;
  readonly scope?: string;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: number;
  /** Host callback that executes this task body. */
  readonly callbackId: string;
}

export interface WireRoute {
  readonly event: string;
  readonly taskId: string;
}

/** Wire shape of a compiled computed selector (RGP/1). The selector body lives
 * in the host; the Driver owns invalidation (declared read set), caching, and
 * the atomic commit of the evaluated value to `state[id]`. */
export interface WireComputed {
  readonly id: string;
  readonly reads?: readonly string[];
  /** Host callback (id convention: `computed:{id}`) that evaluates the
   * selector and returns a TaskSuccess whose patch writes `state[id]`. */
  readonly callbackId: string;
}

/** Cross-language graph definition sent by COMPILE_GRAPH. */
export interface GraphSpec {
  readonly id: string;
  readonly tasks: readonly WireTask[];
  readonly routes?: readonly WireRoute[];
  readonly scopes?: readonly string[];
  readonly computeds?: readonly WireComputed[];
  readonly schemaHash?: string;
}

/** Wire shape of a single state patch produced by a host callback. */
export interface WirePatch {
  readonly path: readonly (string | number)[];
  readonly operation: "set";
  readonly value?: unknown;
}

/** Host callback success payload (RGP/1 §6 TaskSuccess). */
export interface TaskSuccess {
  readonly reads?: readonly string[];
  readonly patches?: readonly WirePatch[];
  readonly writes?: readonly string[];
  readonly return_value?: unknown;
  readonly external_receipts?: readonly unknown[];
  /** Explicit follow-up events for this invocation (RGP/1 §6 extension).
   * Absent → the implicit `<taskId>:written`; `[]` → no downstream event. */
  readonly emits?: readonly string[];
}

/** Result of a host task invocation failure. */
export interface TaskFailure {
  readonly error?: { readonly type?: string; readonly message?: string };
}

/** Injected callback executor: TASK_INVOKE request → TASK_RESULT payload. */
export interface TaskExecutor {
  invokeTask(
    callbackId: string,
    input: unknown,
    snapshotVersion: number,
    runId?: string,
  ): Promise<TaskSuccess> | AsyncGenerator<TaskStreamChunk, TaskSuccess, unknown>;
}

/**
 * Optional persistence backends for the runtime. When provided, thread state,
 * checkpoints, and long-term store items survive across Driver instances that
 * share the same backend (e.g. the same sqlite file).
 */
export interface PersistenceOptions {
  /** Durable event log; recovered on first touch of each thread. */
  readonly log?: DurableLog;
  /** Checkpoint saver (defaults to memory per process). */
  readonly checkpointSaver?: CheckpointSaver;
  /** Long-term store (defaults to memory per process). */
  readonly longTermStore?: LongTermStore;
  /** Cache (defaults to memory per process). */
  readonly cache?: Cache;
  /** Whether thread stores are recovered from the log on first access. */
  readonly recoverThreads?: boolean;
}

/** Bounded in-process retention. Defaults keep long-lived Drivers below a
 * predictable memory ceiling; a value of 0 disables that specific bound. */
export interface RuntimeLimits {
  /** Max completed/interrupted runs retained for RESUME (default 10_000). */
  readonly maxRuns?: number;
  /** Max thread stores cached in memory when they are rehydratable from a
   * durable log/checkpoint backend (default 1_000). Threads without a durable
   * backend are authoritative in-memory state and are never evicted silently. */
  readonly maxThreads?: number;
}

/** Observable runtime retention counters (for leak/ceiling verification). */
export interface RuntimeStats {
  readonly graphs: number;
  readonly threads: number;
  readonly runs: number;
  readonly activeRuns: number;
  readonly memoryCheckpointSavers: number;
}

export interface CompileOptions {
  /** Required schema hash the runtime must match (schema drift guard). */
  readonly schemaHash?: string;
  /** Validate a callback id before wiring it into the graph. */
  readonly validateCallback?: (callbackId: string) => boolean;
}

/** A streamed state update emitted during a run (values-style chunk). */
export interface RunStreamEvent {
  readonly eventType: "values" | "messages" | "custom";
  readonly seq: number;
  readonly payload:
    | { state: Record<string, unknown> } // values
    | { message: unknown } // messages (transient)
    | { event: string; payload: unknown }; // custom (transient)
}

export interface RunOptions {
  readonly threadId?: string;
  readonly event?: string;
  readonly config?: Record<string, unknown>;
  /** Optional caller-assigned run id (defaults to a runtime-generated one). */
  readonly runId?: string;
  /** When true, `onStream` is called with a values snapshot after each task. */
  readonly stream?: boolean;
  /** Called for each streamed value when `stream` is true. */
  readonly onStream?: (event: RunStreamEvent) => void;
  /** Cooperative cancellation: when signalled, the run stops between tasks. */
  readonly signal?: AbortSignal;
}

export interface RunResult {
  readonly runId: string;
  readonly threadId: string;
  readonly state: Record<string, unknown>;
  readonly interrupted?: boolean;
}

export interface CheckpointOpOptions {
  readonly threadId: string;
  readonly op: "get" | "list" | "put" | "delete_thread" | "restore";
  readonly checkpointId?: string;
  readonly record?: unknown;
}

export interface StoreOpOptions {
  readonly namespace: readonly string[];
  readonly key: string;
  readonly op: "get" | "put" | "search" | "delete" | "list_namespaces";
  readonly value?: unknown;
  readonly filter?: Record<string, unknown>;
  readonly limit?: number;
  readonly offset?: number;
}

interface CompiledGraph {
  readonly spec: GraphSpec;
  readonly graph: Graph;
  readonly callbackByTask: ReadonlyMap<string, string>;
  /** Cross-run scheduler state: pure-task fingerprints survive scheduler
   * recreation between runs (selective execution across runs). */
  readonly persistent: SchedulerPersistent;
}

interface RunState {
  readonly runId: string;
  readonly graphId: string;
  readonly threadId: string;
}

interface ActiveSession {
  readonly executor: TaskExecutor;
  readonly version: () => number;
  /** Run this session is executing. Echoed back on TASK_INVOKE so a host with
   * several concurrent runs can attach the right run-scoped context (see
   * `TaskExecutor.invokeTask`). */
  readonly runId?: string;
  /** Thread whose store the active run mutates; never LRU-evicted. */
  readonly threadId?: string;
}

export class GraphError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GraphError";
  }
}

/** Raised instead of silently dropping authoritative in-memory thread state
 * once a runtime retention ceiling is reached. */
export class RuntimeCapacityError extends GraphError {
  constructor(message: string) {
    super(message);
    this.name = "RuntimeCapacityError";
  }
}

function normalizePermissionPolicy(value: unknown): PermissionPolicy | undefined {
  if (!Array.isArray(value)) return undefined;
  const paths = value.map((entry) =>
    Array.isArray(entry) ? entry.map((segment) => String(segment)) : [String(entry)],
  );
  return allowPaths([], paths);
}

function normalizePolicies(value: unknown): readonly Policy[] | undefined {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return undefined;
  return [new BudgetPolicy(value)];
}

/** Raised through the runtime when a host callback reports an interrupt. */
export class TaskInterruptedError extends Error {
  readonly value: unknown;
  constructor(value: unknown) {
    super("task interrupted");
    this.name = "TaskInterruptedError";
    this.value = value;
  }
}

/**
 * In-memory protocol runtime. Persistence (SqliteLog/checkpoint backend) is
 * pluggable; the default is memory-backed, matching the local Driver process
 * lifecycle (single long-lived stdio connection).
 */
export class DriverRuntime {
  private readonly graphs = new Map<string, CompiledGraph>();
  /** Per-thread canonical stores; LRU-bounded only when rehydratable. */
  private readonly stores = new Map<string, ReactiveStore>();
  /** Runs retained for RESUME, bounded by `maxRuns` (oldest evicted first). */
  private readonly runs = new Map<string, RunState>();
  private readonly longTerm = new Map<string, LongTermStore>();
  private readonly persistence?: PersistenceOptions;
  /** Single shared in-memory saver: internally keyed by threadId, so it
   * cannot leak one wrapper object per thread. */
  private readonly memoryCheckpointSaver = new MemoryCheckpointSaver();
  private active: ActiveSession | null = null;
  private readonly executor: TaskExecutor;
  private readonly maxRuns: number;
  private readonly maxThreads: number;
  private runSeq = 0;

  constructor(
    executor: TaskExecutor,
    persistence: PersistenceOptions = {},
    limits: RuntimeLimits = {},
  ) {
    this.executor = executor;
    this.active = { executor, version: () => 0 };
    this.persistence = persistence;
    this.maxRuns = limits.maxRuns ?? 10_000;
    this.maxThreads = limits.maxThreads ?? 1_000;
    if (!Number.isInteger(this.maxRuns) || this.maxRuns < 0) {
      throw new GraphError(`maxRuns must be a non-negative integer, got ${this.maxRuns}`);
    }
    if (!Number.isInteger(this.maxThreads) || this.maxThreads < 0) {
      throw new GraphError(`maxThreads must be a non-negative integer, got ${this.maxThreads}`);
    }
  }

  /** Retention counters for memory-ceiling/leak verification. */
  get stats(): RuntimeStats {
    return {
      graphs: this.graphs.size,
      threads: this.stores.size,
      runs: this.runs.size,
      activeRuns: this.active?.runId ? 1 : 0,
      memoryCheckpointSavers: 1,
    };
  }

  /** Durable log backing this runtime, if configured. */
  get log(): DurableLog | undefined {
    return this.persistence?.log;
  }

  /**
   * Confirmed effect receipts restored from the durable log
   * (`taskId:inputHash` -> receipt). Per the durable-execution spec an
   * idempotency key is confirmed only when BOTH its effect_intent and its
   * effect_receipt are present; a lone receipt is not trusted. Effect tasks
   * only skip re-running for the exact input whose effect was confirmed.
   *
   * Migration note: logs written before this key format (receipts keyed
   * `taskId:receipt` with no intents) restore as empty, so a previously
   * confirmed effect re-runs once after upgrade. This is fail-open (safe:
   * never skips an unverifiable effect), never fail-closed.
   */
  restoredReceipts(): ReadonlyMap<string, string> {
    const log = this.persistence?.log;
    const receipts = new Map<string, string>();
    const intents = new Set<string>();
    if (!log) return receipts;
    let from = 0;
    // A snapshot subsumes every event behind it (including the intent+receipt
    // pair after compaction); start from the newest snapshot when present.
    const snap = latestSnapshot(log);
    if (snap) {
      for (const [key, receipt] of snap.event.confirmedEffects ?? []) {
        receipts.set(key, receipt);
        // The snapshot is only written for effects whose intent AND receipt
        // were both durable, so these keys are already verified. Re-adding
        // them to `intents` prevents the final gate below from discarding
        // compacted receipts just because their original intent was truncated.
        intents.add(key);
      }
      from = snap.seq;
    }
    for (const event of log.readFrom(from)) {
      if (event.kind === "effect_intent") {
        intents.add(event.idempotencyKey);
      } else if (event.kind === "effect_receipt" && event.outcome === "success") {
        // Stage; only kept if the matching intent exists.
        receipts.set(event.idempotencyKey, event.receipt);
      }
    }
    for (const key of [...receipts.keys()]) {
      if (!intents.has(key)) receipts.delete(key);
    }
    return receipts;
  }

  // -- graph registry ------------------------------------------------------

  /** Register a compiled graph; throws on duplicate id or unknown callback. */
  compileGraph(spec: GraphSpec, opts: CompileOptions = {}): string {
    if (this.graphs.has(spec.id)) {
      throw new GraphError(`duplicate graph id: ${spec.id}`);
    }
    if (opts.schemaHash && spec.schemaHash !== opts.schemaHash) {
      throw new GraphError(
        `schema hash mismatch: expected ${opts.schemaHash}, got ${spec.schemaHash ?? "undefined"}`,
      );
    }
    const builder = new GraphBuilder(spec.id);
    const callbackByTask = new Map<string, string>();
    for (const t of spec.tasks) {
      if (callbackByTask.has(t.id)) throw new GraphError(`duplicate task id: ${t.id}`);
      if (opts.validateCallback && !opts.validateCallback(t.callbackId)) {
        throw new GraphError(`unregistered callback: ${t.callbackId}`);
      }
      callbackByTask.set(t.id, t.callbackId);
      const def: TaskDef = {
        id: t.id,
        kind: t.kind,
        // The graph owns task metadata only. The runtime rebinds handlers to
        // the current run session before each execution so concurrent runs
        // cannot observe each other's runId/version.
        handler: () => {
          throw new GraphError(`task ${t.id} is not bound to a run session`);
        },
        on: t.on,
        dependsOn: t.dependsOn,
        writes: t.writes,
        maxRuns: t.maxRuns,
        scope: t.scope,
        retry: t.retry,
        timeoutMs: t.timeoutMs,
      };
      builder.task(def);
    }
    for (const r of spec.routes ?? []) {
      builder.on(r.event, r.taskId);
    }
    for (const s of spec.scopes ?? []) {
      builder.scope(s);
    }
    for (const c of spec.computeds ?? []) {
      if (opts.validateCallback && !opts.validateCallback(c.callbackId)) {
        throw new GraphError(`unregistered callback: ${c.callbackId}`);
      }
      // The selector body is host-side; the local placeholder never runs
      // because computeComputeds only calls scheduler.compute() after the
      // remote callback has already been invoked.
      builder.computed({
        id: c.id,
        reads: c.reads,
        selector: () => {
          throw new GraphError(`computed ${c.id} selector is remote (callbackId=${c.callbackId})`);
        },
      });
    }
    const graph = builder.build();
    this.graphs.set(spec.id, {
      spec,
      graph,
      callbackByTask,
      persistent: { fingerprints: new Map<string, string>() },
    });
    return spec.id;
  }

  /** Forget a compiled graph (RELEASE_GRAPH). */
  releaseGraph(graphId: string): void {
    if (!this.graphs.has(graphId)) throw new GraphNotFoundError(graphId);
    this.graphs.delete(graphId);
  }

  hasGraph(graphId: string): boolean {
    return this.graphs.has(graphId);
  }

  // -- run execution -------------------------------------------------------

  /**
   * Execute a run: emit the entry event, run eligible tasks through the host
   * executor, commit returned patches, return the final thread state. If a task
   * raises an interrupt, the run stops and can be resumed via `resume`.
   */
  async run(graphId: string, input: unknown, opts: RunOptions = {}): Promise<RunResult> {
    const compiled = this.graphs.get(graphId);
    if (!compiled) throw new GraphNotFoundError(graphId);
    const threadId = opts.threadId ?? "default";
    const runId = opts.runId ?? `run-${++this.runSeq}`;
    const store = await this.storeFor(threadId);
    // The checkpoint this run derives from — used as the optimistic-concurrency
    // parent: if another Driver advanced the thread meanwhile, our checkpoint
    // write is rejected instead of silently overwriting its progress.
    const startCheckpoint = this.persistence?.checkpointSaver
      ? (await this.persistence.checkpointSaver.get(threadId))?.checkpointId
      : undefined;
    const event = opts.event ?? "run";
    const taskIds = compiled.graph.routeFor(event);
    if (taskIds.length === 0) {
      throw new GraphError(`no task routes for event ${event} (graph ${graphId})`);
    }
    let streamSeq = 0;
    const session: ActiveSession = {
      executor: this.executor,
      version: () => store.version,
      runId,
      threadId,
    };
    const graph = this.bindGraphToSession(compiled, session);
    const scheduler = new Scheduler({
      graph,
      store,
      runId,
      threadId,
      log: this.persistence?.log,
      initialReceipts: this.restoredReceipts(),
      persistent: compiled.persistent,
      baseInput: input,
      recursionLimit:
        typeof opts.config?.recursionLimit === "number" ? opts.config.recursionLimit : undefined,
      permissionPolicy: normalizePermissionPolicy(opts.config?.allowWritePaths),
      policies: normalizePolicies(opts.config?.budgetLimit),
      onStreamChunk: (taskId, chunk) => {
        if (!opts.stream || !opts.onStream) return;
        if (chunk.type === "messages") {
          opts.onStream({
            eventType: "messages",
            seq: ++streamSeq,
            payload: { message: chunk.payload },
          });
        } else {
          opts.onStream({
            eventType: "custom",
            seq: ++streamSeq,
            payload: { event: taskId, payload: chunk.payload },
          });
        }
      },
      // P3-3 Task 3：span 事件桥接——每个完成的 span（task 级 span 在
      // scheduler 内 start/end）经 custom 通道外发（payload.event =
      // `span:${kind}`），Python 侧消费为 _trace；复用 run_stream 通道，
      // 不新增协议方法。默认关闭（config.trace !== true 不产生 custom
      // span 帧——既有 values/messages 帧流形状不受影响）。
      spanEmitter:
        opts.config?.trace === true
          ? new SpanEmitter({
              onSpan: (span) => {
                if (!opts.stream || !opts.onStream) return;
                opts.onStream({
                  eventType: "custom",
                  seq: ++streamSeq,
                  payload: { event: `span:${span.kind}`, payload: span },
                });
              },
            })
          : undefined,
    });
    const interrupts = new InterruptManager({ runId, threadId, log: this.persistence?.log });
    this.active = session;
    scheduler.emit({ name: event, payload: input });
    const emitValues = (): void => {
      if (!opts.stream || !opts.onStream) return;
      opts.onStream({
        eventType: "values",
        seq: ++streamSeq,
        payload: { state: { ...store.raw } },
      });
    };
    // run 载荷并入线程 store（补缺失键）：state 含调用方输入键——对齐
    // fallback（state=dict(payload)）与 langgraph state 语义；多轮 run /
    // resume 重放时任务仍可读原输入键（同键以输入为准，见任务输入构造）。
    for (const [k, v] of Object.entries((input as Record<string, unknown>) ?? {})) {
      if (!(k in store.raw)) store.raw[k] = v;
    }
    // P3-1 Task 3：初始 values 帧在输入并入后发出——首帧含输入键，
    // 与 fallback（state=dict(payload)）逐帧同构。
    emitValues();
    // 任务级联（P1-3 引擎升级；P3-2 事件轮次批次）：任务输入 = run 载荷
    // merge 线程 store（累积写入）——链式管道读上游写；每批（同一触发
    // 事件）批内 runAll 并行，批后收集 `{tid}:written` 下游为下一批
    // （广度优先；`queued` 去重终止环）。
    try {
      await this.executeTaskQueue(
        taskIds,
        scheduler,
        compiled,
        store,
        (input as Record<string, unknown>) ?? {},
        {
          signal: opts.signal,
          concurrency: opts.config?.concurrency as number | undefined,
          onBatchSuccess: () => emitValues(),
        },
      );
      await this.computeComputeds(scheduler, compiled, store, session);
    } catch (err) {
      const interrupt = this.extractInterrupt(err);
      if (interrupt !== undefined) {
        interrupts.interrupt(interrupt, "resume", store.version);
        this.rememberRun({ runId, graphId, threadId });
        await this.checkpointThread(threadId, store.raw, startCheckpoint);
        return { runId, threadId, state: store.raw, interrupted: true };
      }
      throw err;
    }
    this.rememberRun({ runId, graphId, threadId });
    await this.checkpointThread(threadId, store.raw, startCheckpoint);
    return { runId, threadId, state: store.raw };
  }

  /** Retain a run for RESUME with a hard LRU ceiling. Oldest runs are evicted
   * first so a run storm cannot grow process memory without bound. */
  private rememberRun(run: RunState): void {
    this.runs.delete(run.runId);
    this.runs.set(run.runId, run);
    if (this.maxRuns <= 0) {
      this.runs.clear();
      return;
    }
    while (this.runs.size > this.maxRuns) {
      const oldest = this.runs.keys().next().value;
      if (oldest === undefined) break;
      this.runs.delete(oldest);
    }
  }

  /**
   * P3-2 Task 1：事件轮次批次执行任务级联（run/resume 共用）。
   *
   * 每批 = 同一触发事件的全部订阅任务：批内 `scheduler.runBatch` 并行
   * （并发 + 写冲突检测）；`concurrency === 1` 时批内逐任务串行（等价
   * 旧行为）。批完成后收集该批任务发出的下游事件（默认 `{tid}:written`，
   * 任务可用 `emits` 显式覆盖——含 `[]` 表示终止本分支）为下一批。
   * 批输入 = store 累积打底 + baseInput 覆盖（同批同载荷）；
   * `onBatchSuccess` 每任务回调（串行=真实帧；并行=批后聚合帧，帧数对齐
   * fallback）。
   *
   * 重入预算（`TaskDef.maxRuns`，默认 1）：每个任务在一次 run 内的执行
   * 次数上限。默认 1 保留既有"环在第二轮截断"的 at-most-once 语义；
   * 显式声明 `maxRuns > 1` 的任务才允许在事件环中重入——真实 agent 的
   * model→tools→model 循环由此表达。预算耗尽即停止该分支（fail closed：
   * 绝不再无限重入），总执行次数仍受 `recursionLimit` 兜底。
   */
  private async executeTaskQueue(
    taskIds: string[],
    scheduler: Scheduler,
    compiled: CompiledGraph,
    store: ReactiveStore,
    baseInput: Record<string, unknown>,
    opts: {
      signal?: AbortSignal;
      concurrency?: number;
      onBatchSuccess?: (taskIds: string[]) => void;
    } = {},
  ): Promise<void> {
    // 每个任务的执行次数 / 预算（默认 1 = at-most-once）。
    const runs = new Map<string, number>();
    // Top-level keys this run has already committed. The run payload is the
    // initial state, but once a task writes a key the store owns it: later
    // rounds (re-entry / downstream tasks) must read the committed value, not
    // the stale payload copy. Mirrors the in-process fallback, where
    // `state.update(update)` makes the write win for the rest of the run.
    const writtenKeys = new Set<string>();
    const noteWrites = (writes: readonly string[]): void => {
      for (const write of writes) {
        const top = parsePath(write)[0];
        if (typeof top === "string") writtenKeys.add(top);
      }
    };
    const taskInput = (): Record<string, unknown> => {
      const merged: Record<string, unknown> = { ...store.raw, ...baseInput };
      for (const key of writtenKeys) {
        if (key in store.raw) merged[key] = store.raw[key];
        else delete merged[key];
      }
      return merged;
    };
    const budgetOf = (tid: string): number => {
      const declared = compiled.graph.task(tid)?.maxRuns;
      return typeof declared === "number" && declared > 0 ? declared : 1;
    };
    const exhausted = (tid: string): boolean => (runs.get(tid) ?? 0) >= budgetOf(tid);
    let round: string[] = [];
    const pendingRound = new Set<string>();
    for (const tid of taskIds) {
      if (!pendingRound.has(tid) && !exhausted(tid)) {
        pendingRound.add(tid);
        round.push(tid);
      }
    }
    /** 把 `event` 的下游订阅者加入下一批（批内去重 + 预算闸门）。 */
    const enqueueDownstream = (event: string): void => {
      for (const tid of compiled.graph.routeFor(event)) {
        if (pendingRound.has(tid) || exhausted(tid)) continue;
        pendingRound.add(tid);
        round.push(tid);
      }
    };
    const routeFromOutcome = (tid: string, emits?: readonly string[]): void => {
      for (const event of emits ?? [`${tid}:written`]) enqueueDownstream(event);
    };
    let roundIndex = 0;
    while (round.length > 0) {
      if (opts.signal?.aborted) {
        throw new GraphError(`run cancelled (${opts.signal.reason ?? "client cancel"})`);
      }
      // 预算闸门必须在派发时复查：任务可能在本批执行前就被排入下一批
      // （同一批里 t0 先跑并把 gate 排入下一轮，而 gate 本轮才刚执行完）。
      const batch = round.filter((tid) => !exhausted(tid));
      round = [];
      pendingRound.clear();
      if (batch.length === 0) continue;
      const isFirstRound = roundIndex === 0;
      roundIndex += 1;
      // store 累积打底、载荷优先（调用方输入是"最新意图"——递归/多
      // 事件图同键时以载荷为准）；链式管道读上游写经 store 提供。
      if (opts.concurrency === 1 || isFirstRound) {
        // 串行路径：concurrency=1（显式兜底）或首轮（routeFor(event)
        // 直接订阅者——同事件多任务按注册序可存在读-写顺序依赖，
        // fallback 与旧 Driver 均为串行契约，见 test_driver_chain_reads_
        // upstream_writes）；批内逐任务 runTask（输入**每任务现算**，
        // 含同批上游任务的写——顺序依赖契约），每任务后真实帧。
        for (const tid of batch) {
          if (opts.signal?.aborted) {
            throw new GraphError(`run cancelled (${opts.signal.reason ?? "client cancel"})`);
          }
          const outcome = await scheduler.runTask(tid, taskInput());
          runs.set(tid, (runs.get(tid) ?? 0) + 1);
          noteWrites(outcome.writes);
          if (opts.onBatchSuccess) opts.onBatchSuccess([tid]);
          routeFromOutcome(tid, outcome.emits);
        }
      } else {
        // 并行路径：事件派生的下游批（扇出——任务读同一批输入快照
        // 即上游批完成后的 store，相互间按"无读-写依赖"约定，design 4.②
        // 方案 A）；runBatch 并发 + 写冲突检测，批后聚合帧。
        const batchInput = taskInput();
        const inputs = new Map<string, unknown>();
        // per-task 浅拷贝：handler 若违反只读契约 mutate 输入不跨任务污染
        for (const tid of batch) inputs.set(tid, { ...batchInput });
        const outcomes = await scheduler.runBatch(batch, inputs);
        for (const tid of batch) {
          runs.set(tid, (runs.get(tid) ?? 0) + 1);
          noteWrites(outcomes.get(tid)?.writes ?? []);
          if (opts.onBatchSuccess) opts.onBatchSuccess([tid]);
          routeFromOutcome(tid, outcomes.get(tid)?.emits);
        }
      }
    }
  }

  /** Resume a previously interrupted run with a resume payload. */
  async resume(
    runId: string,
    resumePayload: unknown,
    opts: { threadId?: string } = {},
  ): Promise<RunResult> {
    const run = this.runs.get(runId);
    if (!run) throw new GraphError(`unknown run: ${runId}`);
    const compiled = this.graphs.get(run.graphId);
    if (!compiled) throw new GraphNotFoundError(run.graphId);
    const threadId = opts.threadId ?? run.threadId;
    const store = await this.storeFor(threadId);
    const session: ActiveSession = {
      executor: this.executor,
      version: () => store.version,
      runId,
      threadId,
    };
    const graph = this.bindGraphToSession(compiled, session);
    const scheduler = new Scheduler({
      graph,
      store,
      runId,
      threadId,
      log: this.persistence?.log,
      initialReceipts: this.restoredReceipts(),
      baseInput: resumePayload,
    });
    this.active = session;
    const ids = graph.routeFor("resume");
    const taskIds = ids.length > 0 ? ids : [...compiled.callbackByTask.keys()];
    scheduler.emit({ name: "resume", payload: resumePayload });
    // 用户响应并入线程 store（对象载荷补缺失键）——中断任务与下游级联
    // 任务可读（human-in-the-loop 响应进入 state，对齐 langgraph resume）
    if (resumePayload !== null && typeof resumePayload === "object") {
      for (const [k, v] of Object.entries(resumePayload as Record<string, unknown>)) {
        if (!(k in store.raw)) store.raw[k] = v;
      }
    }
    // 与 run 一致的任务级联（P3-2 事件轮次批次，共用 executeTaskQueue）。
    try {
      await this.executeTaskQueue(
        taskIds,
        scheduler,
        compiled,
        store,
        (resumePayload as Record<string, unknown>) ?? {},
      );
    } catch (err) {
      // 多段 HITL：resume 期间任务可再次挂起——与 run 一致捕获 Interrupt，
      // 报告 interrupted 而非把 TaskInterruptedError 冒泡为 TaskExecutionError。
      const interrupt = this.extractInterrupt(err);
      if (interrupt !== undefined) {
        const rm = new InterruptManager({ runId, threadId, log: this.persistence?.log });
        rm.interrupt(interrupt, "resume", store.version);
        this.rememberRun(run);
        return { runId, threadId, state: store.raw, interrupted: true };
      }
      throw err;
    }
    this.rememberRun(run);
    return { runId, threadId, state: store.raw };
  }

  /** Latest committed state of a thread (GET_STATE). */
  async getState(threadId: string = "default"): Promise<Record<string, unknown>> {
    return (await this.storeFor(threadId)).raw;
  }

  /** Render a compiled graph as Graphviz DOT (graph-as-code inspection). */
  exportDot(graphId: string): string {
    const compiled = this.graphs.get(graphId);
    if (!compiled) throw new GraphNotFoundError(graphId);
    return graphToDot(compiled.graph);
  }

  // -- storage surface -----------------------------------------------------

  /** Read-only historical run explanation from the durable log.

   * The current durable log records committed transactions (task, read set,
   * write set, patches). This is enough to answer which tasks executed and
   * what they produced. Skips/computed decisions are not yet persisted as
   * durable events, so callers get an explicit `decisionsPersisted: false`
   * rather than silently pretending a full historical trace exists.
   */
  explainHistoricalRun(runId: string): Record<string, unknown> {
    const log = this.persistence?.log;
    if (!log) throw new GraphError("historical explain requires a durable log");
    const events = log
      .readFrom(0)
      .filter((event): event is TransactionEvent => event.kind === "transaction")
      .filter((event) => event.runId === runId);
    if (events.length === 0) throw new GraphError(`no historical run found: ${runId}`);
    return {
      runId,
      executed: new Set(events.map((event) => event.taskId)).size,
      transactions: events.length,
      decisionsPersisted: false,
      events: events.map((event) => ({
        event: `task:${event.taskId}`,
        task: event.taskId,
        reads: event.readPaths,
        writes: event.writePaths,
        patches: event.patches,
        stateVersion: event.stateVersion,
      })),
    };
  }

  async checkpointOp(opts: CheckpointOpOptions): Promise<unknown> {
    const saver = this.checkpointSaverFor(opts.threadId);
    switch (opts.op) {
      case "get":
        return stripUndefined(await saver.get(opts.threadId, opts.checkpointId));
      case "list":
        return stripUndefined(await saver.list(opts.threadId));
      case "put": {
        if (
          !this.persistence?.checkpointSaver &&
          this.maxThreads > 0 &&
          !this.memoryCheckpointSaver.hasThread(opts.threadId) &&
          this.memoryCheckpointSaver.threadCount >= this.maxThreads
        ) {
          throw new RuntimeCapacityError(
            `in-memory checkpoint thread ceiling reached (${this.maxThreads}); ` +
              `raise maxThreads or delete_thread first`,
          );
        }
        await saver.put(opts.record as never);
        return { ok: true };
      }
      case "delete_thread": {
        await saver.deleteThread(opts.threadId);
        this.stores.delete(opts.threadId);
        for (const [id, run] of this.runs) {
          if (run.threadId === opts.threadId) this.runs.delete(id);
        }
        return { ok: true };
      }
      case "restore": {
        // Time travel: roll the thread store back to a historical checkpoint
        // (a NEW committed state, not a raw mutation). Subsequent runs on the
        // thread continue from the restored snapshot — fork semantics.
        if (!opts.checkpointId) throw new GraphError("restore requires checkpointId");
        const rec = await saver.get(opts.threadId, opts.checkpointId);
        if (!rec) {
          throw new GraphError(
            `checkpoint not found: ${opts.checkpointId} (thread ${opts.threadId}) — Hint: 用 checkpoint_op list 查看该 thread 的可用 checkpoint`,
          );
        }
        const restored = serializer.loads(rec.values) as Record<string, unknown>;
        const store = await this.storeFor(opts.threadId);
        const keys = new Set<string>([...Object.keys(store.raw), ...Object.keys(restored)]);
        for (const k of keys) {
          if (k in restored) store.state[k] = restored[k];
          else delete store.state[k];
        }
        store.commitVersion(
          new Map([...keys].map((k) => [k, { op: TriggerOpTypes.SET } as const])),
        );
        return {
          threadId: opts.threadId,
          checkpointId: opts.checkpointId,
          state: { ...store.raw },
        };
      }
      default:
        throw new GraphError(`unknown checkpoint op: ${(opts as { op: string }).op}`);
    }
  }

  async storeOp(opts: StoreOpOptions): Promise<unknown> {
    const store = this.longTermFor(opts.namespace);
    switch (opts.op) {
      case "get": {
        const item = await store.get(opts.namespace, opts.key);
        if (!item) return null;
        return { ns: item.namespace, key: item.key, value: serializer.loads(item.value) };
      }
      case "put": {
        const value = opts.value === undefined ? new Uint8Array(0) : serializer.dumps(opts.value);
        await store.put({
          namespace: opts.namespace,
          key: opts.key,
          value,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        });
        return { ok: true };
      }
      case "search": {
        const items = await store.search(opts.namespace, {
          filter: opts.filter ? new Map(Object.entries(opts.filter)) : undefined,
          limit: opts.limit,
          offset: opts.offset,
        });
        return items.map((i) => ({
          ns: i.namespace,
          key: i.key,
          value: serializer.loads(i.value),
        }));
      }
      case "delete":
        await store.delete(opts.namespace, opts.key);
        return { ok: true };
      case "list_namespaces":
        return store.listNamespaces();
      default:
        throw new GraphError(`unknown store op: ${(opts as { op: string }).op}`);
    }
  }

  // -- internals -----------------------------------------------------------

  /** Convert a TaskSuccess/TaskFailure into a scheduler TaskResult (shared by
   * the promise and the streaming-generator paths). */
  /**
   * RGP/1 computed execution (after the task cascade settles): for each wire
   * computed, evaluate the host selector when its declared read set changed
   * and commit the value to `state[id]` as one transaction.
   *
   * The selector body is remote: the host callback is invoked with the current
   * store snapshot, then `scheduler.compute()` records the cache entry /
   * emits `computed:{id}` vs `computed:{id}:hit` spans (the local placeholder
   * selector never runs). Irrelevant-path writes never re-invoke the selector
   * — read-set invalidation in `Scheduler.applyPatches` owns that.
   */
  private async computeComputeds(
    scheduler: Scheduler,
    compiled: CompiledGraph,
    store: ReactiveStore,
    session: ActiveSession,
  ): Promise<void> {
    const wires = new Map((compiled.spec.computeds ?? []).map((c) => [c.id, c]));
    for (const def of compiled.graph.def.computeds) {
      const wire = wires.get(def.id);
      if (!wire) continue;
      // Skip before any host round-trip when nothing changed: the cache is
      // fresh (same read-set hash) and the committed value is already in state.
      if (scheduler.isComputedFresh(def.id) && store.raw[def.id] !== undefined) {
        scheduler.compute(def.id); // records computed_cache_hit span only
        continue;
      }
      const raw = session.executor.invokeTask(
        wire.callbackId,
        store.raw,
        store.version,
        session.runId,
      );
      const success = await (typeof (raw as Promise<TaskSuccess>).then === "function"
        ? (raw as Promise<TaskSuccess>)
        : (async () => {
            let r = await (raw as AsyncGenerator<TaskStreamChunk>).next();
            while (!r.done) r = await (raw as AsyncGenerator<TaskStreamChunk>).next();
            return r.value;
          })());
      // Cache the evaluated value WITHOUT re-applying the patch — scheduler.compute
      // only writes computedCache (selector is a throwing placeholder).
      scheduler.compute(def.id, success.return_value);
      const patches = success.patches ?? [];
      if (patches.length === 0) continue;
      const tx = new Transaction(store, { taskId: `computed:${def.id}` });
      for (const patch of patches) {
        if (patch.operation === "set") {
          tx.set(patch.path, patch.value);
        }
      }
      tx.commit();
    }
  }

  /**
   * Bind a compiled graph's remote handlers to one immutable run session.
   *
   * ``active`` is retained for the legacy version accessor, but it is shared
   * mutable state and therefore cannot identify concurrent runs. Every run
   * gets its own Graph view whose handlers close over the session carrying
   * that run's runId and store version.
   */
  private bindGraphToSession(compiled: CompiledGraph, session: ActiveSession): Graph {
    const def: GraphDef = {
      ...compiled.graph.def,
      tasks: compiled.graph.def.tasks.map((task) => ({
        ...task,
        handler: this.remoteHandler(compiled.callbackByTask.get(task.id)!, session),
      })),
    };
    return new Graph(def);
  }

  private remoteHandler(callbackId: string, session: ActiveSession): TaskHandler {
    return (input: unknown) => {
      const raw = session.executor.invokeTask(callbackId, input, session.version(), session.runId);
      // Streaming executor: pass the async generator through so the scheduler
      // emits each yielded chunk (messages/custom) and commits the return value.
      if (raw && typeof (raw as { next?: unknown }).next === "function") {
        const gen = raw as AsyncGenerator<TaskStreamChunk, TaskSuccess, unknown>;
        return (async function* (): AsyncGenerator<TaskStreamChunk, TaskResult, unknown> {
          let it = await gen.next();
          while (!it.done) {
            yield it.value;
            it = await gen.next();
          }
          return toTaskResult(it.value);
        })();
      }
      return Promise.resolve(raw).then((r) => toTaskResult(r as TaskSuccess & TaskFailure));
    };
  }

  private async storeFor(threadId: string): Promise<ReactiveStore> {
    let store = this.stores.get(threadId);
    if (store) {
      // Map insertion order is the LRU order: re-insert on access.
      this.stores.delete(threadId);
      this.stores.set(threadId, store);
      return store;
    }
    this.ensureThreadCapacity();
    store = new ReactiveStore({});
    if (this.persistence?.recoverThreads || this.persistence?.log) {
      await this.recoverStore(store, threadId);
    }
    this.stores.set(threadId, store);
    this.evictThreadStores();
    return store;
  }

  /** Memory-only runtimes cannot evict authoritative state, so they fail
   * closed at the ceiling instead of silently resetting a thread. Durable
   * runtimes admit the new thread and let `evictThreadStores` drop the LRU
   * entry, which is rebuilt on next access. */
  private ensureThreadCapacity(): void {
    if (this.maxThreads <= 0 || this.stores.size < this.maxThreads) return;
    const recoverable = Boolean(this.persistence?.recoverThreads || this.persistence?.log);
    if (recoverable) return;
    throw new RuntimeCapacityError(
      `in-memory thread ceiling reached (${this.maxThreads}); ` +
        `add persistence/recoverThreads, raise maxThreads, or delete_thread first`,
    );
  }

  /** Evict least-recently-used thread stores once the in-memory cache exceeds
   * its ceiling. Eviction is only safe when the state can be rebuilt from a
   * durable log/checkpoint on the next access; otherwise the stores map is
   * authoritative state and must not be dropped silently. */
  private evictThreadStores(): void {
    if (this.maxThreads <= 0) return;
    const recoverable = Boolean(this.persistence?.recoverThreads || this.persistence?.log);
    if (!recoverable) return;
    const activeThreadId = this.active?.threadId;
    while (this.stores.size > this.maxThreads) {
      let evicted = false;
      for (const threadId of this.stores.keys()) {
        if (threadId === activeThreadId) continue;
        this.stores.delete(threadId);
        evicted = true;
        break;
      }
      if (!evicted) return;
    }
  }

  /** Rehydrate a thread store from the durable log (or latest checkpoint). */
  private async recoverStore(store: ReactiveStore, threadId: string): Promise<void> {
    const p = this.persistence!;
    if (p.log) {
      const recovered = recover(p.log, { threadId });
      for (const [k, v] of Object.entries(recovered.state)) {
        (store.raw as Record<string, unknown>)[k] = v;
      }
      // Bring the store version up to the recovered version so stale-version
      // checks after a restart behave exactly like a continuing process.
      const target = recovered.stateVersion;
      for (let v = store.version; v < target; v++) {
        store.commitVersion(new Map());
      }
      return;
    }
    if (p.checkpointSaver) {
      const rec = await p.checkpointSaver.get(threadId);
      if (rec) {
        const values = serializer.loads(rec.values) as Record<string, unknown>;
        for (const [k, v] of Object.entries(values)) {
          (store.raw as Record<string, unknown>)[k] = v;
        }
      }
    }
  }

  /** Persist the current thread state as a checkpoint (called after a run). */
  private async checkpointThread(
    threadId: string,
    state: Record<string, unknown>,
    expectedParent?: string,
  ): Promise<void> {
    const p = this.persistence;
    if (!p?.checkpointSaver) return;
    await p.checkpointSaver.put({
      threadId,
      checkpointId: `cp-${++this.runSeq}`,
      parentCheckpointId: expectedParent,
      expectedParentCheckpointId: expectedParent,
      ts: new Date().toISOString(),
      values: serializer.dumps(state),
      metadata: serializer.dumps({}),
    });
  }

  private checkpointSaverFor(threadId: string): CheckpointSaver {
    void threadId;
    if (this.persistence?.checkpointSaver) return this.persistence.checkpointSaver;
    return this.memoryCheckpointSaver;
  }

  private longTermFor(namespace: readonly string[]): LongTermStore {
    if (this.persistence?.longTermStore) return this.persistence.longTermStore;
    // A single shared in-memory store: namespaces are prefixes for search,
    // so items must live in one store regardless of their namespace.
    void namespace;
    let store = this.longTerm.get("__memory__");
    if (!store) {
      store = new MemoryStore();
      this.longTerm.set("__memory__", store);
    }
    return store;
  }

  /** Unwrap a scheduler TaskExecutionError to find a host interrupt. */
  private extractInterrupt(err: unknown): unknown {
    if (err instanceof TaskInterruptedError) return err.value;
    if (err instanceof TaskExecutionError) {
      if (err.cause instanceof TaskInterruptedError) return err.cause.value;
    }
    return undefined;
  }

  get activeVersion(): number {
    return this.active?.version() ?? 0;
  }
}

/**
 * Recursively drop `undefined` values before records cross the wire
 * (MessagePack cannot encode undefined; storage rows may carry an absent
 * optional field, e.g. parentCheckpointId).
 */
export function stripUndefined<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((v) => stripUndefined(v)) as unknown as T;
  }
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (v !== undefined) out[k] = stripUndefined(v);
    }
    return out as T;
  }
  return value;
}

/** Convert a TaskSuccess/TaskFailure into a scheduler TaskResult. Shared by
 * the promise and the streaming-generator executor paths. */
function toTaskResult(result: TaskSuccess & TaskFailure): TaskResult {
  if (result.error) {
    const type = result.error.type ?? "TaskError";
    if (type === "Interrupt" || type === "GraphInterrupt" || type === "NodeInterrupt") {
      throw new TaskInterruptedError(result.error.message);
    }
    throw new GraphError(`${type}: ${result.error.message ?? "host callback failed"}`);
  }
  const patches = (result.patches ?? []) as WirePatch[];
  const writes = (result.writes ?? []) as readonly string[];
  return {
    patches,
    reads: result.reads ?? [],
    writes,
    receipt: (result.external_receipts?.[0] as { receipt?: string } | undefined)?.receipt,
    emits: result.emits,
  };
}
