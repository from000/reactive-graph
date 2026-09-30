/**
 * Unified reactive scheduler (Task 7).
 *
 * Eligibility:
 *  - explicit events and graph routes decide whether effect tasks run;
 *  - computed invalidation (read-set change) makes dependent tasks eligible;
 *  - pure tasks may be skipped when their input fingerprint is unchanged;
 *  - effect tasks never auto-rerun (idempotency receipt gate).
 *
 * Concurrency is limited; conflicts are checked against committed write claims;
 * only tasks with a retry policy are retried; every decision is recorded on the
 * durable log. Ties are broken deterministically (task id order).
 */

import { Graph } from "../graph/model.js";
import type { TaskHandler, TaskResult, TaskStreamChunk } from "../graph/model.js";
import { ReactiveStore } from "../state/store.js";
import { Transaction } from "../state/transaction.js";
import { getAtPath, parsePath } from "../state/path.js";
import { canonicalHash } from "../state/hash.js";
import type { DurableLog } from "../durability/log.js";
import { events } from "../durability/events.js";
import { decideRetry, type RetryPolicy } from "./retry.js";
import { detectWriteConflict, type WriteClaim } from "./conflicts.js";
import { compact } from "../durability/recovery.js";
import { validatePatches, PermissionDeniedError, type PermissionPolicy } from "../permissions.js";
import { checkAll, type Policy } from "../policies.js";
import type { SpanEmitter } from "../otel.js";

export interface SchedulerOptions {
  readonly concurrency?: number;
  readonly graph: Graph;
  readonly store: ReactiveStore;
  readonly log?: DurableLog;
  readonly runId?: string;
  readonly threadId?: string;
  readonly now?: () => number;
  /** Compact the durable log when it grows past this many events (0 disables). */
  readonly compactAfter?: number;
  /** Optional field-level permission policy enforced at patch commit time
   * (a patch touching a denied path raises PermissionDeniedError and is
   * never committed). */
  readonly permissionPolicy?: PermissionPolicy;
  /** Optional task policies (budget / rate-limit / circuit-breaker / priority)
   * consulted before dispatch; a denied task is not run. */
  readonly policies?: readonly Policy[];
  /** Optional recursion limit: max number of HANDLER executions per scheduler
   * lifetime (skipped pure/receipt tasks don't count). Guards runaway agent
   * loops; undefined = unlimited (backward compatible). */
  readonly recursionLimit?: number;
  /** Optional OTel span emitter: task executions, retries, computed
   * recomputations and cache hits are emitted as spans (observability hook). */
  readonly spanEmitter?: SpanEmitter;
  /** Optional transient-chunk sink: called for every chunk a streaming task
   * yields (type messages/custom) — wired to STREAM_EVENT in the runtime. */
  readonly onStreamChunk?: (taskId: string, chunk: TaskStreamChunk) => void;
  /**
   * Confirmed effect receipts restored from a durable log, keyed by
   * `taskId:inputHash` -> receipt. The idempotency gate only applies to the
   * exact input whose effect was confirmed — a different input still runs.
   */
  readonly initialReceipts?: ReadonlyMap<string, string>;
  /** The run's user payload: effect idempotency keys derive from THIS input
   * (not the task-merged input `{...store, ...baseInput}` — restored store
   * state must not drift an effect's idempotency key across restarts). */
  readonly baseInput?: unknown;
  /** Cross-run persistent state shared with the runtime: pure-task input
   * fingerprints survive scheduler recreation between runs, so identical
   * inputs skip on subsequent runs (selective-execution across runs). */
  readonly persistent?: SchedulerPersistent;
}

/** Cross-run scheduler state (shared reference; lives on the compiled graph). */
export interface SchedulerPersistent {
  /** taskId -> last input fingerprint (pure skip gate). */
  readonly fingerprints: Map<string, string>;
}

/** Outcome of one task execution as observed by the runtime. */
export interface TaskRunOutcome {
  readonly patches: unknown[];
  readonly writes: readonly string[];
  /** Explicit follow-up events; absent → implicit `<taskId>:written`. */
  readonly emits?: readonly string[];
}

export interface ScheduleEvent {
  readonly name: string;
  readonly payload?: unknown;
}

export interface SchedulerTraceEntry {
  readonly seq: number;
  readonly kind: string;
  readonly taskId?: string;
  readonly computedId?: string;
  readonly reason?: string;
  readonly stateVersion?: number;
}

/** Internal task runtime state. */
interface RuntimeTask {
  readonly id: string;
  readonly kind: "pure" | "effect" | "opaque";
  readonly handler: TaskHandler;
  readonly retry?: RetryPolicy;
  readonly timeoutMs: number;
  /** Declared write paths (conflict pre-check in runAll). */
  readonly writes: readonly string[];
  /** Named sub-state namespace; reads/writes are prefixed with it. */
  readonly scope?: string;
  /** Confirmed effect receipts (inputHash -> receipt) — idempotency gate. */
  readonly receipts: Map<string, string>;
  /** Last input fingerprint — pure skip gate. */
  fingerprint: string;
}

export class TaskExecutionError extends Error {
  readonly taskId: string;
  readonly attempt: number;
  override readonly cause?: unknown;
  constructor(taskId: string, attempt: number, message: string, cause?: unknown) {
    super(message);
    this.name = "TaskExecutionError";
    this.taskId = taskId;
    this.attempt = attempt;
    this.cause = cause;
  }
}

export interface WriteConflictDetail {
  readonly kind: "write_conflict";
  readonly reason: "declared_write_conflict" | "actual_write_conflict";
  readonly taskId: string;
  readonly winnerTaskId: string;
  readonly path: string;
  readonly declaredWrites: Record<string, readonly string[]>;
  readonly actualWrites: Record<string, readonly string[]>;
}

/** Raised before a task's patches commit when they collide with a write claim
 * already committed by another task in the same parallel batch. Not retryable:
 * the conflict is with concurrent work, not a transient failure. */
export class WriteConflictError extends TaskExecutionError {
  readonly conflict: WriteConflictDetail;
  constructor(taskId: string, path: string, otherTaskId: string, reason: string) {
    super(taskId, 1, `write conflict: ${reason} (${path}) vs task ${otherTaskId}`);
    this.name = "WriteConflictError";
    this.conflict = {
      kind: "write_conflict",
      reason:
        reason === "actual_write_conflict" ? "actual_write_conflict" : "declared_write_conflict",
      taskId,
      winnerTaskId: otherTaskId,
      path,
      declaredWrites: {},
      actualWrites: {},
    };
  }
}

function writeSets(claims: readonly WriteClaim[]): Record<string, readonly string[]> {
  return Object.fromEntries(claims.map((c) => [c.taskId, [...c.writes]]));
}

function withWriteSets(
  err: WriteConflictError,
  declared: readonly WriteClaim[],
  actual: readonly WriteClaim[],
  reason: WriteConflictDetail["reason"],
): WriteConflictError {
  const conflict: WriteConflictDetail = {
    ...err.conflict,
    reason,
    declaredWrites: writeSets(declared),
    actualWrites: writeSets(actual),
  };
  return Object.create(err, {
    conflict: { value: conflict, enumerable: true },
  });
}

export class RecursionLimitError extends Error {
  constructor(limit: number) {
    super(
      `recursion limit exceeded: ${limit} handler executions — Hint: 任务循环未收敛,检查事件路由/终止条件,或调大 recursionLimit`,
    );
    this.name = "RecursionLimitError";
  }
}

export class Scheduler {
  private readonly graph: Graph;
  private readonly store: ReactiveStore;
  private readonly log?: DurableLog;
  private readonly _runId: string;
  private readonly threadId: string;
  private readonly concurrency: number;
  private readonly now: () => number;
  private readonly compactAfter: number;
  private readonly permissionPolicy?: PermissionPolicy;
  private readonly policies?: readonly Policy[];
  private readonly recursionLimit?: number;
  private executedSteps = 0;
  private readonly spanEmitter?: SpanEmitter;
  private readonly onStreamChunk?: (taskId: string, chunk: TaskStreamChunk) => void;
  private readonly runtime = new Map<string, RuntimeTask>();
  private readonly computedCache = new Map<string, { value: unknown; readsHash: string }>();
  private readonly baseInput: unknown;
  private readonly persistent?: SchedulerPersistent;
  private seq = 0;
  private readonly trace: SchedulerTraceEntry[] = [];
  private readonly decided = new Map<string, SchedulerTraceEntry>();

  /** The run identifier recorded on every trace entry and durable event. */
  get runId(): string {
    return this._runId;
  }

  constructor(opts: SchedulerOptions) {
    this.graph = opts.graph;
    this.store = opts.store;
    this.log = opts.log;
    this._runId = opts.runId ?? "run-1";
    this.threadId = opts.threadId ?? "thread-1";
    this.concurrency = opts.concurrency ?? 8;
    this.now = opts.now ?? (() => Date.now());
    this.compactAfter = opts.compactAfter ?? 10_000;
    this.permissionPolicy = opts.permissionPolicy;
    this.policies = opts.policies;
    this.recursionLimit = opts.recursionLimit;
    this.spanEmitter = opts.spanEmitter;
    this.onStreamChunk = opts.onStreamChunk;
    this.baseInput = opts.baseInput;
    this.persistent = opts.persistent;
    const initialReceipts = opts.initialReceipts ?? new Map<string, string>();
    for (const t of this.graph.tasks()) {
      // Restored receipts are keyed `taskId:inputHash`; seed only this task's.
      // Task ids must not contain ':' (GraphBuilder enforces non-empty unique
      // ids; the prefix contract is `taskId:` + hex input hash).
      const receipts = new Map<string, string>();
      const prefix = `${t.id}:`;
      for (const [key, receipt] of initialReceipts) {
        if (key.startsWith(prefix)) receipts.set(key.slice(prefix.length), receipt);
      }
      this.runtime.set(t.id, {
        id: t.id,
        kind: t.kind,
        handler: t.handler,
        retry: t.retry,
        timeoutMs: t.timeoutMs ?? 0,
        writes: t.writes ?? [],
        scope: t.scope,
        fingerprint: this.persistent?.fingerprints.get(t.id) ?? "",
        receipts,
      });
    }
  }

  /** Feed an explicit event into the scheduler (non-blocking). */
  emit(event: ScheduleEvent): void {
    const routeTaskIds = this.graph.routeFor(event.name);
    this.record("event", undefined, { taskId: routeTaskIds.join(","), reason: event.name });
    for (const taskId of routeTaskIds) {
      this.record("eligible", taskId, { reason: `event:${event.name}` });
    }
  }

  /** Task ids that listen to the given event (delegates to the graph). */
  routeFor(event: string): string[] {
    return this.graph.routeFor(event);
  }

  /** Run a single task to completion with retries; returns its resulting patches.
   *
   * When `committed` is supplied (parallel batch), the task's actual write set
   * is checked against claims already committed in this batch before its
   * patches commit — a same-path write raises `WriteConflictError` (never
   * retried). This backstops the declared-write pre-check in `runAll` for
   * tasks that under-declare their writes.
   */
  async runTask(
    taskId: string,
    input: unknown,
    committed?: WriteClaim[],
    actualWrites?: ReadonlyMap<string, readonly string[]>,
  ): Promise<TaskRunOutcome> {
    const task = this.runtime.get(taskId);
    if (!task) throw new Error(`unknown task ${taskId}`);
    // Pure-task fingerprint skip. 指纹基于 run 输入（baseInput，与 effect 幂等
    // 键一致）而非任务合并输入 `{...store, ...baseInput}`：跨 run 时 store
    // 携带上一轮写入，合并输入恒变会让跨 run 跳过失效。
    if (task.kind === "pure") {
      const fp = JSON.stringify(this.baseInput !== undefined ? this.baseInput : (input ?? null));
      if (fp === task.fingerprint) {
        this.record("skip", taskId, { reason: "fingerprint_unchanged" });
        this.spanEmitter?.emit("skip", `task:${taskId}:skip`, {
          taskId,
          runId: this.runId,
          reason: "fingerprint_unchanged",
        });
        return { patches: [], writes: [] };
      }
      task.fingerprint = fp;
      // 持久化到跨 run 共享状态：后续 run（新 Scheduler）同输入仍跳过。
      this.persistent?.fingerprints.set(taskId, fp);
    }
    // Effect-task idempotency gate: never re-run an effect whose receipt for
    // THIS run payload (baseInput) is already confirmed. Different payloads
    // still execute. The key derives from the user input (baseInput when
    // provided by the runtime; otherwise the task input as passed to runTask
    // — direct scheduler callers pass the user input themselves). Never the
    // task-merged input `{...store, ...baseInput}` — restored store state
    // (other tasks' writes) must not drift the key across restarts.
    let inputHash: string | undefined;
    if (task.kind === "effect") {
      inputHash = canonicalHash(this.baseInput !== undefined ? this.baseInput : (input ?? null));
      if (task.receipts.has(inputHash)) {
        this.record("skip", taskId, { reason: "receipt_present" });
        this.spanEmitter?.emit("skip", `task:${taskId}:skip`, {
          taskId,
          runId: this.runId,
          reason: "receipt_present",
        });
        return { patches: [], writes: [] };
      }
      this.log?.append(
        events.effectIntent({
          idempotencyKey: `${taskId}:${inputHash}`,
          taskId,
          effectId: taskId,
          inputHash,
          stateVersion: this.store.version,
          runId: this.runId,
        }),
      );
    }

    let attempt = 1;
    let backoff = 0;
    while (true) {
      if (backoff > 0) {
        await sleep(backoff * 1000);
      }
      this.record("start", taskId, { reason: `attempt:${attempt}` });
      // Task-policy admission gate (budget / rate-limit / circuit-breaker /
      // priority). A denied task is not run (denial is not a failure, so it
      // must not trip circuit-breaker failure accounting).
      if (this.policies && this.policies.length > 0) {
        const decision = checkAll(this.policies, { taskId, runId: this.runId });
        if (!decision.allowed) {
          throw new TaskExecutionError(
            taskId,
            attempt,
            `policy denied: ${decision.reason ?? "task policy"}`,
          );
        }
      }
      const started = this.now();
      // Observability: one OTel span per attempt.
      const span = this.spanEmitter?.start("task", `task:${taskId}`, {
        runId: this.runId,
        taskId,
        attempt,
      });
      if (this.recursionLimit !== undefined) {
        this.executedSteps += 1;
        if (this.executedSteps > this.recursionLimit) {
          throw new RecursionLimitError(this.recursionLimit);
        }
      }
      try {
        const result = await this.withTimeout(
          (async () => {
            const raw = task.handler(input, { taskId, timeoutMs: task.timeoutMs });
            // Streaming task: handler is an async generator — each yielded chunk
            // is a transient stream event; the generator's return value is the
            // committed TaskResult.
            if (raw && typeof (raw as { next?: unknown }).next === "function") {
              const gen = raw as AsyncGenerator<TaskStreamChunk, TaskResult, unknown>;
              let it = await gen.next();
              while (!it.done) {
                this.onStreamChunk?.(taskId, it.value);
                it = await gen.next();
              }
              return it.value;
            }
            return raw as TaskResult | Promise<TaskResult>;
          })(),
          task.timeoutMs,
        );
        // Scope isolation: a task with a declared scope reads/writes under
        // state[scope], so same-named keys in different scopes never collide.
        const scoped = this.scopeResult(task, result);
        if (committed && scoped.writes.length > 0 && committed.length > 0) {
          const verdict = detectWriteConflict({ taskId, writes: scoped.writes }, committed);
          if (verdict.kind === "conflict") {
            const observed = actualWrites
              ? [...actualWrites].map(([id, writes]) => ({ taskId: id, writes }))
              : committed.filter((claim) => claim.taskId !== taskId);
            throw withWriteSets(
              new WriteConflictError(taskId, verdict.path, verdict.otherTaskId, verdict.reason),
              this.declaredWriteClaims(),
              [
                ...observed.filter((claim) => claim.taskId !== taskId),
                { taskId, writes: scoped.writes },
              ],
              "actual_write_conflict",
            );
          }
        }
        this.record("done", taskId, {
          stateVersion: this.store.version,
          reason: `attempt:${attempt}`,
        });
        this.applyPatches(taskId, scoped.patches, scoped.writes, scoped.reads);
        // Confirm the effect receipt only AFTER its patches committed — a
        // receipt recorded but never committed would permanently gate the
        // effect off for this input.
        if (task.kind === "effect" && result.receipt) {
          task.receipts.set(inputHash!, result.receipt);
          this.log?.append(
            events.effectReceipt({
              idempotencyKey: `${taskId}:${inputHash!}`,
              taskId,
              receipt: result.receipt,
              outcome: "success",
            }),
          );
        }
        this.maybeCompact();
        span?.end();
        for (const p of this.policies ?? []) (p as { onSuccess?: () => void }).onSuccess?.();
        return { patches: scoped.patches, writes: scoped.writes, emits: scoped.emits };
      } catch (err) {
        if (err instanceof WriteConflictError) throw err; // never retry a conflict
        if (err instanceof PermissionDeniedError) throw err; // permission errors are not transient
        if (err instanceof RecursionLimitError) throw err; // runaway loops never retry
        span?.setError(err);
        span?.end();
        for (const p of this.policies ?? []) (p as { onError?: () => void }).onError?.();
        this.record("failed", taskId, { reason: (err as Error).message });
        this.log?.append(
          events.retryDecision({
            taskId,
            attempt,
            decision: "retry",
            backoffMs: 0,
            reason: (err as Error).message,
          }),
        );
        const decision = task.retry
          ? decideRetry(task.retry, err, attempt, taskId)
          : { retry: false, attempt, backoffSeconds: 0, reason: "no_retry_policy" };
        if (!decision.retry) {
          this.log?.append(
            events.retryDecision({
              taskId,
              attempt,
              decision: "give_up",
              backoffMs: 0,
              reason: decision.reason ?? "max_attempts",
            }),
          );
          throw new TaskExecutionError(taskId, attempt, (err as Error).message, err);
        }
        this.record("retry", taskId, { reason: `attempt:${attempt}->${decision.attempt}` });
        this.spanEmitter?.emit("retry", "retry", { taskId, attempt, runId: this.runId });
        attempt = decision.attempt;
        backoff = decision.backoffSeconds;
      } finally {
        const elapsed = this.now() - started;
        void elapsed;
      }
    }
  }

  /** Execute a batch of independent tasks with a concurrency limit.
   *
   * Write-conflict detection: each task's actual write set is checked against
   * write claims already committed by other tasks in this batch before its
   * patches commit (`WriteConflictError`, never retried). Serialized runs that
   * call `runTask` directly skip detection — sequential same-path writes are
   * legal follow-on updates.
   */
  async runAll(taskIds: string[], inputs: Map<string, unknown>): Promise<unknown[]> {
    const outcomes = await this.runBatch(taskIds, inputs);
    return [...outcomes.values()].map((o) => o.patches);
  }

  /** Execute a batch of independent tasks and return each outcome keyed by task
   * id (insertion order = completion order). The runtime uses this to read the
   * per-task `emits` follow-up events that drive re-entry routing. */
  async runBatch(
    taskIds: string[],
    inputs: Map<string, unknown>,
  ): Promise<Map<string, TaskRunOutcome>> {
    // Deterministic tie-break: run in task-id order, respecting concurrency.
    const sorted = [...taskIds].sort();
    const results = new Map<string, TaskRunOutcome>();
    const committed: WriteClaim[] = [];
    const actualWrites = new Map<string, readonly string[]>();
    let cursor = 0;
    // Conflict detection applies only to genuinely concurrent batches —
    // with concurrency 1 (serialized) same-path writes are legal follow-on
    // updates, matching the conflict policy in conflicts.ts.
    const checkConflicts = this.concurrency > 1 && sorted.length > 1;
    const worker = async (): Promise<void> => {
      while (true) {
        const idx = cursor++;
        if (idx >= sorted.length) return;
        const id = sorted[idx]!;
        const task = this.runtime.get(id);
        let claimed = false;
        if (checkConflicts && task) {
          // Declared writes are scoped like runtime writes so cross-scope
          // same-named declarations never collide.
          const declared = task.writes.map((w) => (task.scope ? `${task.scope}.${w}` : w));
          if (declared.length > 0) {
            const verdict = detectWriteConflict({ taskId: id, writes: declared }, committed);
            if (verdict.kind === "conflict") {
              const declaredClaims = taskIds.map((tid) => ({
                taskId: tid,
                writes: this.runtime.get(tid)?.writes ?? [],
              }));
              throw withWriteSets(
                new WriteConflictError(
                  id,
                  verdict.path,
                  verdict.otherTaskId,
                  "declared_write_conflict",
                ),
                declaredClaims,
                committed,
                "declared_write_conflict",
              );
            }
            committed.push({ taskId: id, writes: declared });
            claimed = true;
          }
        }
        try {
          const outcome = await this.runTask(
            id,
            inputs.get(id),
            checkConflicts ? committed : undefined,
            checkConflicts ? actualWrites : undefined,
          );
          results.set(id, outcome);
          if (checkConflicts && outcome.writes.length > 0) {
            actualWrites.set(id, outcome.writes);
          }
          if (checkConflicts) {
            if (outcome.writes.length > 0) {
              // 实际写集替换声明 claim：欠声明实际冲突——与实际替换后的
              // 其他 claims 比对（同一并行批内两任务写同 path 即冲突，
              // 无论声明与否；计划 P3-2 Task 2）。
              const observed = committed.findIndex((c) => c.taskId === id);
              const next = { taskId: id, writes: outcome.writes };
              const others = observed >= 0 ? committed.filter((_, i) => i !== observed) : committed;
              const verdict = detectWriteConflict(next, others);
              if (verdict.kind === "conflict") {
                // 替换阶段冲突：本任务 patches 已由 runTask 内 applyPatches
                // 落地（区别于 runTask 内检测的提交前拦截）——失败 run 不写
                // checkpoint，无持久化影响；内存 state 残留由调用方丢弃。
                throw withWriteSets(
                  new WriteConflictError(
                    id,
                    verdict.path,
                    verdict.otherTaskId,
                    "actual_write_conflict",
                  ),
                  this.declaredWriteClaims(),
                  [...results]
                    .map(([tid, result]) => ({
                      taskId: tid,
                      writes: result.writes,
                    }))
                    .filter((claim) => claim.writes.length > 0),
                  "actual_write_conflict",
                );
              }
              if (observed >= 0) committed[observed] = next;
              else committed.push(next);
            } else if (claimed) {
              // Declared but wrote nothing: drop the ghost claim so later
              // tasks are not rejected for a write that never happened.
              const observed = committed.findIndex((c) => c.taskId === id);
              if (observed >= 0) committed.splice(observed, 1);
            }
          }
        } catch (err) {
          // A failed task leaves no claim behind (no writes committed).
          if (claimed) {
            const observed = committed.findIndex((c) => c.taskId === id);
            if (observed >= 0) committed.splice(observed, 1);
          }
          throw err;
        }
      }
    };
    const workers: Promise<void>[] = [];
    for (let i = 0; i < Math.min(this.concurrency, sorted.length); i++) {
      workers.push(worker());
    }
    await Promise.all(workers);
    return results;
  }

  /** All task declarations, scoped the same way as runtime write sets. */
  private declaredWriteClaims(): WriteClaim[] {
    return this.graph.tasks().map((task) => ({
      taskId: task.id,
      writes: (task.writes ?? []).map((w) => (task.scope ? `${task.scope}.${w}` : w)),
    }));
  }

  /** Map a task's result into its scope namespace: patches under
   * `state[scope][...path]` and writes/reads prefixed `scope.path`. */
  private scopeResult(task: RuntimeTask, result: TaskResult): TaskResult {
    const scope = task.scope;
    if (!scope) return result;
    const prefix = (p: readonly (string | number)[]): (string | number)[] => [scope, ...p];
    return {
      ...result,
      patches: result.patches.map((p) => {
        const patch = p as { path: (string | number)[] };
        return { ...patch, path: prefix(patch.path) };
      }),
      writes: result.writes.map((w) => `${scope}.${w}`),
      reads: result.reads.map((r) => `${scope}.${r}`),
    };
  }

  private applyPatches(
    taskId: string,
    patches: unknown[],
    writes: readonly string[],
    reads: readonly string[] = [],
  ): void {
    if (patches.length === 0) return;
    // Field-level permission gate: a patch touching a denied path is rejected
    // before anything is committed (PermissionDeniedError propagates).
    if (this.permissionPolicy) {
      validatePatches(
        this.permissionPolicy,
        patches as { path: (string | number)[]; value?: unknown }[],
      );
    }
    const tx = new Transaction(this.store, { taskId });
    for (const patch of patches as { path: (string | number)[]; value: unknown }[]) {
      tx.set(patch.path, patch.value);
    }
    const committed = tx.commit();
    this.log?.append(
      events.transaction({
        graphHash: this.graph.id,
        runId: this.runId,
        threadId: this.threadId,
        stateVersion: this.store.version,
        txId: committed[0]?.transactionId ?? tx.id,
        taskId,
        readPaths: [...reads],
        writePaths: [...writes],
        patches: committed,
      }),
    );
    // Invalidate dependent computeds.
    for (const c of this.graph.def.computeds) {
      const hit = c.reads?.some((r) =>
        writes.some((w) => w === r || w.startsWith(`${r}.`) || r.startsWith(`${w}.`)),
      );
      if (hit) {
        this.computedCache.delete(c.id);
        this.record("invalidated", undefined, {
          computedId: c.id,
          reason: `writes:${writes.join(",")}`,
        });
        this.spanEmitter?.emit("invalidate", `computed:${c.id}:invalidate`, {
          computedId: c.id,
          runId: this.runId,
          reason: "read_set_changed",
          reads: c.reads ?? [],
        });
      }
    }
  }

  /** Periodically snapshot + compact the durable log so it cannot grow
   * unboundedly. The snapshot carries the confirmed effect receipts so
   * compaction never breaks idempotency across restarts. */
  private maybeCompact(): void {
    const log = this.log;
    if (!log || this.compactAfter <= 0 || log.count < this.compactAfter) return;
    const confirmedEffects: Array<[string, string]> = [];
    for (const t of this.runtime.values()) {
      for (const [inputHash, receipt] of t.receipts) {
        confirmedEffects.push([`${t.id}:${inputHash}`, receipt]);
      }
    }
    compact(
      log,
      events.snapshot({
        runId: this.runId,
        threadId: this.threadId,
        stateVersion: this.store.version,
        stateHash: canonicalHash(this.store.raw),
        state: this.store.raw,
        baseSeq: log.lastSeq,
        confirmedEffects,
      }),
    );
  }

  /** Evaluate (and cache) a computed value against the current store.
   *
   * Invalidation is driven by the computed's declared read set: recompute only
   * when any read path's value changed (irrelevant-path writes never invalidate).
   */
  compute(id: string, value?: unknown): unknown {
    const c = this.graph.computed(id);
    if (!c) throw new Error(`unknown computed ${id}`);
    const readsHash = this.readsHash(c.reads ?? []);
    const cached = this.computedCache.get(id);
    if (value === undefined && cached && cached.readsHash === readsHash) {
      this.record("computed_cache_hit", undefined, { computedId: id });
      this.spanEmitter?.emit("cache_hit", `computed:${id}:hit`, {
        computedId: id,
        runId: this.runId,
      });
      return cached.value;
    }
    // Pre-evaluated value (RGP/1: the selector body ran host-side): record the
    // cache entry without running the local selector placeholder.
    const v = value !== undefined ? value : c.selector(this.store.raw);
    this.computedCache.set(id, { value: v, readsHash });
    this.record("computed", undefined, { computedId: id, stateVersion: this.store.version });
    this.spanEmitter?.emit("computed", `computed:${id}`, { computedId: id, runId: this.runId });
    return v;
  }

  /** Whether the cached computed is fresh for the current read set (no span,
   * no recompute) — lets wire-computed runtimes skip the host round-trip when
   * the read set is unchanged. */
  isComputedFresh(id: string): boolean {
    const c = this.graph.computed(id);
    if (!c) return false;
    const cached = this.computedCache.get(id);
    return !!cached && cached.readsHash === this.readsHash(c.reads ?? []);
  }

  /** Stable hash of the current values at the given canonical read paths. */
  private readsHash(reads: readonly string[]): string {
    const parts = reads.map((r) => JSON.stringify([r, getAtPath(this.store.raw, parsePath(r))]));
    parts.sort(); // deterministic regardless of declaration order
    return parts.join("|");
  }

  private withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    if (timeoutMs <= 0) return promise;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`task timed out after ${timeoutMs}ms`)),
        timeoutMs,
      );
      promise.then(
        (v) => {
          clearTimeout(timer);
          resolve(v);
        },
        (e) => {
          clearTimeout(timer);
          reject(e);
        },
      );
    });
  }

  private record(
    kind: string,
    taskId: string | undefined,
    extra?: { taskId?: string; computedId?: string; reason?: string; stateVersion?: number },
  ): void {
    const entry: SchedulerTraceEntry = {
      seq: ++this.seq,
      kind,
      taskId: extra?.taskId ?? taskId,
      computedId: extra?.computedId,
      reason: extra?.reason,
      stateVersion: extra?.stateVersion,
    };
    this.trace.push(entry);
    this.decided.set(`${kind}:${entry.taskId ?? entry.computedId ?? ""}:${entry.seq}`, entry);
  }

  /** Deterministic trace after normalization (task-id order stable). */
  getTrace(): readonly SchedulerTraceEntry[] {
    return this.trace;
  }

  get decidedCount(): number {
    return this.decided.size;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
