/**
 * Function/decorator workflows (Task 8).
 *
 * Mirrors upstream `langgraph.func` entrypoint/task pattern in a Driver-native
 * way: a decorated function becomes a task whose handler runs the function;
 * entrypoints compose tasks through explicit event routes. Persistence is
 * delegated to the scheduler (fingerprint skip / receipt gate).
 */

import { GraphBuilder, type TaskDef, type TaskKind, type TaskResult } from "./graph/model.js";
import type { ScheduleEvent } from "./scheduler/scheduler.js";

export interface FunctionTaskOptions {
  readonly id?: string;
  readonly kind?: TaskKind;
  readonly on?: readonly string[];
  readonly timeoutMs?: number;
  readonly writes?: readonly string[];
}

type AnyFn = (input: unknown) => unknown;

export interface FunctionTask {
  readonly id: string;
  readonly def: TaskDef;
}

/**
 * `functask` turns a plain function into a task. If the function returns an
 * object with `patches`, it is treated as a TaskResult; otherwise it returns no
 * patches (callers use explicit writes via the returned object).
 */
export function functask(fn: AnyFn, opts: FunctionTaskOptions = {}): FunctionTask {
  const id = opts.id ?? fn.name;
  const kind = opts.kind ?? "pure";
  const handler = async (input: unknown): Promise<TaskResult> => {
    const out = await fn(input);
    if (out && typeof out === "object" && "patches" in (out as object)) {
      return out as TaskResult;
    }
    return { reads: [], writes: opts.writes ?? [], patches: [] };
  };
  return {
    id,
    def: { id, kind, handler, on: opts.on, timeoutMs: opts.timeoutMs, writes: opts.writes },
  };
}

export interface EntrypointPlan {
  readonly graphId: string;
  readonly taskIds: readonly string[];
  readonly emitted: ScheduleEvent[];
}

/** `entrypoint` plans a graph from task defs (build via buildFunctionGraph). */
export function entrypoint(
  graphId: string,
  tasks: readonly FunctionTask[],
  routes?: readonly { event: string; taskId: string }[],
): EntrypointPlan {
  return {
    graphId,
    taskIds: tasks.map((t) => t.id),
    emitted: (routes ?? []).map((r) => ({
      name: r.event,
      payload: { taskId: r.taskId },
    })),
  };
}

/** Build a GraphBuilder from functask definitions (convenience). */
export function buildFunctionGraph(
  graphId: string,
  tasks: readonly FunctionTask[],
  routes?: readonly { event: string; taskId: string }[],
): GraphBuilder {
  const builder = new GraphBuilder(graphId);
  for (const t of tasks) {
    builder.task(t.def);
  }
  for (const r of routes ?? []) {
    builder.on(r.event, r.taskId);
  }
  return builder;
}
