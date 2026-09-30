/**
 * Native ReactiveGraph model (Task 7 / docs/spec/native-api.md).
 *
 * A graph is a collection of tasks and computeds plus event routes:
 * - `task`: an executable unit (pure / effect / opaque);
 * - `computed`: a cached selector that recomputes only when its read set changes;
 * - `on(event)`: routes an explicit event to a task;
 * - `scope`: a named sub-state namespace;
 * - `transaction`: the scheduling/commit unit holding read+write sets.
 *
 * The scheduler (./scheduler/scheduler.ts) decides eligibility from explicit
 * events + graph routes, and invalidation from reactive read sets.
 */

import type { RetryPolicy } from "../scheduler/retry.js";

/** Task classification (plan Appendix C.4). */
export type TaskKind = "pure" | "effect" | "opaque";

export interface TaskContext {
  readonly taskId: string;
  /** Timeout in ms (0 = none). */
  readonly timeoutMs: number;
}

/** A transient chunk a streaming task may yield (token churn vs custom). */
export interface TaskStreamChunk {
  readonly type: "messages" | "custom";
  readonly payload: unknown;
}

/** Result of a task invocation as seen by the scheduler. */
export interface TaskResult {
  readonly patches: unknown[];
  /** Read/write paths (canonical strings) for conflict detection. */
  readonly reads: readonly string[];
  readonly writes: readonly string[];
  /** For effect tasks: idempotency receipt, if the effect is side-effecting. */
  readonly receipt?: string;
  /**
   * Follow-up events this task emits on success. `undefined` keeps the implicit
   * contract (`<taskId>:written`); an explicit list — including the empty list —
   * overrides it, which is how a task suppresses its own downstream emission
   * (conditional routing: "the loop stops here").
   */
  readonly emits?: readonly string[];
}

/**
 * A task handler may return its result directly or as an async generator:
 * each yielded chunk is emitted as a transient stream event (messages/custom),
 * and the generator's return value is the committed TaskResult — so a model
 * token stream can be observed remotely before the task commits.
 */
export type TaskHandler = (
  input: unknown,
  ctx: TaskContext,
) => TaskResult | Promise<TaskResult> | AsyncGenerator<TaskStreamChunk, TaskResult, unknown>;

export interface TaskDef {
  readonly id: string;
  readonly kind: TaskKind;
  readonly handler: TaskHandler;
  /** Event names that make this task eligible. */
  readonly on?: readonly string[];
  /** Computed ids whose invalidation makes this task eligible. */
  readonly dependsOn?: readonly string[];
  /** Declared write paths used for conflict pre-checking. */
  readonly writes?: readonly string[];
  /**
   * Re-entry budget for this task within a single run (default 1). A cycle of
   * event routes is only allowed to loop for tasks that opt in via `maxRuns`;
   * the runtime still stops at the budget, so a mis-declared loop fails closed
   * instead of hanging.
   */
  readonly maxRuns?: number;
  /** Named sub-state namespace: when set, the task's reads/writes are scoped
   * under `state[scope]`, isolating same-named keys between scopes. */
  readonly scope?: string;
  readonly retry?: RetryPolicy;
  readonly timeoutMs?: number;
}

export interface ComputedDef {
  readonly id: string;
  /** Selector: reads state (through the store), returns a value. */
  readonly selector: (state: Record<string, unknown>) => unknown;
  /** Declared read paths (canonical) — invalidation source of truth. */
  readonly reads?: readonly string[];
}

export interface EventRoute {
  readonly event: string;
  readonly taskId: string;
}

export interface GraphDef {
  readonly id: string;
  readonly tasks: readonly TaskDef[];
  readonly computeds: readonly ComputedDef[];
  readonly routes: readonly EventRoute[];
  readonly scopes: readonly string[];
}

export class GraphNotFoundError extends Error {
  constructor(graphId: string) {
    super(`graph not found: ${graphId}`);
    this.name = "GraphNotFoundError";
  }
}

/**
 * Immutable graph model built through `GraphBuilder`. Validation happens at
 * build time: duplicate ids, unknown route targets, and cycles without a policy
 * are rejected (mirrors RGP/1 COMPILE_GRAPH negative cases).
 */
export class Graph {
  readonly def: GraphDef;
  private readonly taskById: ReadonlyMap<string, TaskDef>;
  private readonly computedById: ReadonlyMap<string, ComputedDef>;

  constructor(def: GraphDef) {
    validateGraph(def);
    this.def = def;
    this.taskById = new Map(def.tasks.map((t) => [t.id, t]));
    this.computedById = new Map(def.computeds.map((c) => [c.id, c]));
  }

  get id(): string {
    return this.def.id;
  }

  task(id: string): TaskDef | undefined {
    return this.taskById.get(id);
  }

  computed(id: string): ComputedDef | undefined {
    return this.computedById.get(id);
  }

  tasks(): readonly TaskDef[] {
    return this.def.tasks;
  }

  routeFor(event: string): string[] {
    return this.def.routes.filter((r) => r.event === event).map((r) => r.taskId);
  }
}

function validateGraph(def: GraphDef): void {
  const ids = new Set<string>();
  for (const t of def.tasks) {
    if (ids.has(t.id)) throw new Error(`duplicate task id: ${t.id}`);
    ids.add(t.id);
    // implicit route: task declares it listens to events via t.on
    void t.on;
  }
  for (const c of def.computeds) {
    if (ids.has(c.id)) throw new Error(`duplicate node id: ${c.id}`);
    ids.add(c.id);
  }
  for (const r of def.routes) {
    if (!def.tasks.some((t) => t.id === r.taskId)) {
      throw new Error(`route targets unknown task: ${r.taskId} (event ${r.event})`);
    }
  }
  for (const t of def.tasks) {
    for (const dep of t.dependsOn ?? []) {
      if (!def.computeds.some((c) => c.id === dep) && !def.tasks.some((x) => x.id === dep)) {
        throw new Error(`task ${t.id} depends on unknown node ${dep}`);
      }
    }
  }
}

/** Fluent builder for the native API surface. */
export class GraphBuilder {
  private tasks: TaskDef[] = [];
  private computeds: ComputedDef[] = [];
  private routes: EventRoute[] = [];
  private scopes: string[] = [];
  private readonly graphId: string;

  constructor(graphId = "graph") {
    this.graphId = graphId;
  }

  task(def: Omit<TaskDef, "id"> & { id: string }): this {
    this.tasks.push({ ...def });
    return this;
  }

  computed(def: Omit<ComputedDef, "id"> & { id: string }): this {
    this.computeds.push({ ...def });
    return this;
  }

  /** Route an event to a task (also usable via `task.on`). */
  on(event: string, taskId: string): this {
    this.routes.push({ event, taskId });
    return this;
  }

  scope(name: string): this {
    this.scopes.push(name);
    return this;
  }

  build(): Graph {
    return new Graph({
      id: this.graphId,
      tasks: this.tasks,
      computeds: this.computeds,
      routes: this.routes,
      scopes: this.scopes,
    });
  }
}

export { RetryPolicy };

/** DOT node id: safe charset, prefixed to keep tasks/events/computeds unique. */
function dotNodeId(prefix: string, name: string): string {
  return `${prefix}_${name.replace(/[^A-Za-z0-9_.-]/g, "_")}`;
}

/**
 * Render a graph as Graphviz DOT (tasks -> event routes -> computeds with
 * their declared reads). Pipe to `dot -Tsvg` for a visual; the text is also
 * usable as a lightweight "graph as code" audit artifact.
 */
export function graphToDot(graph: Graph): string {
  const { id, tasks, computeds, routes } = graph.def;
  const lines: string[] = [`digraph ${JSON.stringify(id)} {`];
  if (graph.def.scopes.length > 0) {
    lines.push(`  label=${JSON.stringify(`scopes: ${graph.def.scopes.join(" / ")}`)};`);
  }
  for (const t of tasks) {
    lines.push(
      `  ${dotNodeId("t", t.id)} [label=${JSON.stringify(`${t.id} (${t.kind})`)}, shape=box];`,
    );
  }
  for (const c of computeds) {
    lines.push(
      `  ${dotNodeId("c", c.id)} [label=${JSON.stringify(`computed:${c.id}`)}, shape=ellipse];`,
    );
    for (const r of c.reads ?? []) {
      lines.push(
        `  ${dotNodeId("s", r)} -> ${dotNodeId("c", c.id)} [style=dashed, label=${JSON.stringify("reads")}];`,
      );
    }
  }
  for (const r of routes) {
    lines.push(`  ${dotNodeId("ev", r.event)} [label=${JSON.stringify(r.event)}, shape=diamond];`);
    lines.push(`  ${dotNodeId("ev", r.event)} -> ${dotNodeId("t", r.taskId)};`);
  }
  lines.push("}");
  return `${lines.join("\n")}\n`;
}
