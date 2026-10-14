/**
 * ReactiveGraph native JS SDK.
 *
 * Thin in-process binding over the Driver's scheduler + store. Exposes the
 * native build surface (GraphBuilder: task / computed / on / scope), invoke,
 * and causal-trace export; streaming over a remote session lives in
 * remote.ts (RemoteClient).
 */

import {
  GraphBuilder as DriverGraphBuilder,
  Graph,
  ReactiveStore,
  Scheduler,
  normalizeCausalTrace,
  type TaskDef,
  type ComputedDef,
  type TaskResult,
  type ScheduleEvent,
  type TracePath,
  type CausalTrace,
} from "@reactivegraph/driver";

export type { Graph, ScheduleEvent };

export interface GraphOptions {
  readonly graphId?: string;
  readonly concurrency?: number;
}

/** The native JS graph handle: build + invoke. */
export class ReactiveGraph {
  readonly id: string;
  readonly scheduler: Scheduler;
  readonly store: ReactiveStore;
  private readonly graph: Graph;

  private constructor(id: string, graph: Graph, opts: GraphOptions) {
    this.id = id;
    this.graph = graph;
    this.store = new ReactiveStore();
    this.scheduler = new Scheduler({
      graph,
      store: this.store,
      concurrency: opts.concurrency,
      runId: id,
      threadId: "default",
    });
  }

  /** Compile a graph from a builder callback and verify it. */
  static build(build: (b: GraphBuilder) => void, opts: GraphOptions = {}): ReactiveGraph {
    const builder = new GraphBuilder(opts.graphId ?? "graph");
    build(builder);
    const graph = builder.build();
    return new ReactiveGraph(graph.id, graph, opts);
  }

  /** Export the run's causal trace (every scheduling decision, normalized to
   * the @reactivegraph/devtools-protocol CausalTrace schema). Sensitive state
   * paths can be redacted before export. */
  trace(opts: { redact?: readonly TracePath[] } = {}): CausalTrace {
    return normalizeCausalTrace(this.scheduler, {
      runId: this.scheduler.runId,
      redact: opts.redact,
    });
  }
}

/** Fluent native builder (mirrors GraphBuilder in the Driver). */
export class GraphBuilder {
  private readonly inner: DriverGraphBuilder;

  constructor(graphId = "graph") {
    this.inner = new DriverGraphBuilder(graphId);
  }

  task(def: {
    id: string;
    kind: "pure" | "effect" | "opaque";
    handler: (
      input: unknown,
      ctx: { taskId: string; timeoutMs: number },
    ) => TaskResult | Promise<TaskResult>;
    on?: readonly string[];
    retry?: {
      maxAttempts?: number;
      initialInterval?: number;
      backoffFactor?: number;
      maxInterval?: number;
      jitter?: boolean;
      retryOn?: (err: unknown) => boolean;
    };
    timeoutMs?: number;
    scope?: string;
  }): this {
    this.inner.task(def as TaskDef);
    return this;
  }

  computed(def: {
    id: string;
    reads?: readonly string[];
    selector: (state: Record<string, unknown>) => unknown;
  }): this {
    this.inner.computed(def as ComputedDef);
    return this;
  }

  on(event: string, taskId: string): this {
    this.inner.on(event, taskId);
    return this;
  }

  scope(name: string): this {
    this.inner.scope(name);
    return this;
  }

  build(): Graph {
    return this.inner.build();
  }
}

/** Convenience: single task + invoke pattern (see python/graph.py mirror). */
export async function invoke(
  graph: ReactiveGraph,
  event: string,
  input: unknown,
): Promise<unknown> {
  const taskIds = graph.scheduler.routeFor(event);
  if (taskIds.length === 0) throw new Error(`no task routes for event ${event}`);
  await graph.scheduler.runAll(taskIds, new Map(taskIds.map((id) => [id, input])));
  return { state: graph.store.raw };
}

/** Causal trace export shape (re-exported for callers). */
export type { CausalTrace, TracePath, SchedulerTraceEntry } from "@reactivegraph/driver";
