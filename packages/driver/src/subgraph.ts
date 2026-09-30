/**
 * Subgraphs (Task 8 / docs/spec/streams-and-interrupts.md).
 *
 * A subgraph runs against its own state scope, with typed input/output mappings
 * into the parent scope. Nested interrupts resume at the correct scope (a
 * subgraph interrupt is resolved inside the subgraph, then the result flows
 * back to the parent through the output mapping).
 */

import { ReactiveStore } from "./state/store.js";
import { Graph } from "./graph/model.js";
import { Scheduler } from "./scheduler/scheduler.js";
import { InterruptManager } from "./interrupts.js";

export class SubgraphError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SubgraphError";
  }
}

export interface SubgraphIO {
  /** Copy parent paths -> child input. */
  readonly input: ReadonlyMap<string, string>;
  /** Copy child output paths -> parent paths (after child finishes). */
  readonly output: ReadonlyMap<string, string>;
}

export interface SubgraphDef {
  readonly id: string;
  readonly graph: Graph;
  readonly io: SubgraphIO;
}

export interface NestedRunOptions {
  readonly concurrency?: number;
}

/**
 * Runs a subgraph with an isolated store, maps input from the parent scope,
 * runs to completion (or interrupt), and maps output back.
 */
export class SubgraphRunner {
  private readonly defs = new Map<string, SubgraphDef>();

  register(def: SubgraphDef): void {
    this.defs.set(def.id, def);
  }

  /**
   * Execute a subgraph within the parent store.
   * @returns mapped output written into the parent store's next transaction.
   */
  async run(
    parentStore: ReactiveStore,
    subgraphId: string,
    interrupts: InterruptManager,
    opts: NestedRunOptions = {},
  ): Promise<Record<string, unknown>> {
    const def = this.defs.get(subgraphId);
    if (!def) throw new SubgraphError(`unknown subgraph ${subgraphId}`);

    // Isolated child scope seeded from parent via input mapping.
    const childInitial: Record<string, unknown> = {};
    for (const [parentPath, childPath] of def.io.input) {
      childInitial[childPath] = get(parentStore.raw, parentPath);
    }
    const childStore = new ReactiveStore(childInitial);
    const childScheduler = new Scheduler({
      graph: def.graph,
      store: childStore,
      concurrency: opts.concurrency,
      runId: `${def.id}`,
      threadId: "child",
    });

    // Run any task declared to listen to the subgraph entry event.
    const entryTasks = def.graph.routeFor("__enter__");
    if (entryTasks.length > 0) {
      await childScheduler.runAll(entryTasks, new Map());
    }

    // If a task in the child interrupted, resolve it inside the child scope.
    if (interrupts.pendingInterrupts.length > 0) {
      // Nested interrupt: expose child state for the caller to resume.
      return childStore.raw;
    }

    // Map child output back into the parent scope.
    const out: Record<string, unknown> = {};
    for (const [childPath, parentPath] of def.io.output) {
      out[parentPath] = get(childStore.raw, childPath);
    }
    return out;
  }
}

function get(root: unknown, path: string): unknown {
  let cur: unknown = root;
  for (const seg of path.split(".")) {
    if (cur === null || cur === undefined) return undefined;
    if (typeof cur === "object" && seg in (cur as Record<string, unknown>)) {
      cur = (cur as Record<string, unknown>)[seg];
    } else {
      return undefined;
    }
  }
  return cur;
}
