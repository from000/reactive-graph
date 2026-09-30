# Native ReactiveGraph API Specification

Status: implemented (Task 7). TypeScript in `packages/driver/src/{graph,scheduler}`
and `packages/sdk-js/src/graph.ts`; Python mirror in `python/reactivegraph/reactivegraph/graph.py`.

## 1. Model

A graph is a collection of **tasks** and **computeds** plus **event routes** and
**scopes**:

- `task` — an executable unit classified as `pure`, `effect`, or `opaque`
  (plan Appendix C.4):
  - `pure` may be skipped when its input fingerprint is unchanged;
  - `effect` never auto-reruns after a confirmed idempotency receipt;
  - `opaque` is always considered changed once eligible.
- `computed` — a cached selector; recomputes only when its declared read set
  changes (irrelevant-path writes never invalidate).
- `on(event)` — routes an explicit event to one or more tasks.
- `scope` — a named sub-state namespace.
- `transaction` — the store commit unit carrying read/write sets and patches.

## 2. Scheduling semantics

Eligibility:

1. explicit events + graph routes decide whether effect tasks are eligible;
2. computed invalidation (read-set change) makes dependent tasks eligible;
3. pure tasks skip when their input fingerprint is unchanged;
4. effect tasks skip while a confirmed receipt exists.

Concurrency is limited (`SchedulerOptions.concurrency`); ties are broken
deterministically (task-id order). Only tasks with a retry policy are retried;
retries use exponential backoff with optional jitter (deterministic per task:
`docs/research` note on reproducible traces). Every decision (start/done/failed/
retry/skip) is recorded in the scheduler trace; durable decisions (transaction,
retry, effect receipt) are appended to the `DurableLog` when provided (Task 6).

Conflict policy (see `scheduler/conflicts.ts`): reads are optimistic; disjoint
writes may commit concurrently; same-path writes require a reducer, explicit
priority, or serialization; a stale pure task may rerun, a stale effect task may
not until its receipt is inspected.

## 3. JS native API

```ts
const graph = ReactiveGraph.build((b) => {
  b.task({ id: "greet", kind: "effect", handler: (input) => ({ patches: [...], writes: ["msg"] }) });
  b.on("visit", "greet");
});
const out = await invoke(graph, "visit", { name: "Ada" });
```

Also `GraphBuilder` (task/computed/on/scope/build), `ReactiveGraph.build`,
`graph.scheduler.compute(id)`, `graph.store raw/state` access.

## 4. Python native API

```python
graph = ReactiveGraph.build(
    lambda b: b.task("greet", fn=lambda x: {"msg": f"hi {x['name']}"}).on("visit", "greet"),
)
out = graph.invoke("visit", {"name": "Ada"})
```

`GraphDef`, `GraphBuilder`, `ComputedDef.evaluate(state)` (cached, read-set
invalidation), `ReactiveGraph.invoke`. When a `DriverHost` is attached, tasks
execute as the host callback (Task 4 RUN semantics); otherwise a pure in-process
fallback runs stateful tasks directly.

## 5. Conditional branching and dynamic routing

There is no separate `add_conditional_edges` API: **conditional branches ARE
event dispatch**. Two levels:

- **Static**: the caller picks the event based on a condition; `routeFor(event)`
  routes only to that event's tasks.
- **Dynamic**: a task decides the follow-up by which event is emitted next
  (from its result or a caller decision); a task may listen to multiple events
  via `on=("a", "b")`.

```python
def build(b):
    b.task("parse", fn=parse).on("go", "parse")
    b.task("accept", fn=accept).on("accept", "accept")
    b.task("reject", fn=reject).on("reject", "reject")

g = ReactiveGraph.build(build)
g.invoke("go", {...})       # parse first
g.invoke("accept", {...})   # branch by event (condition lives in the event)
# or parse returns {"branch": "accept"} and the caller emits that event
```

Semantics vs langgraph: the condition lives in the **event choice**, not in a
graph-edge function; both express "pick the next task(s) at runtime". Fan-out
is the same mechanism (one event, many listeners); cycle safety comes from
route validation at build time. See `docs/spec/langgraph-comparison.md` for a
source-annotated capability comparison with LangGraph.

## 6. Lifecycle hooks and stream

Lifecycle (start/done/retry/fail) is observable through `Scheduler.getTrace()`
(typed, ordered). Streaming (`values`/`updates`/`custom`/`messages`) and
interrupts are Task 8; the scheduler trace is the durable, normalized basis for
them.

## 7. Testing

`packages/driver/test/scheduler/scheduler.test.ts`, `packages/sdk-js/test/graph.test.ts`,
`python/reactivegraph/tests/test_native_graph.py` cover: event routes, conditional
routes, computed caching + relevant/irrelevant invalidation, pure-skip,
effect-no-rerun, independent parallel tasks, deterministic tie-break, timeout,
retry backoff/give-up, cycle/route validation.