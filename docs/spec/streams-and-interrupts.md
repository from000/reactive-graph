# Streams, Interrupts, Subgraphs, and Functional Workflows Specification

Status: implemented (Task 8). TypeScript in `packages/driver/src/{stream,interrupts,subgraph,functional}.ts`;
Python mirror for functional workflows in `python/reactivegraph/reactivegraph/functional.py`.

## 1. Stream multiplexer

`StreamMux` (`src/stream/mux.ts`) is a typed multiplexer that separates:

- **transient** chunks: `custom` events, `messages` (model/tool tokens), `debug`;
- **committed** chunks: `values` (channel snapshots), `updates` (per-node dicts),
  `committed` (patch batch + state version).

Guarantees:

1. committed chunks are strictly ordered by `seq` and never dropped;
2. under backpressure, trailing coalescible transient chunks are folded into one
   (token churn cap via `coalesceBytes`);
3. a `readFrom(afterSeq)` cursor supports disconnected consumers / resumption;
4. `terminate()` closes the stream; further pushes raise `StreamTerminatedError`;
5. `maxBuffered` bounds retained committed chunks (0 = unbounded).

Chunk payload shapes (by `type`): `values:{state}`, `updates:{updates}`,
`custom:{event,payload}`, `messages:{message}`, `debug:{payload}`,
`committed:{patches,stateVersion}`.

## 2. Durable interrupts

`InterruptManager` (`src/interrupts.ts`):

- `interrupt(value, checkpointHint, expectedVersion)` records a pending interrupt
  (and appends a durable `interrupt` event when a `DurableLog` is provided);
- `resume(id, currentVersion)` rejects stale resumes with `StaleResumeError` when
  the current state version differs from `expectedVersion`;
- **human edits are new audited transactions**: `recordHumanEdit` enforces an
  actor→permission allow-list, stamps `at`, and appends to the audit log. The
  caller commits the patches through the store — never raw memory mutation.

## 3. Subgraphs

`SubgraphRunner` (`src/subgraph.ts`):

- a subgraph has an isolated `ReactiveStore` seeded via typed `input` mapping;
- `__enter__` event routes start child tasks;
- `output` mapping copies child state back into the parent scope;
- nested interrupts are resolved inside the child scope (child state exposed for
  resume); unknown subgraph ids raise `SubgraphError`.

## 4. Functional workflows

TS (`src/functional.ts`) and Python (`reactivegraph/functional.py`):

- `functask(fn, opts)` turns a plain function into a task (kind, event hooks,
  timeout); a function returning `{patches,...}` is treated as a `TaskResult`;
- `entrypoint(graphId, tasks, routes?)` plans a graph (ids + emitted events);
- `buildFunctionGraph` produces a `GraphBuilder`.

Python decorator form:

```python
@functask(kind="effect", on=("visit",))
def greet(input_):
    return {"msg": f"hi {input_['name']}"}
```

Persistence (fingerprint skip, effect receipt) is owned by the Driver scheduler.

## 5. Testing

`packages/driver/test/streams.test.ts` covers: committed/transient separation,
order, resume cursors, termination, stale resume rejection, permission-checked
audited human edits, durable interrupts, isolated subgraph scopes with typed I/O,
and functask/entrypoint graph building. Python `tests/test_functional.py` mirrors
the decorator/entrypoint surface.