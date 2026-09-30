# Real DeerFlow ReactiveGraph Replacement Report

> Status: 2026-09-30. This report distinguishes three scopes:
>
> 1. the **full DeerFlow fork** in a separate
>    `$DEERFLOW_REACTIVE_ROOT` checkout;
> 2. the **upstream comparison fixture** in `$DEERFLOW_UPSTREAM_ROOT`;
> 3. this benchmark suite, which records reproducible evidence rather than a
>    marketing completion claim.

The full DeerFlow fork now runs the production agent, middleware, runtime,
checkpointer, and store paths on ReactiveGraph. This benchmark remains the
small, reproducible real-factory comparison, not a substitute for the full
fork suite.

- Upstream: `bytedance/deer-flow`
- Upstream commit: `5d8a9492eea97e2f06f28d46df69c29355acf4da`
- Local checkout: `$DEERFLOW_UPSTREAM_ROOT` (external test fixture)
- Benchmark scope: `create_deerflow_agent` real-factory comparison
- Full-fork scope: production agent, middleware, runtime, checkpoint, and store paths

Set `DEERFLOW_UPSTREAM_ROOT` to the checkout root (the directory containing
`backend/`) before running real-upstream tests. When it is unset or missing,
those tests skip rather than claiming parity.

## What was replaced

The real DeerFlow factory exposes:

```python
create_deerflow_agent(
    model,
    tools,
    system_prompt=...,
)
```

The ReactiveGraph version accepts the same baseline arguments and executes:

```text
model task
tools task
final model/response task
```

without importing LangChain or LangGraph in the replacement implementation.

## What was verified

Tests are in `benchmarks/deerflow_real/`:

- model-only turn
- tool-call round trip
- system prompt path
- model `bind_tools` receives the original tool objects/schemas
- async factory execution (`ainvoke`)
- LangGraph-style stream modes: `values`, `updates`, `messages`
- stateless baseline semantics without a checkpointer
- a LangGraph-shaped checkpoint saver (`get_tuple`/`list`/`put`/`put_writes`/`delete_thread` + async mirrors, parent chain, pending writes, thread/namespace isolation) when a checkpointer is supplied
- `get_checkpointer()` / `reset_checkpointer()` provider singleton for memory; missing Postgres connection strings fail closed in this benchmark subset
- DeerFlow ToolErrorHandling subset: exceptions become recoverable ToolMessages
- DeerFlow DanglingToolCall subset: unanswered historical calls get synthetic recovery responses
- DeerFlow TokenUsage subset: provider usage metadata accumulates across turns
- DeerFlow Summarization subset: threshold/keep summary plus summary state
- DeerFlow Clarification subset: structured `ask_clarification` returns interrupted state
- LangGraph `Command` subset: tool updates merge state without importing LangGraph
- `return_direct` command subset stops after tool execution
- same final message count/content as the real DeerFlow factory
- repeated identical run skips all three ReactiveGraph tasks
- changed input reruns
- stream emits values and trace
- unsupported advanced options fail closed
- minimal runtime worker: publishes stream events, emits terminal state, publishes worker errors, and supports cooperative cancellation
- in-memory stream bridge: live subscribe, replay, heartbeat, gap reporting, and end sentinel
- run journal: lifecycle, delivery receipt, token/message completion facts
- durable run store subset: snapshot/thread paging/state transitions/token aggregation/cancel-finalize race/lease takeover/atomic admission

Current benchmark suite:

```text
100 passed, 1 skipped in 29.55s
```

The skipped test records that importing the complete DeerFlow factory needs its
full environment if the real checkout is absent.

## Real source comparison

`run_comparison.py` runs the same baseline flow through:

1. DeerFlow's real `create_deerflow_agent`
2. the ReactiveGraph replacement

Raw data is in `comparison.json`.

| Case | Real DeerFlow median | ReactiveGraph first-run median | ReactiveGraph repeat median | Message parity | Tasks skipped on repeat |
|---|---:|---:|---:|---:|---:|
| model only | 32.765 ms | 0.258 ms | 0.406 ms | 2 / 2 | 3 |
| tool call | 30.803 ms | 0.303 ms | 0.162 ms | 4 / 4 | 3 |

Observations:

- same final message count
- same final content
- same role sequence for the baseline flow
- all three tasks skip on an identical repeated input
- real DeerFlow's full production assembly still executes its graph path
- this timing is a local smoke comparison, not the release benchmark gate

## Why this is a real-path proof

It does not merely reimplement a synthetic tutorial:

- the upstream repository was cloned
- the real `deerflow.agents.factory.create_deerflow_agent` was imported from the upstream checkout
- the upstream path was executed in its own virtual environment
- the same model/tool semantics were run through both sides
- output parity was asserted before comparison
- no LangChain/LangGraph import exists in the replacement module

## Runtime worker subset

`reactive_run_agent` now provides a minimal replacement slice for upstream
`runtime.runs.worker.run_agent`:

- in-memory `RunManager` lifecycle state with optional durable `RunStore` backing
- in-memory `RunEventStore` with per-thread monotonic `seq` and batch writes
- `RunJournal` port: buffered event batching, lifecycle records, delivery receipt, completion data
- worker writes `run.start` / `run.end` / `run.delivery` journal facts when a journal is supplied
- upstream-shaped first `metadata` frame with `run_id` and `thread_id`
- run stream iteration on a worker thread
- bridge publishing for every enabled public stream mode
- live subscribers consume the same run as it executes, not only after completion
- upstream-shaped `ReactiveStreamEvent` / `ReactiveStreamGap`
- live subscriber wake-up, bounded-buffer replay, heartbeat while idle, and `__end__`
- `Last-Event-ID` resume plus explicit gap reporting when a cursor is too old
- cooperative cancellation through the run's `abort_event`
- terminal `success`, `interrupted`, and `error` state transitions with error details
- publishes upstream-shaped `error` events for worker exceptions
- always emits a terminal stream frame and `publish_end`

It intentionally does not claim parity with the full fork or the full upstream
worker. Missing layers in this benchmark subset include `custom` stream mode,
cross-process/Redis bridge, durable replay, durable journal store, run
persistence, rollback/fork, goal continuation, extension hooks, duration
recording, prior-run finalizing, full ownership heartbeat/reconciliation, and
cross-process persistence.

## Honest scope boundary

DeerFlow's production harness is large and deeply coupled to LangChain/LangGraph:

```text
589 harness Python files
152 files with direct LangChain/LangGraph imports
436 direct import occurrences
791 backend test files
```

This benchmark subset does not replace:

- full middleware chain (only ToolErrorHandling/DanglingToolCall baseline behaviors are implemented)
- sandbox
- PostgreSQL provider integration beyond fail-closed missing-config coverage (the native core saver/store exists and the full fork wires it; this benchmark subset does not exercise it)
- full frontend behavior comparison
- channels and all external integrations
- MCP session pooling under production-scale loads
- subagent runtime under production-scale loads
- production-scale PostgreSQL failover/load validation (the native provider exists)
- full goal continuation and `custom` stream mode

Unsupported options explicitly raise errors rather than silently changing
semantics.

## Full fork verification (2026-09-27)

The authoritative full-fork checkout is
a local `deer-flow-reactive` checkout at commit `c128e42`
(upstream merge base `5d8a9492eea97e2f06f28d46df69c29355acf4da`).

```text
backend make test:
  19,098 passed, 168 skipped, 21 deselected, 50 warnings in 2805.92s

backend blocking I/O suite:
  154 passed, 2 warnings in 44.61s

engine suite:
  51 passed in 35.34s

production no-LangGraph import probes:
  6 passed in 26.12s

this benchmark suite:
  100 passed, 1 skipped in 29.55s

ReactiveGraph core:
  802 passed, 12 skipped in 135.47s
```

The fork introduces a native ReactiveGraph package and changes 89
DeerFlow-related files (`+17,407 / -216` against upstream), including the
factory, middleware, worker, journal, checkpointer, store, tools, and runtime
paths. The production import probes prove that the real
`deerflow.agents.lead_agent.agent`, middleware modules, and client factory
import without LangGraph.

## Real full-factory dual run

The same full `create_deerflow_agent` script was executed in both real
checkouts. The fork used the real DeerFlow middleware-compatible state schema
and the native engine.

| Metric | upstream LangGraph | fork ReactiveGraph |
|---|---:|---:|
| engine | `langgraph.pregel.Pregel` | `reactivegraph.create_agent.AgentGraph` |
| first invoke | 6.975 ms | 0.971 ms |
| repeated invoke | 3.765 ms | 0.466 ms |
| repeat output stable | yes | yes |
| normalized result parity | yes | yes |
| observed speed-up, first run | 7.19x | — |
| observed speed-up, repeat | 8.07x | — |

Raw normalized artifacts are in the fork checkout under
`backend/.verification/factory_full_{upstream,fork}.normalized.json`.

Two non-semantic message fields differed before normalization:

- upstream sets AI message `name` from the graph name;
- ReactiveGraph stores `invalid_tool_calls` in `additional_kwargs` as well as
  the normalized public field.

After removing only those presentation fields, the full state result is
identical.

## Durable recovery proof

Two new core tests start an independent writer process, close it, then reopen
SQLite in the current process:

- checkpoint test: process 1 writes `n=7`, exits; process 2 reads the
  checkpoint and metadata;
- store test: process 1 writes preferences, exits; process 2 reads them and
  continues with an update.

```text
python/reactivegraph tests: 802 passed, 12 skipped
SQLite checkpoint tests: 16 passed
SQLite store tests: 15 passed
```

This is a real cross-process restart proof, not just a new object opening the
same file in one process.

## Live-model status

The original Ark endpoint was unavailable during the first run, leaving four
live tests with provider connection errors. The live gate was then completed
against two reachable OpenAI-compatible providers using existing credentials:

```text
test_client_e2e.py::TestToolCallFlow::test_tool_call_produces_events
  1 passed

test_create_deerflow_agent_live.py::test_minimal_agent_responds
  1 passed

test_create_deerflow_agent_live.py::test_agent_with_custom_tool
test_create_deerflow_agent_live.py::test_features_mode_middleware_chain
  2 passed
```

The e2e tool test now explicitly configures the real `bash` tool in its test
app config; previously the fixture allowed host bash but did not register the
tool. The same-file offline regression remains green:

```text
32 passed, 11 deselected in 13.22s
```

The live gate is therefore complete for the previously failing four tests.
Docker Engine 28 remains an environment requirement for one sandbox network
test and is tracked separately rather than treated as a framework failure.

## Additional compatibility status

The factory now also supports:

- `model.bind_tools(tools)` with original tool objects and schemas
- `ainvoke`
- DeerFlow public runtime stream modes `values`, `messages-tuple`, `updates`, `debug`, `tasks`, and `checkpoints`
- stateless behavior when no checkpointer is supplied
- a LangGraph-shaped checkpoint saver (`get_tuple`/`list`/`put`/`put_writes`/`delete_thread` + async mirrors, parent chain, pending writes, thread/namespace isolation) when a checkpointer is supplied
- `get_checkpointer()` / `reset_checkpointer()` provider singleton for memory; missing Postgres connection strings fail closed in this benchmark subset

See `TEST_MATRIX.md` for the full upstream test-area boundary.

## Remaining real-project migration boundary

The full fork now replaces the central execution, runtime, checkpoint, and
store base, but the following product areas still require explicit
equivalence work:

```text
backend harness Python files       589
backend tests                        791 files / 7449 tests
frontend source files                658
direct LangChain/LangGraph imports  152 files / 436 occurrences
```

Largest remaining subsystems:

| Area | Files | Direct upstream imports |
|---|---:|---:|
| agents | 105 | 58 |
| runtime | 50 | 12 |
| persistence | 87 | 2 |
| tools | 20 | 18 |
| models | 14 | 12 |
| MCP | 16 | 6 |
| subagents | 19 | 5 |
| sandbox | 21 | 3 |
| skills | 33 | 1 |
| extensions | 16 | 4 |
| gateway | 93 | not included above |
| channels | 26 | not included above |

This confirms that replacing the complete product base is not a small patch;
it is a staged product migration. The completed cut points are:

1. real `create_deerflow_agent` execution base
2. model/tool baseline
3. async + all non-custom public stream modes
4. minimal thread checkpoint/get_state semantics
4b. LangGraph-shaped checkpointer saver contract + memory provider singleton
5. Command update + return_direct
6. tool error recovery
7. dangling tool-call recovery
8. token usage aggregation
9. summarization threshold/keep
10. clarification/interrupted state
11. upstream factory execution comparison
12. minimal runtime worker stream bridge, lifecycle, cancellation, and terminal events
13. LangGraph-shaped checkpointer saver contract + memory provider singleton
14. multi-worker ownership: durable reservation, cancel outcomes, heartbeat, reconciliation

## Checkpointer status (latest)

`ReactiveCheckpointStore` / `ReactiveCheckpointSaver` now implement the
subset of LangGraph's saver contract that the real harness consumes:

| Contract | Status |
|---|---|
| `get_tuple` / `aget_tuple` (latest + explicit id) | ✅ |
| `list` / `alist` (newest-first, `before` cursor, `limit`, metadata `filter`) | ✅ |
| `put` / `aput` (immutable, parent chain) | ✅ |
| `put_writes` / `aput_writes` (pending writes) | ✅ |
| `delete_thread` / `adelete_thread` | ✅ |
| `get_next_version` | ✅ |
| thread + `checkpoint_ns` isolation | ✅ |
| memory provider singleton + reset | ✅ |
| SQLite backend | ✅ native durable store, cross-process reopen |
| PostgreSQL backend | ✅ native in core; full-fork sync/async providers wired; real PostgreSQL core suite passing; production-scale failover/load not yet measured |
| delta-channel history cache (`CachedHistorySaver`) | ❌ not in this benchmark alias; implemented in the full fork |

Scope note: this table combines benchmark-subset evidence with core provider
status. The benchmark subset itself directly covers the memory provider and
the missing-Postgres-configuration fail-closed branch; native PostgreSQL and
the full-fork wiring are covered by core and fork tests respectively.

The graph now writes through the supplied saver rather than a private dict,
so two agent instances sharing one saver observe the same thread state.

## Ownership status (latest)

`ReactiveRunManager` now mirrors upstream multi-worker ownership semantics for
the subset the harness exercises:

| Behavior | Status |
|---|---|
| `generate_worker_id()` (`host:uuid`) + per-manager default id | ✅ |
| `reserve_thread_operation()` durable non-run reservation | ✅ |
| Reject/interrupt against live peer lease | ✅ |
| Expired reservation reclaimed by interrupting run | ✅ |
| `CancelOutcome` (cancelled/requested/taken_over/not_cancellable/not_active_locally/unknown) | ✅ |
| Peer cancel with valid lease -> durable request -> owner heartbeat observes | ✅ |
| Peer cancel with expired/null lease -> takeover as `error` | ✅ |
| `reconcile_orphaned_inflight_runs()` lease-aware, local-live skip | ✅ |
| `compute_retry_after()` | ✅ |
| Rollback / fork / pre-run snapshot restore | ❌ not ported |
| Periodic heartbeat/reconciliation background loops | ❌ not ported |
