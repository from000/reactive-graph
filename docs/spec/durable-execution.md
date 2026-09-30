# Durable Execution Specification

Status: implemented (Task 6). TypeScript implementation in
`packages/driver/src/durability/` (`events.ts`, `log.ts`, `recovery.ts`).

## 1. Model

Execution state is an append-only, ordered event log. Every outcome the runtime
must survive a crash is a typed, replayable event with a monotonically
increasing `seq`:

| Event kind | Persists | Written |
|---|---|---|
| `transaction` | graph hash, run/thread IDs, state version, read/write paths, patches | after commit |
| `effect_intent` | idempotency key, task/effect id, input hash, state version | BEFORE an external call |
| `effect_receipt` | idempotency key, task id, receipt, outcome | AFTER the call, BEFORE committing the patch |
| `retry_decision` | task id, attempt, decision, backoff | at each retry decision |
| `interrupt` | run/thread id, value, checkpoint hint | at interrupt |
| `stream_cursor` | run/thread id, cursor, terminal | at stream advances |
| `snapshot` | canonical state + state hash + base seq | periodically, for compaction |

## 2. Confirmed-effect ordering (never repeat a side effect)

For every external effect the durable order is:

1. append `effect_intent` (durable intent),
2. execute the external call,
3. append `effect_receipt`,
4. append the resulting `transaction`.

Recovery treats an idempotency key as **confirmed** only when BOTH its intent
and receipt are present. A crash:
- before intent → nothing recorded, task reruns;
- after intent, before receipt → key unconfirmed, task **must not** be
  re-executed blindly (the effect may have happened);
- after receipt, before commit → effect confirmed, patch not yet applied, replay
  applies it;
- after commit → transaction replays to canonical state.

`UNKNOWN_COMMIT` is never retried blindly: recovery inspects the log, resolves
the outcome from the persisted receipt, and either continues or re-drives with
the original idempotency key.

## 3. Backends

`DurableLog` interface with two implementations:

- `MemoryLog` — in-process, for unit tests and non-durable modes;
- `SqliteLog` — `node:sqlite` file-backed; a fresh instance over the same file
  recovers all prior events (verified cross-instance in the test suite).

`node:sqlite` is loaded lazily via `createRequire` (it is a Node built-in that
vite-node cannot statically externalize; a type-only import keeps the module
graph clean).

## 4. Snapshots and compaction

- `recover(log)` rebuilds canonical state: starts at the newest `snapshot`
  (empty state if none), then replays `transaction` patches in seq order.
- `compact(log, snapshot)` appends the snapshot and truncates everything strictly
  before the snapshot's own seq; because the snapshot subsumes all prior events,
  recovery from the compacted log equals recovery from the full log (asserted by
  the test suite, including a file-backed SQLite persistence check).

## 5. Testing

The failure-injection suite (`packages/driver/test/durability/recovery.test.ts`)
crashes the durable lifecycle at every boundary: before intent, after intent
before receipt, after receipt before commit, after commit, plus compaction, and
both backends. Every test asserts the confirmed-effect map and the replayed
canonical state.
## 6. Time travel (replay / fork)

`CHECKPOINT_OP restore`(Python:`restore_thread(thread_id, checkpoint_id)`)
rolls a thread's committed state back to a historical checkpoint snapshot.
Restoration is a **new committed transaction** — it bumps the store version,
so a subsequent `RUN` on the thread continues from the restored snapshot
(fork semantics), never a raw memory mutation. An unknown `checkpointId`
raises an error carrying a Hint (use `checkpoint_op list` first).
