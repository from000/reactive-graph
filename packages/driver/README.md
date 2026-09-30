# @reactivegraph/driver

The native ReactiveGraph execution engine: dependency-aware scheduling,
transactional state, durable execution, and pluggable checkpoint / store
backends.

This package is the engine itself. Host language bindings (for example the
Python `reactivegraph` package) talk to it over the RGP/1 wire protocol rather
than reimplementing the scheduler.

## Install

```bash
npm install @reactivegraph/driver
```

## What is in here

| Area | Exports |
|---|---|
| Graph model | `Graph`, `GraphBuilder`, `GraphNotFoundError`, `graphToDot` |
| Runtime | `DriverRuntime`, `GraphError`, `RuntimeCapacityError` |
| Scheduling + state | `./scheduler`, `./state` (selective execution, read-set invalidation) |
| Durability | `MemoryLog`, `SqliteLog`, `events`, `recover`, `compact`, `latestSnapshot` |
| Storage backends | `Memory*` / `Sqlite*` / `Postgres*` / `RedisCache`, `EncryptedSerializer` |
| Streaming + gateway | `./stream`, `RgpGateway`, `DriverLink`, `RgpTcpServer`, `startStdioGateway` |
| Process host | `startDriverProcess`, `defaultDriverEntry` |
| Interrupts | `InterruptManager`, `InterruptError`, `StaleResumeError` |
| Subgraphs | `SubgraphRunner`, `SubgraphError` |
| Functional API | `buildFunctionGraph`, `entrypoint`, `functask` |
| Interop | `normalizeCausalTrace`, permissions, policies, OpenTelemetry export |

## Selective execution

Tasks declare what they read and write. A recomputation only happens when its
read-set actually changes, so a graph with one changed field does not re-run
everything:

```ts
import { Scheduler, ReactiveStore, GraphBuilder } from "@reactivegraph/driver";

const builder = new GraphBuilder("example");
builder.computed({
  id: "total",
  reads: ["a", "b"],
  selector: (s) => (s.a as number) + (s.b as number),
});
const graph = builder.build();

const store = new ReactiveStore();
const scheduler = new Scheduler({ graph, store });
store.raw.a = 1;
store.raw.b = 2;
scheduler.compute("total"); // 3 — computed once
scheduler.compute("total"); // cached, no recompute
```

## Durability

Durable logs record scheduling decisions and task receipts so a restart can
replay exactly from the log instead of guessing which effects already ran:

```ts
import { SqliteLog, recover } from "@reactivegraph/driver";

const log = new SqliteLog({ location: "run.db", name: "run-123" });
// ... the engine appends lifecycle events and receipts to the log ...
const { state, stateVersion, confirmedEffects } = recover(log);
```

Pass `{ threadId }` to `recover` when replay must be restricted to one thread,
and `{ runId }` to scope it to a single run.

## Storage backends

Every backend implements the same interfaces (`Cache`, `CheckpointSaver`,
`LongTermStore`), so you can swap them per deployment:

```ts
import { SqliteCheckpointSaver, PostgresStore, RedisCache } from "@reactivegraph/driver";

const checkpoints = new SqliteCheckpointSaver("checkpoints.db");
const store = new PostgresStore("postgresql://user:pass@localhost:5432/app");
await store.init();
const cache = new RedisCache("redis://localhost:6379/0");
```

`Postgres*` requires a reachable server; `RedisCache` is an eviction cache, not
a checkpoint store. `EncryptedSerializer` wraps any serializer when state at
rest must be encrypted.

## Running the engine as a process

`startDriverProcess` spawns the driver entry point and wires up stdio; the
gateway exports let you host RGP/1 over an in-memory duplex, TCP, or stdio:

```ts
import { startDriverProcess } from "@reactivegraph/driver";

const driver = startDriverProcess(); // cleans up the child on exit
```

## License

MIT — see [`LICENSE`](./LICENSE). Third-party attributions are in
[`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md).
