# Real DeerFlow Factory / Full-Fork Test Matrix

This tracks the real `backend/tests/test_create_deerflow_agent.py` test areas
from upstream commit `5d8a9492eea97e2f06f28d46df69c29355acf4da`.

| Upstream area | Official tests | ReactiveGraph status | Notes |
|---|---:|---|---|
| Minimal model-only factory | 1 | ✅ ported | model callable and assistant response |
| Tools | 1 | ✅ ported | tool invocation and final model turn |
| System prompt | 1 | ✅ ported | prompt reaches model input |
| Model tool binding | implicit | ✅ ported | `bind_tools(tools)` before graph build |
| State schema | multiple | ✅ full-fork path | real DeerFlow thread state/reducers run through the full fork; benchmark subset uses the baseline state |
| Delta channel | multiple | ✅ full-fork key path | delta rollback/resume and snapshot cadence covered by fork tests; full distributed history cache remains outside this matrix |
| Checkpointer | 1 | ✅ saver contract + durable SQLite | `get_tuple/list/put/put_writes/delete_thread`, async mirrors, parent chain, pending writes, ns/thread isolation; memory/SQLite tested, missing Postgres DSN fails closed in this benchmark; full fork wires native Postgres and core real-Postgres tests pass; cross-process reopen tested |
| Features/middleware | many | ✅ full-fork production path | real lead-agent middleware chain runs on ReactiveGraph; benchmark subset pins the baseline behaviors |
| Subagents | several | 🟡 full-fork subset | production subagent execution path is switched; production-scale capacity/load equivalence remains to be measured |
| Token budget | several | 🟡 full-fork subset | middleware integration exists; production-scale budget behavior remains to be measured |
| `@Next`/`@Prev` ordering | several | ✅ full-fork path | production middleware ordering is used by the full fork |
| Graph execution | 3 end-to-end tests | ✅ two baseline paths, Command update, and return-direct stop | real official factory executed for baseline |
| Async streaming | runtime tests elsewhere | ✅ `ainvoke` and `values|updates|messages` stream modes | tuple-shaped mode events |
| Production runtime | runtime/worker tests | ✅ major full-fork path | worker lifecycle/store, stream bridge, cancellation, delivery, ownership, delta resume/rollback, SQLite recovery, and all non-custom public modes are covered; production-scale Postgres failover/load validation, goal continuation, and `custom` remain |
| Sandbox | many | 🟡 full-fork path | production sandbox modules run through the fork; production-scale policy equivalence remains to be measured |
| MCP | many | 🟡 full-fork path | production session pooling/auth tests run through the fork; production-scale MCP loads remain to be measured |

## Current verification

Benchmark suite:

```text
100 passed, 1 skipped in 29.55s
```

Full fork / engine:

```text
DeerFlow backend make test: 19,098 passed, 168 skipped, 21 deselected
DeerFlow blocking I/O: 154 passed, 2 warnings
DeerFlow engine suite: 51 passed
Production no-LangGraph probes: 6 passed
ReactiveGraph core: 802 passed, 12 skipped
```

Real full-factory dual run:

```text
upstream: LangGraph Pregel, first 6.975 ms, repeat 3.765 ms
fork: ReactiveGraph AgentGraph, first 0.971 ms, repeat 0.466 ms
normalized result parity: true
repeat output stable: true
```

Cross-process durable recovery:

```text
SQLite checkpoint writer process exit -> reader process reopen: passed
SQLite store writer process exit -> reader process update: passed
```

Live model gate:

```text
original batch: 42 passed, 4 provider-connection failures
retried failures: 4 passed
same-file offline regression: 32 passed, 11 deselected
```

## Honest conclusion

The full fork's central execution base, production middleware/runtime path,
SQLite checkpoint and store durability, and real factory semantics are
ReactiveGraph-backed and verified against upstream. Remaining product-scale
boundaries include the live-model rerun, production-scale PostgreSQL
failover/load validation, goal continuation, custom stream mode,
channels/frontend equivalence, and production-scale integrations.
