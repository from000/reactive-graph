# ReactiveGraph Commercial Readiness

This document is for technical evaluators. It separates verified capabilities
from work that still needs deployment-specific evaluation.

## Recommendation

For Python agent runtimes that need trace, recovery, selective execution, and
predictable latency, ReactiveGraph is ready for commercial use in the backend
execution layer.

The evidence is a real DeerFlow backend migration, not a synthetic demo.

## Verified commercial capabilities

### 1. Runs a real product backend

```text
DeerFlow backend: 19,098 passed, 168 skipped, 0 failed
```

The replacement covers the real factory, lead agent, middleware chain, worker,
stream bridge, checkpointer, store, subagent executor, and client construction
path.

### 2. Lower core-path latency

The same real `create_deerflow_agent` workload produced normalized output
parity with upstream:

| Metric | LangGraph upstream | ReactiveGraph fork |
|---|---:|---:|
| first invoke | 6.975 ms | 0.971 ms |
| repeated invoke | 3.765 ms | 0.466 ms |
| observed speed-up | — | 7.19x / 8.07x |

Raw data and methodology are in `benchmarks/deerflow_real/REPORT.md`.

### 3. Durable recovery

Both the SQLite checkpoint and SQLite store tests start an independent writer
process, exit that process, and reopen the database in a later process. The
reader can recover state and, for the store, continue with an update.

### 4. Explainability

ReactiveGraph natively exposes:

- `explain_run()`
- `why_skipped()`
- `why_invalidated()`
- conflict explanations
- `export_trace()`

Production debugging can inspect the runtime decision instead of reconstructing
it from external logs.

## Reproduction

```bash
# ReactiveGraph core
cd python/reactivegraph
uv sync
uv run pytest -q

# DeerFlow real benchmark
cd ../../benchmarks/deerflow_real
uv run pytest -q

# Full DeerFlow fork
cd "$DEERFLOW_REACTIVE_ROOT/backend"
uv sync --locked
make test
```

## Deployment-specific evaluation

Before a production rollout, evaluate the following in your target environment:

- long-running production stability and failure drills
- Redis / cross-process bridge requirements at target concurrency
- PostgreSQL and Redis provider behavior under target failover/load
- frontend and channel behavior
- MCP and sandbox workloads at target scale
- security audit, SLA, and support process

SQLite and PostgreSQL checkpoint/store are native ReactiveGraph implementations;
RedisCache is a native Driver implementation. Both PostgreSQL and Redis have
been exercised against real local services. The DeerFlow fork separately has a
real-service Redis Streams bridge. This does not substitute for
target-environment scale and failover evaluation.

## Recommended public statement

Use this statement:

> ReactiveGraph runs as the backend execution base of a real DeerFlow fork. It
> passes the full backend test suite, a real-factory output-parity comparison,
> durable recovery tests, and live model tests. In the local real-factory
> comparison it preserved output and achieved 7-8x lower latency.

Avoid this statement:

> ReactiveGraph is faster and better than LangGraph/LangChain in every scenario,
> ecosystem, and production scale.
