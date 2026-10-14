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
DeerFlow backend: 19,101 passed, 82 skipped, 0 failed
```

(CI provisions real `postgres:17` and `redis:7-alpine` services, so the
integration tests run instead of skipping; a DSN-less local run reports
19,098 passed / 168 skipped.)

The replacement covers the real factory, lead agent, middleware chain, worker,
stream bridge, checkpointer, store, subagent executor, and client construction
path.

### 2. Lower core-path latency

**Production path (`create_deerflow_agent`, middleware chain included).** Paired A/B
measurement on 2026-10-15 with output parity asserted before timing: **~2.3x lower
latency** (3.01 ms vs 7.09 ms model-only; 6.15 ms vs 11.81 ms tool-call round trip)
and **~4x higher concurrency throughput** (117 vs 29 sessions/s, 64 sessions /
8 workers). With a real LLM the end-to-end wall time shows no advantage; the
engine-side residual CPU cost is ~1.45-2.34x lower across four scenarios
(model_only / tool_call / multi_turn / big_state, n=6/6/3/3; the multi-turn
scenarios attach a checkpointer, since the factory is ephemeral without one).

**Narrower scope (`create_agent` only, no middleware chain).** A historical
`create_agent(...).AgentGraph` in-process run measured:

| Metric | LangGraph upstream | ReactiveGraph |
|---|---:|---:|
| first invoke | 6.975 ms | 0.971 ms |
| repeated invoke | 3.765 ms | 0.466 ms |
| observed speed-up (in-process Python executor vs full LangGraph stack) | — | 7.19x / 8.07x |

This second table **excludes the middleware chain** and must not be quoted as a
production speed-up. Raw data and methodology for both scopes are in
`benchmarks/deerflow_real/REPORT.md` and the fork's `REAL_SCENARIOS.md`.

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
> comparison it preserved output and achieved 7-8x lower latency using the
> in-process Python executor; the Driver-backed end-to-end path is a
> single-digit multiple (see `competitive-proof.md` §1b).

Avoid this statement:

> ReactiveGraph is faster and better than LangGraph/LangChain in every scenario,
> ecosystem, and production scale.
