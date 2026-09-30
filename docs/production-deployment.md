# Production Deployment Guide

Install the runtime and CLI:

```bash
pip install reactivegraph reactivegraph-cli
```

The `reactivegraph` console command is provided by `reactivegraph-cli`; install
the CLI package when you want `health`, `new`, `dockerfile`, or `up`.

## Container image

Generate Docker and Compose artifacts:

```bash
reactivegraph dockerfile ./project
```

Generated images include:

- `STOPSIGNAL SIGTERM`
- `HEALTHCHECK CMD reactivegraph health || exit 1`
- 30-second Compose stop grace period

Validate Compose:

```bash
reactivegraph up ./project
```

## Readiness

```bash
reactivegraph health
```

The command imports the runtime and reports readiness. Extend it in a wrapper
if you also need to check an external database.

## Persistence

Set one durable backend:

```bash
REACTIVEGRAPH_DB=./data/rgp.db
# or
REACTIVEGRAPH_PG_DSN=postgresql://rgp:change-me@127.0.0.1:5432/rgp
```

SQLite stores thread state, checkpoints, the long-term store, vectors, and the
durable event log. Postgres provides multi-Driver durable state.

## Remote security

- Use TLS for non-local traffic.
- Give each tenant its own token.
- Rotate tokens with `RgpGateway.rotateToken`.
- Set `perTenantConcurrency` to reject overload.
- Enforce field permissions and budgets at run configuration.

## Observability

Export spans as OTLP JSON:

```ts
const payload = emitter.exportOTLPJSON("my-driver");
// POST payload to an OTLP /v1/traces collector.

const snapshot = emitter.spanMetrics();
// Expose snapshot.kinds[].p50Ms/p95Ms/p99Ms to the latency dashboard.
```

Default trace exports are redacted when sensitive attribute names or state
paths are configured. Span retention is bounded by default (`maxSpans:
10,000`); monitor `droppedSpans` when the bounded window is evicting samples.

## Verification

The release gate is:

```bash
scripts/check-release-gate.sh
```

The script keeps Python workspace commands serial because all workspace members
share one uv-managed `.venv`; parallel `uv run` commands can replace shared
dependencies while another test process is importing them.

Scale smoke tests cover 1,000 threads, a 10,000-node graph, and durable-log
compaction under write pressure.
