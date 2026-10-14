# Observability and Security (Task 12 / D.9.105-106)

ReactiveGraph exposes a stable causal-trace protocol, OpenTelemetry-style spans,
field-level permissions, and reusable task policies — without leaking Vue
internals and without depending on a live collector in tests.

## Causal trace protocol

The Driver normalizes its internal scheduler records (`event` / `eligible` /
`start` / `done` / `failed` / `retry` / `invalidated` / `computed` /
`cache_hit`) into the stable `@reactivegraph/devtools-protocol` schema:

* `protocol: "reactivegraph.causal-trace.v1"`
* events in the causal order a developer needs: state mutation -> commit ->
  trigger -> selector invalidation -> scheduled task -> callback -> span
  (model/tool) -> retry -> patch -> checkpoint.

Event kinds: `state_set, commit, trigger, invalidate, schedule, callback,
retry, span_start, span_end, patch, checkpoint, cache_hit`. Values are carried
as a `TracePath` (`["a","b",0]`) and dotted for tooling
(`pathToDotted` → `a[0].c`).

`normalizeCausalTrace(source, { runId, redact, extra })`:

* maps scheduler records to the protocol (`event→trigger`,
  `invalidated→invalidate`, `computed→patch`, `cache_hit→cache_hit`, ...);
* appends explicit `extra` events (patches, checkpoints);
* **redacts configured state paths** — a value under `["credentials"]` is
  replaced by the `REDACTED` symbol before export, never the raw secret.

## OpenTelemetry spans

`SpanEmitter` produces in-process spans (no live OTLP collector required for
tests; a collector adapter plugs the same shape):

* `queue_delay` — time a task waited for its dispatch slot;
* `model_call` / `tool_call` — model/tool invocation spans with
  `SpanHandle.tokens({ input, output, total })` for provider token usage;
* `retry` — each retry decision;
* `cache_hit` — computed-value cache hits.

Attributes are sanitized at emission: any attribute matching the configured
redaction list (e.g. `api_key`) is replaced by `REDACTED`.

Completed spans are retained in a bounded ring buffer (10,000 by default,
configurable through `SpanEmitterOptions.maxSpans`; `0` disables retention).
`spanMetrics()` exposes a dashboard-ready
`reactivegraph.span-metrics.v1` snapshot with `count`, `min`, `mean`, `p50`,
`p95`, `p99`, and `max` durations grouped by span kind. It also reports
`retainedSpans`, `droppedSpans`, `totalSpans`, and `errorSpans`, so operators
can distinguish the bounded window from lifetime totals. `exportSpans()` and
`exportOTLPJSON()` use the same bounded window and preserve chronological
order.

## Field-level permissions

`PermissionPolicy` maps `read`/`write` to allowed path prefixes. Enforced:

* at **patch validation** — `validatePatches` throws `PermissionDeniedError`
  on the first patch whose path is not within an allowed write prefix;
* at **nested proxy access** — `assertReadAllowed` checks segment-wise so
  `["a","b"]` is allowed under `["a"]` but `["credits","ledger"]` stays
  off-limits.

## Task policies

Reusable, composable policies consulted before dispatch
(`checkAll(policies, ctx)` short-circuits on the first denial):

* `BudgetPolicy(limit)` — cumulative cost cap across a run;
* `RateLimitPolicy(max, windowMs)` — sliding-window call rate with
  `delayMs` backoff hint;
* `CircuitBreakerPolicy(threshold, cooldownMs)` — fail-open after N
  consecutive errors, half-open probe after cooldown;
* `PriorityPolicy(priority, concurrencyAllowance)` — static ordering hint.

Gateway slow-consumer metrics (`RgpGateway.metrics()`) report active sessions,
blocked sessions, queued outbound bytes, and evicted slow consumers. These
counters are kept alongside the latency dashboard so an operator can correlate
worker stalls with a transport that is not draining.

## CLI

`reactivegraph trace-export <dir>` writes a `trace.json` snapshot of the causal
trace; `reactivegraph dev` runs the local Driver in-process for inspection.
