# @reactivegraph/devtools-protocol

The stable, language-neutral schema for ReactiveGraph causal traces — the
contract that devtools, tracers, and the OpenTelemetry exporter all read.

The TypeScript types and the Python binding share this schema, so a trace
produced by the engine can be consumed without either side knowing about the
other's internals. It deliberately exposes **no** reactive-library internals
(no `onTrack` / `onTrigger` leak) — only normalized, well-typed events.

## Install

```bash
npm install @reactivegraph/devtools-protocol
```

## Trace shape

```ts
import type { CausalTrace, TraceEvent } from "@reactivegraph/devtools-protocol";

const trace: CausalTrace = {
  protocol: "reactivegraph.causal-trace.v1",
  runId: "run-123",
  events: [
    { seq: 0, kind: "state_set", path: ["a"], value: undefined },
    { seq: 1, kind: "commit", txId: "tx-1" },
    { seq: 2, kind: "trigger", path: ["a"] },
    { seq: 3, kind: "invalidate", selector: "total" },
    { seq: 4, kind: "schedule", taskId: "render" },
    { seq: 5, kind: "callback", taskId: "render", callback: "main" },
  ],
};
```

Events are ordered oldest-first within one run.

## Event kinds

`TRACE_EVENT_KINDS` is the frozen set —

```
state_set, commit, trigger, invalidate, schedule, callback,
retry, span_start, span_end, patch, checkpoint, cache_hit
```

The `kind` chain mirrors the causal path you need when debugging a reactive
graph: state mutation → commit → trigger → selector invalidation → scheduled
task → callback → span (model / tool) → patch → checkpoint.

Use `isTraceEventKind(value)` to narrow an untrusted `string` before processing
(it returns a type predicate).

## Spans and token usage

Model and tool calls are represented as `span_start` / `span_end` events
carrying a `SpanEvent`:

```ts
{ seq: 7, kind: "span_end", span: {
  name: "llm.chat",
  startNs: 0, endNs: 1_000_000, status: "ok",
  tokenUsage: { input: 12, output: 34, total: 46 },
} }
```

## Paths

A `TracePath` is a `readonly (string | number)[]`, e.g. `["messages", 0, "content"]`.
`pathToDotted` renders it for display:

```ts
import { pathToDotted } from "@reactivegraph/devtools-protocol";

pathToDotted(["messages", 0, "content"]); // "messages[0].content"
```

## Redaction

`REDACTED` is the sentinel a producer substitutes for a value that must not be
exported. Consumers should treat it as "present but withheld" rather than
`undefined`, so redacted fields stay visible in tooling without leaking the
value:

```ts
import { REDACTED } from "@reactivegraph/devtools-protocol";
```

## License

MIT — see [`LICENSE`](./LICENSE). Third-party attributions are in
[`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md).
