# @reactivegraph/sdk-js

The native JavaScript SDK for ReactiveGraph: build a graph in process, invoke
it, and export its causal trace — or drive a remote engine over RGP/1.

## Install

```bash
npm install @reactivegraph/sdk-js
```

## Build and invoke

`ReactiveGraph.build` compiles a graph from a builder callback; tasks declare
what they write, and `invoke` routes an event to the tasks registered for it:

```ts
import { GraphBuilder, ReactiveGraph, invoke } from "@reactivegraph/sdk-js";
import type { TaskResult } from "@reactivegraph/driver";

const graph = ReactiveGraph.build((b) => {
  b.task({
    id: "greet",
    kind: "effect",
    handler: (input) =>
      ({
        reads: [],
        writes: ["msg"],
        patches: [
          {
            path: ["msg"],
            operation: "set",
            value: `hi ${(input as { name: string }).name}`,
          },
        ],
      }) satisfies TaskResult,
  });
  b.on("visit", "greet");
});

const out = await invoke(graph, "visit", { name: "Ada" });
(out as { state: Record<string, unknown> }).state.msg; // "hi Ada"
```

Task kinds are `pure`, `effect`, and `opaque` — the scheduler treats them
differently for skipping and idempotency. `b.on(event, taskId)` registers the
route that `invoke` uses; an event with no route throws.

## Computed values

Computed nodes declare their read-set and recompute only when it changes:

```ts
const graph = ReactiveGraph.build((b) => {
  b.computed({ id: "total", reads: ["a", "b"], selector: (s) => (s.a as number) + (s.b as number) });
});

graph.store.raw.a = 1;
graph.store.raw.b = 2;
graph.scheduler.compute("total"); // 3
graph.scheduler.compute("total"); // served from cache
```

`GraphBuilder` also exposes `scope(name)` for namespaced sub-state.

## Causal trace

`trace()` returns the run's scheduling decisions normalized to the
`@reactivegraph/devtools-protocol` schema. Sensitive state paths can be redacted
before export:

```ts
const causal = graph.trace({ redact: [["msg"]] });
```

## Remote sessions

`RemoteClient` drives an engine over any duplex transport (for example the
`DriverLink` / TCP / stdio gateways in `@reactivegraph/driver`):

```ts
import { RemoteClient } from "@reactivegraph/sdk-js";

const client = new RemoteClient(transport);

const graphId = await client.compileGraph({ id: "g" }); // -> graph id
const result = await client.run(graphId, { name: "Ada" });
const state = await client.getState({ threadId: "default" });
await client.cancel(result.runId ?? ""); // or client.resume(...) after an interrupt
client.close();
```

`RemoteClient` issues requests over the transport with a timeout
(`{ timeoutMs }`) and correlates responses by id; the transport only has to
provide `send` / `onData` / `onClose` / `close`.

`MessageAssembler` (with `messageKeyFor`) reassembles streamed message chunks
into whole messages for UI rendering.

## License

MIT — see [`LICENSE`](./LICENSE). Third-party attributions are in
[`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md).
