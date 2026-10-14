# ReactiveGraph

**A reactive graph execution engine** — an agent/workflow runtime built on
reactive updates. It targets the same problem space as
`langchain-ai/langgraph`, but with a different execution model: instead of
re-running the whole graph on every `invoke`, ReactiveGraph builds a
dependency graph at compile time and runs **only the tasks an update actually
affects**.

[![CI](https://github.com/from000/reactive-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/from000/reactive-graph/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-from000.github.io%2Freactive--graph-2f6f4f)](https://from000.github.io/reactive-graph/)
[![Status: v0.1.0 alpha](https://img.shields.io/badge/status-v0.1.0%20alpha-orange)](RELEASE_CHECKLIST.md)

**中文文档:** [README.md](README.md)

> **History and commit references.** The public repository was published as a
> single squashed commit, so commit hashes quoted in `docs/` identify private
> development checkpoints and do **not** resolve here. Fork revisions are
> quoted as `<current> (was <pre-rewrite>)` where the fork's author identity was
> rewritten: `ab0737f` (was `57e8d2d`) and `606187b` (was `3b55c36`). An older
> proof document cites `c128e42`, a 2026-09-27 checkout the rewrite dropped; the
> measurements it records were reproduced afterwards on `606187b` and
> `ab0737f`. `d2aca13` and `f9f3127` are ordinary upstream commits; the
> LangGraph pins (`11ee1859…`) belong to `langchain-ai/langgraph`.

## Why reactive

LangGraph's Pregel executes every node on every `invoke`. ReactiveGraph
analyses dependencies and invalidates caches, so only affected nodes run. Same
deterministic graph, same machine:

| Scenario (1000 nodes, 10 affected) | upstream langgraph | ReactiveGraph |
|---|---:|---:|
| End-to-end Python `invoke`, upstream at its best (per-node `key_func`, same-thread reuse) | ~2278 ms | ~257 ms (~9x) |

A second, decisive metric is how many nodes actually execute. Changing 10
inputs makes us run 10/1000; langgraph 1.2.11 with its documented
`CachePolicy()` + `InMemoryCache` measured **1000/1000 (zero hits)** — its
default `key_func` serialises the whole node input, so with 1000 nodes sharing
one state any field change invalidates every cache entry. Selectivity there
requires a hand-written per-node `key_func`; here, declaring `reads` is enough.

> ⚠️ **Scope.** The 2026-10-12 end-to-end numbers (Python `DriverHost` over
> RGP/1 + Node child, median of 3) replace an earlier "~0.004 ms / 11602 ms"
> table that came from an uncommitted measurement of the **bare `Scheduler`
> component**. Method, the no-gain workload, and the O(n²) input-merge fix are
> in [`docs/competitive-proof.md`](docs/competitive-proof.md) §1a/§1b.

## Core capabilities

* **Selective execution** — event routing decides effect-task eligibility;
  computeds invalidate by read-set; pure tasks are skipped when their input
  fingerprint is unchanged; effect tasks are idempotent through receipts and
  are not replayed once confirmed.
* **Transactional state** — a `transaction` carries read/write sets plus
  patches and commits atomically, with explicit conflict policies
  (reducer / priority / serial) instead of "recompute everything and merge".
* **Computed caching** — selectors declare their read-set; recomputation
  happens only when that set changes.
* **Scoped substate** — named sub-state namespaces, modular by construction.
* **Durable execution** — durable event log (SQLite / in-memory) plus
  checkpoints and recovery; restart replays exactly from the log.
* **Causal traces** — every scheduling decision
  (start/done/retry/skip/invalidate) is ordered and exportable, so debugging
  and auditing do not require guessing.
* **Streaming and interrupts** — `stream` for values/custom events;
  `interrupt`/`resume` for human-in-the-loop.

## Quick start

> **Distribution status:** this project ships from GitHub source. It is not on
> PyPI or npm, so `pip install reactivegraph` and
> `npm install @reactivegraph/sdk-js` do not work yet.

Python (install all four together — `reactivechain`, `reactivegraph-sdk` and
`reactivegraph-cli` depend on `reactivegraph>=0.1.0`, so installing any of them
alone fails dependency resolution):

```bash
pip install \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivechain" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_sdk" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_cli"
```

Core engine only:

```bash
pip install "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
```

> **Install fails with `SSL_ERROR_SYSCALL` or a connect timeout?** That is
> usually a local `~/.gitconfig` `url.<base>.insteadOf` / `http.proxy` override
> intercepting GitHub, not the repository. Use the SSH form instead (needs a
> configured SSH key), or retry with the proxy cleared:
>
> ```bash
> pip install "git+ssh://git@github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
> # or
> GIT_CONFIG_GLOBAL=/dev/null pip install "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
> ```

TypeScript packages build from source (npm cannot consume the monorepo's
`workspace:*` dependencies):

```bash
git clone https://github.com/from000/reactive-graph.git
cd reactive-graph && pnpm install && pnpm -r build
```

```python
from reactivegraph import ReactiveGraph

def build(b):
    b.task("greet", kind="effect", fn=lambda i: {"msg": f"hi {i['name']}"},
           on=("visit",))          # or b.on("visit", "greet")

g = ReactiveGraph.build(build)
out = g.invoke("visit", {"name": "Ada"})   # instance method, returns the state dict
print(out["msg"])  # "hi Ada"
```

The native TypeScript API (`packages/sdk-js`) uses an object-style signature:

```ts
import { ReactiveGraph, invoke } from "@reactivegraph/sdk-js";

const graph = ReactiveGraph.build((b) => {
  b.task({
    id: "greet",
    kind: "effect",
    handler: (input) => ({
      reads: ["name"], writes: ["msg"],
      patches: [{ path: ["msg"], operation: "set", value: `hi ${(input as { name: string }).name}` }],
    }),
  });
  b.on("visit", "greet");
});
const out = await invoke(graph, "visit", { name: "Ada" });
// out.state.msg === "hi Ada"
```

## Commands

| Command | Purpose |
|---|---|
| `make test` | Native Python tests |
| `pnpm typecheck` / `pnpm test` | All TypeScript packages |
| `uv run --directory python/reactivegraph pytest -q` | Native Python |
| `pnpm benchmark` | Selective-update benchmarks |

## Repository layout

```
packages/                 TypeScript: protocol / driver / sdk-js / devtools-protocol / testkit
python/                   Python: reactivegraph / reactivegraph_sdk / reactivegraph_cli
benchmarks/               Benchmark harness and workloads
docs/                     Specs, benchmarks, performance comparisons
```

## Proof on a real project

We migrated the [DeerFlow](https://github.com/bytedance/deer-flow) backend
execution substrate to ReactiveGraph while keeping its product logic, and ran
it against the real thing. Results, commands and boundaries are in
`benchmarks/deerflow_real/REPORT.md`.

These are the CI figures (CI provisions real `postgres:16` / `redis:7`
services; a local run without them skips the same integration cases):

```text
ReactiveGraph core:      931 passed (CI, real Postgres + Redis)   (CI, real Postgres + Redis)
ReactiveChain:           221 passed
DeerFlow backend:        19,101 passed, 82 skipped, 0 failed  (shards combined)
real full-factory dual:  output parity, 7.19x/8.07x (in-process Python engine vs full LangGraph
                         stack; see docs/competitive-proof.md §1b for end-to-end numbers)
SQLite recovery:         checkpoint and store both survive process restart
```

For a commercial evaluation entry point see `docs/commercial-readiness.md`.
For common questions (including "why is LangChain still visible?") see
`docs/faq.md`.

## Benchmarks

Performance data and reproduction steps live in `docs/benchmarks.md`
(selective updates plus differential measurements against real upstream
langgraph, including an honest no-gain scenario).

## Security

See `SECURITY.md` (gateway auth, field-level permissions, state redaction,
read-only importer).

## Contributing

See `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`. Issues, PRs, and [Discussions](https://github.com/from000/reactive-graph/discussions) are welcome.

We are also looking for a small number of teams to pilot v0.1.0 on a real agent or workflow workload (tools, retries, durable state, human review, multi-tenant gateway, or a state-graph migration). See [`.github/PILOT_INVITATION.md`](.github/PILOT_INVITATION.md) and open a ["Pilot user" issue](https://github.com/from000/reactive-graph/issues/new?template=pilot_user.md) if you are interested.

## License

MIT — see `LICENSE`.

## Third-party notices

The compatibility layer (`messages`, `tools`, `ToolNode`, `channels`,
`middleware`, `checkpoint`, `store`, `message_utils`, ...) adapts interfaces
and behaviour from `langchain-ai/langgraph` and `langchain-ai/langchain`
(MIT); the `uuid6` generator adapts `oittaa/uuid6-python` (MIT). Full
copyright notices, reference versions and license texts are in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
