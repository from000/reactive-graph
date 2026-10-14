# ⚡ ReactiveGraph

**A reactive graph execution engine** — it builds a dependency graph at compile
time and runs **only the tasks an update actually affects**.

This is not another agent framework that re-runs everything on every `invoke`.
Selective execution is the core abstraction: declare `reads`/`writes`, and the
engine decides which tasks re-run and which can be skipped.

[![CI](https://github.com/from000/reactive-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/from000/reactive-graph/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-from000.github.io%2Freactive--graph-2f6f4f)](https://from000.github.io/reactive-graph/)
[![Status: v0.1.0 alpha](https://img.shields.io/badge/status-v0.1.0%20alpha-orange)](RELEASE_CHECKLIST.md)

**中文文档:** [README.md](README.md)

---

## Why it matters

| Pain point | Cost with LangGraph | ReactiveGraph approach |
|---|---|---|
| Full re-run on every call | 1000 nodes, 10 changed inputs → runs 1000/1000 | dependency analysis + cache invalidation → **runs 10/1000** |
| Selective execution is hard | per-node `key_func`; shared state defeats the cache | declaring `reads` is enough |
| Debugging by guessing | scheduling decisions opaque | causal traces, ordered and exportable |

> Measured 2026-10-12 (Python `DriverHost` over RGP/1 + Node child, median of
> 3): with 1000 nodes and 10 affected, langgraph at its best (hand-written
> `key_func`, same-thread reuse) took ~**2278 ms**; ReactiveGraph took ~**257 ms**.
> Full method, the no-gain workload, and the O(n²) input-merge fix are in
> [`docs/competitive-proof.md`](docs/competitive-proof.md).

---

## Core capabilities

- **⚡ Selective execution** — event routing decides effect-task eligibility;
  computeds invalidate by read-set; pure tasks are skipped when their input
  fingerprint is unchanged; effect tasks are idempotent through receipts.
- **📦 Transactional state** — a `transaction` carries read/write sets plus
  patches and commits atomically, with explicit conflict policies
  (reducer / priority / serial).
- **💾 Durable execution** — durable event log (SQLite / in-memory) plus
  checkpoints and recovery; restart replays exactly from the log.
- **🔎 Causal traces** — every scheduling decision
  (start/done/retry/skip/invalidate) is ordered and exportable.
- **🔌 Streaming and interrupts** — `stream` for values/custom events;
  `interrupt`/`resume` for human-in-the-loop.
- **🧩 Scoped substate** — named sub-state namespaces, modular by construction.

---

## Quick start

> **Distribution status:** shipped from GitHub source; not on PyPI/npm yet.
> `pip install reactivegraph` and `npm install @reactivegraph/sdk-js` do not
> work yet — use the commands below.

Python (install all four together — `reactivechain`, `reactivegraph-sdk` and
`reactivegraph-cli` depend on `reactivegraph>=0.1.0`, so installing one alone
fails dependency resolution):

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

> **Install failing with `SSL_ERROR_SYSCALL` or a timeout?** Usually a local
> `~/.gitconfig` `url.<base>.insteadOf` / `http.proxy` override intercepting
> GitHub, not the repository. Use SSH or clear the proxy:
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

### Hello World in 3 lines

```python
from reactivegraph import ReactiveGraph

def build(b):
    b.task("greet", kind="effect", fn=lambda i: {"msg": f"hi {i['name']}"},
           on=("visit",))

g = ReactiveGraph.build(build)
print(g.invoke("visit", {"name": "Ada"})["msg"])   # "hi Ada"
```

The native TypeScript API (`packages/sdk-js`) uses an object-style signature:

```ts
import { ReactiveGraph, invoke } from "@reactivegraph/sdk-js";

const graph = ReactiveGraph.build((b) => {
  b.task({
    id: "greet", kind: "effect",
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

---

## Proof on a real project: DeerFlow

We migrated the [DeerFlow](https://github.com/bytedance/deer-flow) backend
execution substrate to ReactiveGraph and ran it against the real thing
(commands, results and boundaries in `benchmarks/deerflow_real/REPORT.md`).
CI figures (CI provisions real `postgres:16` / `redis:7`; a local run without
them skips the same integration cases):

```text
ReactiveGraph core:      931 passed (CI, real Postgres + Redis)
ReactiveChain:           221 passed
DeerFlow backend:        19,101 passed, 82 skipped, 0 failed (shards combined)
production path:         output parity; ~2.3x lower latency, ~4x concurrency throughput
SQLite recovery:         checkpoint and store both survive process restart
```

> **Which number to quote.** The production path (`create_deerflow_agent`,
> middleware chain included) is the figure to quote — ~2.3x lower latency,
> ~4x concurrency throughput. The earlier 7.19x/8.07x figure is a narrower
> `create_agent`-only scope without the middleware chain; see
> `docs/competitive-proof.md` §1b.

---

## Repository layout

```
packages/                 TypeScript: protocol / driver / sdk-js / devtools-protocol / testkit
python/                   Python: reactivegraph / reactivegraph_sdk / reactivegraph_cli
benchmarks/               Benchmark harness and workloads
docs/                     Specs, benchmarks, performance comparisons
```

| Command | Purpose |
|---|---|
| `make test` | Native Python tests |
| `pnpm typecheck` / `pnpm test` | All TypeScript packages |
| `pnpm benchmark` | Selective-update benchmarks |

---

## Learn more

- **[Benchmarks & reproduction](docs/benchmarks.md)** — selective updates plus differential measurements against real upstream langgraph, including an honest no-gain scenario
- **[Commercial evaluation](docs/commercial-readiness.md)** — positioning, boundaries, adoption
- **[FAQ](docs/faq.md)** — including "why is LangChain still visible?"
- **[Security](SECURITY.md)** — gateway auth, field-level permissions, state redaction, read-only importer

## Contributing & pilot

See [`CONTRIBUTING.md`](CONTRIBUTING.md) and [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
Issues, PRs, and [Discussions](https://github.com/from000/reactive-graph/discussions) are welcome.

We are looking for a small number of teams to pilot v0.1.0 on a real agent or
workflow workload (tools, retries, durable state, human review, multi-tenant
gateway, or a state-graph migration). See
[`.github/PILOT_INVITATION.md`](.github/PILOT_INVITATION.md) and open a
["Pilot user" issue](https://github.com/from000/reactive-graph/issues/new?template=pilot_user.md).

---

## License

MIT — see [`LICENSE`](LICENSE).

## Third-party notices

The compatibility layer (`messages`, `tools`, `ToolNode`, `channels`,
`middleware`, `checkpoint`, `store`, `message_utils`, ...) adapts interfaces
and behaviour from `langchain-ai/langgraph` and `langchain-ai/langchain`
(MIT); the `uuid6` generator adapts `oittaa/uuid6-python` (MIT). Full
copyright notices, reference versions and license texts are in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

## Commit history note

The public repository was published as a single squashed commit, so commit
hashes quoted in `docs/` identify private development checkpoints and do
**not** resolve here. Fork revisions are quoted as `<current> (was
<pre-rewrite>)` (`ab0737f` (was `57e8d2d`), `606187b` (was `3b55c36`)); an
older proof document cites `c128e42`, a 2026-09-27 checkout the rewrite
dropped, whose measurements were reproduced afterwards on `606187b` and
`ab0737f`. `d2aca13` / `f9f3127` are ordinary upstream commits; the LangGraph
pins (`11ee1859…`) belong to `langchain-ai/langgraph`.
