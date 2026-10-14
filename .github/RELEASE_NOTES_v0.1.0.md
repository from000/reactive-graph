# ReactiveGraph v0.1.0

> Draft GitHub release copy. Do not publish this release until every external
> checkbox in `RELEASE_CHECKLIST.md` is complete.

ReactiveGraph is a reactive runtime for agent and workflow systems. Version
0.1.0 is the first workspace release of the native engine: graph execution is
tracked as declared dependencies, transactions, durable events, and
explainable runtime decisions rather than an unconditional full replay.

## Highlights

- **Selective execution** — event routing, computed read-set invalidation,
  input fingerprints for pure tasks, and effect receipts minimize repeated work.
- **Transactional state** — tasks return read/write sets and patches that commit
  atomically with explicit conflict policies.
- **Durable runtime** — SQLite, PostgreSQL, and Redis-backed durability paths,
  checkpoints, replay, retention, and restart recovery.
- **Agent orchestration** — native ToolNode/ReAct-style agent primitives,
  bounded parallel tool calls, interrupts, permissions, budgets, and an MCP
  adapter.
- **Observability** — causal trace export, run explanation, OTLP JSON, bounded
  span retention, and latency metrics.
- **Migration and ecosystem** — a read-only LangGraph importer, LangChain tool
  and message adapters, Python and TypeScript SDKs, and a remote gateway.

## Install

> **Distribution status:** v0.1.0 is distributed from GitHub source. It is not
> on PyPI or npm, so `pip install reactivegraph` and
> `npm install @reactivegraph/sdk-js` do **not** work yet.

Python runtime plus CLI (install both together — the CLI depends on
`reactivegraph>=0.1.0`):

```bash
pip install \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_cli"
```

TypeScript packages build from source, because npm cannot resolve this
monorepo's `workspace:*` dependencies:

```bash
git clone https://github.com/from000/reactive-graph.git
cd reactive-graph && pnpm install && pnpm -r build
```

The npm distribution set is `@reactivegraph/protocol`,
`@reactivegraph/devtools-protocol`, `@reactivegraph/driver`, and
`@reactivegraph/sdk-js`. `@reactivegraph/testkit` is private.

## Verification

The release candidate is accepted only when the release checklist and CI are
green. The current engineering evidence includes:

- clean-install smoke tests for all four Python wheels and all four public npm
  tarballs;
- Python sdist/wheel metadata validation through `twine check`;
- npm publish dry-runs for all four public packages;
- Python, TypeScript, PostgreSQL, Redis, documentation, and browser E2E suites;
- reproducible competitive benchmarks linked from `docs/competitive-proof.md`.

See `docs/plan-completion-audit.md` for the dated verification record and
`RELEASE_CHECKLIST.md` for the remaining external actions.

## Compatibility and limits

- This is a `0.x` release; public APIs may change before 1.0.
- The LangGraph compatibility layer is not an execution path. Migration uses
  the read-only importer plus native ReactiveGraph APIs.
- Production gateway deployments must configure tenant authentication; the
  no-tenant default is local no-auth mode.
- Public npm/PyPI publication and five external pilot confirmations remain
  external release gates.
