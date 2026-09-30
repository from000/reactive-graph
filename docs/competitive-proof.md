# Competitive Proof — ReactiveGraph vs LangGraph / LangChain

This report only claims capabilities that are backed by reproducible commands,
pinned upstream packages, and committed tests. It does not claim ecosystem
parity.

## 1. Same-output performance matrix

The benchmark orchestrator first asserts output equality; timing is printed
only after outputs match. This prevents comparing two engines that silently
produced different results.

## 1a. Performance and selective execution

The differential benchmark runs the same deterministic graphs under
ReactiveGraph and upstream `langgraph==1.2.11`, asserts identical outputs, and
reports medians.

Run:

```bash
uv sync --directory benchmarks/differential
uv run --directory benchmarks/differential python run_differential.py --refresh
```

CI publishes the raw differential result JSON as the
`differential-benchmark-matrix` artifact from the manual benchmark workflow.

Latest committed evidence is in:

- `benchmarks/differential/.results/reactive.json`
- `benchmarks/differential/.results/langgraph.json`
- `benchmarks/deerflow/SELECTIVE_GAIN_REPORT.md`

Key architectural result:

| Workload | Reactive behavior | LangGraph behavior |
|---|---|---|
| Wide fan-out, one changed input | executes only affected tasks | full reachable graph walk |
| Selective, 10 affected nodes | executes affected subset | full reachable graph walk |
| Same input rerun | pure tasks skip | nodes rerun |
| No-gain fully changed chain | all tasks run | all nodes run |

The no-gain row is always reported. Selective execution is not claimed where
nothing can be skipped.

## 2. No-gain matrix

The no-gain workload is intentionally included:

| Workload | Expectation | Meaning |
|---|---|---|
| Fully changed dependency chain | all tasks execute | reactive selection has nothing to skip |
| Changed query in RAG | retrieval/model tasks rerun | only unchanged segments skip |
| Distinct tool inputs | tools execute | receipt is input-scoped |

This matrix prevents cherry-picking only favorable selective workloads.

## 2a. Duplicate side-effect matrix

ReactiveGraph records an effect intent before an external call and a receipt
only after its patch commits. On restart, a matching input plus confirmed
receipt skips the effect.

Evidence:

```bash
uv run --directory python/reactivechain \
  python -m pytest tests/test_tools.py -q
uv run --directory python/reactivechain \
  python -m pytest benchmarks/deerflow/ -q
```

The tool suite covers same-run and Driver-restart receipt reuse.

## 3. Explainability comparison

ReactiveGraph exposes native decision queries:

```python
g.explain_run()
g.explain_run(run_id="...")
g.why_skipped("task")
g.why_invalidated("selector")
g.explain_conflict()
g.export_trace(format="json")
g.export_trace(format="dot")
g.cost_saved()
```

The comparison point is semantic, not cosmetic: LangGraph streams updates and
checkpoints; ReactiveGraph records the runtime decision itself (skip reason,
invalidation read-set, conflict winner/loser/write sets, estimated savings).

Evidence:

```bash
uv run --directory python/reactivegraph \
  python -m pytest tests/test_observability.py -q
```

## 4. Correctness model

ReactiveGraph tasks have a runtime contract:

- `kind`: `pure` / `effect` / `opaque`
- actual tracked reads
- declared writes
- transactional patches
- effect receipts

Parallel same-path writes are rejected with a structured conflict explanation.
Field permissions and budgets can reject work before side effects.

Evidence:

```bash
pnpm --dir packages/driver test
uv run --directory python/reactivegraph python -m pytest tests/test_observability.py -q
```

## 5. Migration-cost comparison

Supported ordinary LangGraph state graphs can be inspected and imported without
executing node functions:

```python
from reactivechain import inspect_langgraph, import_langgraph

report = inspect_langgraph(upstream_graph)
native_graph = import_langgraph(upstream_graph)
```

Conditional edges are explicitly reported unsupported; they are not silently
converted with changed semantics.

Existing LangChain-compatible tools and messages can use optional adapters.
ReactiveChain runtime has no LangChain dependency.

Evidence:

```bash
uv run --directory python/reactivechain \
  python -m pytest tests/test_langgraph_import.py tests/test_tools.py -q
```

## 6. Measured comparison of selective-execution mechanics

Selective execution exists on both sides, but the *mechanism* differs. Measured
on this machine with the same 1000-node graph shape (independent nodes, one
field changes between runs), using each library's documented API:

| Configuration | Nodes executed | Notes |
|---|---:|---|
| ReactiveGraph Driver (task declares reads) | 1 / 1000 | default behaviour |
| ReactiveGraph Python fallback (task declares reads) | 1 / 1000 | default behaviour |
| ReactiveGraph (no reads declared) | 1000 / 1000 | unscoped fallback, never under-skips |
| langgraph 1.2.11 + per-node CachePolicy(key_func=...) | 1 / 1000 | requires an explicit key per node |
| langgraph 1.2.11 + set_node_defaults(cache_policy=CachePolicy()) | 1000 / 1000 | measured: the global default does not skip |
| langgraph 1.2.11, no cache | 1000 / 1000 | default |

What this shows, and what it does not:

- **Shows**: the Driver's fingerprint granularity is available without writing a
  cache key per node; declaring reads (which a task already needs for conflict
  detection and scope isolation) is sufficient.
- **Does not show**: that LangGraph is incapable of selective execution. It can
  skip with a correctly written key_func — it just requires that explicit
  per-node configuration, and the one-line set_node_defaults form measured
  above does not achieve it.
- The 1000x figures elsewhere in this document come from that difference in
  *configuration*, not from LangGraph being architecturally unable to skip. Read
  them together with this table.

Reproduce: `packages/driver/test/scheduler/reads-scoped-fingerprint.test.ts`
covers the ReactiveGraph rows; the LangGraph rows use the same graph shape with
`langgraph==1.2.11` pinned in `benchmarks/differential/uv.lock`.

## 7. Honest gaps

LangGraph and LangChain remain stronger in:

- integration count
- community size
- public production deployments
- documentation and examples
- package maturity and installed base

ReactiveGraph's public release also requires repository-side secrets
(`NPM_TOKEN`, `PYPI_TOKEN`) and an `@reactivegraph` npm organization. These are
external operational requirements, not engineering claims.

The architectural lead is in the reactive runtime. Full ecosystem parity is not
claimed.
