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

## 6. Honest gaps

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
