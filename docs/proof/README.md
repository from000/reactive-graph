# Local Proof Bundle

> **Frozen historical snapshot (2026-09-23).** The counts, hashes and logs in
> this directory describe commit
> `f439e3c74cc9c36bcf7757d30f793ba5f7632939` only. They are not the current
> release gate and must not be cited as current test counts. For current
> evidence see `docs/plan-completion-audit.md`; raw logs are intentionally not
> rewritten so the historical bundle stays auditable.

This bundle proves the engineering state of commit
`f439e3c74cc9c36bcf7757d30f793ba5f7632939` without requiring a public push,
clone, external package installation, or external users.

It is a local attestation, not a public/community proof.

## Environment

See [`environment.txt`](environment.txt).

## Real upstream differential benchmark

The benchmark was rerun on this commit with the real upstream package
`langgraph==1.2.11`.

Commands:

```bash
uv sync --directory benchmarks/differential
uv run --directory benchmarks/differential \
  python run_differential.py --refresh
```

Raw results:

- [`reactive-benchmark.json`](reactive-benchmark.json)
- [`langgraph-benchmark.json`](langgraph-benchmark.json)
- [`differential-benchmark.log`](differential-benchmark.log)
- [`benchmark-summary.json`](benchmark-summary.json)

Output equality was asserted before timing was accepted.

### Results

| Workload | Reactive median | LangGraph median | Ratio | Reactive tasks/run | LangGraph tasks/run |
|---|---:|---:|---:|---:|---:|
| chat | 0.218 ms | 1.147 ms | 5.3× | 2 | 2 |
| wide | 2.960 ms | 3251.446 ms | 1098.3× | 1 | 1000 |
| selective | 1.847 ms | 3180.731 ms | 1721.8× | 10 | 1000 |
| nogain | 120.178 ms | 2750.486 ms | 22.9× | 1000 | 1000 |

The no-gain row is intentionally included: all tasks/nodes genuinely execute.

## Tests

Logs:

- `reactivegraph-tests.log` — 152 passed, 1 skipped
- `reactivechain-tests.log` — 219 passed
- `reactivegraph-sdk-tests.log` — 12 passed
- `reactivegraph-cli-tests.log` — 9 passed
- `e2e-tests.log` — 43 passed
- `deerflow-tests.log` — 26 passed
- `examples-deerflow-tests.log` — 25 passed
- `protocol-tests.log` — 17 passed
- `driver-tests.log` — 192 passed

## Static verification

- `ruff.log` — all Python packages passed
- `mypy-reactivegraph.log` — no issues
- `mypy-reactivechain.log` — no issues
- `mypy-reactivegraph-sdk.log` — no issues
- `mypy-reactivegraph-cli.log` — no issues
- `protocol-typecheck.log` — passed
- `driver-typecheck.log` — passed
- `typescript-build.log` — workspace build passed
- `mkdocs-build.log` — strict documentation build passed
- `docs-links.log` — 51 relative links resolved

## Build artifacts

- `build-reactivegraph.log`
- `build-reactivechain.log`
- `build-reactivegraph-sdk.log`
- `build-reactivegraph-cli.log`
- `artifacts.txt`
- `artifacts.sha256`

All four Python packages produced a source distribution and wheel. Artifact
SHA-256 hashes are recorded in `artifacts.sha256`.

## What this proves

At this commit and environment, ReactiveGraph has:

- working selective execution
- lower wall-clock latency on all four tested workloads
- output parity with upstream LangGraph
- substantially fewer executed tasks on selective workloads
- full local regression/build/docs verification

## What this does not prove

It does not prove:

- public package installation
- public CI execution
- external user adoption
- community trust
- cross-machine/cross-platform performance

Those require publication and external users.
