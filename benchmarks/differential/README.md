# Differential Benchmark: ReactiveGraph vs upstream langgraph

**Reproducibility record(plan §16)**:the same deterministic graphs run under
BOTH engines on the SAME machine, with per-run inputs aligned; the orchestrator
asserts **identical outputs** before printing timing ratios. Never compare only
the reactive side.

## Layout

| file | side | what |
|---|---|---|
| `harness.test.ts` | ReactiveGraph | native Driver Scheduler; 4 workloads → `.results/reactive.json` |
| `langgraph_side.py` | upstream | real `langgraph==1.2.11`; 4 workloads → `.results/langgraph.json` |
| `run_differential.py` | both | runs both, asserts output equality, prints table |
| `pyproject.toml` | — | pins `langgraph==1.2.11` (uv) |

## Workloads

- `chat`:2-node graph,warm median invoke latency
- `wide`:1000 independent nodes (fan-out),one field changed per run
- `selective`:1000 nodes / 10 affected per run
- `nogain`:1000-node dependency chain — every input differs,nothing skippable
  (**no-gain scenario must be published, never cherry-picked away**)

Outputs must match per workload after projection (chat `x/y`, wide `v0/v999`,
selective `v0/v9/v999`, nogain `v999`).

## Run

```bash
uv sync --directory benchmarks/differential          # installs langgraph 1.2.11
uv run --directory benchmarks/differential python run_differential.py --refresh
```

## Discipline

- median of ≥3 runs, not a single sample
- same machine, sequential execution
- no-gain workload reported alongside the gain workloads
- CI job (`.github/workflows/benchmark.yml`) is manual (`workflow_dispatch`)
  because it takes minutes; results are advisory, not a merge gate
- CI job (`.github/workflows/benchmark.yml`) is manual (`workflow_dispatch`)
  because it takes minutes; results are advisory, not a merge gate
