# ReactiveGraph benchmarks (Task 13)

Release-gate benchmark suite. Every workload runs **both semantics** from the
same graph definition, so we never compare only a ReactiveGraph-friendly graph:

* `reactive` — the native Driver scheduler (selective: only affected tasks and
  invalidated computeds run);
* `naive` — a semantically equivalent full-scan executor (all tasks/computeds
  run every step, LangGraph-style barrier semantics).

## Workloads

| workload | nodes | shape | change |
|---|---|---|---|
| `selective` | 100–1,000 | each node reads a disjoint or partially overlapping path | one field |
| `fanout` | 8 × 32 | independent read/write sets, intentional conflicts | N/A |

## Running

```bash
pnpm --filter @reactivegraph/driver benchmark --workload selective --nodes 200 --runs 5
```

Output (raw samples + medians + p95):

```
reactive: considered=2 executed=1 recomputations=1 median=.. p95=..
naive:    considered=200 executed=200 recomputations=200 median=.. p95=..
```

## Environment pinning (per plan §16)

Run both sides from clean checkouts on the same machine image with identical
Node/Python versions, test data, concurrency limits, and a fixed warmup/
repetition policy. Publish raw samples, harness source, and environment
details. Never report only the reactive side.

## e2e 功能验证套件（benchmarks/ 目录家族）

| 目录 | 内容 | 运行 | 状态 |
|---|---|---|---|
| `e2e/` | 43 功能点矩阵（FEATURE_MATRIX.md 唯一事实源） | `uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/ -q` | 43 passed |
| `deerflow/` | DeerFlow v2 能力验证型移植（DEERFLOW_MATRIX.md）：六条核心链路 + 差异化 7 项 + smoke | `uv run --directory python/reactivechain pytest $PWD/benchmarks/deerflow/ -q` | 26 passed |
| `differential/` | reactivechain vs langgraph 差分（同图同输入输出一致断言） | 见 `differential/README.md` | 全绿 |

> deerflow 套件对照基线 `bytedance/deer-flow @ f9f3127`（源码不进仓库，hash 固定）；
> 移植过程暴露 4 个框架缺陷（F1 list 切片 / F2 工具代理参数 / F3 代理 repr /
> F4 多段 HITL），全部修复并回归全绿——详见 DEERFLOW_MATRIX.md。