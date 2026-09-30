# Benchmarks

ReactiveGraph's release-gate benchmarks (plan Task 13). Every workload runs the
same graph definition under both **reactive** (native Driver scheduler) and
**naive** (LangGraph-style full-scan) semantics — we never compare only a
ReactiveGraph-friendly graph.

Run: `pnpm benchmark` (vitest over `benchmarks/harness/`).

## Differential vs real upstream langgraph (reproducible)

We installed the **real upstream `langchain-ai/langgraph`** (pin
`langgraph==1.2.11` + checkpoint + prebuilt) in an isolated uv venv and ran
identical deterministic graphs under both engines on the same machine. The
harness lives in the repo: `benchmarks/differential/`
(`harness.test.ts` reactive side · `langgraph_side.py` upstream side ·
`run_differential.py` orchestrator). The orchestrator asserts **identical
outputs** for every workload before printing timing ratios — never compare only
the reactive side.

**Measurement protocol:** each side runs serially (never in parallel — parallel
runs steal CPU and inflate both sides), after a warm-up invocation, and we
publish the **median of 3 runs**. The no-gain workload (1000-node chain where
every node genuinely depends on the changed seed) is published alongside the
gain workloads — never cherry-picked away.

**Machine & versions (latest run 2026-09-15):** Intel Core i5-1038NG7 @
2.00GHz (8 cores), 16 GB RAM, macOS; Node v24.15.0, pnpm 11.24.0, uv 0.12.7,
Python 3.12.11, `langgraph==1.2.11`. Absolute numbers vary with machine load;
**ratios are the robust claim**. Reproduce: `uv sync --directory
benchmarks/differential && uv run --directory benchmarks/differential python
run_differential.py --refresh`.

| workload (1000 nodes) | upstream langgraph | ReactiveGraph | ratio |
|---|---|---|---|
| chat (2-node invoke, warm median) | 1.020 ms | 0.203 ms | ~5x |
| wide fan-out, change 1 field | 2359.3 ms | 1.90 ms | ~1242x |
| selective, 1000 nodes / 10 affected | 2404.4 ms | 1.45 ms | ~1658x |
| **no-gain: 1000-node chain, ALL depend** | **2070.1 ms** | **56.8 ms** | **~36x** |

ReactiveGraph tasks executed per run (`executedPerRun`): chat=2, wide=1,
selective=10, nogain=1000 — selectivity is real, not a constant-factor
illusion. Outputs were identical for every workload (`x/y`, `v0/v999`,
`v0/v9/v999`, `v999`).

**No-gain honesty check (plan §16):** the 1000-node *chain* row is the worst
case for a reactive engine — every node genuinely depends on the changed seed,
so selectivity is zero and both engines re-run all nodes. The remaining ~36x is
pure per-node constant overhead (upstream per-node schema/channel bookkeeping
vs our plain function call), published precisely to show the gain is *not* only
selectivity.

> **测量修正记录（透明性）**：初版 no-gain 测量中，langgraph 侧的
> `StateGraph` schema 未声明输入键 `seed`，其输入 schema 会丢弃未知键，导致
> langgraph 每次都用 `seed=0` 计算（输出 `final_v=1000`）而 ReactiveGraph 用
> `seed=1`（`final_v=1001`）——语义不对等。修复方式：把 `seed` 声明进 langgraph
> schema，重测后两侧输出一致（`final_v=1001`）。修正前后的差距方向一致，但
> 只有修正后的数据是语义对等的。

> **早期测量（2026-09-02，仅供量级参考）**：更早版本记录过 chat ~370x、
> wide ~760x、selective ~680x、no-gain ~735x（含 0.004 ms 量级的 chat 数字），
> 该轮测量口径与机器负载和当前 differential 不同，**不可复现**；权威口径以
> 上表 `benchmarks/differential/` 复测结果为准。

## ReactiveChain vs LangChain（组件层差分，M6 成功标准）

ReactiveChain 是构建在 ReactiveGraph 之上的声明式管道组件层，其差异化卖点是
**段级选择性**：管道内某段输入指纹未变（如同 query 重跑时的检索段）→ 整段跳过。
这里与上游 **langchain-core（LCEL）** 在**同机、同语义**管道上对比——上游没有
段级跳过，同输入也全链重跑。脚本入库 `benchmarks/differential/`
（`reactchain_side.py` · `langchain_side.py` · `run_reactchain_differential.py`），
编排器断言**两侧输出逐 workload 一致**后才打印比率。

**langgraph StateGraph 语义对照（2026-09-15）**：用真实 `langgraph==1.2.11`
`StateGraph`（2 节点链：inc→double，TypedDict 状态）与 ReactiveChain
`Pipeline(...).to_graph()`（同一 2 段链包装为 effect task）对比：`invoke({"n": 4})`
两侧语义结果一致（`y=10`）。已知差异（设计文档 §3 记录，非缺陷）：
langgraph 保留完整状态字典（`{'n':4,'x':5,'y':10}`）且图级流式逐节点产出
（`stream` 2 步）；ReactiveChain 的 `to_graph` 是**整链单 effect task**（返回
末段 writes，不暴露图级流式）——逐节点图运行时（节点/边/条件边/checkpoint）
不在组件层对标范围，由 ReactiveGraph 引擎层承担。早期临时验证脚本 `lg_compare.py` 未入库，
不作为可复现基准；正式口径以 LCEL 4 场景的差分基准为准。

**测量协议**：两侧串行 + 预热 + median×3（同 ReactiveGraph 差分协议）；
假模型（FakeLLM / FakeListChatModel）与离线 HashEmbeddings，不依赖外部服务。

**Machine & versions（2026-09-15，同机）：** Intel Core i5-1038NG7、macOS、
Python 3.12.11、uv 0.12.7、`langchain-core==1.6.2`（随 `langgraph==1.2.11`
传递安装）。复现：`uv run --directory benchmarks/differential python
run_reactchain_differential.py`。

| workload（同语义 RAG/工具链） | upstream langchain | ReactiveChain | ratio |
|---|---|---|---|
| rag_first（query X 首次全链） | 1.049 ms | 0.019 ms | ~54x |
| rag_selective（同 query 重跑） | 1.419 ms | 0.026 ms | ~56x |
| rag_different（不同 query 全链） | 0.923 ms | 0.022 ms | ~43x |
| toolchain（工具调用链） | 0.489 ms | 0.029 ms | ~17x |

**选择性证据**（同 query 重跑时检索段实际调用次数）：reactivechain=1（第二次
被指纹缓存跳过）、langchain=2（无跳过、全链重跑）。检索段选择性同样有
单元级证据（`tests/test_retrieval.py::test_retriever_segment_selective_skip`）。

**No-gain 诚实报告**：reactivechain 在**不同 query**（rag_different）与
**工具链**（toolchain）上同样全链执行、不跳过——收益集中在"相同输入重跑"
（rag_selective），这正是选择性执行的差异点；其余倍率来自框架常驻开销
（上游 LCEL 每段 config/tracing vs 本组件层薄封装），如实公布不剔除。

## Selective-update workload (native driver)

100–1,000 nodes, each reading a disjoint path. One field changes per step.

| metric | reactive | naive | release target |
|---|---|---|---|
| tasks considered | 200 | 200 | — |
| tasks executed | 4 (only affected) | 200 (all) | ≥80% fewer unnecessary pure executions ✅ |
| computed recomputations | 4 | 1000 | — |
| p95 completion latency | ~2.1 ms | ~27.3 ms | ≥2x lower p95 ✅ |

Raw samples and the harness source (`benchmarks/harness/selective.test.ts`) are
the reproducibility record. Per plan §16: both sides run on the same machine
image, Node version, concurrency limits, and warmup/repetition policy. We
publish medians and p95 from raw samples, and report a no-gain result when all
nodes genuinely depend on the changed state (not shown here because the
workload is disjoint-path by construction).

## How to extend

Add a workload under `benchmarks/harness/` and a row to `benchmarks/README.md`.
Every performance claim in docs must link to a reproducible benchmark command
and its raw output.

## Cost / streaming / durability / parallelism

Durable-recovery is covered by the driver durability tests; streaming
backpressure workloads are stubbed for environments with Postgres/Redis.