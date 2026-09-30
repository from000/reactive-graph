# 选择性执行收益量化报告（DeerFlow 场景 + 真实 langgraph 对照）

日期:2026-09-20 · 状态:advisory(机器敏感,非 merge gate;differential discipline:median of ≥3,no-gain 必报)

## 结论摘要

ReactiveGraph 的**跨 run 选择性执行**(pure 任务输入指纹不变即跳过)在两种尺度下
都有可量化收益:

| 场景 | 我们 | 对照 | 收益 |
|---|---|---|---|
| DeerFlow lead_agent(3 任务)同输入 50 轮 | 采样轮 **0 次任务执行**(skip 100%) | 全量重跑 150 次 | 任务执行 150 → 3(含首次) |
| 1000 节点图 / 10 节点受影响(selective) | **7.14 ms** | langgraph 1.2.11 **10,574.87 ms** | ≈ **1480×** |
| 1000 节点 fan-out / 改 1 字段(wide) | **6.86 ms** | langgraph 1.2.11 **5,490.82 ms** | ≈ **800×** |
| 2 节点对话 invoke(chat,warm) | **0.76 ms** | langgraph 1.2.11 **2.51 ms** | ≈ 3.3× |

## 方法

- **DeerFlow 场景**(本次新增):`benchmarks/deerflow/run_selective_gain.py`
  — 链路 1 lead_agent 图(planner→executor→verifier,3 个 pure 任务,host 模式
  真实 Node Driver/RGP/1),同输入 50 轮(跨 run 指纹跳过)vs 每轮不同输入 50 轮
  (no-gain);warmup 1 轮含首次执行;`--repeats 3` 取 median of medians。
- **differential 对照**(引用既有数据):`benchmarks/differential/.results/{reactive,langgraph}.json`
  — 同机同图,`langgraph==1.2.11` 固定版,输出一致性断言后计时。
- **指标**:任务执行数(决定性)+ wall-clock median/p95(参考;per-run 含 RGP/1
  进程往返,收益主要体现为任务不执行——省计算/副作用,非 IPC)。

## DeerFlow 场景数据(本次运行,2026-09-20)

```
同输入 gain  : 采样 50 轮执行 0/450 任务(skip_rate=100%;warmup 执行首次 3 任务/轮)
               median=1.63ms per-run（全 skip 路径,RGP/1 往返主导）
变输入 no-gain: 执行 450/450 任务(skip_rate=0%)
               median=6.38ms per-run（每轮全量执行,诚实 no-gain 对照）
环境: Python 3.12.9 / node v24.15.0 / Darwin x86_64
```

解读:同输入重跑时 langgraph(Pregel)会执行全部 3R 个任务节点,我们首次 3 个、
后续 0 个——**任务执行是决定性的**;wall-clock 差异(1.63 vs 6.38ms)是每轮
no-gain 全执行 vs skip 的副产物,绝对数值受 RGP/1 进程往返主导,不作为收益主张。

## differential 引用数据(同机 langgraph 1.2.11 真实对照)

| workload | 图 | 我们 median | langgraph median | 收益 |
|---|---|---|---|---|
| chat | 2 节点,warm invoke | 0.756 ms | 2.506 ms | 3.3× |
| wide | 1000 独立节点,改 1 字段 | 6.859 ms | 5,490.819 ms | ~800× |
| selective | 1000 节点 / 10 受影响 | 7.140 ms | 10,574.870 ms | ~1480× |
| nogain | 1000 节点全依赖链,每输入不同 | 259.952 ms | 4,522.630 ms | ~17×(调度开销仍更低) |

数据来源:`benchmarks/differential/.results/`(reactive.json / langgraph.json;
运行时间早于本报告,环境与复现方法见 `benchmarks/differential/README.md`)。

## 局限（诚实声明）

1. **机器敏感**:所有数值为单机(Darwin x86_64)median,非跨机基准;复现走
   `run_selective_gain.py` / `run_differential.py --refresh`。
2. **no-gain 必报**:变输入场景(或全依赖链)选择性执行无收益——我们的调度开销
   仍低于 langgraph 全量重跑,但收益主张只限于"同输入/小受影响面"场景。
3. **wall-clock 的 IPC 噪声**:host 模式 per-run 含 RGP/1 进程往返,微图收益
   主要体现在任务执行数;真实收益面向副作用/重算/外部调用被跳过。
4. **differential 数据为既有记录**:非本次新跑;权威复现应重跑
   `uv sync --directory benchmarks/differential && uv run --directory benchmarks/differential python run_differential.py --refresh`。

## 复现

```bash
# DeerFlow 场景（本次报告）
uv run --directory python/reactivechain python $PWD/benchmarks/deerflow/run_selective_gain.py --rounds 50 --repeats 3
# 结果: benchmarks/deerflow/.results/selective_gain.json

# differential 权威对照（真实 langgraph 1.2.11,需网络装依赖）
uv sync --directory benchmarks/differential
uv run --directory benchmarks/differential python run_differential.py --refresh
# 结果: benchmarks/differential/.results/{reactive,langgraph}.json
```
