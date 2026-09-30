# Why the Native API is the Product (原生 API 竞争力)

ReactiveGraph 的产品价值在**原生 API**——反应式调度模型，它让 LangGraph
做不到的事情变成可能。

## 一、核心差异：反应式 vs 全量重算

| 维度 | LangGraph (Pregel) | ReactiveGraph 原生 |
|---|---|---|
| 每次 `invoke` | 全量执行所有可达节点 | 只执行受影响节点（依赖分析 + 缓存失效） |
| 纯函数重算 | 每步都重算 | fingerprint 相同即跳过 |
| computed | 无内建缓存 | read-set 变化才重算 |
| 副作用幂等 | 靠外部 checkpointer 判断 | effect receipt 内建，确认即不重跑 |
| 冲突 | 全量屏障串行 | 乐观读 + 写集冲突检测，不相交写集并行提交 |
| 事务 | 无显式事务原语 | transaction 携带读写集 + patches，原子提交 |

## 二、真实数据（docs/benchmarks.md，可复现）

同一确定性图、同一机器、上游真实 `langgraph`（pin
`11ee185999b86bfea2d8c0e69cef9a5e37acf686`，libs/langgraph 1.2.11）对比，
2026-09-02 复测（串行、预热后取中位数；机器负载 9–16/8 核）：

| 场景 | langgraph | ReactiveGraph | 比值 |
|---|---|---|---|
| 对话图 invoke（warm 中位） | ~1.5 ms | ~0.004 ms | ~370x |
| 1000 节点 fan-out 改 1 字段 | ~11602 ms | ~15.3 ms | ~760x |
| 1000 节点 / 10 受影响，改 k0 | ~4669 ms | ~6.9 ms | ~680x |
| **no-gain：1000 节点全依赖链** | **~6428 ms** | **~8.7 ms** | **~735x** |

两侧输出逐字段一致（`{'x':1,'y':2}`、`v0=2, v999=2`、`final_v=1001`）。差异
是架构性的：Pregel 每超步重跑全部节点并做逐节点簿记；ReactiveGraph 编译一
次、运行期反应式更新。

## 三、原生 API 的独有能力（兼容层没有的）

1. **事务性状态** — `transaction` 是提交单元，携带读集/写集与 patches；冲突
   有明确策略（reducer / priority / 串行），不是"全量重算后合并"。
2. **computed 缓存** — `selector` 声明 read-set，只读集变化才失效。1000 个
   computed 只改 1 个输入时只重算 1 个（benchmark：4 recomputations vs 1000）。
3. **纯任务 fingerprint skip** — 相同输入不重跑（200 节点改 1 字段只执行 4 次
   vs 200 次）。
4. **effect receipt 幂等** — 已确认副作用永不重放（Task 6 持久化）。
5. **调度 trace** — 每个决策（start/done/retry/skip/invalidate）都有序、可导出
   为因果 trace（Task 12），是调试与可观测性的基础。
6. **scope 子状态** — 命名子状态命名空间，天然模块化。
7. **并发控制** — 有限并发 + 确定性 tie-break，不相交写集可并行提交。

## 四、上手即原生

```python
from reactivegraph import GraphBuilder, ReactiveGraph

graph = ReactiveGraph.build(lambda b: b.task("t", fn=fn).on("visit", "t"))
out = graph.invoke("visit", payload)
```

原生 API 的文档（docs/spec/native-api.md）刻意保持**简单、有说服力**，
引导用户直接走向反应式模型。

## 五、为什么可以宣称"更强"但不说"全面超越"

- **更强**：选择性执行、事务、computed 缓存、effect 幂等、因果 trace——这些
  是 Pregel 语义里不存在的原语，有可复现 benchmark 支撑。
- **不全面超越**：LangGraph 生态（checkpoint-postgres 家族、prebuilt、
  SDK、CLI、LangSmith 集成、海量文档）比我们成熟；在功能广度上我们仍在追赶。
  诚实表述：**"在反应式执行这一个维度上我们更优，作为完整替代品仍在路上"。**
