# 先行研究扫描（prior art）— 我们到底新在哪

日期：2026-10-17 · 方法：arXiv API 关键词检索 + 摘要精读（只读一手摘要，不引二手评论）

## 结论先行

**三个最核心的卖点，全部已有先行工作，且都早于我们（2026-05 起）：**

| 我们的卖点 | 最接近的先行工作 | 时间 | 覆盖程度 |
|---|---|---|---|
| 事件日志为真相 + 确定性回放 | **The Log is the Agent: Event-Sourced Reactive Graphs for Auditable, Forkable Agentic Systems**（ActiveGraph, Yohei Nakajima） | 2026-05-21 | **高度重合**：append-only log 为 source of truth、working graph 是 log 的确定性投影、行为对 graph 变化反应并 emit 事件、确定性回放契约、从任意事件 fork **且不重跑共享前缀** |
| 可审计的因果 lineage | 同上（end-to-end lineage：从目标到具体 model call） | 2026-05-21 | 重合 |
| LLM 语义/子调用缓存省钱 | 语义缓存一批（LaCache、Closing the Operational Gap in Semantic Caching 等）；tool-result memoization（A Policy-Driven Runtime Layer for Agentic LLM Serving） | 2025–2026 | 方向重合（他们做相似度命中，我们做读集指纹） |
| 选择性重算 / 部分重执行 | Incremental computation / dataflow recomputation（自 2010s 起）；FlowBank（Precompute-and-Reuse） | 长期 | 经典问题，非新问题 |
| exactly-once 工具副作用 | "Where Does Exactly-Once Live? Model, Harness, and Tool-Contract Effects on Duplicate Side Effects in LLM Agents" | 2026 | 已被正面研究 |

（完整检索词与命中数见本文件末。版本号/标题以 arXiv 查询结果为准。）

## 我们仍然不同的地方（诚实版，不是"更优"，只是"不同"）

1. **绑定面**：那些工作多是新 runtime / 新范式（ActiveGraph 要你换到它的 log-first 模型）；
   我们的差异化是**在 LangGraph 兼容 API 之下**做 read-set 指纹跳过 + receipt 幂等，
   即"让你几乎不改代码就拿到选择性执行"。
   ⚠️ 但这同时是弱点：兼容层没有学术贡献，只有工程价值。
2. **省钱的量化口径**：现有语义缓存论文比的是**命中率与延迟**；我们比的是
   **"哪些调用在语义上根本不该发生"**（read-set 未变 → 跳过，而非相似 → 复用），
   并用逐次调用计数验证。这条更窄，但更硬。
3. **DeerFlow 上的真实落地**：把 19k 测试的真实产品换成该底座，其它论文多为原型。

## 能发论文吗（判断）

- **作为"新机制/新范式"投顶会**：**不建议**。核心想法已被 2026-05 的 ActiveGraph 论文占位，
  我们没有理论上的新东西可立。
- **作为"系统/工程论文"投 workshop 或系统类会议**：**有可能**，但需要有别人没有的实证：
  例如"同一 agent 在真实负载下，读集指纹 vs 语义相似度缓存的省钱差异"，
  且要有可复现的 baseline 与统计显著性——我们目前只有 n=2 runs 的 demo，不够。
- **作为技术报告 / 白皮书 / 博客**：**可以且合适**。我们有真实产品落地 + 可复现脚本，
  这是很多论文没有的。定位成"工程实践报告"是诚实的。

## 若要往研究方向发展，唯一有戏的角度

**"确定性边界"问题**：LLM 调用本质不纯（同输入未必同输出），而选择性执行要求纯。
现有工作要么放弃跳过（全跑），要么用相似度（可能错误复用）。
可研究的真问题是：*在什么条件下、以什么保证级别，可以把 LLM 子调用当作 pure 来跳过？*
（例如：显式声明 + 受控 temperature=0 + 版本化模型指纹 + 失败回退到重跑。）
这是一个**真问题**，而且我们的工程经验（哪些地方 fake model 能省、哪些不能）正好是素材。

## 检索词与命中数（arXiv API，`all:"..."`，relevance，max 5–8）

| 检索词 | 命中 |
|---|---:|
| `agentic computation graph` | 2 |
| `LLM agent memoization` | 1 |
| `partial re-execution agent LLM` | 36 |
| `event-sourced agent runtime` | 13 |
| `forkable agent LLM` | 2 |
| `reactive dataflow LLM` | 0 |
| `exactly-once agent` | 21 |
| `semantic caching LLM` | 56 |
| `workflow optimization large language models` | 25 |
| `LangGraph` | 131 |

注：检索命中数是相关性筛选后的数量，不等于全部相关文献；结论基于对最接近的
数篇摘要的精读（尤其 ActiveGraph / AgentTrace / 语义缓存系列）。
