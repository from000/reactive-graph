# ReactiveChain 强化实现计划 — A 段级多 task 图 / B async 面 / C provider 扩展

日期：2026-09-15 · 状态：✅ 已完成（A/B/C 全部实现 + 全量验证：reactivechain 164 passed、
reactivegraph 92 passed、覆盖率 89%、ruff 全绿（含 tests 存量清理）；commit 见
`fix/feat: reactivechain 强化`）
流程：superpowers（brainstorming 已完成=需求选定；实现按 TDD：先写测试后实现）。

## 背景与诚实边界

- ReactiveChain 当前 15 模块 3,567 行；对照 langchain-core 1.6.2（181 文件 70,271 行）
  已产出逐组件差异表（`docs/reactivechain-langchain-comparison.md`）。
- 用户关切："reactive-graph 比 langgraph 更强，reactivechain 应能实现更强的功能"——
  本计划把引擎优势（选择性/状态引擎）带入组件层，并补 async 面与 provider。
- **用户方向（2026-09-15 补充）**："我们做出来的不一样——不要完全参考 langchain
  的功能，应该尽可能发挥我们最大的能力，可以远超 langchain"。执行口径：
  对照表只用于**如实记录差异**，不作为功能对齐清单；本计划的验收标准是
  "ReactiveChain 独有的能力面跑通"（段级图调度、pure 选择性、状态持久化、
  可观测），而非"langchain 有什么我们补什么"。langchain 的 async 方法集
  （B）与 provider 面（C）仅作 API 便利性补齐，标注为"便利性对齐"而非核心价值。

## A：Pipeline 段级多 task 图映射（最有价值一步）

### 设计事实（源码实测）
- `GraphBuilder.task(id, kind="pure"|"effect"|"opaque", fn, on, reads, writes)`；
  reads 为**运行时收集**（TrackedStateProxy 记录实际读路径），writes 为声明
  （test_native_graph.py:128 "reads are runtime-collected, not declared"）。
- Python in-process fallback（host=None，graph.py:281-293）现状：多任务同事件以
  **同一 payload** 为输入并行合并（`state.update(update)` 各自独立）。现测试
  均为单任务/单事件路由，**无多任务同事件链式断言** → 可安全改为链式。
- 注意：接真实 Driver（JS，事件并行语义）时，同事件多任务并行、无任务间依赖边；
  链式只保证于 Python in-process fallback。差异如实标注。

### 改动
1. `python/reactivegraph/reactivegraph/graph.py` `_execute` fallback：
   `state = dict(payload or {})`，每任务 `model = TrackedStateProxy(state)`（累积
   state 传给下一个），输出合并回 state → **链式累积**（与 Pipeline 直觉一致）。
2. `python/reactivechain/reactivechain/runnable.py` `to_graph()`：
   逐段 `b.task(seg.id, kind=..., fn=seg.invoke, on=("run",),
   writes=tuple(sorted(seg.writes)))`；`kind` 由新参数 `pure_segments:
   set[str] | None = None` 指定（None=全 effect，保守默认——有副作用段绝不可
   标 pure）；`reads` 不声明（运行时收集）。
3. 测试：段级图输出链式累积（`{"x": ..., "y": ...}`）；pure 段 wire spec 的
   `kind=="pure"`；Legacy `test_to_graph_invokes_driver`（只断言 `y==12`）不破。

## B：async 面（对齐 langchain 方法集）

- `Runnable` 基类新增 `ainvoke/astream/abatch`：`asyncio.to_thread` 包装同步实现
  （不阻塞事件循环）。
- `Pipeline` 覆写 `ainvoke`（复用段级缓存）：`_cache` 加 `threading.Lock`
  线程安全；`abatch` 保持顺序语义。
- `GeneratorRunnable.astream`：异步生成器包装同步 `stream`。
- 测试：`asyncio.run` 下三方法输出与同步一致、并发 ainvoke 缓存不串。

## C：provider 扩展（Anthropic + 本地模型）

- `llm.py` 新增 `AnthropicCompatChatModel(BaseLanguageModel)`：纯 stdlib HTTP +
  SSE（与 OpenAICompat 同架构，host.py 风格直连、无系统代理）。
  - `POST {base_url}/v1/messages`；头 `x-api-key` + `anthropic-version: 2023-06-01`。
  - 请求体：`model/max_tokens/messages`；`system` 消息拆出为顶层 `system` 字段。
  - SSE 解析：`message_start / content_block_delta(text_delta) / message_stop`；
    `message_delta` 累积输出 token；4xx 抛 ReactiveChainError 不重试、429/5xx 重试
    （与 llm.py 现有 `_request` 语义一致）。
  - invoke 返回 `{"output", "llm_message"}`（既有契约）。
  - 测试：本地 `ThreadingHTTPServer` 模拟 Anthropic SSE（key 校验、system 拆分、
    多块拼接、4xx 不重试）。
- 本地模型（Ollama 等）：提供 OpenAI 兼容端点 → **复用**
  `OpenAICompatChatModel`，docstring/README 注明（不新增类）。

## 顺序与验证

1. A：TDD（先写 fallback 链式测试 → 改 graph.py → to_graph 段级 + 测试）
2. B：TDD（async 三方法测试 → 实现）
3. C：TDD（Anthropic mock 服务测试 → 实现）
4. 全量验证：`pytest`（reactivechain + reactivegraph 两包）+ ruff + 覆盖率
5. 文档更新：api-reference/docs 补 async 面与 Anthropic；对照表补"已强化"标记

## 风险

- A 改动 reactivegraph fallback 语义：若 Driver 语义与链式冲突，文档已标注差异；
  reactivegraph 现测试全绿为兜底。
- B 的缓存线程安全：锁粒度为整个 invoke（简单正确，牺牲并发）。
- C 的 Anthropic SSE 仅覆盖非流式 + 流式最小路径（测试用 mock）。

## 核心 vs 堆叠边界（用户 2026-09-15 确认：核心自研、堆叠拿来）

**原则**：真正要实现的核心不多——Runnable/Pipeline 状态契约、段级选择性、
组合/可观测、Memory 原子语义；langchain 大量代码是行业规范（OpenAI/
Anthropic/JSON Schema/SSE）与海量具体组件的实现（工具库/加载器/provider），
属"堆叠功能"，**不重复自研，直接拿来**。

| 层 | 归属 | 策略 | 现状 |
|---|---|---|---|
| 状态契约（reads/writes/指纹/段级跳过） | 核心 | 自研 | ✅ 已实现（含引擎下沉） |
| Runnable 家族 / 组合 / 回调 / Memory | 核心 | 自研 | ✅ 已实现 |
| 消息/提示/解析器 | 半核心 | 按行业规范实现（不 copy langchain） | ✅ 已实现 |
| OpenAI/Anthropic provider | 适配 | 按公开协议实现 | ✅ 已实现 |
| 生态工具（requests/arxiv/search/...） | 堆叠 | 适配层拿来（用户 2026-09-15 决策：**弃 langchain 适配，改接 MCP/OpenAPI 通用规范**——生态独立，不依赖 langchain） | ⏳ 待实现（MCP/OpenAPI 适配器） |
| 文档加载器 / 向量后端 / embedding 服务 | 堆叠 | 适配层拿来（后续扩展点） | ⏳ 扩展点 |
| 内置 calculator/time/date/terminal | 堆叠 | 保留轻量内置（示例/离线），复杂能力走适配 | ✅ 已有 |