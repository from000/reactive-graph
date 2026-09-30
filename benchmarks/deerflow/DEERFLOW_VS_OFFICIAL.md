# DeerFlow 移植 vs 官方 bytedance/deer-flow — 系统对比

> 对照基线:`github.com/bytedance/deer-flow` @ HEAD `f9f3127`(2026-09 克隆,2956 文件,
> 不进仓库)。本对比只描述**已核实事实**(源码目录/依赖/模块级),逐项可复现。
> 配套:DEERFLOW_MATRIX.md(六链路+差异化逐项测试)、SELECTIVE_GAIN_REPORT.md(执行模型量化)。

## 0. 定位声明（先说清楚，不混淆）

我们做的是**能力验证型移植**：验证 ReactiveGraph/ReactiveChain 能否支撑
super-agent-harness 级别的核心链路——**不是**完整克隆官方产品。官方是
生产级多通道产品(后端 693 个 Python 文件 + Next.js 前端 + 部署栈);我们
是 6 个移植模块 + 26 项 e2e + 浏览器自动化 + 量化报告。对等比较的是
**核心运行链路**与**执行模型**,不是产品面。

## 1. 规模对比（官方 vs 移植）

| 维度 | 官方 deer-flow v2 | 我们（benchmarks/deerflow） |
|---|---|---|
| 仓库规模 | 2956 文件(含前端/部署/docs) | 10 文件(6 模块 + 测试 + 脚本 + 报告) |
| Python 源码 | harness 571 + backend/app 122 ≈ 693 文件 | deerflow_port 6 文件 ≈ 300 行 |
| 依赖 | langgraph 1.2.x 全家桶 + langchain ≥1.3 + 6 模型适配器 + langgraph-api/cli/runtime | reactivegraph + reactivechain(零新增) |
| 前端 | Next.js + shadcn + playwright 配置(官方自带浏览器测试) | examples/deerflow 零依赖工作台(浏览器自动化 5 项) |
| 测试 | 官方 395 个测试文件引用 langchain/langgraph | e2e 26 项 + examples 25 项,全真不 mock |

## 2. 架构对比

| 层 | 官方 | 我们 |
|---|---|---|
| 图执行 | langgraph Pregel(**每次 invoke 全量重跑**),langgraph.json 平台拓扑(graphs/auth/http/checkpointer) | 编译期依赖图 + 事件路由 + pure 指纹跳过(跨 run);RGP/1 协议 + Node Driver 真子进程;fallback 纯 Python 内核 |
| 模型抽象 | `models/factory.py`(claude/mindie/openai_codex/patched_* 8+ provider) | OpenAICompatChatModel + AGNES 网关(FakeLLM 离线面) |
| 持久化 | langgraph-checkpoint-sqlite/postgres + 自定义 checkpointer | sqlite/memory durable event log + checkpoint + LongTermStore |
| 工具 | 12 内置工具 + MCP client(session_pool/oauth) | @tool/StructuredTool/Toolkit + from_openapi(MCP 式) |
| 多通道 | channels(dingtalk/discord/feishu/github/buzz-nostr…) | 无(未移植) |
| 前端 | Next.js 完整产品 UI | 零依赖单页工作台(能力实验室) |

## 3. 核心链路功能面对比（六链路逐项）

| # | 官方实现 | 我们等价 | 状态 |
|---|---|---|---|
| 1 lead_agent 编排 | `create_agent` + 12 middleware(记忆/循环检测/子代理限额/摘要/标题/todo/token 用量/工具错误/澄清/终答/视图/安全)+ 12 内置工具 | GraphBuilder 声明式图 + 事件路由(planner→executor→verifier)+ 确认 gate | verified(3 测试) |
| 2 sub-agents | `subagents/`(registry/executor/capacity,bash_agent/general_purpose) | 事件订阅并行派发 + 容量截断(dispatcher/sub0..n) | verified(3 测试) |
| 3 长期记忆 | `persistence/` + memory_middleware 注入上下文 | load_mem/answer/save_mem 闭环(Driver LongTermStore,线程级隔离) | verified(3 测试) |
| 4 tools + MCP | 12 内置工具 + MCP client(真实协议/session_pool/oauth) | 内置 @tool + from_openapi 发现的 MCP 式工具 | verified(3 测试;**MCP 真实协议未移植**) |
| 5 线程持久化 | persistence/engine + checkpointer + store | checkpoint 落盘 + 重启恢复 + durable 回放 + 断点继续 | verified(3 测试) |
| 6 HITL | runtime + clarification_tool(多次询问) | interrupt/resume 确认 gate(单段/多段循环) | verified(3 测试) |

## 4. 执行模型差异（核心主张，量化见 SELECTIVE_GAIN_REPORT.md）

| | 官方 langgraph | 我们 |
|---|---|---|
| 同输入重跑 | 每次 invoke 全量重跑全部节点 | 跨 run pure 指纹跳过(100% skip 实测) |
| 1000 节点/10 受影响 | 10,574.87 ms | 7.14 ms(≈1480×) |
| 1000 节点 fan-out 改 1 字段 | 5,490.82 ms | 6.86 ms(≈800×) |
| 全依赖链(no-gain) | 4,522.63 ms | 259.95 ms(调度开销仍低 ≈17×) |

## 5. 我们独有（官方无等价物,7 项差异化全部 e2e 断言）

跨 run 选择性执行 / 事务状态写冲突策略(reducer/priority/串行)/ computed 声明式缓存
(read-set 失效)/ scope 子状态命名空间 / 因果 trace + EXPORT_DOT / durable event log
回放(重启精确恢复)/ RGP/1 协议 + Driver 真子进程(引擎跨语言执行)。

## 6. 官方有而我们未覆盖（诚实边界清单）

| 面 | 官方 | 我们 | 说明 |
|---|---|---|---|
| 多通道接入 | channels(dingtalk/discord/feishu/github/buzz-nostr…) | — | 未移植;通道语义 = 外部消息→run 映射,可在我们上实现 |
| 完整产品前端 | Next.js + shadcn | 零依赖工作台 | 工作台覆盖能力面,非产品 UI |
| 扩展技能 | extensions 加载/隔离/安全扫描 | — | 未移植(链路 4 的 from_openapi 是其最小形态) |
| 真实 MCP 协议 | MCP client(session_pool/oauth/user_scoped_auth) | from_openapi 模拟 | 未移植真实 MCP wire |
| authz/沙箱 | authz(principal/provider)+ sandbox(e2b) | — | 未移植 |
| 调度/Webhook | scheduler(scheduled_tasks)+ webhook_delivery | 无 | 未移植 |
| 模型 provider 面 | 8+ provider(含本地 vllm/mindie) | OpenAI 兼容 + AGNES 网关 | 覆盖面窄但模式等价 |

## 7. 结论

- **核心运行链路验证成立**:lead_agent 编排、sub-agents、长期记忆、tools/MCP 式接入、
  线程持久化、HITL 六条链路在我们的引擎上有真实等价实现并通过 e2e(26 项)。
- **执行模型是代际差异**:官方全量重跑 vs 我们选择性执行,同输入场景任务执行
  150→3(实测),1000 节点选择性场景 ≈1480× 时延差(真实 langgraph 1.2.11 对照)。
- **验证发现的框架缺陷已修复**:F1 list 切片 / F2 工具代理参数 / F3 代理 repr /
  F4 多段 HITL,全部修复并回归(见 DEERFLOW_MATRIX.md)。
- **边界诚实**:多通道/完整前端/扩展技能/真实 MCP/authz/沙箱/调度 未移植——它们
  是产品面而非引擎能力面;若需验证,逐项可在我们之上实现(与六链路同法)。

## 8. 复现

```bash
# 移植套件(六链路+差异化+smoke)
uv run --directory python/reactivechain pytest $PWD/benchmarks/deerflow/ -q
# 执行模型量化
uv run --directory python/reactivechain python $PWD/benchmarks/deerflow/run_selective_gain.py
# 官方源码(对照基线,hash 固定)
git clone https://github.com/bytedance/deer-flow.git && git checkout f9f3127
```
