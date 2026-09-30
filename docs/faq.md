# ReactiveGraph FAQ

## 你们说要替换 LangGraph，为什么代码里还能看到 LangChain？

因为它们解决的是不同问题。

ReactiveGraph 替换的是 **orchestration runtime**：agent 图执行、依赖追踪、
任务跳过、状态事务、恢复、trace、调度和 checkpoint。DeerFlow 后端的执行
底座现在由 `reactivegraph.create_agent.AgentGraph` 驱动，生产 agent 路径
在封杀 `langgraph` 的导入探测中可以通过。

`langchain_core` 保留的是 **行业兼容协议**：`BaseMessage` / `HumanMessage` /
`AIMessage` / `ToolMessage`、`BaseTool`、模型调用与工具 schema。它们类似
OpenAI-compatible API 的消息协议，让现有模型、工具和观测生态可以继续工作。

一句话：

> 我们替换的是引擎，不是把消息和工具生态私有化。

## 为什么要替换 LangGraph？

LangGraph 的核心调度是 graph node / superstep / channel reducer；ReactiveGraph
的核心是状态变化驱动的依赖图。对于确定的输入，ReactiveGraph 可以精确知道
哪些任务读到了变化、哪些 pure 任务输入未变、哪些 effect 已有 receipt，并
只执行必要工作。

这不是“所有场景都更快”。如果所有输入都变化、所有任务都必须执行，收益
会来自执行开销和 trace/checkpoint 实现而不是跳过；我们会如实报告 no-gain
场景。

## 为什么还有 host interop 代码？

真实迁移不能只改 import。LangGraph / LangChain 内部有 `isinstance`、
`issubclass`、异常和 channel 身份判断。ReactiveGraph 提供兼容层是为了让
真实宿主系统可以逐步迁移，同时保持 fail-closed，不会静默降级。

## DeerFlow 是完整产品替换吗？

后端执行底座是完整替换；边界仍然如实列出：

- `langchain_core` 消息/工具/模型协议保留
- ReactiveGraph 原生实现 SQLite / PostgreSQL checkpoint+store，并有真实
  SQLite 跨进程恢复和真实 PostgreSQL 16 项测试证据；
- Driver 原生实现 RedisCache（3 项真实 Redis 测试）；DeerFlow fork 另实现了
  Redis Streams bridge（7 项真实 Redis 7.2 测试）。它们不是 Python
  checkpoint/store provider；
- 但"所有生产 provider 的完整语义等价"仍未逐条证明，frontend、
  channels、MCP/sandbox 的生产规模与故障负载也仍待专项验证

## 你们是否宣称超越 LangGraph / LangChain？

我们不宣称生态、社区、集成数量或公开生产部署规模超越。我们宣称的是：

- 真实 DeerFlow 后端全量测试通过
- 真实 factory 输出一致
- 本机真实双跑 7-8 倍延迟优势
- SQLite checkpoint/store 跨进程恢复通过
- 原生 trace / invalidation / selective execution 提供更强的可解释性

## 可以复现吗？

可以。见 `benchmarks/deerflow_real/REPORT.md` 和 `docs/commercial-readiness.md`。
