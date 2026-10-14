# ReactiveGraph Docs

> Rendered site configuration is maintained in root `mkdocs.yml`; CI builds it
> with `mkdocs build --strict`.

面向**开源级项目**的文档索引。引擎定位:反应式图执行内核,选择性执行、事务、
幂等、持久化、可观测内建。

## 快速开始

- [README](../README.md) — 定位与两分钟上手
- [原生 API 规范](spec/native-api.md) — `GraphBuilder`/`task`/`computed`/`scope`/`invoke`
- [示例](../examples/deerflow/README.md) — 全能力端到端验证

## 引擎规范(spec/)

| 文档 | 内容 |
|---|---|
| [native-api.md](spec/native-api.md) | 原生 API 与调度语义 |
| [state-and-transactions.md](spec/state-and-transactions.md) | 反应式状态与事务 |
| [streams-and-interrupts.md](spec/streams-and-interrupts.md) | 流式与 human-in-the-loop |
| [durable-execution.md](spec/durable-execution.md) | 持久化事件日志与恢复 |
| [rgp-1.md](spec/rgp-1.md) | RGP/1 线协议 |
| [remote-api.md](spec/remote-api.md) | gateway / 租户 / 远程执行 |
| [observability-and-security.md](spec/observability-and-security.md) | trace / OTel / 权限 / 脱敏 |
| [native-api-competitive.md](spec/native-api-competitive.md) | 与 langgraph 的执行模型对比 |

## 工程与基准

- [toolchain.md](toolchain.md) — Node/pnpm/uv 工具链
- [benchmarks.md](benchmarks.md) — 选择性更新与差分基准
- [plans/roadmap.md](plans/roadmap.md) — 里程碑与剩余待办

## Migration and production

| 文档 | 内容 |
|---|---|
| [migration-guide.md](migration-guide.md) | LangGraph/LangChain 迁移与 importer |
| [agent-cookbook.md](agent-cookbook.md) | Native ReAct / tool / state / HITL cookbook |
| [example-gallery.md](example-gallery.md) | 可执行示例入口与验证命令 |
| [production-deployment.md](production-deployment.md) | Docker / health / security / observability |
| [competitive-proof.md](competitive-proof.md) | 可复现 competitive proof 与诚实边界 |
| [commercial-readiness.md](commercial-readiness.md) | 商业评估结论、已验证能力与边界 |
| [faq.md](faq.md) | 常见问题：LangChain 边界、性能口径、替换范围 |

## API reference

- [api-reference.md](api-reference.md) — 公开 API 一览(TS + Python)

## 教程(tutorials)

- [tutorials/README.md](tutorials/README.md) — 从"为什么反应式"到 1000 节点应用,
  每篇配可运行代码(CI 自动验证)

## 贡献与安全

见根目录 [CONTRIBUTING.md](../CONTRIBUTING.md)、[SECURITY.md](../SECURITY.md)。
