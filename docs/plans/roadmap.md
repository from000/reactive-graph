# ReactiveGraph Roadmap — Beyond LangGraph

> 定位：不是 langgraph 的兼容实现，而是以**反应式执行内核**为根、在正确性与
> 体验上超越它的开源级框架。所有差距来自 2026-09 的两份子代理盘点
> （能力利用率地图 + 与 langgraph 差距矩阵）。

## 核心判断

引擎层（`packages/driver`）能力已经完整：interrupt、subgraph、checkpoint
（memory+sqlite）、stream mux、事务日志、effect receipt 均已在 driver 内实现。
**真正的差距在三层**：

1. **用户 API 暴露面** — stream / interrupt / resume / Command / store /
   checkpoint 没有进入 Python/JS 原生 API；
2. **死代码接线** — conflicts 冲突检测、permissions、otel、scope 消费、
   sqlite 日志 compact 均无运行时调用者；
3. **生态层** — prebuilt agent、多后端 checkpointer、Store 语义检索、
   可观测性平台缺失。

## 按用户价值排序的未接线能力

| # | 能力 | 现状（代码证据） | 影响 |
|---|---|---|---|
| 1 | 写冲突检测 | `conflicts.ts` 全仓无调用者；scheduler.ts:10 注释承诺未兑现 | 正确性承诺落空 |
| 2 | Python stream/resume | `host.py` 丢弃 STREAM_EVENT；`ReactiveGraph` 无 stream/resume | 循环/HITL 不可用 |
| 3 | Python effect 幂等 | `graph.py` 硬编码 `external_receipts: []` | 幂等 gate 永不命中 |
| 4 | checkpoint/store SDK 入口 | CHECKPOINT_OP/STORE_OP 仅协议级 | 持久化不可编程访问 |
| 5 | sqlite 日志 compact | `compact/latestSnapshot` 无调用者 | 日志无限增长 |
| 6 | scope/permissions/trace/otel 消费 | 均只导出+测试，零运行时消费 | 安全/可观测承诺落空 |
| 7 | gateway 生产传输 | 仅测试用 stub handler | 无法远程部署 |

## 里程碑

### M1 正确性接线（第一批）
- F1: 冲突检测接入 `Scheduler.runAll`（并发批）
- F2: Python `ReactiveGraph.stream()/resume()` + host 转发 STREAM_EVENT
- F3: Python effect receipt（任务返回 `(update, receipt)`）

### M2 持久化与可观测
- F4: checkpoint/store 的 Python/JS 原生入口
- F5: sqlite 日志 compact 接线（snapshot + truncate）
- F6: scope 消费 + permissions 接入调度提交点 + trace 导出

### M3 生态层
- F7: prebuilt agent（ToolNode / create_react_agent 原生实现）
- F8: 多后端 checkpointer（Postgres / Redis）
- F9: gateway 生产传输（TCP/stdio + 租户认证）

## 执行纪律

* 每个功能独立 commit（完成后立即提交）；
* 每个功能带 failing-then-passing 测试；
* 不引入对 langgraph 的兼容承诺——API 以本框架语义为准。

## 进度（2026-09 审查后）

M1 ✅ / M2 ✅ / M3 ✅（F1–F9 全部完成，各带测试独立提交）。三轮子代理审查
（架构 / 安全 / 开源就绪）的 blocking 与高价值项已修复：认证原型链污染、
TCP 握手 DoS/崩溃、DriverLink 假实现、权限段匹配、sdk-js stream 泄漏、
policies 接线、依赖审计进 CI、issue/PR 模板与 CHANGELOG。

### 剩余可选待办（非阻塞）

- 发布自动化：npm/PyPI 发布流程已就绪——4 个 npm 包已解除 `private` 并配
  `files: ["dist"]` 与 `publishConfig.access: public`，发布顺序按依赖
  （protocol → devtools-protocol → driver → sdk-js），Python 侧 `uv build`
  产出 sdist+wheel（含 README）；**仅剩在仓库 Settings 配置
  `NPM_TOKEN` / `PYPI_TOKEN` 两个 secrets，打 `v0.1.0` tag 即可发布**。

✅ 已完成：docs 站点索引 + API reference + 覆盖率上 CI——
`docs/README.md` 索引、`docs/api-reference.md` 公开 API 一览、
`scripts/check-docs.py` 链接检查进 CI；vitest coverage 阈值
（lines≥75/funcs≥80/branches≥70）+ pytest `--cov-fail-under=80` 进 CI
coverage job（当前 driver 79/86/81、python 86%）。

✅ 已完成：TCP 传输 TLS 选项——`RgpTcpServer` 支持 `tls`
server 选项、`connectTcp` 支持 CA 校验；自签名证书端到端测试通过。

✅ 已完成：gateway 租户隔离——`DriverLink` 支持每租户
factory，认证后每个 tenant 的图注册表/线程状态/checkpoint/store 完全
分区（同名 graphId/threadId 互不可见）。

✅ 已完成：otel.ts `SpanEmitter` 接入调度器——任务执行
（每 attempt 一个 task span，失败 setError）、retry（仅实际重试时）、
computed 重算与 cache_hit 均发 span；未配置 emitter 零开销。

## Beyond LangGraph gap plan — 2026-09-23

The roadmap in
[`2026-09-23-beyond-langgraph-gap-plan.md`](2026-09-23-beyond-langgraph-gap-plan.md)
is implemented in stages on the `beyond-langgraph-p4` branch. P4, P5 and P7 are
engineering-complete; P6 and P8 still have the open items called out below.

- **P4 Explainability closure** ✅ — conflict explanation, durable historical
  explain, causal-trace JSON/DOT export, token/USD savings estimates.
- **P5 Agent orchestration parity+** ✅ — bounded parallel tool calls, typed
  versioned agent state, interrupt/human transaction state, run-level
  permissions/budgets, and the MCP adapter.
- **P6 Production hardening** 🟡 — token rotation, tenant concurrency,
  malformed/oversized frame tests, OTLP JSON export, bounded span retention,
  run/task/tool p50/p95/p99 metrics, 1,000-thread and 10,000-node scale smoke,
  write-pressure compaction, health check, graceful shutdown, and deploy
  artifacts are implemented; atomic token rotation, concurrent auth-race and
  slow-consumer backpressure tests, and bounded slow-consumer eviction are
  implemented; restart-storm recovery and memory-ceiling/leak tests remain
  open.
- **P7 Ecosystem and migration** ✅ — read-only LangGraph importer, LangChain
  tool/message adapters, OpenAPI/MCP auth semantics, migration guide, agent
  cookbook, and production deployment guide.
- **P8 Release/community readiness** 🟡 (engineering-complete; external
  release not done) — all Python
  distributions build; the clean-install smoke test builds and installs all
  Python wheels and npm tarballs in fresh environments and is covered by the
  CI matrix; TypeScript workspace builds; mkdocs site builds in strict mode
  and CI; benchmark matrix artifacts are published by the manual
  workflow; contribution/security policy and reproducible issue templates
  exist; the executable example gallery is documented. Public release
  additionally requires the external repository settings `NPM_TOKEN` and
  `PYPI_TOKEN`, an npm `@reactivegraph` organization, `v0.1.0` tag, and five
  external pilot users/teams. These are external-state requirements tracked in
  `RELEASE_CHECKLIST.md` and audited in `plan-completion-audit.md`; they are
  deliberately not marked complete until the artifacts and pilots are publicly
  verified.
