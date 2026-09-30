# DeerFlow v2 能力验证型移植 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
> 依据设计:docs/plans/2026-09-18-deerflow-port-design.md(已批准,commit 3c4fbc3)

**Goal:** 在 `benchmarks/deerflow/` 用 reactivechain/reactivegraph 重写 DeerFlow v2 的六条核心运行链路,逐条 e2e 对照 + DEERFLOW_MATRIX 唯一事实源,并展示我们独有的差异化能力。

**Architecture:** 移植实现(`deerflow_port/` 六模块)全部走真实 DriverHost(Node Driver,RGP/1)或链挂 Driver 路径,不 mock;对照基线是由 `$DEERFLOW_UPSTREAM_ROOT` 指定的未入库 checkout (HEAD `f9f3127`)。每链路 = 一个测试文件矩阵行 + 一个差异化断言。

**Tech Stack:** Python 3.12 / reactivegraph(引擎,43 功能点全绿)/ reactivechain(链组件)/ pytest / DriverHost / uv。TS 侧不改(Driver 已就绪)。

---

## 前置:环境与验证惯例(本仓库纪律,每个 Task 必须遵守)

- 验证命令:
  - 移植套件:`uv run --directory python/reactivechain pytest "$PWD/benchmarks/deerflow/" -q`
  - 链全量:`uv run --directory python/reactivechain --extra test python -m pytest -q`(当前 190)
  - Driver 全量:`cd packages/driver && pnpm test`(当前 179)
  - ruff(绝对路径,uv 切 cwd 会 E902):`ruff check benchmarks/deerflow/ --fix` → `ruff check benchmarks/deerflow/`
- bash 纪律:build/test 分条命令跑;`| tail` 管道不产生 verification receipt;read→edit 需跨 provider round,edit 前必须重读目标文件。
- 真实 LLM 地域封锁:测试一律用 FakeLLM 或 AGNES 网关模式(C3 同款),不依赖外网。
- fixture 参照:`benchmarks/e2e/conftest.py` 的 `durable_host`(DriverHost 生命周期)。

## 参照(e2e 已验证的能力面,直接复用其调用形态)

| 链路 | 复用 API | 参照测试 |
|---|---|---|
| 1 lead_agent 编排 | `ReactiveGraph.build` / `b.task(kind="effect", on=..., reads=, writes=)` / `graph.invoke` / `graph.stream` | benchmarks/e2e/test_e2e_engine.py::test_a1/a2 |
| 2 sub-agents | `Parallel`/`Branch` 组合器 + 嵌套 `invoke`(子图) | benchmarks/e2e/test_e2e_chain.py::test_c2 |
| 3 长期记忆 | `MemoryStore` 7 类 + Driver `store_put`/`store_get` | test_e2e_chain.py::test_c11、test_e2e_engine.py::test_a14 |
| 4 tools + MCP | `@tool`/`StructuredTool`/`Toolkit` + `from_openapi`(模拟 MCP 工具发现) | test_e2e_chain.py::test_c12/c14 |
| 5 线程持久化 | `thread_id` + checkpoint/恢复 + durable 回放 | test_e2e_engine.py::test_a6、test_e2e_scenarios.py::test_d4 |
| 6 interrupt/resume | `interrupt`/`resume` 会话恢复 | test_e2e_engine.py::test_a5、test_e2e_scenarios.py::test_d3 |

---

### Task 1: 骨架 + DEERFLOW_MATRIX.md 初版

**Files:**
- Create: `benchmarks/deerflow/__init__.py`(空)
- Create: `benchmarks/deerflow/deerflow_port/__init__.py`(空)
- Create: `benchmarks/deerflow/tests/conftest.py`(复制 benchmarks/e2e/conftest.py 的 `durable_host` fixture 形态)
- Create: `benchmarks/deerflow/DEERFLOW_MATRIX.md`

**Step 1:** 写矩阵初版——六条链路行(链路/对应 deer-flow 模块/我们的等价组件/测试文件/状态=pending),头部记录对照基线 `bytedance/deer-flow @ f9f3127`。
**Step 2:** 建目录 + conftest,跑 `uv run --directory python/reactivechain pytest "$PWD/benchmarks/deerflow/" -q` → 空套件 PASS(no tests collected 可接受,或加一个 smoke 测试断言 DriverHost 可起)。
**Step 3:** commit:`chore(bench): deerflow 移植骨架 + DEERFLOW_MATRIX 初版`

### Task 2: 链路 1 — lead_agent 多 agent 编排

**Files:**
- Create: `benchmarks/deerflow/deerflow_port/lead_agent.py`
- Test: `benchmarks/deerflow/tests/test_matrix_link1_lead_agent.py`

**对照事实(deer-flow):** `agents/lead_agent/agent.py` 用 `langchain.agents.create_agent` + 12+ middleware(记忆/循环检测/子代理限额/摘要/标题/todo/工具错误处理)+ 12 内置工具;`models/factory.py` 抽象模型。

**Step 1: 写失败测试**——构造"研究问题路由"场景:输入 `{"query": ...}`,图有三个 effect 任务(planner/executor/verifier)+ 一个条件路由(on 事件驱动),断言:
- `graph.invoke` 返回最终 state 含全部阶段产物;
- `graph.stream` 事件帧序列覆盖三阶段(done 帧有序);
- 同输入二次 invoke 命中跨 run skip(`calls` 计数不增长——选择性执行,链路 1 即展示差异化)。

**Step 2:** 跑红(module not found)。
**Step 3: 实现**——`lead_agent.py`:`build_lead_agent()` 返回 `ReactiveGraph`;阶段任务带 `reads/writes` 声明;模型调用走 FakeLLM(可注入);任务内可调用工具函数。
**Step 4:** 跑绿。
**Step 5:** commit:`feat(bench): 链路1 lead_agent 编排 e2e`

### Task 3: 链路 2 — sub-agents

**Files:**
- Create: `benchmarks/deerflow/deerflow_port/subagents.py`
- Test: `benchmarks/deerflow/tests/test_matrix_link2_subagents.py`

**对照事实(deer-flow):** `subagents/executor.py` + `registry.py`(内置 `bash_agent`、`general_purpose`),`capacity.py` 控制并发。

**Step 1: 写失败测试**——lead_agent 内并行派发 N 个子任务(sub-agent 各自是独立小图/纯函数),断言:
- `Parallel` 组合器并发执行、结果聚合到主 state;
- 子 agent 数量 = 配置限额内的全部完成(容量控制);
- 子任务执行顺序与并发行为可观测(stream 帧含各子任务 done)。
**Step 2-4:** 红 → 实现(`subagents.py` 用 `Parallel` + 子图 invoke)→ 绿。
**Step 5:** commit:`feat(bench): 链路2 sub-agents 并行执行 e2e`

### Task 4: 链路 3 — 长期记忆

**Files:**
- Create: `benchmarks/deerflow/deerflow_port/memory.py`
- Test: `benchmarks/deerflow/tests/test_matrix_link3_memory.py`

**对照事实(deer-flow):** `persistence/` + `agents/middlewares/memory_middleware.py`——记忆注入 agent 上下文;`runtime/store` 长期存储。

**Step 1: 写失败测试**——跨 run 记忆:run1 存事实到 Driver store → run2(同 thread)agent 上下文检索到该事实并用于回答;断言:
- `MemoryStore` 写入 Driver store 持久(A14 模式);
- 新 run 通过记忆检索命中历史事实(断言输出引用记忆内容);
- 记忆按 thread 隔离(不同 thread 互不可见,B6 模式)。
**Step 2-4:** 红 → 实现(`memory.py`:MemoryStore + store_* 读写 + 上下文注入)→ 绿。
**Step 5:** commit:`feat(bench): 链路3 长期记忆跨run e2e`

### Task 5: 链路 4 — tools + MCP

**Files:**
- Create: `benchmarks/deerflow/deerflow_port/tools_mcp.py`
- Test: `benchmarks/deerflow/tests/test_matrix_link4_tools_mcp.py`

**对照事实(deer-flow):** `tools/builtins/` 12 工具 + `mcp/`(client/session_pool/task_tool_caller——MCP 服务器经 task 工具暴露)。

**Step 1: 写失败测试**——agent 用工具集完成任务,其中一部分工具来自 `from_openapi`(本地 mock OpenAPI server 模拟"外部 MCP 服务发现"):
- `@tool`/`StructuredTool` 注册 + ToolNode 执行;
- MCP 式工具经 OpenAPI 发现并调用成功(C14 模式);
- 工具结果回填 agent 上下文并影响最终输出。
**Step 2-4:** 红 → 实现(`tools_mcp.py`)→ 绿。
**Step 5:** commit:`feat(bench): 链路4 tools+MCP e2e`

### Task 6: 链路 5 — 线程持久化

**Files:**
- Create: `benchmarks/deerflow/deerflow_port/persistence.py`
- Test: `benchmarks/deerflow/tests/test_matrix_link5_persistence.py`

**对照事实(deer-flow):** `persistence/engine.py` + `runtime/checkpointer` + `runtime/store`。

**Step 1: 写失败测试**——run 中断(模拟中途退出)→ 新 DriverHost 重启 → 同 thread 恢复完整继续(D4 模式);断言:
- checkpoint 落盘(重启后线程存在);
- 恢复 run 从断点继续而非重头(A6);
- durable 回放:事件日志可精确重放(trace 帧一致)。
**Step 2-4:** 红 → 实现 → 绿。
**Step 5:** commit:`feat(bench): 链路5 线程持久化+回放 e2e`

### Task 7: 链路 6 — interrupt / resume

**Files:**
- Create: `benchmarks/deerflow/deerflow_port/hitl.py`
- Test: `benchmarks/deerflow/tests/test_matrix_link6_hitl.py`

**对照事实(deer-flow):** `runtime/` HITL(clarification_tool 即 ask user)。

**Step 1: 写失败测试**——工具需要用户确认:`interrupt` 挂起 → resume 提供澄清输入 → 继续工具链并完成(D3 模式);断言:
- interrupt 事件携带待确认项;
- resume 后从挂起点继续(后续工具调用完成);
- 会话可多次 interrupt/resume。
**Step 2-4:** 红 → 实现 → 绿。
**Step 5:** commit:`feat(bench): 链路6 interrupt/resume e2e`

### Task 8: 差异化能力测试

**Files:**
- Create: `benchmarks/deerflow/tests/test_differentiators.py`

**Step 1: 写失败测试**——7 项差异化,每项断言"我们做到、langgraph 做不到/不这样"的量化行为:
1. 跨 run 选择性执行:同输入二次 run 纯任务 `calls` 不增长(D6 模式);
2. 事务写冲突:并发写冲突触发明确策略(reducer/priority/串行)而非静默合并(A3);
3. computed 缓存:read-set 不变则重算调用不触发(A1,host 走 fallback 注明);
4. scope 隔离:子状态命名空间互不污染(A13);
5. 因果 trace:`export_dot`/trace 帧含 start/done/skip 决策(A8/B7);
6. durable 回放:重启后事件日志精确重放(D4);
7. RGP/1 真子进程:断言 DriverHost 协议握手 + 引擎/链跨语言执行(A12/B1)。
**Step 2:** 跑红 → 实现(复用 e2e 已验证能力组装断言)→ 绿。
**Step 3:** commit:`feat(bench): 差异化能力7项 e2e`

### Task 9: 矩阵点亮 + README + 回归收尾

**Files:**
- Modify: `benchmarks/deerflow/DEERFLOW_MATRIX.md`(pending → verified,注明 unverified 及原因)
- Modify: `benchmarks/README.md`(登记新套件)
- Modify: `benchmarks/e2e/README.md` 或顶层 README 视需要

**Step 1:** 矩阵逐行点亮,汇总段记录:e2e 暴露的框架边界(如有)。
**Step 2:** ruff 清理:`ruff check benchmarks/deerflow/ --fix` → 全清。
**Step 3:** 全量回归:移植套件 + 链 190 + `pnpm test` 179,全绿。
**Step 4:** commit:`feat(bench): deerflow 移植完成——六链路 + 差异化 7 项全绿,矩阵 verified`

---

## 验收标准

- `uv run --directory python/reactivechain pytest "$PWD/benchmarks/deerflow/" -q` 全绿(矩阵行 + 差异化);
- 链全量 190 不回归;Driver `pnpm test` 179 不回归;
- `ruff check benchmarks/deerflow/` 干净;
- DEERFLOW_MATRIX.md 每行 verified/unverified 有据,唯一事实源;
- 工作区干净,commit 历史清晰(每 Task 一 commit)。
