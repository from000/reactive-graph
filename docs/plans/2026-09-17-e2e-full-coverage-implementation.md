# 全功能 e2e 验证 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 对 43 个功能点逐一建立可追踪的 e2e 验证（FEATURE_MATRIX 唯一事实源），全绿 + 无回归。

**Architecture:** `benchmarks/e2e/` 新增按域分组的 pytest 套件：`test_e2e_engine.py`（域 A）、
`test_e2e_driver.py`（域 B）、`test_e2e_chain.py`（域 C）、`test_e2e_scenarios.py`（域 D）。
引擎/D 域走 DriverHost 真子进程（跨语言 e2e）；链纯 Python 组件走组件调用链；C16 链挂 Driver 图。
外部依赖（OpenAI/PG/Redis/TCP/TLS）标记 unverified/skip。每域：写测试 → 跑 → 缺陷则修复 → commit → 更新矩阵。

**Tech Stack:** Python 3.12 / pytest / uv；Node Driver（bundled）；sqlite。

**验证惯例（沿用 realworld）**：引擎 `uv run --directory python/reactivegraph python -m pytest -q`；
e2e/链 `uv run --directory python/reactivechain --extra test python -m pytest -q`（pytest 需 conftest.py
使目录进 sys.path；rootdir 外绝对路径）；ruff 用绝对路径；git commit 每域一个。

---
## Task 1: e2e 套件骨架 + FEATURE_MATRIX

**Files:**
- Create: `benchmarks/e2e/conftest.py`（sys.path 注入 reactivegraph/reactivechain + `e2e_host` fixture：DriverHost 起停 + tmp sqlite db）
- Create: `benchmarks/e2e/FEATURE_MATRIX.md`（43 行矩阵，全 `pending`）
- Create: `benchmarks/e2e/README.md`（运行说明）

**Step 1**: 写 conftest（host fixture：`DriverHost(env={"REACTIVEGRAPH_DB": tmp})` start/handshake/yield/close）。
**Step 2**: 跑 `pytest benchmarks/e2e/ -q`（空套件 exit 5 预期——先确认 conftest 可导入）。
**Step 3**: commit（docs+骨架）。

## Task 2: 域 A 引擎 e2e（A1–A14）

**Files:**
- Create: `benchmarks/e2e/test_e2e_engine.py`

**Tests**（每功能一条，走 host 真 Driver）：
- A1 `test_a1_graphbuilder_declarative`：task/on/computed/scope/reads/writes/retry 声明 → build → invoke 断言
- A2 `test_a2_selective_execution`：computed 读集变才重算；pure 同输入跳过（计数）；effect receipt 幂等（同事件不重放）
- A3 `test_a3_write_conflict`：并行批两任务写同 path → 冲突错误（host.run 带 config 或图级）
- A4 `test_a4_stream_frames`：stream 值流事件序列（values/task 事件）
- A5 `test_a5_interrupt_resume`：gate raise Interrupt → resume 恢复（复用 realworld 模式）
- A6 `test_a6_durable_recover`：sqlite checkpoint → 新 host 恢复线程状态
- A7 `test_a7_time_travel`：restore_thread(历史 checkpoint) → 继续执行（test_time_travel.py 路径）
- A8 `test_a8_trace_dot`：get_state + trace 事件 + export_dot 含节点名
- A9 `test_a9_functional`：functask/entrypoint/build_function_graph invoke 输出
- A10 `test_a10_react_agent`：ToolSpec/ToolNode/create_react_agent 多轮工具调用 + token 级 stream
- A11 `test_a11_async`：ainvoke/astream 在 asyncio.run 下输出一致 + 事件循环不阻塞
- A12 `test_a12_host_lifecycle`：DriverHost start/handshake/协议版本/close（DriverError 语义）
- A13 `test_a13_scope`：scope 任务写 state[scope] 不串键
- A14 `test_a14_store_native`：store_put/get 跨 run 同线程持久读

**Step 1**: 写全部 14 测试（引用 realworld/引擎测试既有模式，避免重复实现）。
**Step 2**: 跑 `uv run --directory python/reactivechain pytest "$PWD/benchmarks/e2e/test_e2e_engine.py" -q`。
  失败=框架缺陷→修复（TDD 反向）；全绿→矩阵 A1–A14 标 verified。
**Step 3**: ruff 绝对路径检查 → commit。

## Task 3: 域 B Driver 协议 e2e（B1–B7）

**Files:**
- Create: `benchmarks/e2e/test_e2e_driver.py`

**Tests**（全部经 host 协议面）：
- B1 `test_b1_run_release_shutdown`：RUN 结果 + RELEASE_GRAPH + SHUTDOWN 干净退出
- B2 `test_b2_store_op_persist`：STORE_OP put/get 重启存活（sqlite）
- B3 `test_b3_checkpoint_op`：checkpoint get/list/put/delete_thread 生命周期
- B4 `test_b4_vector_ops`：VECTOR_UPSERT/SEARCH（sqlite 跨重启 + memory 对照）
- B5 `test_b5_backend_select`：REACTIVEGRAPH_DB 选 sqlite；无 env 内存（PG/Redis unverified）
- B6 `test_b6_concurrent_threads`：4 线程隔离（复用 realworld 并发测试）
- B7 `test_b7_export_dot`：DOT 含任务节点

**Step 1-3**: 同上（写→跑→修→矩阵 B1–B7 verified→commit）。

## Task 4: 域 C 链组件 e2e（C1–C16）

**Files:**
- Create: `benchmarks/e2e/test_e2e_chain.py`

**Tests**：
- C1 runnable：Pipeline+RunnableLambda+Passthrough 执行；GeneratorRunnable yield 流
- C2 combiner：RunnableParallel 合并、RunnableBranch 路由、RunnableFallback 降级、RunnableAssign 注入
- C3 llm：FakeLLM 全链路（prompt→llm→parser）；OpenAICompatChatModel 构造 + 消息转换（不真调 API）
- C4 embeddings：HashEmbeddings 距离/维度
- C5 messages：四消息类型 + messages_to_api 载荷
- C6 prompt：PromptTemplate/ChatPromptTemplate/FewShot/PipelinePromptTemplate/MessagePlaceholder 渲染
- C7 parsers：9 个 parser 各解析断言
- C8 retriever：VectorStoreRetriever/BM25/Ensemble/MultiQuery top-k
- C9 vectorstore：InMemoryVectorStore 相似度 + DriverVectorStore（host）检索
- C10 documents：loaders（Text/CSV/JSON/Url/Directory）+ splitters（Recursive/Character/Token/Separator）
- C11 memory：MemoryStore + 7 memory 类累积/窗口/摘要/实体
- C12 tools：@tool/StructuredTool/Toolkit/calculator/current_date/current_time/web_search（mock）/ToolNodeAdapter
- C13 agents：AgentExecutor/create_react_agent/create_tool_calling_agent 多轮循环
- C14 openapi：from_openapi 建工具 + 调用
- C15 callback：CallbackManager 统计 ChainStats/SegmentStat
- C16 chain-to-graph：链任务挂 ReactiveGraph（host）执行

**Step 1-3**: 同上。web_search/UrlLoader 网络面 mock 或标记 unverified。

## Task 5: 域 D 跨层场景 e2e（D1–D6）

**Files:**
- Create: `benchmarks/e2e/test_e2e_scenarios.py`

**Tests**：
- D1 RAG 链 → host（复用 realworld build_rag 路径简化）
- D2 ReAct agent（工具+记忆）在 Driver：多轮 + memory 累积
- D3 会话中断 → resume → 继续工具链（状态延续）
- D4 重启恢复完整会话（新 host 恢复）
- D5 并发多线程会话隔离（复用）
- D6 选择性执行收益（cache 命中跳过计数）

**Step 1-3**: 同上。

## Task 6: 矩阵报告 + 全量回归 + 收尾

**Files:**
- Modify: `benchmarks/e2e/FEATURE_MATRIX.md`（全 verified/unverified 终态）
- Modify: `benchmarks/e2e/README.md`（结果记录）
- Modify: `docs/plans/2026-09-17-e2e-full-coverage-design.md`（回填结果？不——矩阵为准）

**Step 1**: 跑全量：e2e 4 文件 + 链 190 + 引擎 122+1skip + TS 178 + realworld 10 + ruff 双包。
**Step 2**: 矩阵终态 + README 记录（unverified 项清单：OpenAI 真实 API、PG/Redis、TCP/TLS 网关）。
**Step 3**: commit → 收尾说明。

---
**验收标准**：43 功能点每项 `verified` 或 `unverified(环境原因)`；矩阵无 `pending`；
e2e 套件全绿；既有套件无回归；每 Task 独立 commit。
