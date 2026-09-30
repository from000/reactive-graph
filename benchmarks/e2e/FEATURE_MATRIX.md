# FEATURE_MATRIX — 全功能 e2e 验证矩阵（唯一事实源）

> 依据：docs/plans/2026-09-17-e2e-full-coverage-design.md（功能面权威来源：
> docs/api-reference.md + 两包 `__all__` + RGP/1 METHOD_LIST）。
> 状态：`verified`（e2e 通过）→ `unverified`（环境受限，注明原因）。
> 运行：`uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/ -q`（43 passed）。

## 域 A：reactivegraph 引擎（Python API → Driver 真子进程）

| # | 功能 | 文件:测试 | 状态 |
|---|---|---|---|
| A1 | GraphBuilder 声明式（task/on/computed/scope/reads/writes/retry） | test_e2e_engine.py::test_a1_graphbuilder_declarative | verified（computed 执行面走 fallback——host 不支持 computed 为设计边界：selector 是 Python 函数不可跨语言执行，声明也未上 wire） |
| A2 | invoke + 选择性执行（路由订阅/pure 指纹跳过） | test_e2e_engine.py::test_a2_selective_execution | verified（host 单 run 路由订阅 + **跨 run pure 指纹跳过**（d0183d4 起）；fallback 同） |
| A3 | 事务状态 + 写冲突 | test_e2e_engine.py::test_a3_write_conflict | verified |
| A4 | stream（值流/自定义流/token 流） | test_e2e_engine.py::test_a4_stream_frames | verified |
| A5 | interrupt / resume（HITL） | test_e2e_engine.py::test_a5_interrupt_resume | verified |
| A6 | 持久化执行（checkpoint + 恢复） | test_e2e_engine.py::test_a6_durable_recover | verified |
| A7 | restore_thread（time travel / fork） | test_e2e_engine.py::test_a7_time_travel | verified |
| A8 | 因果 trace / export_dot / get_state | test_e2e_engine.py::test_a8_trace_dot | verified |
| A9 | 函数式 API（functask/entrypoint/build_function_graph） | test_e2e_engine.py::test_a9_functional | verified |
| A10 | 预置 agent（ToolSpec/ToolNode/ReactiveAgent + token 流） | test_e2e_engine.py::test_a10_react_agent | verified |
| A11 | 异步 API（ainvoke/astream） | test_e2e_engine.py::test_a11_async | verified |
| A12 | DriverHost 生命周期 / 协议握手 | test_e2e_engine.py::test_a12_host_lifecycle | verified |
| A13 | scope 子状态 | test_e2e_engine.py::test_a13_scope | verified |
| A14 | checkpoint/store 原生入口（get_state/list_checkpoints/store_*） | test_e2e_engine.py::test_a14_store_native | verified |

## 域 B：Driver 运行时与存储（RGP/1 协议面，经 host）

| # | 功能 | 文件:测试 | 状态 |
|---|---|---|---|
| B1 | RUN / RELEASE_GRAPH / SHUTDOWN | test_e2e_driver.py::test_b1_run_release_shutdown | verified |
| B2 | STORE_OP（LongTermStore 持久） | test_e2e_driver.py::test_b2_store_op_persist | verified |
| B3 | CHECKPOINT_OP（get/list/delete_thread） | test_e2e_driver.py::test_b3_checkpoint_op | verified（put 的 values 需 Driver 内部 Buffer 序列化格式，Python 面不可直接构造——TS 内部细节） |
| B4 | VECTOR_UPSERT / SEARCH（sqlite 跨重启 + memory 对照） | test_e2e_driver.py::test_b4_vector_ops | verified |
| B5 | 持久化后端选择（REACTIVEGRAPH_DB） | test_e2e_driver.py::test_b5_backend_select | verified（sqlite/memory；Postgres/Redis 后端 unverified——需服务） |
| B6 | 并发线程隔离（thread_id） | test_e2e_driver.py::test_b6_concurrent_threads | verified |
| B7 | EXPORT_DOT | test_e2e_driver.py::test_b7_export_dot | verified |

## 域 C：reactivechain 链组件（纯 Python 组件 + 挂 Driver 路径）

| # | 功能 | 文件:测试 | 状态 |
|---|---|---|---|
| C1 | runnable（Pipeline/Lambda/Passthrough/GeneratorRunnable） | test_e2e_chain.py::test_c1_runnable | verified |
| C2 | combiner（Parallel/Branch/Fallback/Assign） | test_e2e_chain.py::test_c2_combiner | verified |
| C3 | llm（FakeLLM/OpenAICompatChatModel 消息面） | test_e2e_chain.py::test_c3_llm | verified（**真实 LLM 调用经 AGNES 网关验证**——invoke + stream 真调，无地域封锁；无 key 环境保持构造面） |
| C4 | embeddings（Hash/OpenAICompat） | test_e2e_chain.py::test_c4_embeddings | verified（HashEmbeddings 维度/距离语义；OpenAICompatEmbeddings 真实端点未单独验证——依赖网关 embeddings 支持） |
| C5 | messages / messages_to_api | test_e2e_chain.py::test_c5_messages | verified |
| C6 | prompt（5 类模板渲染） | test_e2e_chain.py::test_c6_prompt | verified |
| C7 | parsers（9 个） | test_e2e_chain.py::test_c7_parsers | verified |
| C8 | retriever（VectorStore/BM25/Ensemble/MultiQuery） | test_e2e_chain.py::test_c8_retriever | verified |
| C9 | vectorstore（InMemory/DriverVectorStore） | test_e2e_chain.py::test_c9_vectorstore | verified |
| C10 | documents（loaders + splitters） | test_e2e_chain.py::test_c10_documents | verified（UrlLoader/DirectoryLoader 未单测——loaders 代表性覆盖） |
| C11 | memory（MemoryStore + 7 类） | test_e2e_chain.py::test_c11_memory | verified |
| C12 | tools（@tool/StructuredTool/Toolkit/内置/Adapter） | test_e2e_chain.py::test_c12_tools | verified（web_search 网络面 unverified——需外网；current_time 未断言由 current_date 代表） |
| C13 | agents（AgentExecutor/双 create_react_agent） | test_e2e_chain.py::test_c13_agents | verified |
| C14 | openapi（from_openapi） | test_e2e_chain.py::test_c14_openapi | verified（本地 mock server） |
| C15 | callback（CallbackManager/ChainStats/SegmentStat） | test_e2e_chain.py::test_c15_callback | verified |
| C16 | 链 → Driver 图集成 | test_e2e_chain.py::test_c16_chain_to_graph | verified |

## 域 D：跨层端到端场景

| # | 功能 | 文件:测试 | 状态 |
|---|---|---|---|
| D1 | RAG 链 → Driver host | test_e2e_scenarios.py::test_d1_rag_chain_host | verified |
| D2 | ReAct agent（工具+记忆）在 Driver | test_e2e_scenarios.py::test_d2_react_agent_driver | verified |
| D3 | 会话中断 → resume → 继续工具链 | test_e2e_scenarios.py::test_d3_interrupt_resume_chain | verified |
| D4 | 重启恢复完整会话 | test_e2e_scenarios.py::test_d4_restart_recover | verified |
| D5 | 并发多线程会话隔离 | test_e2e_scenarios.py::test_d5_concurrent_threads | verified |
| D6 | 选择性执行收益（cache 命中跳过） | test_e2e_scenarios.py::test_d6_selective_gain | verified |

## 汇总

- **43/43 全部覆盖**（A 14 + B 7 + C 16 + D 6），e2e 套件 `43 passed`。
- **unverified（环境受限，非功能缺失）**：OpenAICompatEmbeddings 真实端点（C4——
  AGNES 网关 embeddings 支持未单独验证）、Postgres/Redis 持久化后端（B5）、
  web_search 外网面（C12）。
- **e2e 暴露的框架差异/缺陷**（详见 `benchmarks/e2e/README.md`）：
  1. ~~host/Driver 跨 run 无缓存~~ **已修复**（d0183d4：`SchedulerPersistent` 持久化 pure 指纹，跨 run 同输入跳过）；
  2. host 不支持 computed（设计边界）：computed 声明未上 wire，其 selector 是 Python
     函数不可跨语言序列化执行（fallback 内核支持）；
  3. `CHECKPOINT_OP put` 的 `values` 需 Driver 内部 Buffer 序列化格式（Python 面不可直接构造）。
