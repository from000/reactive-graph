# 全功能 e2e 验证设计（E2E Full-Coverage Design）

日期：2026-09-17
状态：approved（用户指令"所有支持的功能都需要 e2e 验证一遍，不能遗漏，使用superpowers技能"，goal mode 自主推进）

## 目标

对仓库**当前公开支持的功能面**逐一做端到端（e2e）验证——不是参数穷举，
而是每个功能一条**完整真实调用路径**（构建 → 执行 → 断言结果/副作用）。
交付**可追踪的功能矩阵**（哪些已验证、哪些环境受限），保证"不遗漏"可核验。

## 功能面清单（权威来源：docs/api-reference.md + 两包 `__all__` + RGP/1 METHOD_LIST）

### 域 A：reactivegraph 引擎（Python API → Driver 真子进程）
| # | 功能 | e2e 路径 |
|---|---|---|
| A1 | GraphBuilder（task/computed/on/scope/build、reads/writes/retry/scope 上 wire） | host 建图，断言编译后 trace/state |
| A2 | invoke + 选择性执行（computed 失效 / pure 指纹跳过 / effect receipt 幂等） | 同输入重跑不重算；改输入重算；effect 不重放 |
| A3 | 事务状态 + 写冲突（TrackedStateProxy 读集/patches；并行批冲突抛错） | 并行两任务写同 path → 冲突错误 |
| A4 | stream（值流 / 自定义流 / token 级流） | stream 帧事件序列断言 |
| A5 | interrupt / resume（HITL） | 中断 → 用户响应 → 恢复继续 |
| A6 | 持久化执行（durable log + checkpoint + 恢复，sqlite） | 重启 host 后从 checkpoint 恢复状态 |
| A7 | restore_thread（time travel / fork） | 回滚历史 checkpoint 继续 |
| A8 | 因果 trace / export_dot / get_state | trace 事件序列 + DOT 导出含节点 |
| A9 | 函数式 API（functask/entrypoint/build_function_graph） | 函数式图 invoke 输出断言 |
| A10 | 预置 agent（ToolSpec/ToolNode/ReactiveAgent/create_react_agent + token 流） | 工具调用 → 多轮 → token 事件 |
| A11 | 异步 API（ainvoke/astream） | asyncio 跑通 + 事件循环不阻塞 |
| A12 | DriverHost 生命周期 / 协议版本握手 | start/handshake/close、协议版本匹配 |
| A13 | scope 子状态 | scope 任务写 state[scope]，不串键 |
| A14 | checkpoint/store 原生入口（get_state/list_checkpoints/get_checkpoint/store_*） | store_put/get 跨 run 持久读 |

### 域 B：Driver 运行时与存储（RGP/1 面，经 host）
| # | 功能 | e2e 路径 |
|---|---|---|
| B1 | RUN / RESUME / GET_STATE / RELEASE_GRAPH / SHUTDOWN | host.run 结果 + 释放 + 干净退出 |
| B2 | STORE_OP（LongTermStore：Memory/Sqlite） | store_put/get 重启存活（sqlite） |
| B3 | CHECKPOINT_OP（get/list/put/delete_thread） | checkpoint 生命周期 |
| B4 | VECTOR_UPSERT / SEARCH（Memory/Sqlite + cosine） | upsert → search k=1 命中（跨重启 sqlite） |
| B5 | 持久化后端选择（REACTIVEGRAPH_DB） | sqlite 全后端接通；PG/Redis 标记 unverified |
| B6 | 并发线程隔离（thread_id） | 多线程各自状态不串（realworld 已验，归入矩阵） |
| B7 | EXPORT_DOT | DOT 输出断言 |

### 域 C：reactivechain 链组件（Python 纯组件 + 挂 Driver 关键路径）
| # | 功能 | e2e 路径 |
|---|---|---|
| C1 | runnable（Pipeline/RunnableLambda/RunnablePassthrough/GeneratorRunnable） | 流水线执行 + 流式 yield |
| C2 | combiner（Parallel/Branch/Fallback/Assign） | 并行合并 / 分支路由 / fallback 降级 / 赋值 |
| C3 | llm（FakeLLM/OpenAICompatChatModel 消息面） | FakeLLM 全链路；OpenAI 标记 unverified |
| C4 | embeddings（Hash/OpenAICompat） | Hash 向量距离断言 |
| C5 | messages / messages_to_api | 消息构造 + API 载荷转换 |
| C6 | prompt（PromptTemplate/ChatPromptTemplate/FewShot/Pipeline/Placeholder） | 渲染输出断言 |
| C7 | parsers（9 个） | 各 parser 解析断言 |
| C8 | retriever（VectorStoreRetriever/BM25/Ensemble/MultiQuery） | 检索 top-k 断言 |
| C9 | vectorstore（InMemory/DriverVectorStore） | 相似度检索（DriverVectorStore 走 host） |
| C10 | documents + splitters + loaders | 加载 → 切分 → 文档字段 |
| C11 | memory（MemoryStore + 7 memory 类） | 消息累积/窗口/摘要/实体 |
| C12 | tools（tool/StructuredTool/Toolkit/内置 5 工具/ToolNodeAdapter） | 工具调用结果 + schema |
| C13 | agents（AgentExecutor/create_react_agent/create_tool_calling_agent） | 多轮工具循环 |
| C14 | openapi（from_openapi） | 从 spec 建工具并调用 |
| C15 | callback/observability（CallbackManager/ChainStats/SegmentStat） | 统计计数断言 |
| C16 | 链 → Driver 图集成 | 链任务挂 ReactiveGraph 执行（test_to_graph_driver 路径） |

### 域 D：跨层端到端场景（真·全栈）
| # | 功能 | e2e 路径 |
|---|---|---|
| D1 | RAG 链 → Driver host | retriever|prompt|llm 全链经 host（realworld 复用） |
| D2 | ReAct agent（工具 + 记忆）在 Driver | 多轮工具循环 + memory 累积 |
| D3 | 会话中断 → resume → 继续工具链 | 中断恢复后状态延续 |
| D4 | 重启恢复完整会话 | host 重启 → 会话状态保留 |
| D5 | 并发多线程会话 | 4 线程隔离（realworld 复用） |
| D6 | 选择性执行收益 | cache 命中跳过（realworld 复用） |

## 方法

- **e2e 定义**：完整调用路径（非 mock 内核）：Python API →（链组件）→ Driver host 真子进程 → 断言。
  纯 Python 组件（parsers/prompt/memory 等）无 Driver 参与，其完整路径即组件调用链。
- **外部依赖策略**：全部离线可跑（FakeLLM/HashEmbeddings/sqlite/memory）。OpenAI 真实 API、
  Postgres/Redis 后端、TLS/TCP 网关标记 `unverified`（环境受限，与 realworld 先例一致）。
- **"不遗漏"保证**：`benchmarks/e2e/FEATURE_MATRIX.md` 是唯一事实源——每个功能一行，
  状态 = `verified | unverified(原因) | skip(原因)`；e2e 测试文件内按矩阵编号组织测试。
- **执行流程**：写 e2e 测试（先行）→ 跑 → 暴露缺陷则修复（TDD 反向：测试暴露框架 bug
  即修复，如 realworld Task 5 修复 3 缺陷）→ 每域 commit。
- **验证**：e2e 套件全绿 + 链 190 / 引擎 122 / TS 178 无回归 + ruff 干净。

## 产物

- `benchmarks/e2e/FEATURE_MATRIX.md`（功能矩阵，唯一事实源）
- `benchmarks/e2e/test_e2e_engine.py` / `test_e2e_driver.py` / `test_e2e_chain.py` /
  `test_e2e_scenarios.py`（按域分组）
- `benchmarks/e2e/README.md`（运行说明 + 报告）
- 设计/计划文档（本文件 + implementation 计划）

## 交付后收尾

- 全量回归（链/引擎/TS/ruff/realworld）
- git commit（每域一个）+ 收尾说明（unverified 项清单与原因）
