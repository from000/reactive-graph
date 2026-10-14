# API Reference

公开 API 一览(实现为准,按语言分组)。`Task N` 编号已随兼容层移除而取消,
这里只列出当前公开面。

## TypeScript(`packages/driver` / `sdk-js` / `protocol` / `devtools-protocol`)

### 图构建与执行(driver + sdk-js)

| API | 位置 | 说明 |
|---|---|---|
| `GraphBuilder.task/computed/on/scope/build` | `driver/src/graph/model.ts` | 声明式构建;`scope` 让任务读写落在 `state[scope]` |
| `Graph.routeFor(task/…)` | 同上 | 事件 → 任务路由 |
| `Scheduler.runTask/runAll/emit/compute/getTrace` | `driver/src/scheduler/scheduler.ts` | 反应式调度;并发批含写冲突检测;`spanEmitter`/`permissionPolicy`/`policies` 可注入 |
| `ReactiveStore.get/raw/version/commitVersion` | `driver/src/state/store.ts` | Vue-reactivity 包装的原子版本状态 |
| `Transaction.set/read/commit/rollback` | `driver/src/state/transaction.ts` | 读写集 + patches 原子提交 |
| `sdk-js.ReactiveGraph.build/trace` | `sdk-js/src/graph.ts` | 原生 JS 图句柄 + 因果 trace 导出 |
| `sdk-js.invoke(graph, event, input)` | 同上 | 单事件便捷执行 |
| `sdk-js.RemoteClient`(compileGraph/run/stream/resume/cancel) | `sdk-js/src/remote.ts` | 远程 RGP/1 客户端 |

### 运行时与网关(driver)

| API | 位置 | 说明 |
|---|---|---|
| `DriverRuntime.compileGraph/run/resume/getState/checkpointOp/storeOp` | `driver/src/runtime.ts` | 唯一执行体;`REACTIVEGRAPH_DB/PG_DSN/REDIS_URL` 选择持久化后端 |
| `RgpGateway.accept/onRequest/onEvent/authenticate` | `driver/src/gateway/transport.ts` | 认证 + 会话托管(`Object.hasOwn` 防原型链污染) |
| `DriverLink(gateway, handlersOrFactory)` | `driver/src/gateway/link.ts` | 共享或**每租户隔离**的 handler 集 |
| `RgpTcpServer` + `connectTcp`(含 TLS) | `driver/src/gateway/tcp.ts` | TCP 传输(可选 TLS) |
| `startStdioGateway` | `driver/src/gateway/stdio.ts` | stdio 传输(pinnedToken 支持) |

### 存储与安全(driver)

| API | 位置 | 说明 |
|---|---|---|
| `CheckpointSaver/LongTermStore/Cache/Serializer` | `driver/src/storage/types.ts` | 后端接口;`put` 支持 `expectedParentCheckpointId` 乐观并发(防多 Driver 丢更新,冲突抛 `CheckpointConflictError`) |
| `Memory* / Sqlite* / Postgres* / RedisCache` | `driver/src/storage/` | 四后端实现(Postgres/Redis 需服务) |
| `MemoryVectorStore / SqliteVectorStore` + `cosineSimilarity` | `driver/src/storage/vector.ts` | 向量检索:upsert/search/delete,cosine 线性扫描(无 ANN 索引);RGP/1 `VECTOR_UPSERT`/`VECTOR_SEARCH` + `host.vector_upsert/search` 跨语言可用 |
| `host.run(config={"recursionLimit": N})` / `host.export_dot` | `python/reactivegraph/reactivegraph/host.py` | recursion limit 与 dot 导出的 Python 绑定(RGP/1 `EXPORT_DOT`、RUN config) |
| `EncryptedSerializer`(`kdf: "scrypt"` 默认) | `driver/src/storage/serializer.ts` | AES-256-GCM + scrypt 派生 |
| `allowPaths/isPathAllowed/validatePatches` | `driver/src/permissions.ts` | 字段级权限(提交点强制) |
| `BudgetPolicy/RateLimitPolicy/CircuitBreakerPolicy/PriorityPolicy/checkAll` | `driver/src/policies.ts` | 任务策略(调度准入) |
| `SpanEmitter.start/emit/exportSpans/spanMetrics` | `driver/src/otel.ts` | OTel 形状 span(调度器接入)、有界保留窗口与 p50/p95/p99 指标 |
| `normalizeCausalTrace` | `driver/src/trace.ts` | 因果 trace 归一化(脱敏) |
| `RgpGateway.metrics` | `driver/src/gateway/transport.ts` | session/backpressure 运维计数(阻塞、排队字节、慢消费者驱逐) |

### 协议(protocol)

`encodeFrame/decodeValue/FrameDecoder/makeId`、`METHOD_LIST`(17 个 method)、
`Envelope` 校验 —— `packages/protocol/src/*`。

## Python(`reactivegraph`)

| API | 位置 | 说明 |
|---|---|---|
| `GraphBuilder.task(scope=…)/computed/on/scope/build` | `reactivegraph/graph.py` | 构建;`reads/writes/retry/scope` 声明上 wire |
| `ReactiveGraph.invoke/stream/resume` | 同上 | 执行/流式/HITL(走 bundled Driver) |
| `ReactiveGraph.ainvoke/astream` | 同上 | **异步面**:executor 线程跑同步内核,不阻塞 event loop |
| `ReactiveGraph.explain_run/run_id/explain_conflict/export_trace/cost_saved` | 同上 | 历史执行解释、写冲突解释、causal-trace v1 JSON/DOT 导出、token/usd 估算节省 |
| `ReactiveGraph.get_state/list_checkpoints/get_checkpoint/store_*` | 同上 | checkpoint 与长期 store 原生入口 |
| `ReactiveGraph.restore_thread(thread_id, checkpoint_id)` | 同上 | **time travel / fork**:回滚到历史 checkpoint 并以新事务继续 |
| `TrackedStateProxy` | `reactivegraph/state.py` | 读集/变更 patches 跟踪(跨语言一致) |
| `DriverHost.start/handshake/run/run_stream/compile_graph/checkpoint_op/store_op/close` | `reactivegraph/host.py` | Driver 子进程托管(RGP/1) |
| `ToolSpec/ToolNode/ReactiveAgent/create_react_agent` | `reactivegraph/prebuilt.py` | 原生 ReAct agent;`agent.stream()` 支持 **LLM token 级流**(messages 事件,逐 token) |
| `functask/entrypoint/build_function_graph` | `reactivegraph/functional.py` | 函数式工作流 |
| `MCPClient/MCPTool/from_mcp` | `reactivechain/mcp.py` | MCP JSON-RPC 工具发现/调用,映射 Reactive 契约 |

## 验证

各层测试命令见根 `README.md`「命令」与 `Makefile`;CI(覆盖率门禁、依赖审计、
docs 链接检查)见 `.github/workflows/ci.yml`。
