# ReactiveChain × ReactiveGraph 统一运行时（方案 C）设计

> 状态：**已实施完成**（2026-09-16）——决策：R1=仅 pure 段跳过（C）、
> D1=stream 归一化放 P1、P0 引擎改动独立评审后合入。P0→P2 全部分期
> TDD 实施完毕（含引擎 Driver 任务级联升级 e356f8b 作为 P2 前置）。
> 目标读者：架构决策人。本文钉死"引擎为唯一运行时"的接口边界、
> 分阶段落地与迁移清单。

## 1. 背景与动机

ReactiveChain（3,974 行链框架）与 ReactiveGraph（反应式引擎）目前是
**两个自包含执行器**：

- 链默认路径 `Pipeline.invoke` = 进程内 for 循环 + 自建段级缓存 +
  自建回调事件系统——**完全不经过引擎**；
- 引擎只在显式 `to_graph()` 时参与，且仅用上事件基础（`on=("run",)`）
  与写声明；`pure_segments` 的指纹跳过**仅在接 Driver 时生效**
  （in-process fallback 未实现 pure skip，见 §4）。

引擎能力利用矩阵（逐项核对 `reactivegraph/graph.py`、`host.py`）：

| 引擎能力 | 现状利用 |
|---|---|
| 图编译（build DSL） | ✅ `to_graph` 每段一 task |
| 事件路由（`on=`） | ⚠️ 单一 `"run"` 通道，链式累积 |
| 选择性（pure 指纹跳过） | ⚠️ 仅 Driver 生效；fallback 缺失 |
| 事务 / 写冲突检测 | ⚠️ 靠 writes 声明间接受益 |
| effect 幂等 | ⚠️ 同上（引擎侧有，链未利用） |
| 反应式增量（computed/订阅） | ❌ 未用 |
| 并行任务调度 | ❌ 未用（链语义串行） |
| 流式 / 中断（stream/resume） | ❌ `Pipeline.stream` 为进程内 generator |
| 持久化（checkpoint/store/恢复） | ❌ 全内存态 |
| 可观测（CausalTrace/otel） | ❌ 链自建事件系统，未接引擎 trace |

**判断**：引擎差异化能力在默认路径零利用；当前"引擎 = 可选导出目标"，
非"引擎 = 运行时"。方案 C 将两者焊成单一运行时。

**关键事实（已源码验证）**：
- `ReactiveGraph.build(build_fn, host=None)` 无 host 时 **in-process 可用**：
  `invoke(event, payload)` 顺序执行路由任务、链式累积 state 并返回全量 state；
  `stream` 无 host 时 yield 单个 values 事件（best-effort）。
- **依赖方向单向**：`reactivegraph/prebuilt.py` 仅依赖 stdlib + 引擎内部，
  **不 import reactivechain** → `reactivechain → reactivegraph` 无环，
  链可以安全依赖引擎公共面。
- 引擎调度开销微秒级，链每段为 LLM/IO（秒级）→ 开销可忽略（不构成反对理由）。

## 2. 目标与非目标

**目标**
1. 单一执行路径：链的默认 `invoke/stream` 即引擎调度，消灭双实现。
2. 引擎能力按阶段全打通：反应式路由（P1）、持久化/流式中断/trace（P2）。
3. 使用者 API 不变：`a | b | c`、`chain.invoke(state)`、`.stream()` 照旧；
   能力按"接入深度"递进（默认零配置 in-process，显式接 Driver 得增强）。
4. 保持零第三方依赖（reactivegraph 是自家依赖，已声明 `>=0.1.0`）。

**非目标**
- langchain API 兼容（既定方向，不回头）。
- Driver（进程外调度）成为默认——Driver 为**可插拔增强**，不是门槛。
- 本轮不动 reactivegraph 引擎核心语义（pure/effect/computed/幂等）；
  仅补齐 fallback 缺失的 pure skip（§4 缺口）。

## 3. 架构分层与接口边界

```
┌─ 声明层（reactivechain）───────────────────────────┐
│ Pipeline 段 / 组合器 / 模型 / 工具 / 解析器        │
│ 每段：reads / writes / invoke(state)→写键 dict      │
└───────────────┬───────────────────────────────────┘
                │ 编译（只做一次，缓存 GraphDef）
┌───────────────▼───────────────────────────────────┐
│ 编译层：Pipeline → GraphDef（to_graph 现有逻辑）   │
│ 每段一 task：id=seg_i、kind=pure|effect、          │
│ fn=seg.invoke、on、writes=seg.writes               │
└───────────────┬───────────────────────────────────┘
                │ 调度（invoke/stream/resume）
┌───────────────▼───────────────────────────────────┐
│ 执行层（reactivegraph）                            │
│ A. in-process（默认，零配置）：ReactiveGraph.build │
│    host=None → _execute 顺序路由、链式累积         │
│ B. Driver（增强）：compile/run_stream/checkpoint/  │
│    store/resume/CausalTrace                        │
└───────────────────────────────────────────────────┘
```

**接口契约（钉死）**：
- 编译层产物：`GraphDef`（含 tasks/routes）——`to_graph` 与 `invoke` 共用
  单一编译入口。
- reactivechain 只依赖引擎公共面：`ReactiveGraph.build`、`GraphBuilder.task/
  on/computed`、`graph.invoke/stream/resume/get_state`。不 import 引擎内部
  （`host.py`/`state.py` 私有符号不进入链代码）。
- reactivegraph **永不 import reactivechain**（prebuilt 的
  ReactiveAgent/ToolNode 以 duck-typing/协议接受链对象，维持现状）。
- 事件命名：P1 前保持 `"run"` 单一事件；P1 引入段写事件
  `f"{seg_i}:written"`（驱动反应式订阅），`"run"` 作为启动事件保留。

## 4. 执行器抽象

| 能力 | in-process（默认） | Driver（显式接入） |
|---|---|---|
| invoke | ✅ `_execute` 链式累积 | ✅ `host.run` 反应式调度 |
| stream | ✅ 单 values 事件（best-effort） | ✅ 真流式（STREAM_EVENT） |
| pure 指纹跳过 | ❌ **缺口**（fallback 未实现，docstring 却宣称 native） | ✅ |
| 写冲突 / 幂等 | ⚠️ 顺序执行天然无冲突 | ✅ 引擎级 |
| 反应式订阅 | ❌（P1 补 computed 求值） | ✅ |
| resume / checkpoint / store | ❌（需 host） | ✅ |
| trace | ❌ | ✅ CausalTrace |

**引擎侧唯一改动（P0 前置）**：`graph._execute` 的 in-process 分支补齐
pure 指纹跳过——`kind=="pure"` 的任务按 `(task_id, 输入指纹)` 缓存结果，
指纹相同跳过执行（对齐 docstring 承诺与 Driver 语义）。此改动独立可测，
是"选择性单一实现"的前提（否则 chain 缓存无法删除）。

接入方式（对使用者零认知成本）：
- 默认：`chain.invoke(state)` → in-process。
- 增强：`chain.invoke(state, graph_id=...)` 或构造 `ReactiveGraph(..., host=DriverHost())`
  → 持久化/并行/流式中断。同一链对象两种执行方式，语义差异文档化。

## 5. 分阶段设计

### P0 统一执行路径（语义等价迁移）

改动：
1. `Pipeline.invoke`：编译缓存 GraphDef → `graph.invoke("run", state)` →
   **提取末段写键返回**（引擎返回全 state；链 API 保持"返回写键 dict"）：
   `return {k: out[k] for k in self.segments[-1].writes if k in out}`。
2. 引擎 fallback 补 pure skip（§4 唯一引擎改动）。
3. 删除 chain 进程内缓存三件套（`_fingerprint`/`_cache`/`_cache_lock`）：
   段级跳过语义迁移到引擎 pure skip（fn 层无需再包缓存）。
   并发正确性：in-process fallback 顺序执行天然串行；`ainvoke` 经
   `asyncio.to_thread` 仍须互斥——fallback 的 `_execute` 加同一图级锁
   （引擎侧一个 `threading.Lock`，语义与现 `_cache_lock` 等价）。
4. 错误传播保持：段异常经 `ReactiveChainError`（或透传原异常）到调用方，
   与现状一致；事件/统计钩子（instrument_pipeline）**P0 不动**
   （继续在链 invoke 包装层工作，P2 合并进引擎 trace）。
5. `stream`：P0 保持现有 `Pipeline.stream` 实现（进程内 generator），
   不换引擎——引擎无 host stream 是单 values 事件，与链的逐段块语义
   不兼容，归一化放到 P1（见决策点 D1）。
6. `to_graph` 与 `invoke` 共用编译入口（`_compile()` 私有方法），
   `to_graph` 对外行为不变（返回可 invoke 的 `ReactiveGraph`）。

验证（P0 验收）：
- 现有 173 测试全绿（缓存/选择性/async/并发测试语义对齐后）。
- 新增：`test_invoke_runs_via_engine`（patch 断言 `ReactiveGraph.invoke`
  被调用）、`test_pure_skip_in_fallback`（引擎 fallback 指纹跳过）。
- 基准不劣化：`docs/benchmarks.md` 宽/选择性场景重跑。

### P1 反应式路由 + 真并行

改动：
1. 段间依赖改事件订阅：`b.on(f"{seg_{i-1}}:written", f"seg_{i}")`，
   任务按 `reads` 订阅上游写事件；`"run"` 触发首段。
2. `computed` 表达派生键（如消息组装/常量变换）：键变化自动重算
   （`ComputedDef.evaluate` 读路径哈希缓存，引擎已实现）。
3. Driver 下真并行：无依赖段同事件并行、写冲突检测生效、pure skip 生效。
   in-process fallback 保持顺序（文档标注差异，语义与现状一致）。
4. stream 归一化（决策点 D1 在此落地）：链 `stream` 映射引擎流式事件
   （无 host 时由 fallback 产出逐段 values 事件）。

验证：反应式测试（上游写键变化→下游自动重算，无需重发 run）、
并行基准（宽图场景，与现状链式对比）、写冲突测试。

### P2 能力递进（引擎差异化全面兑现）

1. 持久化：接 Driver 后 `checkpoint/restore_thread/get_state` 暴露；
   Memory 段持久化（store_get/store_put）。
2. 流式中断：`graph.stream` + `resume(run_id, human_response)`（human-in-the-loop）。
3. 可观测合并：chain 回调事件（on_llm_*/on_tool_*/on_retry）作为引擎
   CausalTrace 的轻量子集消费；`instrument_pipeline` 退化为引擎 trace 的
   Python 侧 facade（单一事件源）。
4. `interrupt` 协议：段可请求中断（引擎已有协议，链段暴露便捷 API）。

验证：中断-恢复端到端测试、trace 事件断言、memory 持久化测试。

## 6. 迁移清单（现状 → 目标，语义对照）

| # | 项 | 现状实现 | 目标 | 迁移动作 | 风险 |
|---|---|---|---|---|---|
| 1 | 段执行 | `for + _extract_input` | 引擎任务（fn=seg.invoke） | invoke 走图 | 低（顺序语义一致） |
| 2 | 选择性 | chain 缓存三件套 | 引擎 pure skip | 删 chain 缓存 + 引擎 fallback 补 skip | **中**（行为迁移核心） |
| 3 | 并发 | `_cache_lock` 锁整 invoke | 引擎图级锁（fallback）+ Driver 并行 | 锁迁移引擎侧 | 低 |
| 4 | 返回形状 | 末段写键 dict | 全 state → 提取末段写键 | invoke 包装提取 | 低（API 不变） |
| 5 | 回调 | chain emit 事件 | 引擎 trace 子集（P2） | P0 不动，P2 合并 | 低 |
| 6 | stream | 进程内 generator | 引擎流式事件（P1） | D1 决策后落地 | 中 |
| 7 | async | to_thread + 锁 | 同上（内部路径换） | 无 API 变化 | 低 |
| 8 | to_graph | 独立编译 | 单一编译入口共用 | `_compile()` 提取 | 低 |
| 9 | 缓存测试 | 断言执行次数 + 访问 `chain._cache` | 断言引擎 pure skip 语义 | 确定性段标 pure；`_cache` 断言改对象 | 中（R4） |

## 7. 风险与决策点

- **R1（P0 核心风险）**：缓存语义迁移。现状"同输入跳过段"由 chain 缓存
  实现（**对全部段生效，含 effect**——源码实测：`test_fingerprint_skip_skips_
  unchanged_input` 等 8 个测试断言"执行次数 == 1"、`test_async_parallel_
  invoke_cache_safe` 直接访问 `chain._cache`，所用段均为普通 lambda，未标
  pure）。迁移后引擎 pure skip 只对 `kind=="pure"` 段生效 → 三个出路：
  - **C（推荐）**：语义对齐引擎——确定性段显式声明 pure（编译时
    `pure_segments`/段属性），effect 段不再跳过；现有"跳过断言"测试把
    确定性段标记 pure。**语义更严谨**（有副作用的段本就不该被跳过），
    且删除 chain 缓存后无双实现残留。
  - B：effect 段 fn 内保留轻量缓存（过渡）——双实现残留，不推荐。
  - A：引擎 fallback 对全部段做指纹跳过——违背 pure/effect 语义，
    与 Driver 行为不一致，不推荐。
- **D1（stream 时机）**：P0 保持进程内 stream，还是 P0 即归一化引擎流式？
  建议 P0 保持（语义风险隔离），P1 落地。
- **R2**：引擎 fallback 补 pure skip 是引擎行为变更——独立 PR、独立测试，
  不混入链迁移。
- **R3**：P1 事件订阅改变 to_graph 的 in-process 输出形状（从链式累积
  变为订阅重算）——`graph.invoke("run", ...)` 返回结构保持全 state，兼容。
- **R4（测试适配）**：删除 chain 缓存后，直接访问 `chain._cache` 的测试
  （如 `test_async_parallel_invoke_cache_safe` 的 `len(chain._cache) == 1`）
  需改断言对象（引擎缓存或执行次数语义）——迁移清单第 9 项。

## 8. 验收与决策点收口

**已拍板（2026-09-15）**：
1. R1/D2 缓存语义：**选 C**——仅 pure 段跳过（确定性段显式声明 pure，
   effect 段不再跳过，语义对齐引擎）。P0 删除 chain 缓存三件套，现有
   "跳过断言"测试把确定性段标记 pure。理由：修掉"effect 段被跳过返回
   过期结果"的真实缺陷 + 单一选择性实现（in-process 与 Driver 一致）。
2. D1 stream 时机：**选 P1**——P0 保持进程内 stream（风险隔离，流式
   可见性依赖 P1 的事件模型，提前做会返工）。
3. P0 引擎改动（fallback pure skip）：**独立评审后合入**——分层责任 +
   可回滚（先合引擎 skip 再删 chain 缓存）+ 测试归属清晰。

**分阶段验收**：
- P0：173 测试全绿 + 新增执行路径断言 + 基准不劣化 + 引擎改动独立测试。
- P1：反应式/并行/写冲突测试 + 基准（并行 vs 链式）。
- P2：中断恢复端到端 + trace 断言 + memory 持久化。

## 附录 A：引擎 in-process 行为（源码实测，2026-09-15）

- `ReactiveGraph.build(build_fn, host=None)`：无 host 即 in-process。
- `invoke(event, payload)` → `_execute`：无 host 时 `state=dict(payload)`，
  按注册顺序跑路由任务，`td.fn(TrackedStateProxy(state))` → 解析
  `(update, receipt)` → `state.update(update)`，返回**全量 state**。
- `stream(event, payload)`：无 host 时 yield 单 values 事件（含全 state）。
- `resume`/`get_state`/checkpoint/store：均需 host（`_require_host`）。
- `TaskDef`：`kind ∈ {pure, effect, opaque}`、`on`、`reads`、`writes`、
  `timeout_ms`、`retry`、`scope`（wire spec 完整传递）。
- `ComputedDef`：selector + 读路径哈希缓存（反应式表达式基础，引擎已实现）。
- 现有 `to_graph` 测试：`graph.invoke("run", {"n": 5})` 返回
  `{"n":5,"x":6,"y":12}`（全 state，含中间段写键）。

## 附录 B：Driver 能力边界（2026-09-15 源码实测，P1-3 修订）

`packages/driver/src/runtime.ts`、`scheduler/scheduler.ts` 实测 + 引擎
升级（commit e356f8b）后的状态：

1. **任务级联（已升级，P2 前置落地）**：`runtime.run`/`resume` 改为任务
   队列执行——每任务完成后 `routeFor(\`${tid}:written\`)` 入队下游
   （广度优先、queued 去重终止环）。订阅链图在 Driver 与 fallback 事件
   传播语义一致。
2. **任务输入 = run 载荷 merge 线程 store 累积**（store 打底、载荷优先）：
   链式管道读上游写正常；run/resume 载荷并入 store（补缺失键，state 含
   输入键，对齐 fallback 与 langgraph state 语义）。多段链 Driver 输出
   与 fallback 一致（test_driver_scheduling、test_to_graph_driver）。
3. **无持久化时选择性不跨 run**：Scheduler 每次 run 重建（pure 指纹 /
   effect 幂等 receipts 不持久）→ Driver 客户端的"每次 run 全执行"。
   选择性（跨 invoke 指纹跳过）由 fallback 图级缓存承担（P0-1）。
4. **computed 为显式求值**：`Scheduler.compute(id)`（selector + readsHash
   缓存）需显式调用，运行时无自动触发；fallback 在 invoke 末尾自动惰性
   求值（P1-2）。
5. **并行/写冲突**：`Scheduler.runAll`（并发批次 + WriteConflictError）
   存在，但 `runtime.run` 走串行任务队列，**不触发**（后续调度升级项）。

**P2 兑现（本 commit 后，见 §5）**：
- **P2-1 checkpoint/恢复（链侧）**：`Pipeline.to_graph(host=DriverHost)`
  一行接入 Driver（REACTIVEGRAPH_DB 持久化 checkpoint）；返回图可直接
  `get_state`/`list_checkpoints`/`restore_thread` 回滚继续。
- **P2-2 流式中断 resume**：任务抛 `reactivegraph.Interrupt`（别名
  `GraphInterrupt`，host 错误 type 透传 "Interrupt"，Driver 端
  `toTaskResult` 识别）→ run 返回 interrupted 并落 checkpoint
  （`graph._last_run["runId"]` 取 run_id）→ `graph.resume(run_id,
  {"user_response": ...})` 载荷并入 store 后中断任务与下游级联继续。
- **P2-3 Memory 持久化**：`store_put`/`store_get`/`store_search`/
  `store_delete`/`list_namespaces` 经 host STORE_OP 落到长期 store
  （跨 run/跨图同 DB 可读）。可观测合并（链回调 ↔ 引擎 span 事件）
  待 Driver 链式 + OTel spanEmitter 就绪后按 §5 继续。
