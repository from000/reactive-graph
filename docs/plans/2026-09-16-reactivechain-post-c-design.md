# ReactiveChain × ReactiveGraph 未来演进（方案 C 之后）设计

> 状态：**已实施完成**（2026-09-17）——决策记录：范围=四项全部
> （dec-5450db77add4d16a）、方案=①A ②A ③A ④B（dec-a06d477dda5a3be9）。
> 前置：方案 C（`2026-09-15-reactivechain-engine-runtime.md`）已实施完成
> （df9487c）。实施落地（TDD 分阶段）见
> `2026-09-16-reactivechain-post-c-implementation.md`；commit 引用：
> P3-1 流式归一化 9d27106..8430045、P3-2 并行/写冲突 8430045..224fc7f、
> P3-3 可观测合并 778cfc9/83628fe/4aff0d6、P3-4 checkpoint 多后端
> d2e2ada（附录 C 映射表见文末）。

## 1. 背景与动机

方案 C 落地后，"引擎为唯一运行时"的执行统一已完成（fallback 事件传播 +
Driver 任务级联、选择性跳过、computed、中断/恢复、store 持久化），但与
langgraph 的差距盘点（2026-09-16 评估）尚有四项硬缺口：

1. **流式未归一化**（D1 搁置项）：链 `stream` 仍是进程内 generator；
   fallback `graph.stream` 单 values 事件；Driver `run_stream` 独立协议
   流——三种形态互不同构。
2. **并行/写冲突路径未启用**：`Scheduler.runAll`（并发 worker +
   `WriteConflictError`）已实现，但 `runtime.run` 走串行任务队列
   （E2 升级），并发/冲突检测从不触发。
3. **可观测双体系未合并**：链回调（`on_chain_*`/ChainStats）与引擎
   spanEmitter（task/computed/cache_hit/retry）各成体系，无映射、无桥接。
4. **checkpoint 单后端**：持久化仅 `REACTIVEGRAPH_DB` 一个 sqlite 文件；
   driver `storage/` 已有 `memory.ts`/`postgres.ts` 抽象未接入选择。

本计划按依赖与收益优先级（①→②→③→④）逐项 TDD 落地。

## 2. 目标与非目标

**目标**：
- ① `chain.stream(state, mode="values"|"updates"|"messages")`——fallback 与
  Driver 产出**同构事件序列**（对齐 langgraph stream_mode）。
- ② `runtime.run` 按事件轮次分批并行（批内 `runAll`），声明写冲突抛
  `WriteConflictError`；保留串行开关（concurrency=1 = 现行为）。
- ③ 链回调 ↔ 引擎 span 语义映射表（附录 C）+ fallback trace 记录 +
  Driver 桥接（回调事件经协议 span 透传）。
- ④ `checkpoint_backend="sqlite"|"memory"|"postgres"` 切换（Driver env 透传，
  复用 storage 抽象）。

**非目标**：LangSmith 兼容、subgraphs、`Send`、递归限制、生态集成、
流式 token 级 chunking（messages 通道即可）。

## 3. ① D1 流式归一化

**现状（实测）**：
- 链 `Pipeline.stream`：进程内 generator（P0 决策 D1 保持），
  `on_chain_start/end/error` 经包装回调；
- fallback `graph.stream(event, payload)`：yield 单 values 事件（全 state）；
- Driver `host.run_stream(input, on_event)`：onEvent（values/messages/custom
  三类型，逐 chunk 回调）。

**方案对比**：
- **A（选定）**：统一 `chain.stream(state, mode=...)`——fallback 由事件
  传播循环产出段边界事件（start/end/error + 值快照），按 mode 过滤包装；
  Driver 走 `run_stream` 转发真实事件流；两种执行器事件序列同构。
- B：仅归一化事件形状、不接 Driver 流（半吊子）。C：维持现状。

**设计**：
- `stream(state, *, mode="values") -> Iterator[dict]`；mode 语义：
  - `values`：每次段提交后的全 state 快照（兼容现有关注点）；
  - `updates`：段写键增量字典（`{"seg_i": {"k": v}}` 形状，langgraph
    对齐）；
  - `messages`：流式消息块（Driver `custom`/`messages` chunk 透传）。
- fallback 实现：`_execute` 事件传播循环中，段执行前/后/异常分别产出
  start/end/error 事件 → stream 层按 mode 组装。
- Driver 实现：`graph.stream` 有 host 时转 `host.run_stream`（onEvent →
  mode 包装），多线程桥接（reader 线程 → 生成器）。
- 错误处理：段异常 → `on_chain_error` 事件 yield 然后抛（与 invoke
  一致）；监听器内异常向上传播。

**验收**：fallback/Driver 双执行器对同一链产出的事件序列（values 模式）
逐项一致；三模式过滤断言；段错误传播测试。

## 4. ② 并行/写冲突路径启用（runAll）

**现状（实测）**：`Scheduler.runAll(taskIds, inputs)`——按任务 id 排序 +
concurrency worker 池 + `committed` 写声明链，声明冲突/实际写冲突抛
`WriteConflictError`（永不重试）；`runtime.run`（E2 升级后）pending 队列
串行逐个 `runTask`。

**方案对比**：
- **A（选定）**：pending 队列按**事件轮次**分批——同一 `{tid}:written`
  派生的下游任务集为一批，批内 `await runAll(ids, inputs)`（concurrency>1
  → 冲突检测生效）；轮次间串行（保序/保 checkpoint）。
- B：全图拓扑并发（依赖追踪复杂、破坏轮次语义）。C：维持串行。

**设计**：
- `runtime.run`：`pending` 改为"事件轮次队列"——每处理完一批，收集该批
  任务发出的 `{tid}:written` 下游集为下一批；首批 = `routeFor(event)`。
- 批内输入构造沿用现有 merge（store 打底 + run 载荷优先）——同批任务
  输入相同（同事件载荷），无读依赖冲突，冲突只来自**声明/实际 writes**
  重名 → `WriteConflictError` 上抛（run 失败，该批无部分提交）。
- 串行开关：`concurrency=1` 时退化为逐任务（现行为）；默认并发
  `concurrency=8`（Scheduler 现值）。
- 幂等语义不变（effect receipts / pure 指纹在 runTask 内，批内并行下
  每个任务独立判定）。
- 错误处理：批内任一任务失败 → runAll 抛错 → run 中断（interrupt 判定
  在 runTask 抛错路径保持）。

**验收**：同轮两任务写同键 → `WriteConflictError`（含声明冲突与欠声明
实际冲突两类）；同轮无冲突图输出与串行一致（确定性）；基准（并发轮
延迟 < 串行）；`concurrency=1` 行为等价既有测试。

## 5. ③ 可观测合并（链回调 ↔ 引擎 trace）

**现状（实测）**：链回调独立体系（`ChainStats`/`_current_mgr`、
`on_chain_start/end/error`）；引擎 Driver `spanEmitter`（otel.ts：
`task`/`computed`/`computed:*:hit`/`cache_hit`/`retry` span）；fallback 无
span 记录。

**方案对比**：
- **A（选定，两步）**：P3-3a 映射表（附录 C）+ fallback `_execute` trace
  记录（事件列表，可导出）；P3-3b Driver 桥接（链回调钩子 → span 事件
  经协议 custom 通道透传）。
- B：只做文档映射（代码不动）。C：只做代码不做文档（语义漂移风险）。

**设计（P3-3a）**：
- 附录 C 映射表（链回调 ↔ span 语义）：`on_chain_start(seg)` ↔
  `task:{tid}:start`；`on_chain_end` ↔ `task:{tid}`（duration）；skip ↔
  `cache_hit`（fallback 指纹）/`skip`（Driver）；computed 命中 ↔
  `computed:{id}:hit`；错误 ↔ span error 状态。
- fallback：`_execute` 内按映射记录 `self._trace: list[dict]`（事件名 +
  载荷 + 时间戳），供外部读取（可观测钩子不阻断执行）。
**设计（P3-3b）**：Driver 桥接——`graph.stream`/invoke 的链回调事件经
`host.run_stream` custom 通道（或新 `TRACE` 方法）透传到 Python 侧 trace
消费者；链回调注册为 trace 消费端。

**验收**：映射断言（给定链执行，回调序列 ↔ 引擎 trace 事件一一对应）；
fallback trace 记录内容断言；Driver 模式 trace 事件透传测试；trace 失败
不影响执行（降级）。

## 6. ④ 多后端 checkpoint

**现状（实测）**：`DriverHost(env=...)` 经 `REACTIVEGRAPH_DB` 指定 sqlite
文件；`driver/src/storage/` 已有 `memory.ts`（MemoryStore/LongTermStore
内存实现）、`postgres.ts`（PostgresStore 雏形）；runtime `persistence`
构造消费 `checkpointSaver`/`log`。

**方案对比**：
- **B（选定）**：Python 侧 `checkpoint_backend` 参数 → env 透传（新增
  `REACTIVEGRAPH_CHECKPOINT_BACKEND` + postgres 连接参数），Driver 端
  persistence 按 backend 选择实现。
- A：只暴露 sqlite 路径。C：只补 postgres。

**设计**：
- `DriverHost.__init__(..., checkpoint_backend: str | None = None,
  **env)`：`"sqlite"`（默认，REACTIVEGRAPH_DB）、`"memory"`（不落盘）、
  `"postgres"`（需 `REACTIVEGRAPH_PG_DSN`）。
- Driver 端：runtime/CLI 读 env 选择 checkpointer（memory = 会话内；
  sqlite = 现有；postgres = PostgresStore）。
- `checkpoint_op`/`store_op` 协议不变（后端透明）。

**验收**：memory 后端重启丢失 checkpoint（跨进程断言）；sqlite 默认
（现有全部 durable 测试回归）；postgres 有 DSN 才测否则 skip。

## 7. 优先级与依赖

```
① D1 流式归一化  —— 无前置（链可见性收益最大）→ 先行
② runAll 分批并行 —— 引擎执行模型变更，独立 → 次行
③ 可观测：③a 独立（映射表+fallback trace）；③b 依赖①（run_stream 通道）
④ checkpoint 多后端 —— 独立，可与②并行
```

落地顺序：**① → ② → ③（a→b）→ ④**；②④无相互依赖可交换。

## 8. 风险与决策点

- **runAll 分批的行为漂移**：既有串行测试（含 effect 幂等/receipts）必须
  全绿；`concurrency=1` 兜底开关保证可回退。冲突抛错 = 图错误（文档提示
  修正 writes 声明或分轮）。
- **D1 事件形状兼容**：`values/updates/messages` 形状对齐 langgraph
  stream_mode（增量字典键为段 id 而非任务 id——链编译层段 id 即 `seg_i`）。
- **postgres 测试环境**：外部 DB 依赖 → skip 策略（有 DSN 才跑）。
  **已验证**（2026-09-17）：docker postgres:16 实测 `test_driverhost_
  checkpoint_backend_postgres`（`PostgresCheckpointSaver` 真实路径），引擎
  123 passed / 0 skip；postgres.ts 多后端接入（`REACTIVEGRAPH_PG_DSN`，
  见 §4.3/commit d2e2ada 与 19153eb）。
- **桥接协议扩展**：③b 用 custom 通道（复用 run_stream，不新增协议方法）
  待定，实现时若通道不适用再评估新方法。

## 9. 验收总览（每阶段 TDD：测试全绿 + commit + 收尾）

- **P3-1 流式归一化**：fallback/Driver 事件序列一致 + 三模式 + 错误传播。
- **P3-2 并行/写冲突**：WriteConflictError 两类 + 无冲突等价 + 串行开关
  回归 + 并发基准。
- **P3-3a 可观测映射**：映射断言 + fallback trace 记录。
- **P3-3b Driver 桥接**：trace 透传端到端 + 降级。
- **P3-4 checkpoint 多后端**：memory/sqlite/postgres 切换测试。
- **收尾**：全量验证（两包 pytest + ruff + mypy + 链基准）+ 实施计划文档
  状态更新。

## 附录 C：链回调 ↔ 引擎 span/trace 语义映射表

| 链侧事件 | 引擎 span/trace 事件 | 触发点 |
| --- | --- | --- |
| `on_chain_start(seg)` | `task:{tid}:start` | 任务执行前（fallback `_execute` 循环内；Driver 侧 span `task:{tid}` start） |
| `on_chain_end(seg, out)` | `task:{tid}` | 任务成功结束（含 writes 增量；Driver 侧 span end） |
| skip（pure 指纹命中） | `cache_hit` | fallback 缓存命中分支（`_skip_callback` 旁） |
| 段异常 | `task:{tid}` + `error: True` | fallback except 分支（异常透传不吞）；Driver 侧 span error 状态 |
| computed 求值 | `computed:{id}:hit` | fallback computeds 循环（值写入 state 前） |
| Driver run 级 | `run:start` / `run:end`（含 `interrupted`） | host 分支 `_last_run` 记录处（暂无 span 时 run 级事件） |

trace 形状：`graph._trace: list[dict]`，条目含 `event`（事件名）与相关
`task`/`computed_id` 键；每次执行开始重置；记录不阻断执行（异常安全，
trace 自身失败不影响段执行）。