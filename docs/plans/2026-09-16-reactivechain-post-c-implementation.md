# 方案 C 之后四项演进（流式/并行/可观测/checkpoint）实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在方案 C（引擎为唯一运行时）基础上落地四项演进：①D1 流式归一化 ②runAll 分批并行+写冲突 ③可观测合并 ④checkpoint 多后端。

**Architecture:** ①链 `stream` 统一事件序列（fallback 事件传播循环产出段边界事件，Driver 走 `host.run_stream` 转发，mode=values/updates/messages 对齐 langgraph）；②`runtime.run` 的 pending 队列按事件轮次分批、批内 `Scheduler.runAll`（并发+`WriteConflictError`）；③链回调 ↔ 引擎 span 语义映射表 + fallback trace 记录 + Driver custom 通道桥接；④Python `DriverHost` 便捷参数包装 Driver 已具备的后端 env（sqlite/pg/redis/无 env=内存）。

**Tech Stack:** Python（reactivegraph 引擎绑定、reactivechain 链）、TypeScript（packages/driver 调度运行时）、pytest/ruff/mypy、pnpm tsc、真实 Node Driver 端到端测试。

**前置文档：** `docs/plans/2026-09-16-reactivechain-post-c-design.md`（已批准，方案决策细节）。测试命令惯例：引擎 `uv run --directory python/reactivegraph python -m pytest -q`；链 `uv run --directory python/reactivechain --extra test python -m pytest -q`；TS `pnpm --filter @reactivegraph/driver typecheck && pnpm --filter @reactivegraph/driver build`。

---

## P3-1 流式归一化

### Task 1: fallback `graph.stream` 产出段边界事件序列

**Files:**
- Modify: `python/reactivegraph/reactivegraph/graph.py`（`_execute` 加 `on_event` 钩子；`stream` fallback 分支改多事件，现状 419-431 单 values）
- Test: `python/reactivegraph/tests/test_streaming_tokens.py`（追加）

**Step 1: 写失败测试**（fallback 流产出段 start/end/values 事件，不止单 values）

```python
def test_fallback_stream_emits_segment_events():
    def build(b):
        b.task("t0", fn=lambda s: {"x": s["n"] + 1}, on=("run",), writes=("x",))
        b.task("t1", fn=lambda s: {"y": s["x"] * 2}, on=("t0:written",), writes=("y",))
    g = ReactiveGraph.build(build)
    events = list(g.stream("run", {"n": 1}))
    kinds = [ev["eventType"] for ev in events]
    assert "task_start" in kinds and "task_end" in kinds and "values" in kinds
    assert kinds[-1] == "terminal"
```

**Step 2: 跑测试确认失败** — `uv run --directory python/reactivegraph python -m pytest -q tests/test_streaming_tokens.py::test_fallback_stream_emits_segment_events -x`，预期 FAIL（当前 fallback 只 yield 单 values 事件）。

**Step 3: 最小实现** — `graph.py`：
- `_execute(self, event, payload, on_event: Callable[[dict], None] | None = None)`：fallback 事件传播循环里，任务执行前 `on_event({"eventType": "task_start", "task": tid})`，成功后 `task_end`（含 `writes` 增量字典），异常 `task_error`；每次 state 提交后 `values`（全 state 快照）；循环后 computeds 求值若发生重算发 `computed` 事件（`computed_id`）。
- `stream` fallback 分支改为：`events_q` 经 `on_event` 填充，`_execute(event, payload, on_event=feed)` 后按队列 yield，末尾 `{"eventType": "terminal"}`。host 分支保持现状。

**Step 4: 跑测试确认通过**（同上命令，PASS）。**Step 5: Commit** — `git add python/reactivegraph/reactivegraph/graph.py python/reactivegraph/tests/test_streaming_tokens.py && git commit -m "feat(graph): fallback stream 产出段边界事件序列（task_start/end/error + values）"`

### Task 2: `chain.stream(state, mode=...)` 统一 API（fallback 走 graph.stream）

**Files:**
- Modify: `python/reactivechain/reactivechain/runnable.py:390`（`Pipeline.stream` 签名与实现；现状进程内 generator）
- Test: `python/reactivechain/tests/test_stream_modes.py`（新）

**Step 1: 写失败测试**

```python
def test_stream_values_updates_modes():
    chain = Pipeline([RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"})])
    vals = list(chain.stream({"n": 1}, mode="values"))
    assert vals[-1]["x"] == 2  # values：全 state 快照，末帧含最终结果
    updates = list(chain.stream({"n": 1}, mode="updates"))
    assert updates[-1] == {"seg_0": {"x": 2}}  # 段写键增量
```

**Step 2: 确认失败** — `uv run --directory python/reactivechain --extra test python -m pytest -q tests/test_stream_modes.py::test_stream_values_updates_modes -x`，预期 FAIL（现 stream 无 mode 参数）。

**Step 3: 最小实现** — `Pipeline.stream(self, state, *, mode="values")`：
- 惰性编译图后走 `graph.stream("run", dict(state))`；按 mode 包装事件流：`values` → 取 events 的 state 快照；`updates` → 汇总 `task_end.writes` 为 `{tid: writes}`；`messages` → 透传 `custom`/`messages` chunk（host 模式）。未知 mode 抛 `ReactiveChainError`。
- fallback 与 host 共用同一包装；`astream` 经 executor 线程迭代同步适配。
- 兼容：`on_chain_*` 回调包装照旧（`_segment_fn`），stream 事件与回调并存。

**Step 4: 确认通过。Step 5: Commit** — `git commit -m "feat(reactivechain): chain.stream 统一 mode=values|updates|messages（fallback 经 graph.stream）"`

### Task 3: Driver 模式 `chain.stream` 事件序列与 fallback 同构（values 模式逐帧一致）

**Files:**
- Modify: `python/reactivegraph/reactivegraph/host.py:416`（`run_stream` on_event 形状核对）
- Modify: `python/reactivechain/reactivechain/runnable.py`（`Pipeline.stream` host 分支）
- Test: `python/reactivechain/tests/test_to_graph_driver.py`（追加）

**Step 1: 写失败测试** — 同一线性链，`list(chain.stream(state, mode="values"))`（fallback）与 `ReactiveGraph(chain.to_graph().definition, host=durable)` 后 `list(graph.stream("run", state))` 的 values 快照帧序列逐帧一致。

**Step 2: 确认失败** — 预期 FAIL（Driver 模式事件形状与 fallback 不同）。

**Step 3: 实现** — `graph.stream` host 分支（已有多线程桥接）规范 onEvent 形状：`values` 帧直接透传；`custom`/`messages` 透传带 `task` 标记；两执行器 `values` 模式严格同构（Driver 无段级 start 事件的落差以 values 帧为准，`updates` 允许实现差异并在 docstring 标注）。

**Step 4: 确认通过。Step 5: Commit** — `git commit -m "feat(reactivechain): Driver 模式 stream values 序列与 fallback 逐帧一致"`

### Task 4: 流式错误传播 + 全量回归

**Files:** Modify: `python/reactivechain/tests/test_stream_modes.py`（追加）；`runnable.py`（如有需要）

**Step 1:** 失败测试——段抛异常时 `chain.stream` 产出 `task_error` 事件后抛错（不吞）；fallback/Driver 两路径各一断言。
**Step 2:** 确认失败 → **Step 3:** 实现（fallback：段异常经队列传 `task_error` 再 raise；host：DriverError 上抛前补 `task_error` 事件）。
**Step 4:** 全量回归（引擎 + 链 pytest、ruff、mypy）。**Step 5: Commit。**

---

## P3-2 runAll 分批并行 + 写冲突

### Task 1: `runtime.run` pending 队列改事件轮次分批，批内 `runAll`

**Files:**
- Modify: `packages/driver/src/runtime.ts`（run 执行循环，现状 ~382-410 单队列串行；resume 同步改造）
- Test: `python/reactivegraph/tests/test_driver_scheduling.py`（追加扇出并行等价测试）

**Step 1: 写失败测试/等价探针** — 扇出图（t0 on=("run",) 写 x；t1/t2 on=("t0:written",) 写 y/z；t3 on=("t1:written","t2:written") 汇总 w）真实 Driver 输出与 fallback 一致（并行无冲突）：

```python
def test_driver_fanout_batch_parallel_matches_fallback():
    # 输出断言：w = y+z，fallback 与 Driver 一致
```

**Step 2:** 现状串行也正确——本步验证基线绿；核心验收是改造后既有 Driver 测试全绿 + 并行真实发生（并发探针：t1/t2 各 `time.sleep(0.05)`，concurrency=8 总时长 < 串行和；宽松断言避免 flaky）。

**Step 3: 实现** — `runtime.ts`：
- 现单 `pending` 队列改"事件轮次批次"：`batch = routeFor(event)`；处理完一批后收集该批任务发出的 `{tid}:written` 下游 id 集（`queued` 全局去重）为下一批；批内 `await scheduler.runAll(batchIds, batchInputs)`。
- 批输入 = 现有 merge（`{...store.raw, ...input}`，同批同载荷）。
- 中断提取：`runAll` 抛错先 `extractInterrupt`（runTask 的 TaskInterruptedError 经 runAll 上抛），命中走现有 interrupt 路径。
- 串行兜底：`opts.config?.concurrency === 1` 时批内逐任务 runTask（等价现行为）；Scheduler 构造已收 `concurrency`（默认 8）。
- 提取私有 helper `executeTaskQueue(...)` 供 run/resume 共用。

**Step 4:** `pnpm --filter @reactivegraph/driver typecheck && pnpm --filter @reactivegraph/driver build` + 全部 Driver 测试（`test_driver_scheduling.py`/`test_driver_p2.py`/`test_time_travel.py`）绿 + 新扇出/并发探针绿。**Step 5: Commit** — `git commit -m "feat(driver): runtime.run 按事件轮次分批 + 批内 runAll 并行（concurrency=1 串行兜底）"`

### Task 2: 写冲突测试（声明冲突 + 欠声明实际冲突）

**Files:** Test: `python/reactivegraph/tests/test_driver_scheduling.py`（追加）

**Step 1: 失败测试**：
- 声明冲突：t1/t2 订阅同一 written 事件且都 `writes=("w",)` → Driver run 抛错含 `WriteConflictError`（经 DriverError 携带）。
- 欠声明：t1/t2 声明 `writes=("a",)`/`("b",)` 但实际都写 `w` → 实际写冲突同样抛错（runAll 的实际写集替换 claims 逻辑）。
**Step 2:** 确认失败（现状串行无冲突检测）。**Step 3:** Task 1 落地后冲突检测自然生效；必要时微调 `checkConflicts` 条件（`concurrency>1 && sorted.length>1`）。**Step 4:** 绿。**Step 5: Commit。**

### Task 3: concurrency=1 回归 + 并行基准

**Files:** Test: `python/reactivegraph/tests/test_driver_scheduling.py`；基准 `benchmarks/parallel_vs_serial.py`（新）

**Step 1:** 失败测试——同一扇出图 `concurrency=1` 配置（透传 env `REACTIVEGRAPH_CONCURRENCY=1` 或 opts.config）下与既有串行语义等价；基准脚本：真实 Driver 扇出图 N=8 分支，并行 vs concurrency=1 计时对比，断言并行不慢于串行（2 倍宽松阈值）。
**Step 2-4:** 失败→实现（Driver 读 env 默认 concurrency；基准脚本计时）→通过。**Step 5: Commit。**

---

## P3-3 可观测合并

### Task 1: 回调↔span 映射表（附录 C）+ fallback trace 记录

**Files:**
- Modify: `docs/plans/2026-09-16-reactivechain-post-c-design.md`（附录 C 映射表）
- Modify: `python/reactivegraph/reactivegraph/graph.py`（`_execute` 记录 `self._trace`）
- Test: `python/reactivegraph/tests/test_observability.py`（新）

**Step 1: 失败测试** — fallback 执行后 `graph._trace` 与段执行一一对应：`task:{tid}:start`、`task:{tid}`（end）、pure 命中 `cache_hit`、computed 命中 `computed:{id}:hit`、段错误带 `error` 标记。

**Step 2:** 确认失败（无 `_trace`）。**Step 3:** 实现——`_execute` fallback 循环内按映射追加 trace 记录（不阻断执行、异常安全）；Driver 模式记录 RUN 结果级 trace（暂无 span 时 run 级事件）。附录 C：`on_chain_start↔task:{tid}:start`、`on_chain_end↔task:{tid}`、skip↔`cache_hit`、computed 命中↔`computed:{id}:hit`、错误↔error 状态。

**Step 4:** 绿。**Step 5: Commit。**

### Task 2: 链回调 ↔ 引擎 trace 映射断言

**Files:** Test: `python/reactivechain/tests/test_observability.py`（新）

**Step 1: 失败测试** — instrument 链执行：`ChainStats` 回调序列（on_chain_start/end/skip）与 `chain._compiled_graph._trace` 按附录 C 一一对应（如 skip 回调 ↔ `cache_hit`）。
**Step 2-4:** 失败→实现（链侧读 `_compiled_graph._trace` 断言）→绿。**Step 5: Commit。**

### Task 3: Driver 模式 trace 桥接（复用 run_stream custom 通道）

**Files:** Modify: `packages/driver/src/runtime.ts`（spanEmitter 事件外发）与/或 `python/reactivegraph/reactivegraph/host.py`（custom 消费）；Test: `python/reactivegraph/tests/test_observability.py`

**Step 1: 失败测试** — Driver 模式 run 后 Python 侧拿到 span 级 trace（含 `task:{tid}:start` 至少）。
**Step 2:** 确认失败。**Step 3:** 实现——优先复用 `run_stream` 的 custom 通道：Driver 路径请求 stream 模式，把 Scheduler spanEmitter 事件（`spanEmitter?.emit("task", ...)` 现已记录）经 `onStreamChunk` custom 外发 → Python `_trace` 消费；不新增协议方法（设计 §8）。**Step 4:** 绿 + 全量回归。**Step 5: Commit。**

---

## P3-4 checkpoint 多后端

### Task 1: `DriverHost` 后端便捷参数（包装既有 env，Driver 侧 main.ts buildPersistence 已支持 sqlite/pg/redis/memory）

**Files:**
- Modify: `python/reactivegraph/reactivegraph/host.py`（`DriverHost.__init__` 加参数）
- Test: `python/reactivegraph/tests/test_driver_p2.py`（追加）

**Step 1: 失败测试**（现状 `DriverHost(env=...)` 无参数）：
- `memory`：无 DB env 的两个独立 host 实例 run 后 `list_checkpoints` 为空（不残留）。
- `sqlite`：显式 `checkpoint_backend="sqlite", db_path=...` 等价现有 `REACTIVEGRAPH_DB` 语义（持久）。
- `postgres`：`pg_dsn` 提供才测，否则 `pytest.skip`。

**Step 2:** 确认失败（参数不存在）。**Step 3:** 实现——`DriverHost.__init__(..., checkpoint_backend: str | None = None, db_path: str | None = None, pg_dsn: str | None = None, redis_url: str | None = None)`：参数优先合并进 env（`REACTIVEGRAPH_DB`/`REACTIVEGRAPH_PG_DSN`/`REACTIVEGRAPH_REDIS_URL`）；`checkpoint_backend="memory"` 时确保不注入 DB env；docstring 注明 Driver 侧已支持（main.ts buildPersistence）。

**Step 4:** 绿 + 既有 durable 测试回归。**Step 5: Commit** — `git commit -m "feat(host): DriverHost checkpoint_backend 便捷参数（sqlite/memory/postgres/redis）"`

---

## 收尾

### Task: 全量验证 + 状态更新

**Step 1:** 引擎 pytest + 链 pytest + 两包 ruff + mypy + TS typecheck/build + 链基准（`uv run --directory benchmarks/differential python reactchain_side.py` 4 workload 全通且 `rag_selective retrieval_calls==1`）。
**Step 2:** 更新 `docs/plans/2026-09-16-reactivechain-post-c-design.md` 状态为"已实施完成"（附 commit 引用）。
**Step 3: Commit** — `git commit -m "docs(reactivechain): 方案 C 之后四项演进——已实施完成"`。

---

## 执行选项

计划保存在 `docs/plans/2026-09-16-reactivechain-post-c-implementation.md`。两种执行方式：

1. **Subagent-Driven（本会话）**——每 task 派发独立 subagent，任务间审查，迭代快
2. **Parallel Session（另开会话）**——新会话用 executing-plans，带检查点批量执行