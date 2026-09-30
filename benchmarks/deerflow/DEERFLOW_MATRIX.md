# DEERFLOW_MATRIX — DeerFlow v2 能力验证型移植对照矩阵（唯一事实源）

> 对照基线：`github.com/bytedance/deer-flow` @ HEAD `f9f3127`（源码 clone 由
> `$DEERFLOW_UPSTREAM_ROOT` 指定，2956 文件，不进仓库；该 hash 固定，不追上游）。
> 设计依据：docs/plans/2026-09-18-deerflow-port-design.md（已批准）。
> 状态：`verified`（e2e 通过）→ `unverified`（环境受限，注明原因）→ `pending`（未实现）。
> 运行：`uv run --directory python/reactivechain pytest $PWD/benchmarks/deerflow/ -q`（26 passed）
> 系统对比（官方 vs 移植）见 `DEERFLOW_VS_OFFICIAL.md`；执行模型量化见 `SELECTIVE_GAIN_REPORT.md`。

## 六条核心链路

| # | deer-flow v2 能力（源码模块） | 我们的等价组件（e2e 已有覆盖） | 移植文件 | 测试 | 状态 |
|---|---|---|---|---|---|
| 1 | lead_agent 多 agent 编排（`agents/lead_agent/agent.py:make_lead_agent`：create_agent + 12 middleware + 12 内置工具） | GraphBuilder 声明式图 + 事件路由（A1/A2） | deerflow_port/lead_agent.py | tests/test_matrix_link1_lead_agent.py | verified |
| 2 | sub-agent 执行（`subagents/executor.py` + registry，内置 bash_agent/general_purpose，capacity 并发） | 嵌套子图 + Parallel 组合器 + ainvoke（A11/C2） | deerflow_port/subagents.py | tests/test_matrix_link2_subagents.py | verified（dispatcher 容量截断 + 并行聚合） |
| 3 | 长期记忆（`persistence/` + `runtime/store` + memory_middleware 注入上下文） | MemoryStore + Driver store_* 原生入口（A14/B2/C11） | deerflow_port/memory.py | tests/test_matrix_link3_memory.py | verified（跨 run 持久 + thread 隔离 + 换实例命中） |
| 4 | tools + MCP（`tools/builtins/` 12 工具 + `mcp/` client/session_pool/task_tool_caller） | @tool/StructuredTool/Toolkit + from_openapi（C12/C14） | deerflow_port/tools_mcp.py | tests/test_matrix_link4_tools_mcp.py | verified（内置 + MCP 式工具，结果回填输出） |
| 5 | 线程持久化（`persistence/engine.py` + `runtime/checkpointer` + `runtime/store`） | checkpoint/恢复 + thread_id 隔离 + durable 回放（A6/B6/D4） | deerflow_port/persistence.py | tests/test_matrix_link5_persistence.py | verified（重启恢复/断点继续/帧序列确定） |
| 6 | interrupt / resume HITL（`runtime/`，clarification 工具形态） | interrupt/resume 会话恢复（A5/D3） | deerflow_port/hitl.py | tests/test_matrix_link6_hitl.py | verified（单段/多段循环/未确认工具不执行） |

## 差异化能力（我们有，langchain/langgraph 没有）——e2e 断言已落地

| # | 能力 | 移植测试 | deer-flow v2 的对照行为 | 状态 |
|---|---|---|---|---|
| D1 | 跨 run 选择性执行（pure 指纹跳过，d0183d4 起） | tests/test_differentiators.py | langgraph Pregel 每次 invoke 全量重跑全部节点 | verified |
| D2 | 事务状态 + 写冲突策略（reducer/priority/串行） | 同上 | 全量重算后合并，无显式冲突策略 | verified |
| D3 | computed 缓存（read-set 失效） | 同上（host 走 fallback，设计边界注明） | 无等价物 | verified |
| D4 | scope 子状态命名空间 | 同上 | 无等价物 | verified |
| D5 | 因果 trace / EXPORT_DOT | 同上 | 黑盒重放 | verified |
| D6 | durable event log 回放（重启精确恢复） | 同上 | checkpoint 快照式 | verified |
| D7 | RGP/1 协议 + Driver 真子进程（引擎跨语言执行） | 同上 | 纯 Python 进程内 | verified |

## 移植暴露的框架缺陷（全部已修复 + 回归）

| # | 缺陷 | 根因 | 修复 | 回归 |
|---|---|---|---|---|
| F1 | 任务内 list 切片 `s["items"][:cap]` KeyError | TrackedStateProxy 仅支持 int key，list 值被代理包裹 | state.py `__getitem__` 支持 slice（返回原始切片） | 引擎 124+1skip 绿 |
| F2 | 工具调用 `unexpected keyword argument 'value'` | BaseTool.invoke 用 isinstance(args, dict) 判断，代理参数被误判为标量 | tools.py invoke 先 snapshot 转纯 dict | 链 191 绿 |
| F3 | f-string 拼接容器值输出对象地址 | TrackedStateProxy 无内容化 repr | state.py 加 `__repr__`（repr(snapshot())） | 引擎 124+1skip 绿 |
| F4 | 多段 HITL：resume 后再次 Interrupt 抛 TaskExecutionError | ①runtime.resume 未捕获 extractInterrupt；②RGP/1 RESUME 响应丢 interrupted；③Python host.resume 只透传 state | 三层对齐修复（runtime.ts / main.ts / host.py） | TS 180 + 引擎 124+1skip + 链 191 绿 |

## 汇总

- **六条核心链路 verified（6/6）+ 差异化能力 verified（7/7）= 13 项，套件 `26 passed`**
  （smoke 1 + 链路 6×3 + 差异化 7）。
- **移植暴露 4 个框架缺陷（F1–F4），全部修复并回归全绿**——能力验证的核心产出。
- unverified（环境受限）注记沿用 FEATURE_MATRIX 惯例：真实 LLM（AGNES 网关/
  FakeLLM 双路径）、外网面工具（web_search）——本套件未依赖真实模型/外网。
