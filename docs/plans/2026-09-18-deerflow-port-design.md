# DeerFlow v2 能力验证型移植 — 设计文档

日期:2026-09-18 · 状态:已批准(用户确认:v2 super agent harness + 能力验证型移植 + 核心链路六条)

## 目标

克隆字节跳动 `bytedance/deer-flow`(v2 super agent harness,HEAD `f9f3127`,源码完整 clone 由
`$DEERFLOW_UPSTREAM_ROOT` 指定,2956 文件,不进仓库)**,用本仓库 reactivechain/reactivegraph
体系替换其 langchain/langgraph 依赖**,在核心运行链路上重写等价实现,逐条对照其功能面做 e2e 验证,
并发挥我们有而 langchain/langgraph 没有的差异化能力——目的:验证我们框架能否支撑
super agent harness 级别的应用。

诚实边界:完整移植(重写 harness 引擎层)等价于再造一个 langgraph 平台,不在本设计范围;
本设计做「能力验证型移植」——对照 deer-flow v2 真实功能面(非风格复刻),逐条验证等价组件。

## 与既有资产的关系

- `examples/deerflow/`(2026-09-10 批准):DeerFlow 风格前端工作台,12 能力端点,面向**我们能力面**的交互展示
  (当时 deer-flow 官方源码不可达,已核实 404)。
- 本设计:`benchmarks/deerflow/`——面向 **deer-flow v2 真实功能面**的功能对照矩阵 + e2e 套件。
  互补不重叠:examples 做展示,benchmarks 做验证;矩阵行可引用 examples/deerflow 已覆盖的端点能力。

## 架构

```
benchmarks/deerflow/
├── DEERFLOW_MATRIX.md          ← 唯一事实源:deer-flow 能力 → 等价组件 → e2e 测试 → 状态
├── deerflow_port/              ← 移植实现(纯 Python,挂 DriverHost 真子进程)
│   ├── lead_agent.py           ← 多 agent 编排(声明式图)
│   ├── subagents.py            ← sub-agent 执行(嵌套子图 + 组合器)
│   ├── memory.py               ← 长期记忆(Driver store)
│   ├── tools_mcp.py            ← tools + MCP 式工具接入
│   ├── persistence.py          ← 线程持久化 + 恢复
│   └── hitl.py                 ← interrupt / resume
└── tests/
    ├── test_deerflow_port_matrix.py   ← 矩阵每行一个 e2e(全真不 mock,连真实 Driver)
    └── test_deerflow_differentiators.py ← 差异化能力收益演示
```

## 核心链路六条:deer-flow v2 → 我们的等价组件

| # | deer-flow v2 能力(源码模块) | 我们的等价组件(e2e 已有覆盖) |
|---|---|---|
| 1 | lead_agent 多 agent 编排(`agents/lead_agent/agent.py:make_lead_agent`) | ReactiveGraph GraphBuilder 声明式(task/on/reads/writes)+ 链 Pipeline 挂 Driver(A1/C1/C16) |
| 2 | sub-agent 执行(`subagents/`) | 嵌套图/子图 + Parallel/Branch 组合器 + ainvoke/astream(A11/C2) |
| 3 | 长期记忆(`persistence/` + memory 检索) | MemoryStore 7 类 + Driver store_* 原生入口(A14/B2/C11) |
| 4 | tools + MCP(`tools/` + `mcp/`) | @tool/StructuredTool/Toolkit + from_openapi + DriverVectorStore(A10/B4/C9/C12/C14) |
| 5 | 线程持久化(`persistence/` + `runtime/` + `scheduler/`) | checkpoint/restore + thread_id 隔离 + durable 回放(A6/B6/D4) |
| 6 | interrupt / resume(`runtime/` HITL) | interrupt/resume + 会话恢复继续工具链(A5/D3) |

## 差异化能力(我们有,langchain/langgraph 没有)——必须展示

| 能力 | 对应 e2e | deer-flow v2 的对照行为 |
|---|---|---|
| 选择性执行:跨 run pure 指纹跳过(d0183d4 起) | A2/D6 | langgraph Pregel 每次 invoke 全量重跑全部节点 |
| 事务状态 + 写冲突策略(reducer/priority/串行) | A3 | 全量重算后合并,无显式冲突策略 |
| computed 缓存(声明式 read-set 失效) | A1(host 走 fallback,设计边界注明) | 无等价物 |
| scope 子状态命名空间 | A13 | 无等价物 |
| 因果 trace / EXPORT_DOT(调度决策可导出) | A8/B7 | 调试靠黑盒重放 |
| durable event log 回放(重启精确恢复) | D4 | checkpoint 恢复为快照式 |
| RGP/1 协议 + Driver 真子进程(引擎跨语言执行) | B 域全 | 纯 Python 进程内 |

## 对照矩阵与状态惯例

- `DEERFLOW_MATRIX.md` 沿用 FEATURE_MATRIX 惯例:每行 deer-flow 能力 → 等价组件 →
  `文件:测试` → `verified`(e2e 通过)/ `unverified`(环境受限,注明原因)。
- 真实 LLM 调用沿用 C3 的 AGNES 网关模式(地域封锁下 FakeLLM 保构造面,有 key 时真调)。
- 网络面(web_search 等)标注 unverified——与 FEATURE_MATRIX C12 同口径。

## 验证

1. `uv run --directory python/reactivechain pytest "$PWD/benchmarks/deerflow/" -q`(矩阵行全绿)
2. 全量回归:`uv run --directory python/reactivechain --extra test python -m pytest -q`(链 190 不回归)
   + `pnpm test`(Driver 179 不回归)
3. ruff:`ruff check benchmarks/deerflow/ --fix` 后全清
4. 差异化能力测试单独跑,断言「同输入跳过/不重跑」等收益可量化

## 交付物

- `benchmarks/deerflow/DEERFLOW_MATRIX.md`(唯一事实源)
- `benchmarks/deerflow/deerflow_port/`(六模块移植实现)
- `benchmarks/deerflow/tests/`(矩阵 + 差异化 e2e)
- README 段:`benchmarks/README.md` 登记新套件

## 风险

- deer-flow 对照基线固定 `f9f3127`(hash 记录于矩阵头部),不追上游变动;
- 依赖安装面大(其 uv.lock)——移植实现只依赖我们仓库已有栈(reactivechain/reactivegraph),不引入 deer-flow 依赖;
- 真实 LLM 地域封锁 → AGNES/FakeLLM 双路径,与 C3 同策略。
