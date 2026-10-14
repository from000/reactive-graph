# 选择性执行省钱 Demo — 实测报告

日期：2026-10-17 · 引擎：ReactiveGraph（core）vs LangGraph（upstream DeerFlow venv）
模型：Agnes agnes-2.5-flash（真实 LLM，非 fake model）
脚本：`run_savings.py`（fork） / `run_baseline_langgraph.py`（baseline），原始 JSON 在 `results/`

## 场景

1. 一份文档先做**确定性抽取**（`llm_extract`，声明 `kind="pure"` + `reads=("document",)`）。
2. 用户改了**与文档无关**的东西（`tone`），整条 pipeline 重跑。
3. 结果：

| | LangGraph | ReactiveGraph |
|---|---:|---:|
| `llm_extract` 调用 | **2** | **1**（第 2 次指纹命中跳过） |
| `llm_generate` 调用 | 2 | 2 |
| 总调用 | **4** | **3** |
| tokens in / out | 1275 / 890 | 926 / 735 |
| 节省 | — | **1 次调用 / 349 in / 155 out tokens** |

`graph.cost_saved()`（引擎自带，标注为估算）：
`llm_calls_saved=1`，`tokens_in_saved=180`，`tokens_out_saved=40`，`estimated_usd_saved=$0.00012`，
`why_skipped('llm_extract') = fingerprint_unchanged`。

## 这个 demo 证明了什么（能算钱的部分）

- **只有声明为 pure + reads 的确定性子任务，在输入不变时真的会跳过**（指纹命中），
  不调 LLM、不花 token。这是 LangGraph 没有的、跨 run 的**选择性执行**。
- 调用次数是**逐次实测**的（计数器），不是估算。tokens 来自真实 API usage。

## 这个 demo 不证明什么（必须诚实）

1. **agent 主链不省**：fork 的 `create_deerflow_agent` 把 model/tools 注册为 effect，
   每次 invoke 都调用模型（`model_call_count.py`：30 轮 = 30 次）。本 demo 用的是
   底层 core 引擎，不是 fork 的 agent 路径。
2. **首次运行不省**：跳过只发生在"同一输入被重复触达"时。
3. **$0.00012 是引擎估算**（按 `estimated_usd` 任务元数据），不是账单；实际金额由
   provider 定价决定，且本 demo 的文档很小（~100 token）。
4. **线性外推（10/100/1000 次重跑）是示意**，依赖"输入重复率"假设，必须按真实
   workload 校准，不能当承诺。

## 生产上什么情况下它真的省钱

- 大量 run 共享同一批确定性 LLM 子任务（**分类、抽取、摘要、安全扫描**）且输入会重复：
  重试、多风格改写、多轮追问、同一批文档多次下游处理。
- 需要 exactly-once 副作用语义（receipt 幂等）或可审计 trace 的场景。

## 一句话

**"换 ReactiveGraph 省钱"在 agent 主链上是假的；在"确定性 LLM 子任务 + 输入重复"场景上是真的。**
本 demo 把后者做成了可复现、逐次计数的实测（不是推演）。
