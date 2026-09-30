# 真实场景验证套件（RealWorld Verification）设计

> 日期：2026-09-17 · 前置：方案 C 之后四项演进已全部完成（流式归一化、runAll
> 并行+写冲突、可观测 span 桥接、checkpoint 多后端），ReactChain vs LangChain
> 差分基准为自证微基准（9-12x）。本设计把验证提升到**真实环境**：真实 LLM、
> 真实向量库、长会话可靠性，并把跑测纳入 CI（manual）。

## 1. 目标与焦点（用户确认的四项）

1. **真实 LLM 端到端 + 选择性收益**：OpenAI 真实 LLM/embeddings 下 RAG 输出
   正确性 + 同 query 重跑时**真实调用节省**（LLM 调用数 / embeddings 调用数 /
   真实墙钟延迟，含网络）——回答"LLM 网络延迟下段级选择性执行还剩多少收益"。
2. **真实向量库多后端**：docker pgvector 下 `DriverVectorStore`（Driver
   `PostgresStore`）检索正确性与性能，与 `InMemoryVectorStore` 对比。
3. **长会话/可靠性**：多轮会话（记忆累积）+ 确定性 `Interrupt`/`resume` +
   并发多线程 × 独立 `threadId` 状态隔离。
4. **CI 纳入**：`.github/workflows/benchmark.yml` manual job 跑 1–3 并断言
   防回归。

## 2. 架构与组件

新目录 `benchmarks/realworld/`（与 `benchmarks/differential/` 平级，隔离真实
调用的依赖与隐私，不进 pytest 常规跑）：

```
benchmarks/realworld/
├── realworld_side.py      # ReactiveChain 真实场景基准（阶段 1–3 主入口）
├── pgdocker.sh            # docker 起 pgvector:pg16（阶段 2）
├── .results/realworld.json  # 汇总报告（输出/调用计数/延迟）
└── README.md              # 运行方式、env、成本说明
```

复用组件（均已在 `python/reactivechain/reactivechain/` 就绪，零新依赖）：
- `llm.OpenAICompatChatModel` —— OpenAI 兼容 `/chat/completions`（纯 stdlib）
- `embeddings.OpenAICompatEmbeddings` —— OpenAI `/embeddings`
- `vectorstore.InMemoryVectorStore` / `vectorstore.DriverVectorStore` ——
  内存与 Driver STORE_OP 后端；pgvector 经 Driver `PostgresStore`
  （`REACTIVEGRAPH_PG_DSN` 指向 pgvector 容器），无需 Python 数据库驱动。
- `runnable` / `prompt` / `parsers` / `retriever` —— 链组装（同
  `benchmarks/differential/reactchain_side.py` 语义）。

## 3. 分阶段实施

### 阶段 1 — 真实 LLM RAG + 选择性收益（聚焦点 1）
- 固定语料（与 differential 同 5 段中文语料，可扩展为 20 段真实文档；
  语料固定 → 成本可控 + 结果可复现）。
- `OpenAICompatChatModel(model="gpt-4o-mini", base_url=..., api_key=os.environ["OPENAI_API_KEY"])`
  + `OpenAICompatEmbeddings` + `InMemoryVectorStore`。
- workload：`rag_first`（QUERY_X 首查）、`rag_selective`（同 X 重跑）、
  `rag_different`（QUERY_Y）。
- 计量：LLM 调用数、embeddings/检索调用数、每次 invoke 墙钟（含网络）。
- **收益量化**：同 query 重跑时"带段级跳过"vs"强制全量"（env 开关
  `REACTIVEGRAPH_REAL_FULL=1` 强制重算）的调用数/延迟对照——量化纯段级
  收益，区分于微基准的路径开销。
- **embeddings 本地缓存**（`<corpus_hash>.vec.json`）：语料向量只算一次，
  避免每跑重复计费。
- 结果写 `.results/realworld.json`（输出非空断言 + 调用计数符合预期）。

### 阶段 2 — pgvector 多后端（聚焦点 2）
- `pgdocker.sh`：`docker run -d --name rgp-pgv -e POSTGRES_... -p 5434:5432
  pgvector/pgvector:pg16`（容器就绪探测，用后停/删可选项）。
- `DriverVectorStore` + `DriverHost`（`REACTIVEGRAPH_PG_DSN`）doc 入库与
  top-k 检索；与 `InMemoryVectorStore` 同 query **top-k 一致性断言**（交集 ≥
  阈值，允许 pg 排序微差）与延迟对比。

### 阶段 3 — 长会话/可靠性（聚焦点 3）
- 10 轮会话：每轮 query 追加会话记忆（`memory` 组件累积），断言输出与状态
  随轮增长。
- 确定性 `Interrupt`：链中显式 raise Interrupt → `interrupted=true` +
  checkpoint → `resume(载荷)` 完成 → 状态正确。
- 并发隔离：4 线程 × 独立 `threadId` 各跑一轮，断言互不串写。

### 阶段 4 — CI（聚焦点 4）
- `.github/workflows/benchmark.yml` 加 `realworld` job（`workflow_dispatch`
  manual）：`uv run --directory benchmarks/realworld python realworld_side.py`，
  失败即 job 失败；**不设 secrets 依赖**（CI 用 `REACTIVEGRAPH_REAL=0` 跑
  离线壳/或跳过 LLM 段仅验证结构——真实 API 跑由本地 manual 触发，防 CI
  浪费密钥与费用）。

## 4. 错误处理与成本护栏

- 429/超时：指数退避重试（3 次）；最终失败**显式报错**（区分缺失
  `OPENAI_API_KEY` / 网络 / 限流 / 超时，不同退出消息）。
- 成本：固定语料与 query、固定 `max_tokens`（≤200）、embeddings 本地缓存、
  单次运行总 LLM 调用上限（断言超限即失败）。
- 隐私：脚本不打印 api_key；`.results/` 与缓存文件加 `.gitignore`（不进仓库，
  或仅 commit 样例不致泄露的内容）。

## 5. 验收标准

- 阶段 1：真实 OpenAI 下 3 workload 输出非空且结构正确；`rag_selective`
  LLM/检索调用数 < `rag_first`（跳过生效）；`rag_different` 全量重跑；
  `.results/realworld.json` 存在且含调用计数与延迟字段。
- 阶段 2：pgvector 后端 top-k 检索与内存后端一致（交集阈）；性能数字输出。
- 阶段 3：10 轮状态累积正确；resume 后状态/输出正确；4 线程隔离（各自
  threadId 状态互不串）。
- 阶段 4：CI job 定义存在；离线壳跑通过（`REACTIVEGRAPH_REAL=0` 结构校验）。

## 6. 风险与对策

- **LLM 输出非确定** → 断言语义/结构而非精确串（非空、含关键概念）；
  同 query 重跑对比的是**调用数/延迟**，与输出无关。
- **API 成本超限** → 缓存 + max_tokens 固定 + 调用上限断言 + CI 不直跑
  真实 API。
- **pg 排序稳定** → top-k 用合法性/交集断言，不用精确序。
- **CI/本地环境差异** → 版本固定（同 differential pyproject 方式）。

## 7. 与既有基准关系

`benchmarks/differential/`（微基准，自证，恒可跑）保持不变；本套件是
**真实环境补充证据**：选择性收益从"路径开销差"分离出"真实调用节省"。