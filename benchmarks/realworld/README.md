# RealWorld 真实场景验证套件

ReactiveChain 真实环境验证（design: `docs/plans/2026-09-17-realworld-verification-design.md`）：
把验证从微基准提升到**真实 LLM、真实向量库、长会话可靠性**，并把跑测纳入 CI（manual）。

## 运行

```bash
# 离线壳（默认/CI）：FakeLLM + HashEmbeddings，只做结构校验（不打 API）
uv run --directory python/reactivechain python benchmarks/realworld/realworld_side.py

# 真实模式：OpenAI 真实 LLM + embeddings（需可达 api.openai.com 的网络 + 可用 key）
REACTIVEGRAPH_REAL=1 uv run --directory python/reactivechain python benchmarks/realworld/realworld_side.py
```

- 环境变量：`REACTIVEGRAPH_REAL=1`（真实模式）、`OPENAI_API_KEY`（真实模式必需）。
- 网络：`trust_env=True` 已启用——走系统代理（urllib 读 macOS 系统配置或
  `HTTPS_PROXY` 环境变量）。直连被墙时确保代理可用。
- 单元测试：`uv run --directory python/reactivechain pytest benchmarks/realworld/test_realworld.py -q`。

## 输出 `.results/realworld.json`

```json
{
  "mode": "real" | "offline",
  "rag_first":      {"median_ms": ..., "output": ..., "retrieval_calls": 1},
  "rag_selective":  {"median_ms": ..., "retrieval_calls": 0},   // 同 query 重跑：检索段跳过
  "rag_different":  {"median_ms": ..., "retrieval_calls": 1},   // 不同 query：全量重检索
  "measure_skip_vs_full": {"skip_ms":..., "full_ms":..., "skip_retrieval_calls":0,
                           "full_retrieval_calls":1},           // 跳过 vs 强制全量（clear_cache）
  "outputs_nonempty": true, "selective_skips": true             // 断言（false → 退出码 1）
}
```

## 成本护栏（design §4）

- 固定语料（5 段中文）与固定 query；`max_tokens≤200`；embeddings 本地缓存
  （`<corpus_hash>.vec.json`，`EmbeddingCache`）避免重复计费。
- 失败显式报错（退出码 2=缺 key / 3=执行失败；429 自动退避重试 3 次）。
- CI/常规跑永不打真实 API（`REACTIVEGRAPH_REAL` 未设即离线壳）。

## 本机验证记录（2026-09-17）

- 离线壳：3 workload 全绿（retrieval_calls 1/0/1），`measure_skip_vs_full`
  skip=0 / full=1（收益量化真实），`outputs_nonempty`/`selective_skips` 均 true。
- 单元测试 5 passed（缓存/跳过语义/收益量化/离线壳结构）。
- 真实模式：**403 `unsupported_country_region_territory`**——本机出口 IP 被 OpenAI
  地域封锁（key 有效）。脚本与离线验证就绪；在可达 OpenAI 的网络（或代理出口
  受支持地区）执行 `REACTIVEGRAPH_REAL=1 ...` 即可完成端到端真实验证。
- 多后端向量库（Task 4）：Driver 的 VECTOR_UPSERT/SEARCH 接线 sqlite 持久化
  （`REACTIVEGRAPH_DB` 存在时用 `SqliteVectorStore`）——**跨重启存活**测试 +
  内存后端重启即失对照各 1（2 passed）。**pgvector（PostgresVectorStore）尚未
  实现**（Driver 的 vectorStore 原为进程内内存单例；PostgresStore 无向量列），
  记为后续项。
- 长会话/可靠性（Task 5）：10 轮会话历史累积、Interrupt → resume、4 线程
  并发隔离各 1（3 passed）。测试暴露并修复三个引擎/Driver 并发缺陷：
  `graph._ensure_compiled` 编译竞态（duplicate graph id）、
  `TrackedStateProxy.__iter__` 嵌套 list 迭代恒空（会话历史无法读取）、
  `SqliteCheckpointSaver.put` BEGIN 后 await 导致事务嵌套。
- 单元测试共 10 passed；链 190 / 引擎 122+1skip / TS 178 全绿无回归。

## CI

`.github/workflows/benchmark.yml` 的 `realworld` manual job：跑离线壳
（`realworld_side.py`，引擎内断言 + JSON 报告），上传 `realworld.json` artifact。
不打真实 API、不需 secrets；真实 LLM 验证留给本地有网环境
（`REACTIVEGRAPH_REAL=1`）。
