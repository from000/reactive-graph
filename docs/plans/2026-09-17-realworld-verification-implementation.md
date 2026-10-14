# 真实场景验证套件（realworld）Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use executing-plans / 本会话顺序执行本计划。

**Goal:** 在 `benchmarks/realworld/` 建真实场景验证套件——真实 OpenAI LLM/embeddings 下
验证 RAG 正确性 + 段级选择性收益（真实调用节省），docker pgvector 多后端检索，长会话
（多轮/Interrupt/resume/并发隔离）可靠性，并纳入 CI（manual）。

**Architecture:** 复用 `python/reactivechain/reactivechain/` 既有组件（
`OpenAICompatChatModel`/`OpenAICompatEmbeddings`/`InMemoryVectorStore`/
`DriverVectorStore`），新增独立基准目录（不污染 pytest 常规跑），env 门控真实调用。

**Tech Stack:** Python 3.12 / reactivechain 组件 / stdlib（urllib+threading）/
docker pgvector / GitHub Actions manual job。

**设计依据:** `docs/plans/2026-09-17-realworld-verification-design.md`（§1-§7）。

---

## Task 1: 套件骨架 + embeddings 本地缓存（TDD）

**Files:**
- Create: `benchmarks/realworld/realworld_side.py`（骨架 + `EmbeddingCache`）
- Create: `benchmarks/realworld/test_realworld.py`（fake 测试）
- Create: `benchmarks/realworld/.gitignore`（`.results/`、`*.vec.json`、`__pycache__/`）

**Step 1 写失败测试**（`test_realworld.py`）：
```python
import os, tempfile
from realworld_side import EmbeddingCache

def test_embedding_cache_hits_after_first_call():
    calls = []
    def embed(texts):
        calls.append(1)
        return [[0.1, 0.2]] * len(texts)
    with tempfile.TemporaryDirectory() as d:
        cache = EmbeddingCache(embed, d)
        v1 = cache.get(["你好"])
        v2 = cache.get(["你好"])
        assert v1 == v2 and len(calls) == 1  # 第二次命中缓存，不重算

def test_cache_different_inputs_miss():
    calls = []
    def embed(texts):
        calls.append(1)
        return [[0.1, 0.2]] * len(texts)
    with tempfile.TemporaryDirectory() as d:
        cache = EmbeddingCache(embed, d)
        cache.get(["你好"]); cache.get(["再见"])
        assert len(calls) == 2
```
**Step 2** 运行确认失败：`uv run --directory python/reactivechain pytest benchmarks/realworld/test_realworld.py -q`（
`EmbeddingCache` 不存在 → ImportError）。
**Step 3 最小实现**（`realworld_side.py`）：`EmbeddingCache` 类——`get(texts)` 按
语料哈希缓存到 `<dir>/<sha1(corpus)>.vec.json`（json 读写，缺失时调 embed 并落盘）。
**Step 4** 重跑测试确认 PASS。
**Step 5** Commit：`feat(bench): realworld 套件骨架 + embeddings 本地缓存（避免重复计费）`

## Task 2: RAG 链 + 调用计数 + 收益量化（fake 可测，TDD）

**Files:**
- Modify: `benchmarks/realworld/realworld_side.py`（加 `CountingLLM`/`CountingEmbeddings`
  包装 + `build_rag()` + `run_workload()` + `measure_skip_vs_full()`）
- Modify: `benchmarks/realworld/test_realworld.py`

**Step 1 写失败测试**：
```python
from realworld_side import build_rag, CountingLLM, CountingEmbeddings

def test_selective_rerun_skips_llm():
    from reactivechain.llm import FakeLLM
    llm = CountingLLM(FakeLLM(["答案"]))
    emb = CountingEmbeddings()
    chain = build_rag(llm=llm, embeddings=emb)  # 内存向量 + 固定语料
    chain.invoke({"query": "X"})
    n1 = llm.calls
    chain.invoke({"query": "X"})          # 同 query 重跑
    assert llm.calls == n1                # 段级跳过：LLM 不再调用
    chain.invoke({"query": "Y"})
    assert llm.calls == n1 + 1            # 不同 query 全量
```
**Step 2** 确认失败（`build_rag` 未实现/或跳过未生效）。
**Step 3 实现**：
- `CountingLLM`/`CountingEmbeddings`：包装委托 + `calls` 计数。
- `build_rag(llm=..., embeddings=..., vector_backend="memory")`：复用
  `benchmarks/differential/reactchain_side.py` 的链式（retriever→context→prompt→
  llm→parser），语料 5 段中文固定。
- `run_workload(name, fn, rounds=3)` → `{median_ms, output, llm_calls, embed_calls}`。
- `measure_skip_vs_full()`：同 query 重跑（带跳过）vs `REACTIVEGRAPH_REAL_FULL=1`
  强制全量，输出两者调用数/延迟对照（收益 = full - skip）。
**Step 4** PASS + 断言跳过语义成立。
**Step 5** Commit：`feat(bench): realworld RAG 链 + 调用计数 + 选择性收益量化（fake 可测）`

## Task 3: 真实 API 集成验证（env 门控，本地跑）

**Files:**
- Modify: `benchmarks/realworld/realworld_side.py`（`main()`：读
  `OPENAI_API_KEY`/`REACTIVEGRAPH_REAL`；`REACTIVEGRAPH_REAL=0` 时用 fake 只做结构校验）
- Create: `benchmarks/realworld/README.md`（运行方式/env/成本护栏说明）

**Step 1** 写测试：`REACTIVEGRAPH_REAL=0` 时 `main()` 用 FakeLLM 跑通且输出
`.results/realworld.json`（含 `llm_calls`/`embed_calls`/`median_ms` 字段）。
**Step 2** 失败（main 未实现）。
**Step 3** 实现 `main()`：预热 → `rag_first`/`rag_selective`/`rag_different`（真实
LLM 时 OpenAICompatChatModel 走 `base_url=https://api.openai.com/v1`，`max_tokens≤200`）
→ `measure_skip_vs_full()` → 汇总写 json → 断言（输出非空、selective 调用数下降）。
**Step 4** 本地真实跑：`OPENAI_API_KEY=... uv run --directory python/reactivechain
python benchmarks/realworld/realworld_side.py`（**env 门控 REACTIVEGRAPH_REAL=1 才调
真实 API**），确认真实输出与收益数字。
**Step 5** Commit：`feat(bench): realworld 真实 API 集成验证（env 门控，json 报告）`

## Task 4: pgvector 多后端（阶段 2）

**Files:**
- Create: `benchmarks/realworld/pgdocker.sh`（起 `pgvector/pgvector:pg16` 容器：
  `docker run -d --name rgp-pgv -e POSTGRES_USER=rgp -e POSTGRES_PASSWORD=rgp -e
  POSTGRES_DB=rgp -p 5434:5432 pgvector/pgvector:pg16`，就绪探测 `pg_isready`）
- Modify: `benchmarks/realworld/realworld_side.py`（`vector_backend="pgvector"`：
  `DriverVectorStore` + `DriverHost`，`REACTIVEGRAPH_PG_DSN=postgresql://rgp:rgp@localhost:5434/rgp`）
- Modify: `benchmarks/realworld/test_realworld.py`

**Step 1** 写失败测试：pgvector 后端 doc 入库 → top-k 检索与内存后端**交集 ≥ min(k)**
（允许 pg 排序微差）+ 性能数字输出。
**Step 2** 失败（backend 未实现）。
**Step 3** 实现 pgvector 分支（`DriverHost` 已支持 pg_dsn；`DriverVectorStore` 走
STORE_OP——复用既有类，只加连线）。
**Step 4** docker 起库 → 本地真跑断言通过 → 停容器。
**Step 5** Commit：`feat(bench): realworld pgvector 多后端检索一致性（docker 验证）`

## Task 5: 长会话/可靠性（阶段 3）

**Files:**
- Modify: `benchmarks/realworld/realworld_side.py`（`run_session()`：10 轮记忆累积 +
  确定性 `Interrupt` + `resume` + 4 线程并发隔离）
- Modify: `benchmarks/realworld/test_realworld.py`

**Step 1** 写失败测试（fake LLM）：10 轮会话状态随轮增长；`Interrupt` → `interrupted`
→ `resume(载荷)` → 状态正确；4 线程 × 独立 threadId 互不串写。
**Step 2** 失败。
**Step 3** 实现（复用 `graph.py`/`host.py` 既有 resume 路径；确定性中断点
`raise Interrupt`；`threading.Thread` 并发）。
**Step 4** PASS。
**Step 5** Commit：`feat(bench): realworld 长会话/中断 resume/并发隔离可靠性`

## Task 6: CI + 收尾（阶段 4）

**Files:**
- Modify: `.github/workflows/benchmark.yml`（加 `realworld` manual job：
  `uv run --directory python/reactivechain pytest benchmarks/realworld/test_realworld.py
  -q` 离线壳 + `REACTIVEGRAPH_REAL=0 python realworld_side.py` 结构校验；不依赖 secrets）
- Modify: `benchmarks/realworld/README.md`

**Step 1** 写 CI job（YAML）。
**Step 2** 全量验证：链 pytest（含 test_realworld）+ ruff + mypy + 引擎 pytest 回归。
**Step 3** Commit：`ci(bench): realworld manual job + 收尾验证`

---

## 执行纪律
- 每任务独立 commit；TDD failing→passing；真实 API 仅本地 manual（
  `REACTIVEGRAPH_REAL=1`），CI/常规跑永不打真实 API。
- 验证命令：链 `uv run --directory python/reactivechain --extra test python -m pytest -q`；
  套件单测 `uv run --directory python/reactivechain pytest benchmarks/realworld/test_realworld.py -q`；
  ruff/mypy 同项目惯例。
