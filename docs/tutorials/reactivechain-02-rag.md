# ReactiveChain 02 · RAG 检索增强生成

**目标**：`retriever → context → prompt → llm → parser` 一条 RAG 链，验证检索段选择性。

## 核心组件（M4）

- `Document` / `RecursiveCharacterTextSplitter`：切分语料（中文标点也参与递归分隔）
- `HashEmbeddings`（离线）或 `OpenAICompatEmbeddings`：文本 → 向量
- `InMemoryVectorStore`（进程内余弦）；`DriverVectorStore` 经 host `VECTOR_UPSERT/SEARCH` 协议持久化
- `VectorStoreRetriever`：`invoke({"query": ...})` → `{"docs": [...]}`（Runnable 段）
- **检索段选择性**：同 query 重跑时 retriever 段被指纹缓存跳过（基准见 `docs/benchmarks.md`）

## 运行

```bash
uv run --directory python/reactivechain python docs/tutorials/code/reactchain_02_rag.py
# 1) 语料切分 5 块并入库
# 2) RAG 回答： ReactiveChain 提供了声明式管道与段级选择性执行。
# 3) 同 query 两次重跑，检索实际执行 1 次（期望 1）
```

## 进阶

- `BM25Retriever`：无需嵌入的语料内建检索；`MultiQueryRetriever` / `EnsembleRetriever`：查询扩展 / 加权合并
- 检索源切换：把 `InMemoryVectorStore` 换成 `DriverVectorStore(host, embeddings, namespace=[...])` 即可持久化
- `OutputFixingParser` 包装：解析失败时调 LLM 修复重试