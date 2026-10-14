"""ReactiveChain 教程 02：RAG 检索增强生成（离线演示，不依赖外部服务）。

运行：uv run --directory python/reactivechain python docs/tutorials/code/reactchain_02_rag.py
"""
import os
import sys

# 引导：把 reactivechain 源码包加入 sys.path（教程脚本独立可跑）
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "python", "reactivechain"))
)

from reactivechain import (
    ChatPromptTemplate,
    Document,
    FakeLLM,
    HashEmbeddings,
    InMemoryVectorStore,
    RecursiveCharacterTextSplitter,
    RunnableLambda,
    StrOutputParser,
    VectorStoreRetriever,
)

# --- 1. 语料 → 切分 → 向量化入库 ---
raw = (
    "ReactiveGraph 是反应式图执行引擎，支持事件路由与指纹跳过。\n"
    "ReactiveChain 是构建在其上的声明式管道组件层。\n"
    "向量检索通过嵌入相似度做语义搜索，用于 RAG 召回。\n"
    "段级选择性执行：输入未变时跳过段调用，只重跑受影响的段。\n"
    "OpenAI 兼容模型通过 /chat/completions 接口接入。\n"
)
splitter = RecursiveCharacterTextSplitter(chunk_size=40, chunk_overlap=4)
chunks = splitter.split_text(raw)
embeddings = HashEmbeddings(dims=64)
store = InMemoryVectorStore(embeddings)
store.add_documents([Document(c, {"i": i}) for i, c in enumerate(chunks)])
print(f"1) 语料切分 {len(chunks)} 块并入库")

# --- 2. RAG 链：retriever → context → prompt → llm → parser ---
retriever = VectorStoreRetriever(store, k=2)


def build_context(state: dict) -> dict:
    return {"context": "\n".join(d.page_content for d in state["docs"])}


chain = (
    retriever
    | RunnableLambda(build_context, reads={"docs"}, writes={"context"})
    | ChatPromptTemplate(
        [
            ("system", "仅基于以下资料回答：\n{context}"),
            ("human", "{query}"),
        ]
    )
    | FakeLLM(["ReactiveChain 提供了声明式管道与段级选择性执行。"])
    | StrOutputParser()
)

answer = chain.invoke({"query": "ReactiveChain 的优势？"})
print("2) RAG 回答：", answer["output"])
assert "ReactiveChain" in answer["output"]

# --- 3. 检索段选择性：同 query 重跑时 retriever 段被跳过（指纹缓存）---
search_calls = [0]
orig = store.similarity_search
store.similarity_search = lambda q, k=4, embeddings=None: (
    search_calls.__setitem__(0, search_calls[0] + 1) or orig(q, k=k, embeddings=embeddings)
)
from reactivechain import Pipeline

retriever_chain = Pipeline([retriever])
retriever_chain.invoke({"query": "ReactiveChain 的优势？"})
retriever_chain.invoke({"query": "ReactiveChain 的优势？"})  # 相同 → 跳过
print(f"3) 同 query 两次重跑，检索实际执行 {search_calls[0]} 次（期望 1）")
assert search_calls[0] == 1

print("教程 02 OK")