"""M4：嵌入/向量库/检索器 + RAG 链端到端 + 检索段选择性测试。"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from reactivechain import ReactiveChainError, RunnableLambda
from reactivechain.documents import Document
from reactivechain.embeddings import HashEmbeddings
from reactivechain.llm import FakeLLM
from reactivechain.parsers import StrOutputParser
from reactivechain.prompt import ChatPromptTemplate
from reactivechain.retriever import (
    BM25Retriever,
    EnsembleRetriever,
    MultiQueryRetriever,
    VectorStoreRetriever,
)
from reactivechain.vectorstore import DriverVectorStore, InMemoryVectorStore

_DOCS = [
    Document("ReactiveGraph 是反应式图执行引擎", id="d1"),
    Document("ReactiveChain 是声明式管道组件层", id="d2"),
    Document("向量检索用于语义搜索", id="d3"),
]


def _store() -> InMemoryVectorStore:
    return InMemoryVectorStore(HashEmbeddings(dims=64))


def test_hash_embeddings_deterministic() -> None:
    emb = HashEmbeddings(dims=16)
    v1 = emb.embed_query("hello world")
    v2 = emb.embed_query("hello world")
    assert v1 == v2
    assert len(v1) == 16
    assert abs(sum(x * x for x in v1) - 1.0) < 1e-6  # 归一化


def test_in_memory_store_roundtrip() -> None:
    store = _store()
    ids = store.add_documents(_DOCS)
    assert len(ids) == 3
    docs = store.similarity_search("管道组件", k=2)
    assert docs[0].page_content == "ReactiveChain 是声明式管道组件层"
    assert len(docs) == 2


def test_vector_store_retriever_segment() -> None:
    store = _store()
    store.add_documents(_DOCS)
    retriever = VectorStoreRetriever(store, k=1)
    out = retriever.invoke({"query": "图执行引擎"})
    assert "ReactiveGraph" in out["docs"][0].page_content


def test_retriever_missing_query_hint() -> None:
    store = _store()
    store.add_documents(_DOCS)
    with pytest.raises(ReactiveChainError, match="缺少输入键 'query'"):
        VectorStoreRetriever(store).invoke({})


def test_bm25_retriever_ranks_relevant() -> None:
    docs = [
        Document("猫喜欢鱼，猫是宠物"),
        Document("狗喜欢骨头，狗是宠物"),
        Document("天气晴朗适合散步"),
    ]
    retriever = BM25Retriever(docs, k=1)
    out = retriever.invoke({"query": "猫"})
    assert out["docs"][0].page_content.startswith("猫")


def test_multi_query_retriever_merges() -> None:
    llm = FakeLLM(["图引擎\n管道层"])
    base = VectorStoreRetriever(_store(), k=2)
    base.vectorstore.add_documents(_DOCS)
    retriever = MultiQueryRetriever(base, llm, n_queries=3)
    out = retriever.invoke({"query": "引擎"})
    contents = [d.page_content for d in out["docs"]]
    assert any("ReactiveGraph" in c for c in contents)
    assert any("ReactiveChain" in c for c in contents)


def test_ensemble_retriever_rrf_merges() -> None:
    a = VectorStoreRetriever(_store(), k=2)
    a.vectorstore.add_documents(_DOCS[:2])
    b = VectorStoreRetriever(_store(), k=2)
    b.vectorstore.add_documents(_DOCS)
    ens = EnsembleRetriever([a, b], k=3)
    out = ens.invoke({"query": "管道"})
    assert len(out["docs"]) == 3


class _FakeHost:
    """mock VECTOR_* host：记录 upsert/search 调用。"""

    def __init__(self) -> None:
        self.upserts: list[dict] = []
        self.points: list[dict] = []
        self.searches: list[dict] = []

    def vector_upsert(self, vector, namespace=None, id="v", metadata=None) -> None:  # noqa: A002
        self.upserts.append({"id": id, "namespace": namespace, "metadata": metadata})
        self.points.append({"id": id, "namespace": namespace, "metadata": metadata})

    def vector_search(self, query, namespace=None, limit=None, min_score=None) -> list[dict]:
        self.searches.append({"namespace": namespace, "limit": limit})
        return [
            {"id": p["id"], "metadata": p["metadata"], "score": 0.9}
            for p in self.points[: (limit or 4)]
        ]


def test_driver_vector_store_uses_protocol() -> None:
    host = _FakeHost()
    store = DriverVectorStore(host, HashEmbeddings(dims=32), namespace=["rc", "demo"])
    store.add_documents([Document("内容A", id="a1")])
    assert host.upserts[0]["id"] == "a1"
    assert host.upserts[0]["namespace"] == ("rc", "demo")
    docs = store.similarity_search("内容", k=2)
    assert docs[0].page_content == "内容A"
    assert host.searches[0]["limit"] == 2


def test_driver_vector_store_requires_protocol_host() -> None:
    with pytest.raises(ReactiveChainError, match="VECTOR_\\*"):
        DriverVectorStore(object(), HashEmbeddings())  # type: ignore[arg-type]


def test_rag_chain_end_to_end() -> None:
    store = _store()
    store.add_documents(_DOCS)
    retriever = VectorStoreRetriever(store, k=2)

    def context(state: dict) -> dict:
        return {"context": "\n".join(d.page_content for d in state["docs"])}

    chain = (
        retriever
        | RunnableLambda(context, reads={"docs"}, writes={"context"})
        | ChatPromptTemplate(
            [("system", "基于资料回答：\n{context}"), ("human", "{query}")]
        )
        | FakeLLM(["ReactiveChain 是管道组件层"])
        | StrOutputParser()
    )
    out = chain.invoke({"query": "什么是 ReactiveChain？"})
    assert "ReactiveChain" in out["output"]


def test_retriever_segment_selective_skip() -> None:
    """同 query 重跑时检索段（进程内指纹缓存）应跳过。"""
    store = _store()
    store.add_documents(_DOCS)
    calls: list[int] = []

    class _CountingRetriever(VectorStoreRetriever):
        def _search(self, query, state):
            calls.append(1)
            return super()._search(query, state)

    retriever = _CountingRetriever(store, k=1)
    chain = retriever_wrapper(retriever)
    chain.invoke({"query": "图引擎"})
    chain.invoke({"query": "图引擎"})  # 相同 query → 检索段跳过
    assert len(calls) == 1
    chain.invoke({"query": "管道"})  # 不同 query → 执行
    assert len(calls) == 2


def retriever_wrapper(retriever):
    from reactivechain import Pipeline

    return Pipeline([retriever])


# -- OpenAICompatEmbeddings（本地 mock 端点） -------------------------------


class _EmbHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        payload = {
            "data": [{"embedding": [0.1, 0.2, 0.3]}],
            "model": body.get("model"),
        }
        self.wfile.write(json.dumps(payload).encode())


@pytest.fixture(scope="module")
def emb_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _EmbHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_openai_compat_embeddings(emb_server) -> None:
    from reactivechain.embeddings import OpenAICompatEmbeddings

    emb = OpenAICompatEmbeddings(emb_server, api_key="k", model="mock-emb")
    assert emb.embed_query("你好") == [0.1, 0.2, 0.3]
    assert emb.embed_documents(["a", "b"]) == [[0.1, 0.2, 0.3], [0.1, 0.2, 0.3]]


def test_openai_compat_embeddings_missing_key_hint() -> None:
    from reactivechain.embeddings import OpenAICompatEmbeddings

    emb = OpenAICompatEmbeddings("http://127.0.0.1:1", api_key=None, env_key="RC_NO_SUCH_ENV")
    with pytest.raises(ReactiveChainError, match="未配置 api_key"):
        emb.embed_query("x")


def test_openai_compat_embeddings_bad_response_hint() -> None:
    from reactivechain.embeddings import OpenAICompatEmbeddings

    class _BadHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence
            pass

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers["Content-Length"])
            self.rfile.read(length)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"data": []}')

    server = ThreadingHTTPServer(("127.0.0.1", 0), _BadHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        emb = OpenAICompatEmbeddings(
            f"http://127.0.0.1:{server.server_address[1]}", api_key="k"
        )
        with pytest.raises(ReactiveChainError, match="缺少 data\\[0\\].embedding"):
            emb.embed_query("x")
    finally:
        server.shutdown()