"""域 C：reactivechain 链组件 e2e（C1–C16）。

覆盖矩阵见 FEATURE_MATRIX.md。运行：
    uv run --directory python/reactivechain pytest $PWD/benchmarks/e2e/test_e2e_chain.py -q
"""

from __future__ import annotations

import math
import os
import threading
from datetime import datetime
from enum import Enum
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pydantic
from reactivechain import (
    AgentExecutor,
    AIMessage,
    BaseLanguageModel,
    BM25Retriever,
    CallbackManager,
    ChatPromptTemplate,
    CommaSeparatedListOutputParser,
    ConversationBufferMemory,
    ConversationBufferWindowMemory,
    ConversationSummaryMemory,
    ConversationTokenBufferMemory,
    CSVLoader,
    DatetimeOutputParser,
    Document,
    DriverVectorStore,
    EnsembleRetriever,
    EntityMemory,
    EnumOutputParser,
    FakeLLM,
    FewShotPromptTemplate,
    GeneratorRunnable,
    HashEmbeddings,
    HumanMessage,
    InMemoryVectorStore,
    JSONLoader,
    JsonOutputParser,
    JsonRegexParser,
    MemoryStore,
    MessagePlaceholder,
    MultiQueryRetriever,
    OpenAICompatChatModel,
    OutputFixingParser,
    Pipeline,
    PipelinePromptTemplate,
    PromptTemplate,
    PydanticOutputParser,
    ReactiveChainError,
    RecursiveCharacterTextSplitter,
    RetryOutputParser,
    RetryWithErrorOutputParser,
    RunnableBranch,
    RunnableFallback,
    RunnableLambda,
    RunnableParallel,
    RunnablePassthrough,
    SegmentStat,
    StrOutputParser,
    StructuredTool,
    SummaryBufferMemory,
    SystemMessage,
    TextLoader,
    Toolkit,
    ToolMessage,
    ToolNodeAdapter,
    VectorStoreRetriever,
    calculator,
    create_react_agent,
    create_tool_calling_agent,
    current_date,
    from_openapi,
    messages_to_api,
    tool,
)
from reactivechain.callback import instrument_pipeline
from reactivegraph import DriverHost


# -- C1 runnable ---------------------------------------------------------------
def test_c1_runnable() -> None:
    """Pipeline/RunnableLambda/RunnablePassthrough 链式 + GeneratorRunnable 流。"""
    chain = (
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"})
        | RunnableLambda(lambda s: {"y": s["x"] * 2}, reads={"x"}, writes={"y"})
    )
    out = chain.invoke({"n": 2})
    assert out["y"] == 6
    # passthrough 透传输入
    assert RunnablePassthrough().invoke({"a": 1}) == {"a": 1}

    class _Gen(GeneratorRunnable):
        def generate(self, state: dict):
            yield "a"
            yield "b"
            return {"done": True}

    gen_chain = Pipeline([_Gen()])
    assert list(gen_chain.stream({}, mode="messages")) == [
        {"chunk": "a"},
        {"chunk": "b"},
    ]


# -- C2 combiner ---------------------------------------------------------------
def test_c2_combiner() -> None:
    """Parallel 合并 / Branch 路由 / Fallback 降级 / assign 注入。"""
    par = RunnableParallel({
        "inc": RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"}),
        "dbl": RunnableLambda(lambda s: {"y": s["n"] * 2}, reads={"n"}, writes={"y"}),
    })
    out = par.invoke({"n": 3})
    assert out["inc"]["x"] == 4 and out["dbl"]["y"] == 6

    branch = RunnableBranch([
        (lambda s: s["n"] > 0, RunnableLambda(lambda s: {"sign": "正"}, writes={"sign"})),
    ], default=RunnableLambda(lambda s: {"sign": "非正"}, writes={"sign"}))
    assert branch.invoke({"n": 1})["sign"] == "正"
    assert branch.invoke({"n": -1})["sign"] == "非正"

    def boom(state: dict) -> dict:
        raise ValueError("boom")

    fallback = RunnableFallback(
        RunnableLambda(boom, writes={"v"}),
        RunnableLambda(lambda s: {"v": "回退"}, writes={"v"}),
    )
    assert fallback.invoke({})["v"] == "回退"

    base = RunnableLambda(lambda s: {"out": "基础"}, writes={"out"})
    assigned = base.assign(extra=RunnableLambda(lambda s: {"extra": "注入"}, writes={"extra"}))
    a_out = assigned.invoke({"seed": 1})
    assert a_out["out"] == "基础" and a_out["extra"] == "注入"


# -- C3 llm --------------------------------------------------------------------
def test_c3_llm() -> None:
    """FakeLLM 全链路；OpenAICompatChatModel 构造面 + AGNES 网关真实调用。"""
    llm = FakeLLM(["答案"], input_key="prompt")
    chain = PromptTemplate("问题：{q}") | llm | StrOutputParser()
    assert chain.invoke({"q": "你好"})["output"] == "答案"
    # OpenAI 兼容模型构造（本地 mock 端点面）
    m = OpenAICompatChatModel(base_url="http://127.0.0.1:1", model="gpt-x",
                              api_key="k", max_tokens=16)
    assert m.model == "gpt-x"
    # AGNES 网关真实调用（环境有 AGNES_API_KEY 时真调 invoke + stream；
    # 无 key 时保持构造面验证，测试仍绿——真实调用不 mock 冒充）。
    if os.environ.get("AGNES_API_KEY"):
        real = OpenAICompatChatModel(
            base_url=os.environ.get("CUSTOM_LLM_URL", "https://apihub.agnes-ai.com/v1"),
            api_key=os.environ["AGNES_API_KEY"],
            model=os.environ.get("CUSTOM_MODEL", "agnes-2.5-flash"),
            max_tokens=512,  # 推理模型：token 先耗于 reasoning，需余量才出文本
            timeout_s=120,
        )
        out = real.invoke({"prompt": "用一句话介绍 ReactiveGraph"})
        assert out["output"].strip(), "AGNES 真实 invoke 应返回非空文本"
        chunks = list(real.stream({"prompt": "1+1="}))
        assert chunks and any(c["chunk"].strip() for c in chunks), (
            "AGNES 真实流式应产出 token"
        )


# -- C4 embeddings -------------------------------------------------------------
def test_c4_embeddings() -> None:
    """HashEmbeddings 维度与距离语义。"""
    emb = HashEmbeddings(dims=8)
    q = emb.embed_query("猫")
    docs = emb.embed_documents(["猫", "狗"])
    assert len(q) == 8 and len(docs) == 2 and len(docs[0]) == 8

    def _cos(a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b, strict=False))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(y * y for y in b))
        return dot / (na * nb) if na and nb else 0.0

    same = _cos(emb.embed_query("猫"), docs[0])
    other = _cos(emb.embed_query("猫"), docs[1])
    assert same > other, "同词相似度应高于异词"


# -- C5 messages / messages_to_api --------------------------------------------
def test_c5_messages() -> None:
    """四消息类型构造 + messages_to_api 载荷转换。"""
    msgs = [
        SystemMessage("sys"),
        HumanMessage("问"),
        AIMessage("答"),
        ToolMessage("tool", tool_call_id="t1"),
    ]
    payload = messages_to_api(msgs)
    assert payload == [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "问"},
        {"role": "assistant", "content": "答"},
        {"role": "tool", "content": "tool", "tool_call_id": "t1"},
    ]


# -- C6 prompt -----------------------------------------------------------------
def test_c6_prompt() -> None:
    """PromptTemplate/ChatPromptTemplate/FewShot/Pipeline/Placeholder 渲染。"""
    pt = PromptTemplate("请用{lang}回答：{question}")
    assert pt.invoke({"lang": "中文", "question": "你好"}) == {"prompt": "请用中文回答：你好"}

    ct = ChatPromptTemplate([("system", "你是{lang}助手"), ("human", "{question}")])
    out = ct.invoke({"lang": "中文", "question": "Q"})
    assert out["messages"] == [
        {"role": "system", "content": "你是中文助手"},
        {"role": "user", "content": "Q"},
    ]

    fs = FewShotPromptTemplate(
        PromptTemplate("Q:{q} A:{a}"),
        [{"q": "1", "a": "一"}],
        "示例：\n{examples}\n现在：{question}",
    )
    fout = fs.invoke({"question": "2"})
    assert "示例：" in fout["prompt"] and "现在：2" in fout["prompt"]

    pp = PipelinePromptTemplate(
        PromptTemplate("总：{summary}"),
        {"summary": PromptTemplate("{a}+{b}")},
    )
    assert pp.invoke({"a": 1, "b": 2})["prompt"] == "总：1+2"

    ph = ChatPromptTemplate(
        [("system", "助手"), MessagePlaceholder("history"), ("human", "{question}")]
    )
    phout = ph.invoke({"question": "继续", "history": [{"role": "user", "content": "先前"}]})
    assert phout["messages"][1] == {"role": "user", "content": "先前"}


# -- C7 parsers ----------------------------------------------------------------
def test_c7_parsers() -> None:
    """9 个 parser 解析断言。"""
    assert StrOutputParser().invoke({"output": "x"}) == {"output": "x"}
    assert JsonOutputParser().invoke({"output": '{"a": 1}'}) == {"json_output": {"a": 1}}
    assert CommaSeparatedListOutputParser().invoke({"output": "a, b"}) == {
        "list_output": ["a", "b"]
    }

    class _Mood(Enum):
        HAPPY = "happy"

    assert EnumOutputParser(_Mood).invoke({"output": "HAPPY"}) == {"enum_output": _Mood.HAPPY}
    dt = DatetimeOutputParser().invoke({"output": "2026-09-15T08:30:00"})
    assert dt["datetime_output"] == datetime(2026, 9, 15, 8, 30)
    assert JsonRegexParser().invoke({"output": 'x {"v": 42} y'}) == {"json_output": {"v": 42}}

    class _P(pydantic.BaseModel):
        name: str

    structured = PydanticOutputParser(_P).invoke({"output": '{"name": "ada"}'})
    assert structured["structured"].name == "ada"

    inner = JsonOutputParser()
    fixer = OutputFixingParser(inner, FakeLLM(['{"a": 1}']))
    assert fixer.invoke({"output": "not json"})["json_output"] == {"a": 1}

    # Retry：瞬态失败重试后成功（重试同 base_parser，确定性失败无意义）
    def make_flaky(calls: list[int]):
        def flaky(s: dict) -> dict:
            calls.append(1)
            if len(calls) == 1:
                raise ReactiveChainError("暂时失败")  # Retry 只捕获业务错误
            return {"json_output": {"a": 9}}

        return flaky

    retry_calls: list[int] = []
    retry = RetryOutputParser(RunnableLambda(make_flaky(retry_calls), writes={"json_output"}))
    assert retry.invoke({}) == {"json_output": {"a": 9}}
    assert len(retry_calls) >= 2, "RetryOutputParser 应重试"
    retry_err_calls: list[int] = []
    retry_err = RetryWithErrorOutputParser(
        RunnableLambda(make_flaky(retry_err_calls), writes={"json_output"})
    )
    assert retry_err.invoke({}) == {"json_output": {"a": 9}}


# -- C8 retriever --------------------------------------------------------------
def test_c8_retriever(e2e_host: DriverHost) -> None:
    """VectorStoreRetriever/BM25/Ensemble/MultiQuery top-k。"""
    store = InMemoryVectorStore(HashEmbeddings(dims=8))
    store.add_documents([
        Document(page_content="反应式图执行引擎", metadata={"id": 1}),
        Document(page_content="向量检索工作原理", metadata={"id": 2}),
    ])
    vs = VectorStoreRetriever(store, k=1)
    assert vs.invoke({"query": "图执行引擎"})["docs"][0].metadata["id"] == 1

    docs = [Document(page_content="猫是动物"), Document(page_content="狗是动物")]
    bm = BM25Retriever(docs, k=1)
    b_out = bm.invoke({"query": "猫"})
    assert "猫" in b_out["docs"][0].page_content

    ens = EnsembleRetriever([VectorStoreRetriever(store, k=2), BM25Retriever(docs, k=1)])
    assert len(ens.invoke({"query": "猫"})["docs"]) >= 1

    mq = MultiQueryRetriever(VectorStoreRetriever(store, k=1), FakeLLM(["图执行"]))
    assert mq.invoke({"query": "引擎"})["docs"]


# -- C9 vectorstore ------------------------------------------------------------
def test_c9_vectorstore(e2e_host: DriverHost) -> None:
    """InMemory 相似度检索 + DriverVectorStore（host 持久化检索）。"""
    mem = InMemoryVectorStore(HashEmbeddings(dims=8))
    mem.add_documents([Document(page_content="苹果", metadata={"k": "fruit"})])
    hits = mem.similarity_search("苹果", k=1)
    assert hits and hits[0].metadata["k"] == "fruit"

    dvs = DriverVectorStore(e2e_host, HashEmbeddings(dims=8), namespace=["rc", "e2e"])
    dvs.add_documents([Document(page_content="山", metadata={"k": "geo"})])
    d_hits = dvs.similarity_search("山", k=1)
    # Driver 端 metadata 为 doc.to_dict()（含嵌套 metadata 键）
    assert d_hits and d_hits[0].metadata["metadata"]["k"] == "geo"


# -- C10 documents -------------------------------------------------------------
def test_c10_documents(tmp_path: Path) -> None:
    """loaders（Text/CSV/JSON）+ splitter。"""
    txt = tmp_path / "a.txt"
    txt.write_text("第一段。\n\n第二段。")
    assert TextLoader(str(txt)).load()[0].page_content == "第一段。\n\n第二段。"

    csv = tmp_path / "a.csv"
    csv.write_text("name,age\nada,36\n")
    rows = CSVLoader(str(csv)).load()
    assert rows and rows[0].metadata["source"] == str(csv)

    js = tmp_path / "a.json"
    js.write_text('{"body": "内容", "title": "标题"}')
    jdocs = JSONLoader(str(js), content_key="body", metadata_keys=["title"]).load()
    assert jdocs[0].page_content == "内容" and jdocs[0].metadata["title"] == "标题"

    split = RecursiveCharacterTextSplitter(chunk_size=5, chunk_overlap=0)
    parts = split.split_documents([Document(page_content="一二三四五六七八九十")])
    assert len(parts) >= 2


# -- C11 memory ----------------------------------------------------------------
def test_c11_memory() -> None:
    """MemoryStore + 7 memory 类累积/窗口/摘要/实体。"""
    store = MemoryStore()
    store.set("t1", [HumanMessage("hello")])
    assert store.get("t1")[0].content == "hello"

    buf = ConversationBufferMemory()
    buf.save({"input": "你好"}, {"output": "你好！"})
    assert [m.content for m in buf.load()["history"]] == ["你好", "你好！"]

    win = ConversationBufferWindowMemory(k=1)
    for i in range(3):
        win.save({"input": f"q{i}"}, {"output": f"a{i}"})
    assert [m.content for m in win.load()["history"]] == ["q2", "a2"]

    summ = ConversationSummaryMemory(FakeLLM(["摘要"]))
    summ.save({"input": "q"}, {"output": "a"})
    assert "摘要" in str(summ.load()["summary"])

    tok = ConversationTokenBufferMemory(max_tokens=6)
    tok.save({"input": "q0"}, {"output": "a0"})
    assert tok.load()["history"]

    ent = EntityMemory()
    ent.save({"input": "我叫张三，住在杭州"}, {"output": "好的"})
    entities = ent.load()["entities"]
    assert "张三" in entities and "杭州" in entities

    sbuf = SummaryBufferMemory(k=1)
    for i in range(3):
        sbuf.save({"input": f"q{i}"}, {"output": f"a{i}"})
    sb_out = sbuf.load()
    assert len(sb_out["history"]) == 2  # 最近 1 轮窗口
    assert "q0" in sb_out["summary"] and "a0" in sb_out["summary"]


# -- C12 tools -----------------------------------------------------------------
def test_c12_tools() -> None:
    """@tool/StructuredTool/Toolkit/内置工具/ToolNodeAdapter（tool_call 调用格式）。"""

    @tool
    def add(a: int, b: int) -> int:
        """两数相加。"""
        return a + b

    call = {"tool_call": {"id": "c1", "name": "add", "arguments": {"a": 1, "b": 2}}}
    assert add.invoke(call)["tool_result"] == 3
    assert add.to_schema()["function"]["name"] == "add"
    assert add.to_schema()["function"]["description"] == "两数相加。"
    assert add.to_spec().fn(a=1, b=2) == 3

    st = StructuredTool(lambda x: x * 2, name="dbl", description="翻倍")
    assert st.invoke({"tool_call": {"id": "c2", "arguments": {"x": 21}}})["tool_result"] == 42

    tk = Toolkit([add, st])
    assert {t.name for t in tk} == {"add", "dbl"}  # Toolkit 迭代返回工具

    assert calculator.invoke(
        {"tool_call": {"id": "c3", "arguments": {"expression": "1+2*3"}}}
    )["tool_result"] == "7"
    date_res = current_date.invoke({"tool_call": {"id": "c4", "arguments": {}}})
    assert len(date_res["tool_result"]) >= 8  # YYYY-MM-DD

    tk2 = Toolkit([add])
    specs = tk2.to_specs()
    assert specs[0].name == "add" and specs[0].description == "两数相加。"

    adapter = ToolNodeAdapter([add])
    node = adapter.node()  # 对接 reactivegraph ToolNode（agent 循环用）
    assert node is not None


# -- C13 agents ----------------------------------------------------------------
class _FakeToolCallingLLM(BaseLanguageModel):
    """按剧本返回 tool_calls 或最终文本（e2e 用最小实现）。"""

    supports_tool_calling = True

    def __init__(self, script: list[dict]) -> None:
        super().__init__(name="fake_tc")
        self.script = list(script)
        self.history: list[list[dict]] = []

    def _call(self, messages: list[dict]) -> dict:
        self.history.append(messages)
        return self.script.pop(0)


def test_c13_agents() -> None:
    """create_tool_calling_agent 多轮工具循环 + create_react_agent + AgentExecutor。"""

    @tool
    def add(a: int, b: int) -> int:
        """两数相加。"""
        return a + b

    llm = _FakeToolCallingLLM([
        {"tool_calls": [{"id": "c1", "name": "add", "arguments": {"a": 1, "b": 2}}]},
        {"content": "结果是 3"},
    ])
    agent = create_tool_calling_agent(llm, Toolkit([add]), max_iterations=5)
    out = agent.invoke({"messages": [{"role": "user", "content": "1+2=?"}]})
    assert out["output"] == "结果是 3"
    roles = [m["role"] for m in llm.history[-1] if isinstance(m, dict) and "role" in m]
    assert "tool" in roles, f"工具结果应回传模型，实际 roles={roles}"

    react = create_react_agent(FakeLLM(["答案"]), Toolkit([add]), max_iterations=3)
    assert react.invoke({"question": "hi"})["output"]

    ex = AgentExecutor(agent=create_tool_calling_agent(
        _FakeToolCallingLLM([{"content": "ok"}]),
        Toolkit([add]),
    ))
    ex_out = ex.invoke({"messages": [{"role": "user", "content": "hi"}]})
    assert ex_out["output"] == "ok"


# -- C14 openapi ---------------------------------------------------------------
class _ApiHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — stdlib 命名
        body = b'{"item": "book"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:  # noqa: ANN401
        pass


_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "demo", "version": "1"},
    "paths": {
        "/items": {
            "get": {
                "operationId": "get_item",
                "responses": {"200": {"description": "ok"}},
            }
        }
    },
}


def test_c14_openapi() -> None:
    """from_openapi 从 spec 建工具并调用（本地 mock server）。"""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ApiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        tools = from_openapi(_SPEC, base_url=base_url)
        by_name = {t.name: t for t in tools}
        assert set(by_name) == {"get_item"}
        result = by_name["get_item"].invoke({"tool_call": {"id": "c1", "arguments": {}}})
        assert "book" in str(result)
    finally:
        server.shutdown()


# -- C15 callback --------------------------------------------------------------
def test_c15_callback() -> None:
    """CallbackManager 事件 + ChainStats/SegmentStat 统计。"""
    events: list[tuple[str, str]] = []

    class _Recorder:
        def on_chain_start(self, runnable, state, **kw) -> None:  # noqa: ANN001
            events.append(("start", runnable.id))

        def on_chain_end(self, runnable, output, duration_ms, **kw) -> None:  # noqa: ANN001
            events.append(("end", runnable.id))

    mgr = CallbackManager([_Recorder()])
    a = RunnableLambda(lambda s: {"x": 1}, reads={"n"}, writes={"x"}, name="a")
    b = RunnableLambda(lambda s: {"y": s["x"] + 1}, reads={"x"}, writes={"y"}, name="b")
    chain = Pipeline([a, b])
    instrument_pipeline(chain, mgr)
    chain.invoke({"n": 0})
    assert len(events) >= 2, f"callback 应记录事件，实际 {events}"

    chain2 = Pipeline([a])
    instrument_pipeline(chain2)
    chain2.invoke({"n": 1})
    stats = chain2._stats  # type: ignore[attr-defined]
    assert stats is not None and stats.segments
    assert all(isinstance(s, SegmentStat) for s in stats.segments.values())


# -- C16 链 → Driver 图 ---------------------------------------------------------
def test_c16_chain_to_graph(e2e_host: DriverHost) -> None:
    """Pipeline.to_graph(host) 链驱动力下输出与 fallback 一致。"""
    chain = (
        RunnableLambda(lambda s: {"x": s["n"] + 1}, reads={"n"}, writes={"x"})
        | RunnableLambda(lambda s: {"y": s["x"] * 10}, reads={"x"}, writes={"y"})
    )
    fb = chain.invoke({"n": 2})
    graph = chain.to_graph(host=e2e_host, graph_id="g_c16")
    out = graph.invoke("run", {"n": 2})
    assert out["y"] == fb["y"] == 30
