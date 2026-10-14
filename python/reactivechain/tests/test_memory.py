"""M3：记忆家族测试——窗口/摘要/token 边界 + thread 维度持久化。"""

from __future__ import annotations

import pytest

from reactivechain import ReactiveChainError
from reactivechain.llm import FakeLLM
from reactivechain.memory import (
    ConversationBufferMemory,
    ConversationBufferWindowMemory,
    ConversationSummaryMemory,
    ConversationTokenBufferMemory,
    EntityMemory,
    MemoryStore,
    SummaryBufferMemory,
)
from reactivechain.prompt import ChatPromptTemplate, MessagePlaceholder


def test_buffer_memory_save_load() -> None:
    mem = ConversationBufferMemory()
    mem.save({"input": "你好"}, {"output": "你好！"})
    out = mem.load()
    assert len(out["history"]) == 2
    assert out["history"][0].content == "你好"
    assert out["history"][1].content == "你好！"


def test_window_memory_keeps_last_k_turns() -> None:
    mem = ConversationBufferWindowMemory(k=2)
    for i in range(3):  # 3 轮，窗口保留最近 2 轮 = 4 条
        mem.save({"input": f"q{i}"}, {"output": f"a{i}"})
    history = mem.load()["history"]
    assert len(history) == 4
    assert history[0].content == "q1" and history[2].content == "q2"


def test_window_memory_invalid_k_hint() -> None:
    with pytest.raises(ReactiveChainError, match="k 必须"):
        ConversationBufferWindowMemory(k=0)


def test_memory_thread_isolation() -> None:
    mem = ConversationBufferMemory()
    mem.save({"input": "A"}, {"output": "1"}, thread_id="t1")
    mem.save({"input": "B"}, {"output": "2"}, thread_id="t2")
    assert [m.content for m in mem.load()["history"]] == []
    assert [m.content for m in mem.load({"thread": "t1"})["history"]] == []
    mem.thread_id = "t1"
    assert [m.content for m in mem.load()["history"]] == ["A", "1"]


def test_memory_store_shared_across_instances() -> None:
    store = MemoryStore()
    a = ConversationBufferMemory(store=store)
    b = ConversationBufferMemory(store=store)
    a.save({"input": "x"}, {"output": "y"})
    assert [m.content for m in b.load()["history"]] == ["x", "y"]


def test_token_buffer_memory_trims_oldest() -> None:
    mem = ConversationTokenBufferMemory(max_tokens=16)
    for _i in range(6):
        mem.save({"input": "很长的用户消息内容填充" * 3}, {"output": "ok"})
    history = mem.load()["history"]
    # 只保留能塞进预算的最新消息
    assert len(history) < 12
    assert history[-1].content == "ok"


def test_summary_memory_uses_llm() -> None:
    llm = FakeLLM(["用户问了天气"])
    mem = ConversationSummaryMemory(llm)
    mem.save({"input": "今天天气？"}, {"output": "晴天"})
    out = mem.load()
    assert "天气" in out["summary"]
    # save 之后历史被摘要折叠
    assert len(out["history"]) == 1


def test_summary_buffer_memory_window_plus_summary() -> None:
    mem = SummaryBufferMemory(k=1)
    for i in range(3):
        mem.save({"input": f"q{i}"}, {"output": f"a{i}"})
    out = mem.load()
    assert len(out["history"]) == 2  # 最近 1 轮
    assert "q0" in out["summary"] and "a0" in out["summary"]


def test_entity_memory_extracts_and_merges() -> None:
    mem = EntityMemory()
    mem.save({"input": "我叫张三，住在杭州"}, {"output": "好的"})
    mem.save({"input": "李四也住在杭州"}, {"output": "收到"})
    entities = mem.load()["entities"]
    assert "张三" in entities and "李四" in entities and "杭州" in entities


def test_persist_callback_receives_messages() -> None:
    seen: list[dict] = []

    def persist(thread_id: str, messages: list[dict]) -> None:
        seen.append({"thread": thread_id, "messages": messages})

    mem = ConversationBufferMemory(persist=persist)
    mem.save({"input": "hi"}, {"output": "hello"})
    assert seen and seen[0]["thread"] == "default"
    assert [m["content"] for m in seen[0]["messages"]] == ["hi", "hello"]


def test_memory_feeds_chat_prompt_placeholder() -> None:
    mem = ConversationBufferMemory()
    mem.save({"input": "以前的问题"}, {"output": "以前的回答"})
    tmpl = ChatPromptTemplate(
        [MessagePlaceholder("history"), ("human", "{question}")]
    )
    chain = mem | tmpl
    out = chain.invoke({"question": "新问题"})
    assert out["messages"] == [
        {"role": "user", "content": "以前的问题"},
        {"role": "assistant", "content": "以前的回答"},
        {"role": "user", "content": "新问题"},
    ]


def test_memory_clear() -> None:
    mem = ConversationBufferMemory()
    mem.save({"input": "a"}, {"output": "b"})
    mem.store.clear()
    assert mem.load()["history"] == []