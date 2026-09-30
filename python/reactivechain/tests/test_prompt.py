"""M2：消息模型 + 提示模板家族测试。"""

from __future__ import annotations

import pytest

from reactivechain import ReactiveChainError, RunnableLambda
from reactivechain.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
    messages_to_api,
)
from reactivechain.prompt import (
    ChatPromptTemplate,
    FewShotPromptTemplate,
    MessagePlaceholder,
    PipelinePromptTemplate,
    PromptTemplate,
)


def test_message_roles_and_serialization() -> None:
    msgs = [
        SystemMessage("你是助手"),
        HumanMessage("你好"),
        AIMessage("有什么可以帮你？"),
        ToolMessage("42", tool_call_id="call_1"),
    ]
    api = messages_to_api(msgs)
    assert api == [
        {"role": "system", "content": "你是助手"},
        {"role": "user", "content": "你好"},
        {"role": "assistant", "content": "有什么可以帮你？"},
        {"role": "tool", "content": "42", "tool_call_id": "call_1"},
    ]


def test_ai_message_tool_calls_roundtrip() -> None:
    ai = AIMessage(
        "",
        tool_calls=[
            {"id": "c1", "type": "function", "function": {"name": "calc", "arguments": "{}"}}
        ],
    )
    api = ai.to_api_dict()
    assert api["tool_calls"][0]["function"]["name"] == "calc"
    restored = AIMessage.from_api_message(api)
    assert restored.tool_calls[0]["id"] == "c1"


def test_prompt_template_formats_variables() -> None:
    tmpl = PromptTemplate("请用{lang}回答：{question}")
    assert tmpl.invoke({"lang": "中文", "question": "你好"}) == {
        "prompt": "请用中文回答：你好"
    }


def test_prompt_template_autodiscovers_reads() -> None:
    tmpl = PromptTemplate("{a}与{b}")
    assert tmpl.reads == {"a", "b"}


def test_prompt_template_missing_var_hint() -> None:
    tmpl = PromptTemplate("{a}{b}")
    with pytest.raises(ReactiveChainError, match="缺少变量"):
        tmpl.invoke({"a": 1})


def test_prompt_template_partial_variables() -> None:
    tmpl = PromptTemplate("常驻：{x}；输入：{y}", partial_variables={"x": "固定"})
    assert tmpl.invoke({"y": 2}) == {"prompt": "常驻：固定；输入：2"}


def test_chat_prompt_template_messages() -> None:
    tmpl = ChatPromptTemplate(
        [
            ("system", "你是{lang}助手"),
            ("human", "{question}"),
        ]
    )
    out = tmpl.invoke({"lang": "中文", "question": "什么是反应式？"})
    assert out["messages"] == [
        {"role": "system", "content": "你是中文助手"},
        {"role": "user", "content": "什么是反应式？"},
    ]


def test_chat_prompt_message_placeholder() -> None:
    tmpl = ChatPromptTemplate(
        [
            ("system", "助手"),
            MessagePlaceholder("history"),
            ("human", "{question}"),
        ]
    )
    out = tmpl.invoke(
        {
            "question": "接着聊",
            "history": [HumanMessage("第一句"), AIMessage("回复一")],
        }
    )
    assert out["messages"] == [
        {"role": "system", "content": "助手"},
        {"role": "user", "content": "第一句"},
        {"role": "assistant", "content": "回复一"},
        {"role": "user", "content": "接着聊"},
    ]


def test_chat_prompt_placeholder_bad_value_hint() -> None:
    tmpl = ChatPromptTemplate([MessagePlaceholder("history"), ("human", "{q}")])
    with pytest.raises(ReactiveChainError, match="需要消息列表"):
        tmpl.invoke({"q": "x", "history": "不是列表"})


def test_few_shot_prompt() -> None:
    example_prompt = PromptTemplate("问：{q} → 答：{a}")
    tmpl = FewShotPromptTemplate(
        example_prompt,
        [
            {"q": "1+1", "a": "2"},
            {"q": "2+2", "a": "4"},
        ],
        "示例：\n{examples}\n现在：{q}",
    )
    out = tmpl.invoke({"q": "3+3"})
    assert "问：1+1 → 答：2" in out["prompt"]
    assert "现在：3+3" in out["prompt"]


def test_pipeline_prompt_template() -> None:
    system_tmpl = PromptTemplate("你是{lang}专家")
    tmpl = PipelinePromptTemplate(
        PromptTemplate("{system}  现在回答问题：{q}"),
        {"system": system_tmpl},
    )
    out = tmpl.invoke({"lang": "Python", "q": "什么是装饰器？"})
    assert out["prompt"] == "你是Python专家  现在回答问题：什么是装饰器？"


def test_prompt_is_runnable_in_pipeline() -> None:
    chain = PromptTemplate("值={v}") | RunnableLambda(
        lambda s: {"len": len(s["prompt"])}, reads={"prompt"}, writes={"len"}
    )
    out = chain.invoke({"v": "abc"})
    assert out["len"] == len("值=abc")


def test_str_or_prompt_creates_pipeline() -> None:
    step = RunnableLambda(lambda s: {"ok": s["prompt"] == "你好"}, reads={"prompt"}, writes={"ok"})
    chain = "你好" | step
    assert chain.invoke({}) == {"ok": True}


def test_placeholder_messages_accept_api_dicts() -> None:
    tmpl = ChatPromptTemplate([MessagePlaceholder("history"), ("human", "{q}")])
    out = tmpl.invoke({"q": "x", "history": [{"role": "user", "content": "旧消息"}]})
    assert out["messages"][0] == {"role": "user", "content": "旧消息"}