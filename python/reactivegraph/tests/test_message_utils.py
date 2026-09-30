"""Parity tests for the engine-owned message utility surface."""

from __future__ import annotations

import pytest

from reactivegraph.message_utils import (
    count_tokens_approximately,
    get_buffer_string,
    trim_messages,
)
from reactivegraph.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage


def test_xml_buffer_escapes_content_and_attributes() -> None:
    rendered = get_buffer_string(
        [
            HumanMessage(content="x < y & z > 1"),
            AIMessage(
                content="calling",
                tool_calls=[{"id": "call<1>", "name": "search", "args": {"q": "a&b"}}],
            ),
        ],
        format="xml",
    )
    assert 'type="human">x &lt; y &amp; z &gt; 1</message>' in rendered
    assert 'id="call&lt;1&gt;"' in rendered
    assert 'name="search"' in rendered
    assert '{"q": "a&amp;b"}' in rendered


def test_xml_buffer_supports_multimodal_urls_and_skips_base64() -> None:
    rendered = get_buffer_string(
        [
            HumanMessage(
                content=[
                    {"type": "text", "text": "look"},
                    {"type": "image", "url": "https://example.test/a?x=1&y=2"},
                    {"type": "image", "base64": "AAAA"},
                ]
            )
        ],
        format="xml",
    )
    assert "look" in rendered
    assert 'url="https://example.test/a?x=1&amp;y=2"' in rendered
    assert "AAAA" not in rendered


def test_invalid_buffer_format_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unrecognized format"):
        get_buffer_string([HumanMessage(content="x")], format="bogus")  # type: ignore[arg-type]


def test_approximate_counter_matches_known_contract() -> None:
    messages = [
        HumanMessage(content="hello"),
        AIMessage(
            content="",
            tool_calls=[{"id": "1", "name": "t", "args": {"x": 1}}],
        ),
        ToolMessage(content="result", tool_call_id="1"),
    ]
    assert count_tokens_approximately(messages) > 0
    assert count_tokens_approximately(messages, extra_tokens_per_message=0) > 0
    assert count_tokens_approximately(messages, tokens_per_image=10) == (
        count_tokens_approximately(messages)
    )


def test_trim_last_preserves_system_and_starts_on_human() -> None:
    messages = [
        SystemMessage(content="system"),
        HumanMessage(content="old"),
        AIMessage(content="old answer"),
        HumanMessage(content="recent"),
        AIMessage(content="recent answer"),
    ]
    trimmed = trim_messages(
        messages,
        max_tokens=3,
        token_counter=len,
        strategy="last",
        include_system=True,
        start_on="human",
    )
    assert [message.content for message in trimmed] == ["system", "recent", "recent answer"]


def test_trim_partial_text_keeps_a_prefix_or_suffix() -> None:
    messages = [HumanMessage(content="one\ntwo\nthree")]
    first = trim_messages(
        messages,
        max_tokens=5,
        token_counter=count_tokens_approximately,
        strategy="first",
        allow_partial=True,
    )
    last = trim_messages(
        messages,
        max_tokens=7,
        token_counter=count_tokens_approximately,
        strategy="last",
        allow_partial=True,
    )
    assert first[0].content in {"one\n", "one\ntwo\n"}
    assert last[0].content in {"three", "two\nthree"}


def test_trim_callable_annotation_classification_matches_upstream() -> None:
    def one(message: object) -> int:
        return 1

    # A string annotation (as produced by ``from __future__ import
    # annotations``) is a list counter upstream, even if it spells
    # ``BaseMessage``. Match that boundary rather than guessing at runtime.
    one.__annotations__["message"] = "BaseMessage"
    trimmed = trim_messages(
        [HumanMessage(content="a"), AIMessage(content="b")],
        max_tokens=1,
        token_counter=one,
    )
    assert [message.content for message in trimmed] == ["a", "b"]


def test_optional_differential_parity_with_langchain_core() -> None:
    upstream = pytest.importorskip("langchain_core.messages.utils")
    lc = pytest.importorskip("langchain_core.messages")

    ours = [
        HumanMessage(content="x < y"),
        AIMessage(
            content="ok",
            tool_calls=[{"id": "1", "name": "search", "args": {"q": "a&b"}}],
        ),
    ]
    theirs = [
        lc.HumanMessage(content="x < y"),
        lc.AIMessage(
            content="ok",
            tool_calls=[{"id": "1", "name": "search", "args": {"q": "a&b"}}],
        ),
    ]
    assert get_buffer_string(ours, format="xml") == upstream.get_buffer_string(theirs, format="xml")
    assert count_tokens_approximately(ours) == upstream.count_tokens_approximately(theirs)


class _ForeignMessage:
    """Structural foreign message: no native BaseMessage subclass."""

    def __init__(self, kind: str, content: object, **extra: object) -> None:
        self.type = kind
        self.content = content
        self.__dict__.update(extra)

    def model_copy(self, *, deep: bool = False) -> _ForeignMessage:
        return type(self)(
            self.type,
            self.content,
            **{k: v for k, v in self.__dict__.items() if k not in {"type", "content", "role"}},
        )


class _FunctionMessage(_ForeignMessage):
    def __init__(self, content: object) -> None:
        super().__init__("function", content)


def test_message_container_is_unpacked_through_to_messages() -> None:
    class Container:
        def to_messages(self) -> list[HumanMessage]:
            return [HumanMessage(content="from container")]

    assert get_buffer_string(Container().to_messages()) == "Human: from container"
    assert count_tokens_approximately(Container()) > 0


def test_foreign_and_legacy_message_types_render_like_upstream() -> None:
    function = _FunctionMessage("legacy")
    chat = _ForeignMessage("chat", "hola", role="assistant")
    assert get_buffer_string([function, chat]) == "Function: legacy\nassistant: hola"
    assert count_tokens_approximately([function, chat]) > 0


def test_xml_renders_reasoning_media_server_blocks_and_function_calls() -> None:
    message = AIMessage(
        content=[
            "plain",
            {"type": "reasoning", "reasoning": "because <x>"},
            {"type": "image", "file_id": "img-1"},
            {"type": "audio", "url": "https://example.test/a.mp3"},
            {"type": "video", "file_id": "vid-1"},
            {"type": "image_url", "image_url": {"url": "https://example.test/i.png"}},
            {"type": "text-plain", "text": "tail"},
            {"type": "server_tool_call", "id": "t1", "name": "lookup", "args": {"q": "x"}},
            {
                "type": "server_tool_result",
                "tool_call_id": "t1",
                "status": "ok",
                "output": {"n": 1},
            },
        ],
        additional_kwargs={"function_call": {"name": "legacy", "arguments": '{"a": 1}'}},
    )
    rendered = get_buffer_string([message], format="xml")
    assert "<reasoning>because &lt;x&gt;</reasoning>" in rendered
    assert '<image file_id="img-1" />' in rendered
    assert '<audio url="https://example.test/a.mp3" />' in rendered
    assert '<video file_id="vid-1" />' in rendered
    assert '<image url="https://example.test/i.png" />' in rendered
    assert "tail" in rendered
    assert '<server_tool_call id="t1" name="lookup">' in rendered
    assert '<server_tool_result tool_call_id="t1" status="ok">' in rendered
    assert '<function_call name="legacy">' in rendered


def test_approximate_counter_scales_to_the_last_ai_usage() -> None:
    messages = [
        HumanMessage(content="x" * 400),
        AIMessage(
            content="ok",
            response_metadata={"model_provider": "openai"},
            usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 300},
        ),
    ]
    scaled = count_tokens_approximately(messages, use_usage_metadata_scaling=True)
    unscaled = count_tokens_approximately(messages)
    assert scaled > unscaled


def test_approximate_counter_invalidates_mixed_model_providers() -> None:
    messages = [
        AIMessage(
            content="a",
            response_metadata={"model_provider": "openai"},
            usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 50},
        ),
        AIMessage(
            content="b",
            response_metadata={"model_provider": "anthropic"},
            usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 50},
        ),
    ]
    assert count_tokens_approximately(
        messages, use_usage_metadata_scaling=True
    ) == count_tokens_approximately(messages)


def test_trim_supports_model_shortcut_string_splitter_and_validation() -> None:
    class Counter:
        def get_num_tokens_from_messages(self, messages: list[object]) -> int:
            return len(messages)

    class Splitter:
        def split_text(self, text: str) -> list[str]:
            return text.split("|")

    messages = [HumanMessage(content="a|b|c")]
    assert len(
        trim_messages(
            messages,
            max_tokens=1,
            token_counter=Counter(),
            allow_partial=True,
            text_splitter=Splitter(),
        )
    ) == 1
    assert trim_messages(
        messages, max_tokens=10, token_counter="approximate"
    ) == messages
    with pytest.raises(ValueError, match="Invalid token_counter"):
        trim_messages(messages, max_tokens=10, token_counter="nope")
    with pytest.raises(ValueError, match="get_num_tokens_from_messages"):
        trim_messages(messages, max_tokens=10, token_counter=object())
    with pytest.raises(ValueError, match="start_on"):
        trim_messages(
            messages,
            max_tokens=10,
            token_counter=len,
            strategy="first",
            start_on="human",
        )
    with pytest.raises(ValueError, match="include_system"):
        trim_messages(
            messages,
            max_tokens=10,
            token_counter=len,
            strategy="first",
            include_system=True,
        )
    with pytest.raises(ValueError, match="Unrecognized strategy"):
        trim_messages(messages, max_tokens=10, token_counter=len, strategy="middle")
