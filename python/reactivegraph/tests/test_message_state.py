"""Contract tests for the native message reducer used by AgentState."""

from __future__ import annotations

from functools import partial

import pytest

from reactivegraph.constants import REMOVE_ALL_MESSAGES
from reactivegraph.message_state import EphemeralValue, add_messages
from reactivegraph.messages import (
    AIMessage,
    AIMessageChunk,
    HumanMessage,
    RemoveMessage,
    ToolMessage,
)


class TestAddMessages:
    def test_appends_new_messages(self) -> None:
        left = [HumanMessage(content="hello", id="h1")]
        right = [AIMessage(content="hi", id="a1")]

        merged = add_messages(left, right)

        assert [message.id for message in merged] == ["h1", "a1"]
        assert merged[1].content == "hi"

    def test_replaces_message_with_same_id(self) -> None:
        left = [HumanMessage(content="hello", id="same")]
        right = [HumanMessage(content="hello again", id="same")]

        merged = add_messages(left, right)

        assert len(merged) == 1
        assert merged[0].content == "hello again"

    def test_remove_message_deletes_existing_id(self) -> None:
        left = [
            HumanMessage(content="hello", id="h1"),
            AIMessage(content="hi", id="a1"),
        ]

        merged = add_messages(left, [RemoveMessage(id="h1")])

        assert [message.id for message in merged] == ["a1"]

    def test_remove_unknown_id_raises(self) -> None:
        with pytest.raises(ValueError, match="doesn't exist"):
            add_messages([HumanMessage(content="hello", id="h1")], [RemoveMessage(id="missing")])

    def test_remove_all_discards_left_and_prior_right(self) -> None:
        left = [HumanMessage(content="hello", id="h1")]
        right = [
            AIMessage(content="discarded", id="a1"),
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            ToolMessage(content="kept", tool_call_id="t1", id="t1"),
        ]

        merged = add_messages(left, right)

        assert [message.id for message in merged] == ["t1"]

    def test_assigns_uuid_to_messages_without_id(self) -> None:
        merged = add_messages([], [HumanMessage(content="hello")])

        assert len(merged) == 1
        assert isinstance(merged[0].id, str)
        assert merged[0].id

    def test_chunk_is_promoted_to_message(self) -> None:
        merged = add_messages([], [AIMessageChunk(content="par", id="a1")])

        assert isinstance(merged[0], AIMessage)
        assert not isinstance(merged[0], AIMessageChunk)
        assert merged[0].content == "par"

    def test_partial_application_supports_reducer_protocol(self) -> None:
        reducer = add_messages(format=None)

        assert isinstance(reducer, partial)
        assert [message.id for message in reducer([], [HumanMessage(content="x", id="x1")])] == [
            "x1"
        ]

    def test_one_sided_arguments_raise(self) -> None:
        with pytest.raises(ValueError, match="both 'left' and 'right'"):
            add_messages([HumanMessage(content="x", id="x1")])  # type: ignore[call-arg]

    def test_unknown_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Unrecognized format"):
            add_messages([], [], format="other")  # type: ignore[arg-type]

    def test_langchain_openai_format_fails_closed_until_implemented(self) -> None:
        with pytest.raises(NotImplementedError, match="langchain-openai"):
            add_messages([], [], format="langchain-openai")


class TestEphemeralValue:
    def test_repr_and_equality(self) -> None:
        marker = EphemeralValue(int)

        assert marker.typ is int
        assert marker.guard is True
        # Upstream equality only considers `guard` (verified against langgraph 1.3.14).
        assert marker == EphemeralValue(int)
        assert marker == EphemeralValue(str)
        assert marker != EphemeralValue(int, guard=False)
