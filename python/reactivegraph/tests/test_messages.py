"""Message layer pinned against real ``langchain_core.messages``.

DeerFlow imports messages in 79 places — more than any other single module —
so this is the foundation the agent/middleware port sits on. The contract is
the observable upstream behaviour: field names, ``model_dump()`` shape,
``type`` discriminators, ``text`` access, chunk merging, and the conversion
helpers the harness calls directly.
"""

from __future__ import annotations

import pytest


class TestBaseMessageShape:
    def test_human_message_dumps_upstream_shape(self) -> None:
        from reactivegraph.messages import HumanMessage

        message = HumanMessage("hi")
        assert message.model_dump() == {
            "content": "hi",
            "additional_kwargs": {},
            "response_metadata": {},
            "type": "human",
            "name": None,
            "id": None,
        }

    def test_type_discriminators_match_upstream(self) -> None:
        from reactivegraph.messages import (
            AIMessage,
            AIMessageChunk,
            HumanMessage,
            RemoveMessage,
            SystemMessage,
            ToolMessage,
        )

        assert HumanMessage("x").type == "human"
        assert AIMessage("x").type == "ai"
        assert SystemMessage("x").type == "system"
        assert ToolMessage("x", tool_call_id="c1").type == "tool"
        assert AIMessageChunk("x").type == "AIMessageChunk"
        assert RemoveMessage(id="m1").type == "remove"

    def test_numeric_id_is_coerced_to_str(self) -> None:
        from reactivegraph.messages import SystemMessage

        assert SystemMessage("sys", id=3).id == "3"

    def test_extra_fields_are_preserved_and_dumped(self) -> None:
        from reactivegraph.messages import SystemMessage

        message = SystemMessage(content="sys", tone="x")
        assert message.tone == "x"
        assert message.model_dump()["tone"] == "x"

    def test_equality_ignores_identity_but_includes_content(self) -> None:
        from reactivegraph.messages import HumanMessage

        assert HumanMessage("hi") == HumanMessage("hi")
        assert HumanMessage("hi") != HumanMessage("hi", id="x")
        assert HumanMessage("hi") != HumanMessage("other")

    def test_messages_are_unhashable_like_pydantic_models(self) -> None:
        from reactivegraph.messages import HumanMessage

        with pytest.raises(TypeError):
            hash(HumanMessage("hi"))

    def test_repr_omits_none_id(self) -> None:
        from reactivegraph.messages import HumanMessage

        assert repr(HumanMessage("hi")) == (
            "HumanMessage(content='hi', additional_kwargs={}, response_metadata={})"
        )

    def test_content_is_required_like_upstream(self) -> None:
        from reactivegraph.messages import AIMessage, HumanMessage

        with pytest.raises(Exception):  # noqa: B017 - upstream raises ValidationError
            HumanMessage()
        with pytest.raises(Exception):  # noqa: B017
            HumanMessage(None)
        with pytest.raises(Exception):  # noqa: B017
            AIMessage()


class TestTextAccess:
    def test_text_property_returns_string_content(self) -> None:
        from reactivegraph.messages import HumanMessage

        assert HumanMessage("hi").text == "hi"

    def test_text_extracts_only_text_blocks(self) -> None:
        from reactivegraph.messages import HumanMessage

        message = HumanMessage(
            [{"type": "text", "text": "a"}, {"type": "image_url", "image_url": {}}, "b"]
        )
        assert message.text == "ab"

    def test_text_is_still_callable_with_a_deprecation_warning(self) -> None:
        from reactivegraph.messages import HumanMessage

        with pytest.warns(DeprecationWarning):
            assert HumanMessage("hi").text() == "hi"


class TestAIMessage:
    def test_ai_message_defaults_match_upstream(self) -> None:
        from reactivegraph.messages import AIMessage

        dump = AIMessage("a").model_dump()
        assert dump["tool_calls"] == []
        assert dump["invalid_tool_calls"] == []
        assert dump["usage_metadata"] is None

    def test_ai_message_accepts_dict_tool_calls_and_tags_them(self) -> None:
        from reactivegraph.messages import AIMessage

        message = AIMessage("a", tool_calls=[{"name": "t", "args": {"x": 1}, "id": "c1"}])
        assert message.tool_calls == [
            {"name": "t", "args": {"x": 1}, "id": "c1", "type": "tool_call"}
        ]

    def test_ai_message_rejects_string_tool_call_args(self) -> None:
        from reactivegraph.messages import AIMessage

        with pytest.raises(Exception):  # noqa: B017 - upstream raises ValidationError
            AIMessage("a", tool_calls=[{"name": "t", "args": "{}", "id": "c1"}])

    def test_ai_message_rejects_malformed_json_tool_call_args(self) -> None:
        from reactivegraph.messages import AIMessage

        with pytest.raises(Exception):  # noqa: B017
            AIMessage("a", tool_calls=[{"name": "t", "args": "{bad", "id": "c1"}])

    def test_tool_call_missing_name_raises_type_error(self) -> None:
        from reactivegraph.messages import AIMessage

        with pytest.raises(TypeError):
            AIMessage("a", tool_calls=[{"args": {}, "id": "c1"}])

    def test_ai_message_keeps_invalid_tool_calls(self) -> None:
        from reactivegraph.messages import AIMessage

        message = AIMessage(
            "a",
            invalid_tool_calls=[{"name": "t", "args": "not json", "id": "c1", "error": "bad"}],
        )
        assert message.invalid_tool_calls[0]["name"] == "t"

    def test_invalid_tool_call_dump_key_order_matches_upstream(self) -> None:
        from reactivegraph.messages import AIMessage

        message = AIMessage(
            "a",
            invalid_tool_calls=[{"name": "t", "args": "not json", "id": "c1", "error": "bad"}],
        )
        assert list(message.invalid_tool_calls[0]) == [
            "type",
            "id",
            "name",
            "args",
            "error",
        ]

    def test_legacy_additional_kwargs_tool_calls_are_parsed(self) -> None:
        from reactivegraph.messages import AIMessage

        message = AIMessage(
            "a",
            additional_kwargs={
                "tool_calls": [
                    {
                        "id": "c1",
                        "function": {"name": "t", "arguments": '{"x": 1}'},
                    }
                ]
            },
        )
        assert message.tool_calls == [
            {"name": "t", "args": {"x": 1}, "id": "c1", "type": "tool_call"}
        ]

    def test_legacy_additional_kwargs_bad_json_becomes_invalid(self) -> None:
        from reactivegraph.messages import AIMessage

        message = AIMessage(
            "a",
            additional_kwargs={
                "tool_calls": [{"id": "c1", "function": {"name": "t", "arguments": "{bad"}}]
            },
        )
        assert message.tool_calls == []
        assert message.invalid_tool_calls[0]["name"] == "t"
        assert message.invalid_tool_calls[0]["args"] == "{bad"


class TestToolMessage:
    def test_tool_call_id_is_required(self) -> None:
        from reactivegraph.messages import ToolMessage

        with pytest.raises(Exception):  # noqa: B017 - upstream raises ValidationError
            ToolMessage("x")

    def test_status_defaults_to_success_and_artifact_is_optional(self) -> None:
        from reactivegraph.messages import ToolMessage

        message = ToolMessage("x", tool_call_id="c1")
        assert message.status == "success"
        assert message.artifact is None

    def test_status_rejects_unknown_values(self) -> None:
        from reactivegraph.messages import ToolMessage

        with pytest.raises(Exception):  # noqa: B017
            ToolMessage("x", tool_call_id="c1", status="nope")

    def test_dump_matches_upstream_field_order(self) -> None:
        from reactivegraph.messages import ToolMessage

        dump = ToolMessage("res", tool_call_id="c1", name="t", artifact={"k": 1}).model_dump()
        assert list(dump) == [
            "content",
            "additional_kwargs",
            "response_metadata",
            "type",
            "name",
            "id",
            "tool_call_id",
            "artifact",
            "status",
        ]

    def test_missing_tool_call_id_raises_key_error(self) -> None:
        from reactivegraph.messages import ToolMessage

        with pytest.raises(KeyError):
            ToolMessage("x")

    def test_non_string_content_is_coerced(self) -> None:
        from reactivegraph.messages import ToolMessage

        assert ToolMessage(5, tool_call_id="c1").content == "5"
        assert ToolMessage([1, {"k": 1}], tool_call_id="c1").content == ["1", {"k": 1}]

    def test_repr_omits_unused_default_fields(self) -> None:
        from reactivegraph.messages import ToolMessage, ToolMessageChunk

        assert repr(ToolMessage("res", tool_call_id="c1")) == (
            "ToolMessage(content='res', tool_call_id='c1')"
        )
        assert repr(ToolMessageChunk("r", tool_call_id="c")) == (
            "ToolMessageChunk(content='r', tool_call_id='c')"
        )


class TestRemoveMessage:
    def test_remove_message_dump(self) -> None:
        from reactivegraph.messages import RemoveMessage

        assert RemoveMessage(id="m1").model_dump() == {
            "content": "",
            "additional_kwargs": {},
            "response_metadata": {},
            "type": "remove",
            "name": None,
            "id": "m1",
        }

    def test_non_empty_content_is_rejected(self) -> None:
        from reactivegraph.messages import RemoveMessage

        with pytest.raises(ValueError, match="does not support 'content'"):
            RemoveMessage(id="m1", content="nope")

    def test_empty_content_is_accepted(self) -> None:
        from reactivegraph.messages import RemoveMessage

        assert RemoveMessage(id="m1", content="").content == ""

    def test_repr_includes_empty_content(self) -> None:
        from reactivegraph.messages import RemoveMessage

        assert repr(RemoveMessage(id="m1")) == (
            "RemoveMessage(content='', additional_kwargs={}, response_metadata={}, id='m1')"
        )


class TestChunks:
    def test_chunk_merge_concatenates_content_and_keeps_first_id(self) -> None:
        from reactivegraph.messages import AIMessageChunk

        merged = AIMessageChunk("he", id="x") + AIMessageChunk("llo")
        assert merged.content == "hello"
        assert merged.id == "x"
        assert isinstance(merged, AIMessageChunk)

    def test_chunk_merge_keeps_first_usage_metadata(self) -> None:
        from reactivegraph.messages import AIMessageChunk

        usage = {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        merged = AIMessageChunk("a", usage_metadata=usage) + AIMessageChunk("b")
        assert merged.usage_metadata == usage

    def test_chunk_plus_full_message_is_a_type_error(self) -> None:
        from reactivegraph.messages import AIMessage, AIMessageChunk

        with pytest.raises(TypeError):
            AIMessageChunk("a") + AIMessage("b")

    def test_message_chunk_to_message_promotes_ai_chunk(self) -> None:
        from reactivegraph.messages import AIMessage, AIMessageChunk, message_chunk_to_message

        promoted = message_chunk_to_message(AIMessageChunk("he", id="x"))
        assert isinstance(promoted, AIMessage)
        assert not isinstance(promoted, AIMessageChunk)
        assert promoted.model_dump()["tool_calls"] == []

    def test_message_chunk_to_message_promotes_tool_chunk(self) -> None:
        from reactivegraph.messages import ToolMessage, ToolMessageChunk, message_chunk_to_message

        promoted = message_chunk_to_message(ToolMessageChunk("r", tool_call_id="c"))
        assert isinstance(promoted, ToolMessage)
        assert not isinstance(promoted, ToolMessageChunk)
        assert promoted.tool_call_id == "c"
        assert promoted.content == "r"


class TestConversionHelpers:
    def test_convert_to_messages_accepts_str_tuple_and_dict(self) -> None:
        from reactivegraph.messages import HumanMessage, convert_to_messages

        converted = convert_to_messages(
            ["hello", ("user", "hi"), {"role": "user", "content": "yo"}]
        )
        assert [type(m) for m in converted] == [HumanMessage, HumanMessage, HumanMessage]
        assert [m.content for m in converted] == ["hello", "hi", "yo"]

    def test_convert_to_messages_maps_known_roles(self) -> None:
        from reactivegraph.messages import (
            AIMessage,
            SystemMessage,
            ToolMessage,
            convert_to_messages,
        )

        converted = convert_to_messages(
            [
                {"role": "system", "content": "s"},
                {"role": "assistant", "content": "a"},
                {"role": "tool", "content": "t", "tool_call_id": "c1"},
            ]
        )
        assert isinstance(converted[0], SystemMessage)
        assert isinstance(converted[1], AIMessage)
        assert isinstance(converted[2], ToolMessage)

    def test_convert_to_messages_rejects_unknown_role(self) -> None:
        from reactivegraph.messages import convert_to_messages

        with pytest.raises(ValueError):
            convert_to_messages([{"role": "wizard", "content": "x"}])

    def test_convert_to_messages_tool_without_id_raises_key_error(self) -> None:
        from reactivegraph.messages import convert_to_messages

        with pytest.raises(KeyError):
            convert_to_messages([("tool", "t")])

    def test_get_buffer_string_matches_upstream_format(self) -> None:
        from reactivegraph.messages import AIMessage, HumanMessage, get_buffer_string

        assert get_buffer_string([HumanMessage("hi"), AIMessage("a")]) == "Human: hi\nAI: a"


class TestDifferentialParityRound2:
    """Gaps found by the second differential probe against langchain-core 1.4.9."""

    def test_partial_json_tool_call_chunks_parse(self) -> None:
        from reactivegraph.messages import AIMessageChunk

        chunk = AIMessageChunk(
            "a", tool_call_chunks=[{"name": "t", "args": '{"x":', "id": "c", "index": 0}]
        )
        assert chunk.tool_calls == [{"name": "t", "args": {}, "id": "c", "type": "tool_call"}]
        assert chunk.invalid_tool_calls == []

    def test_chunk_additional_kwargs_tool_calls_are_parsed(self) -> None:
        from reactivegraph.messages import AIMessageChunk

        chunk = AIMessageChunk(
            "a",
            additional_kwargs={
                "tool_calls": [{"id": "c", "function": {"name": "t", "arguments": '{"x":1}'}}]
            },
        )
        assert chunk.tool_calls == [{"name": "t", "args": {"x": 1}, "id": "c", "type": "tool_call"}]
        assert chunk.tool_call_chunks == [
            {"name": "t", "args": '{"x":1}', "id": "c", "index": None, "type": "tool_call_chunk"}
        ]

    def test_content_blocks_translate_text_and_tool_calls(self) -> None:
        from reactivegraph.messages import AIMessage, HumanMessage

        assert HumanMessage("a").content_blocks == [{"type": "text", "text": "a"}]
        assert HumanMessage("").content_blocks == []
        ai = AIMessage("a", tool_calls=[{"name": "t", "args": {"x": 1}, "id": "c"}])
        assert ai.content_blocks == [
            {"type": "text", "text": "a"},
            {"type": "tool_call", "id": "c", "name": "t", "args": {"x": 1}},
        ]

    def test_content_blocks_wrap_unknown_types_as_non_standard(self) -> None:
        from reactivegraph.messages import HumanMessage

        assert HumanMessage([{"type": "mystery", "x": 1}]).content_blocks == [
            {"type": "non_standard", "value": {"type": "mystery", "x": 1}}
        ]

    def test_tool_chunk_merge_merges_artifacts(self) -> None:
        from reactivegraph.messages import ToolMessageChunk

        merged = ToolMessageChunk("a", tool_call_id="c", artifact={"x": 1}) + ToolMessageChunk(
            "b", tool_call_id="c", artifact={"y": 2}
        )
        assert merged.artifact == {"x": 1, "y": 2}

    def test_subtract_usage_none_none_returns_zeroes(self) -> None:
        from reactivegraph.messages import subtract_usage

        assert subtract_usage(None, None) == {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
        }

    def test_constructor_envelope_maps_chunk_names_to_full_messages(self) -> None:
        from reactivegraph.messages import AIMessage, convert_to_messages

        converted = convert_to_messages(
            [
                {
                    "lc": 1,
                    "type": "constructor",
                    "id": ["langchain", "schema", "messages", "AIMessageChunk"],
                    "kwargs": {"content": "hi"},
                }
            ]
        )
        assert isinstance(converted[0], AIMessage)


class TestForeignMessageInterop:
    """Hosts hand us *their* message objects; we must not choke on them.

    DeerFlow passes ``langchain_core`` messages straight into the engine, and
    real chat models hand ``langchain_core`` messages back. The two class
    hierarchies are disjoint (LangChain blocks virtual-subclassing for
    ``BaseMessage``), so the only correct bridge is an explicit structural
    conversion at the boundary.

    These tests use a local stand-in rather than importing langchain_core: the
    engine must stay dependency-free, and the contract is *structural* - any
    object exposing ``type``/``content``/``model_dump()`` in the upstream shape
    must convert.
    """

    class ForeignMessage:
        """Mimics ``langchain_core.messages``'s duck-typed surface."""

        def __init__(self, type_: str, content: str, **extra: object) -> None:
            self.type = type_
            self.content = content
            self._extra = extra

        def model_dump(self) -> dict:
            return {
                "content": self.content,
                "additional_kwargs": {},
                "response_metadata": {},
                "type": self.type,
                "name": None,
                "id": None,
                **self._extra,
            }

    def test_foreign_human_message_converts_to_our_class(self) -> None:
        from reactivegraph.messages import HumanMessage, convert_to_messages

        (converted,) = convert_to_messages([self.ForeignMessage("human", "hi")])
        assert isinstance(converted, HumanMessage)
        assert converted.content == "hi"

    def test_foreign_ai_message_preserves_tool_calls(self) -> None:
        from reactivegraph.messages import AIMessage, convert_to_messages

        foreign = self.ForeignMessage(
            "ai",
            "",
            tool_calls=[
                {"name": "add", "args": {"a": 1}, "id": "c1", "type": "tool_call"}
            ],
            invalid_tool_calls=[],
        )
        (converted,) = convert_to_messages([foreign])
        assert isinstance(converted, AIMessage)
        assert converted.tool_calls == [
            {"name": "add", "args": {"a": 1}, "id": "c1", "type": "tool_call"}
        ]

    def test_foreign_tool_message_preserves_call_id(self) -> None:
        from reactivegraph.messages import ToolMessage, convert_to_messages

        foreign = self.ForeignMessage(
            "tool", "5", tool_call_id="c1", name="add", artifact=None, status="success"
        )
        (converted,) = convert_to_messages([foreign])
        assert isinstance(converted, ToolMessage)
        assert converted.tool_call_id == "c1"
        assert converted.name == "add"

    def test_foreign_ai_message_preserves_usage_metadata(self) -> None:
        """Token accounting reads ``usage_metadata`` off the adopted message.

        LangChain's own dict coercion folds a top-level ``usage_metadata`` key
        into ``additional_kwargs`` (see ``_create_message_from_message_type``),
        which is why the boundary must pass it through explicitly: DeerFlow's
        TokenBudgetMiddleware sums ``msg.usage_metadata`` and a swallowed value
        makes every budget check silently pass.
        """
        from reactivegraph.messages import AIMessage, convert_to_messages

        usage = {"input_tokens": 250_000, "output_tokens": 0, "total_tokens": 250_000}
        foreign = self.ForeignMessage("ai", "", usage_metadata=usage)
        (converted,) = convert_to_messages([foreign])
        assert isinstance(converted, AIMessage)
        assert converted.usage_metadata == usage

    def test_foreign_objects_without_message_shape_still_fail_closed(self) -> None:
        from reactivegraph.messages import convert_to_messages

        class NotAMessage:
            pass

        with pytest.raises(NotImplementedError):
            convert_to_messages([NotAMessage()])

    def test_our_messages_serialize_to_a_shape_foreign_models_accept(self) -> None:
        """The outbound bridge: a flat ``{type, **dump}`` dict.

        ``langchain_core.messages.convert_to_messages`` accepts this shape, and
        it needs no langchain_core import on our side.
        """
        from reactivegraph.messages import ToolMessage, message_to_foreign_dict

        flat = message_to_foreign_dict(
            ToolMessage(content="5", tool_call_id="c1", name="add")
        )
        assert flat["type"] == "tool"
        assert flat["content"] == "5"
        assert flat["tool_call_id"] == "c1"
        assert flat["name"] == "add"
        # The envelope key must not leak into the flat form.
        assert "data" not in flat


class TestLangChainTypeIdentity:
    """Our messages must *be* langchain-core messages when it is installed.

    DeerFlow checks ``isinstance(message, langchain_core.messages.ToolMessage)``
    (and AIMessage/HumanMessage/...) in 161 places across the harness. Editing
    every site is exactly the batch-import rewrite the migration plan forbids;
    the coupling rule requires the type and its consumers to move together.

    ``langchain_core.messages.BaseMessage`` permits real subclassing (unlike
    the virtual-registration path, which LangChain disables for performance).
    Inheriting from it therefore lets the host's identity checks cross the
    engine boundary unchanged, while the engine keeps its own behaviour
    (validators, reducers, ``model_dump`` parity).

    langchain-core stays optional: without it installed the classes fall back
    to plain pydantic models with the identical field surface.
    """

    UPSTREAM_NAMES = (
        "BaseMessage",
        "BaseMessageChunk",
        "HumanMessage",
        "HumanMessageChunk",
        "SystemMessage",
        "SystemMessageChunk",
        "ChatMessage",
        "AIMessage",
        "AIMessageChunk",
        "ToolMessage",
        "ToolMessageChunk",
        "RemoveMessage",
    )

    def test_every_message_class_subclasses_its_upstream_counterpart(self) -> None:
        lc = pytest.importorskip("langchain_core.messages")
        from reactivegraph import messages as ours

        for name in self.UPSTREAM_NAMES:
            assert issubclass(getattr(ours, name), getattr(lc, name)), name

    def test_instances_satisfy_the_upstream_isinstance_checks(self) -> None:
        lc = pytest.importorskip("langchain_core.messages")
        from reactivegraph.messages import (
            AIMessage,
            HumanMessage,
            RemoveMessage,
            SystemMessage,
            ToolMessage,
        )

        human = HumanMessage(content="hi")
        ai = AIMessage(content="ok")
        tool = ToolMessage(content="5", tool_call_id="c1")
        system = SystemMessage(content="sys")
        remove = RemoveMessage(id="m1")

        assert isinstance(human, lc.HumanMessage)
        assert isinstance(ai, lc.AIMessage)
        assert isinstance(tool, lc.ToolMessage)
        assert isinstance(system, lc.SystemMessage)
        assert isinstance(remove, lc.RemoveMessage)
        for message in (human, ai, tool, system, remove):
            assert isinstance(message, lc.BaseMessage)

    def test_upstream_dump_shape_is_unchanged_by_the_subclassing(self) -> None:
        pytest.importorskip("langchain_core.messages")
        from reactivegraph.messages import ToolMessage

        assert ToolMessage(content="5", tool_call_id="c1").model_dump() == {
            "content": "5",
            "additional_kwargs": {},
            "response_metadata": {},
            "type": "tool",
            "name": None,
            "id": None,
            "tool_call_id": "c1",
            "artifact": None,
            "status": "success",
        }
