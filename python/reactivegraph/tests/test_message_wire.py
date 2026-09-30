"""Messages must cross the RGP/1 Driver boundary.

Real DeerFlow state is ``{"messages": [BaseMessage, ...]}``. The RGP/1
canonical value set has no class instances, so the host boundary converts
messages to a tagged canonical dict and restores them on the way back. Both
the state store and the patch pre-image hashes must see the *same* bytes on
the Python and TypeScript sides, otherwise transactions fail pre-image checks.
"""

from __future__ import annotations

import os
from pathlib import Path

from reactivegraph.create_agent import create_agent
from reactivegraph.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from reactivegraph.protocol import encode_value
from reactivegraph.wire import from_wire_value, to_wire_value

REPO_ROOT = Path(__file__).resolve().parents[3]


def _driver_env() -> dict:
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(
        REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    )
    env["REACTIVEGRAPH_NODE_BIN"] = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    return env


class _FakeModel:
    def __init__(self, responses: list[AIMessage]) -> None:
        self._responses = list(responses)

    def bind_tools(self, tools, **kwargs):  # noqa: ANN001, ANN003
        return self

    def invoke(self, messages, **kwargs):  # noqa: ANN001, ANN003
        return self._responses.pop(0)


def _call_message() -> AIMessage:
    return AIMessage(
        content="",
        additional_kwargs={"provider": "fake"},
        id="ai-1",
        tool_calls=[
            {"name": "add", "args": {"a": 1, "b": 2}, "id": "c1", "type": "tool_call"}
        ],
    )


class TestWireTransform:
    def test_messages_become_canonical_tagged_dicts(self) -> None:
        value = {"messages": [HumanMessage(content="hi", id="h1")]}
        wire = to_wire_value(value)
        # Must satisfy the canonical codec (this is the RED assertion today).
        encode_value(wire)
        assert wire == {
            "messages": [
                {
                    "__type__": "reactivegraph_message",
                    "value": {
                        "type": "human",
                        "data": {
                            "content": "hi",
                            "additional_kwargs": {},
                            "response_metadata": {},
                            "type": "human",
                            "name": None,
                            "id": "h1",
                        },
                    },
                }
            ]
        }

    def test_round_trip_preserves_tool_calls_and_extras(self) -> None:
        original = _call_message()
        restored = from_wire_value(to_wire_value({"messages": [original]}))["messages"][0]
        assert isinstance(restored, AIMessage)
        assert restored.tool_calls == original.tool_calls
        assert restored.additional_kwargs == {"provider": "fake"}
        assert restored.id == "ai-1"

    def test_plain_dicts_are_untouched(self) -> None:
        value = {"a": [1, {"b": "c"}]}
        assert to_wire_value(value) == value
        assert from_wire_value(value) == value

    def test_tool_message_round_trips(self) -> None:
        original = ToolMessage(content="3", tool_call_id="c1", name="add")
        restored = from_wire_value(to_wire_value([original]))[0]
        assert isinstance(restored, ToolMessage)
        assert restored.tool_call_id == "c1"
        assert restored.name == "add"


class TestDriverMessageRoundTrip:
    def test_driver_invoke_returns_message_objects(self) -> None:
        from reactivegraph.host import DriverHost

        agent = create_agent(_FakeModel([AIMessage(content="hello")]))
        host = DriverHost(env=_driver_env())
        try:
            host.start()
            host.handshake()
            agent._graph.host = host
            out = agent.invoke({"messages": [HumanMessage(content="hi", id="h1")]})
        finally:
            host.close()
        assert [m.content for m in out["messages"]] == ["hi", "hello"]
        assert all(isinstance(m, BaseMessage) for m in out["messages"])
        assert out["messages"][0].id == "h1"

    def test_driver_tool_loop_preserves_tool_calls(self) -> None:
        from reactivegraph.host import DriverHost

        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        model = _FakeModel([_call_message(), AIMessage(content="3")])
        agent = create_agent(model, tools=[add])
        host = DriverHost(env=_driver_env())
        try:
            host.start()
            host.handshake()
            agent._graph.host = host
            out = agent.invoke({"messages": [HumanMessage(content="1+2?")]})
        finally:
            host.close()
        roles = [type(m).__name__ for m in out["messages"]]
        assert roles == ["HumanMessage", "AIMessage", "ToolMessage", "AIMessage"]
        ai = out["messages"][1]
        assert ai.tool_calls[0]["name"] == "add"
        assert out["messages"][2].content == "3"

    def test_driver_values_stream_returns_message_objects(self) -> None:
        from reactivegraph.host import DriverHost

        agent = create_agent(_FakeModel([AIMessage(content="hello")]))
        host = DriverHost(env=_driver_env())
        try:
            host.start()
            host.handshake()
            agent._graph.host = host
            frames = list(
                agent.stream(
                    {"messages": [HumanMessage(content="hi")]},
                    stream_mode="values",
                )
            )
        finally:
            host.close()
        assert frames, "expected at least one values frame"
        final = frames[-1]
        assert all(isinstance(m, BaseMessage) for m in final["messages"])
        assert [m.content for m in final["messages"]] == ["hi", "hello"]
