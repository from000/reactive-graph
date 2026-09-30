"""Native prebuilt agent tests (M3-F7): ToolNode + ReAct loop."""

from __future__ import annotations

import pytest

from reactivegraph.prebuilt import (
    AgentIterationError,
    AgentState,
    ReactiveAgent,
    ToolNode,
    ToolSpec,
    create_react_agent,
)


def weather(city: str = "unknown") -> dict:
    return {"city": city, "temp_c": 20}


def always_fails(_: str = "") -> None:
    raise ValueError("boom")


class TestToolNode:
    def test_executes_batch_and_returns_tool_messages(self) -> None:
        tools = ToolNode([ToolSpec(name="weather", fn=weather, description="weather lookup")])
        msgs = tools([{"id": "c1", "name": "weather", "arguments": {"city": "Beijing"}}])
        assert msgs == [
            {
                "role": "tool",
                "tool_call_id": "c1",
                "name": "weather",
                "content": '{"city": "Beijing", "temp_c": 20}',
            }
        ]

    def test_tool_exception_becomes_error_message(self) -> None:
        tools = ToolNode([ToolSpec(name="f", fn=always_fails)])
        (msg,) = tools([{"id": "c1", "name": "f", "arguments": {}}])
        assert msg["role"] == "tool"
        assert msg["content"].startswith("Error: ValueError: boom")

    def test_unknown_tool_is_reported_not_raised(self) -> None:
        tools = ToolNode([ToolSpec(name="a", fn=lambda: None)])
        (msg,) = tools([{"id": "c1", "name": "nope", "arguments": {}}])
        assert "unknown tool" in msg["content"]

    def test_duplicate_tool_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            ToolNode([ToolSpec(name="a", fn=lambda: None), ToolSpec(name="a", fn=lambda: None)])

    def test_specs_in_openai_schema_form(self) -> None:
        tools = ToolNode([ToolSpec(name="w", fn=weather, parameters={"type": "object"})])
        (spec,) = tools.specs()
        assert spec["type"] == "function"
        assert spec["function"]["name"] == "w"


class TestReactiveAgent:
    def test_loop_runs_tools_then_returns_final_answer(self) -> None:
        def model(messages):
            if len(messages) == 1:
                return {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"id": "c1", "name": "weather", "arguments": {"city": "Paris"}}],
                }
            # second call: sees the tool result, answers
            return {"role": "assistant", "content": "It is 20C in Paris."}

        agent = create_react_agent(model, [ToolSpec(name="weather", fn=weather)])
        out = agent.invoke([{"role": "user", "content": "weather?"}])
        roles = [m["role"] for m in out["messages"]]
        assert roles == ["user", "assistant", "tool", "assistant"]
        assert out["messages"][-1]["content"] == "It is 20C in Paris."
        assert out["messages"][2]["name"] == "weather"

    def test_no_tool_calls_returns_immediately(self) -> None:
        agent = create_react_agent(lambda messages: "hi", [])
        out = agent.invoke([{"role": "user", "content": "hey"}])
        assert [m["role"] for m in out["messages"]] == ["user", "assistant"]
        assert out["messages"][-1]["content"] == "hi"

    def test_max_iterations_raises(self) -> None:
        def model(messages):
            # Always asks for a tool -> never terminates.
            return {"role": "assistant", "content": "", "tool_calls": [{"name": "a"}]}

        agent = create_react_agent(model, [ToolSpec(name="a", fn=lambda: None)], max_iterations=3)
        with pytest.raises(AgentIterationError, match="max_iterations=3"):
            agent.invoke([{"role": "user", "content": "go"}])

    def test_plain_string_model_response_wrapped_as_assistant(self) -> None:
        agent = ReactiveAgent(model=lambda messages: "ok", tools=ToolNode([]))
        out = agent.invoke([])
        assert out["messages"] == [{"role": "assistant", "content": "ok"}]

    def test_agent_recovers_from_tool_error(self) -> None:
        def model(messages):
            if len(messages) == 1:
                return {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"id": "c1", "name": "bad", "arguments": {}}],
                }
            return {"role": "assistant", "content": "handled"}

        agent = create_react_agent(model, [ToolSpec(name="bad", fn=always_fails)])
        out = agent.invoke([{"role": "user", "content": "run"}])
        assert out["messages"][2]["content"].startswith("Error")
        assert out["messages"][-1]["content"] == "handled"


class TestTokenStreaming:
    def test_stream_yields_one_messages_event_per_token(self) -> None:
        def model(messages):
            # generator: token-level stream
            yield "Hel"
            yield "lo"
            yield "!"

        agent = create_react_agent(model, [])
        events = list(agent.stream([{"role": "user", "content": "hi"}]))
        msg_events = [e for e in events if e.get("eventType") == "messages"]
        assert [e["payload"]["message"]["content"] for e in msg_events] == ["Hel", "lo", "!"]
        result = [e for e in events if e.get("eventType") == "result"][-1]
        assistant = result["payload"]["messages"][-1]
        assert assistant["role"] == "assistant"
        assert assistant["content"] == "Hello!"  # reassembled from tokens

    def test_stream_emits_tool_messages_and_final_result(self) -> None:
        def model(messages):
            if len(messages) == 1:
                return {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"id": "c1", "name": "weather", "arguments": {}}],
                }
            return "It is 20C in Paris."

        agent = create_react_agent(model, [ToolSpec(name="weather", fn=lambda: "20C")])
        events = list(agent.stream([{"role": "user", "content": "weather?"}]))
        kinds = [e["eventType"] for e in events]
        assert kinds.count("messages") >= 2  # tool result + final assistant
        result = [e for e in events if e.get("eventType") == "result"][-1]
        roles = [m["role"] for m in result["payload"]["messages"]]
        assert roles == ["user", "assistant", "tool", "assistant"]


def test_tool_spec_reactive_metadata_and_receipt() -> None:
    calls = []

    def effect(value: int):
        calls.append(value)
        return value + 1, f"receipt:{value}"

    spec = ToolSpec(
        name="effect",
        fn=effect,
        kind="effect",
        reads=("input",),
        writes=("output",),
        receipt=True,
    )
    assert spec.kind == "effect"
    assert spec.reads == ("input",)
    assert spec.writes == ("output",)
    assert spec.receipt is True
    assert spec.fn(value=1) == (2, "receipt:1")


class TestParallelToolCalls:
    def test_parallel_execution_preserves_tool_call_order(self) -> None:
        import threading
        import time

        lock = threading.Lock()
        execution: list[str] = []

        def slow_a() -> str:
            time.sleep(0.03)
            with lock:
                execution.append("a")
            return "A"

        def fast_b() -> str:
            with lock:
                execution.append("b")
            return "B"

        tools = ToolNode(
            [
                ToolSpec(name="a", fn=slow_a, kind="effect"),
                ToolSpec(name="b", fn=fast_b, kind="effect"),
            ]
        )
        messages = tools(
            [
                {"id": "a1", "name": "a", "arguments": {}},
                {"id": "b1", "name": "b", "arguments": {}},
            ],
            concurrency=2,
        )
        assert execution == ["b", "a"]
        assert [m["tool_call_id"] for m in messages] == ["a1", "b1"]
        assert [m["content"] for m in messages] == ["A", "B"]

    def test_bounded_concurrency_is_enforced(self) -> None:
        import threading
        import time

        active = 0
        peak = 0
        lock = threading.Lock()

        def tool(value: int) -> int:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.01)
            with lock:
                active -= 1
            return value

        tools = ToolNode([ToolSpec(name=f"t{i}", fn=lambda i=i: tool(i)) for i in range(5)])
        calls = [{"id": f"c{i}", "name": f"t{i}", "arguments": {}} for i in range(5)]
        tools(calls, concurrency=2)
        assert peak == 2

    def test_one_failure_does_not_hide_other_results(self) -> None:
        def ok() -> str:
            return "ok"

        tools = ToolNode(
            [
                ToolSpec(name="ok", fn=ok),
                ToolSpec(name="bad", fn=always_fails),
            ]
        )
        messages = tools(
            [
                {"id": "bad1", "name": "bad", "arguments": {}},
                {"id": "ok1", "name": "ok", "arguments": {}},
            ],
            concurrency=2,
        )
        assert messages[0]["content"].startswith("Error")
        assert messages[1]["content"] == "ok"

    def test_default_concurrency_is_serial_for_backwards_compatibility(self) -> None:
        order: list[str] = []

        def a() -> str:
            order.append("a-start")
            order.append("a-end")
            return "A"

        def b() -> str:
            order.append("b-start")
            order.append("b-end")
            return "B"

        tools = ToolNode([ToolSpec(name="a", fn=a), ToolSpec(name="b", fn=b)])
        tools(
            [
                {"id": "a", "name": "a", "arguments": {}},
                {"id": "b", "name": "b", "arguments": {}},
            ]
        )
        assert order == ["a-start", "a-end", "b-start", "b-end"]


class TestAgentStateSchema:
    def test_agent_state_has_versioned_stable_fields(self) -> None:
        state = AgentState(
            messages=({"role": "user", "content": "hello"},),
            tool_calls=({"id": "call-1", "name": "weather", "arguments": {"city": "Paris"}},),
            artifacts={"call-1": {"raw": "data"}},
            receipts={"call-1": "receipt:1"},
            interrupts=({"id": "human-1", "value": {"question": "Proceed?"}},),
            human_decisions=({"id": "human-1", "response": "yes"},),
        )
        data = state.to_dict()
        assert data == {
            "schema": "reactivegraph.agent-state.v1",
            "messages": [{"role": "user", "content": "hello"}],
            "tool_calls": [
                {"id": "call-1", "name": "weather", "arguments": {"city": "Paris"}}
            ],
            "artifacts": {"call-1": {"raw": "data"}},
            "receipts": {"call-1": "receipt:1"},
            "interrupts": [{"id": "human-1", "value": {"question": "Proceed?"}}],
            "human_decisions": [{"id": "human-1", "response": "yes"}],
        }
        assert AgentState.from_dict(data) == state

    def test_rejects_unsupported_schema_version(self) -> None:
        with pytest.raises(ValueError, match="unsupported agent state schema"):
            AgentState.from_dict({"schema": "reactivegraph.agent-state.v99"})

    def test_rejects_invalid_message_shape(self) -> None:
        with pytest.raises(ValueError, match="messages"):
            AgentState.from_dict(
                {
                    "schema": "reactivegraph.agent-state.v1",
                    "messages": [{"content": "missing role"}],
                }
            )

    def test_agent_state_is_inspectable_without_execution(self) -> None:
        state = AgentState(messages=[{"role": "user", "content": "inspect"}])
        assert state.messages[0]["content"] == "inspect"
        assert state.to_dict()["messages"][0]["content"] == "inspect"


class TestAgentInterrupts:
    def test_interrupt_before_and_after_boundaries(self) -> None:
        calls: list[str] = []

        def model(messages):
            if len(messages) == 1:
                return {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"id": "c1", "name": "t", "arguments": {}}],
                }
            return "done"

        agent = ReactiveAgent(
            model=model,
            tools=ToolNode([ToolSpec(name="t", fn=lambda: calls.append("tool") or "ok")]),
            interrupt_before=("t",),
            interrupt_after=("t",),
        )
        state = AgentState(messages=({"role": "user", "content": "go"},))
        first = agent.invoke_state(state)
        assert first.interrupts == ({"id": "before:t", "value": {"taskId": "t"}},)
        assert calls == []

        second = agent.resume_state(first, {"id": "before:t", "response": "approve"})
        assert second.interrupts == ({"id": "after:t", "value": {"taskId": "t"}},)
        assert calls == ["tool"]

        third = agent.resume_state(second, {"id": "after:t", "response": "continue"})
        assert third.interrupts == ()
        assert third.messages[-1]["content"] == "done"

    def test_multiple_pending_interrupts_are_individually_addressable(self) -> None:
        agent = ReactiveAgent(
            model=lambda messages: "done",
            tools=ToolNode([]),
            interrupt_before=("user",),
        )
        state = AgentState(
            messages=({"role": "user", "content": "one"}, {"role": "user", "content": "two"})
        )
        first = agent.invoke_state(state)
        assert [item["id"] for item in first.interrupts] == ["before:user:0", "before:user:1"]
        second = agent.resume_state(first, {"id": "before:user:1", "response": "approve"})
        assert [item["id"] for item in second.interrupts] == ["before:user:0"]
