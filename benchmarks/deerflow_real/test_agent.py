"""Real-source-derived tests for the ReactiveGraph DeerFlow baseline."""

from __future__ import annotations

from typing import Any

import pytest

from deerflow_reactive import ReactiveCheckpointSaver, create_deerflow_agent


class FakeModel:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls = 0

    def invoke(self, messages: list[dict[str, Any]]) -> Any:
        self.calls += 1
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]


class FakeTool:
    name = "echo"
    description = "Echo input"

    def invoke(self, args: dict[str, Any]) -> str:
        return f"echo:{args.get('value')}"


def test_model_only_turn_preserves_deerflow_state_shape() -> None:
    model = FakeModel([{"role": "assistant", "content": "hello"}])
    graph = create_deerflow_agent(model, system_prompt="be concise")
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "hi"}]},
        {"configurable": {"thread_id": "t1"}},
    )
    assert result["messages"][-1] == {"role": "assistant", "content": "hello"}
    assert model.calls == 1


def test_tool_call_round_trip_matches_deerflow_message_shape() -> None:
    model = FakeModel(
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "call-1", "name": "echo", "args": {"value": "world"}}],
            },
            {"role": "assistant", "content": "done"},
        ]
    )
    graph = create_deerflow_agent(model, tools=[FakeTool()])
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "echo world"}]},
        {"configurable": {"thread_id": "t2"}},
    )
    messages = result["messages"]
    assert [m["role"] for m in messages] == ["user", "assistant", "tool", "assistant"]
    assert messages[-2]["tool_call_id"] == "call-1"
    assert messages[-2]["content"] == "echo:world"
    assert messages[-1]["content"] == "done"
    assert model.calls == 2


def test_repeated_identical_turn_skips_model_and_tools() -> None:
    model = FakeModel([{"role": "assistant", "content": "same"}])
    graph = create_deerflow_agent(model, tools=[FakeTool()])
    input_value = {"messages": [{"role": "user", "content": "same question"}]}
    config = {"configurable": {"thread_id": "repeat"}}

    first = graph.invoke(input_value, config)
    second = graph.invoke(input_value, config)

    assert first == second
    assert model.calls == 1
    assert second.get("model_calls") == 1
    assert graph.graph.explain_run()["skipped"] == 3
    assert graph.graph.why_skipped("model")["reason"] == "fingerprint_unchanged"


def test_changed_input_reruns_model() -> None:
    model = FakeModel([{"role": "assistant", "content": "one"}, {"role": "assistant", "content": "two"}])
    graph = create_deerflow_agent(model)
    config = {"configurable": {"thread_id": "change"}}
    first = graph.invoke({"messages": [{"role": "user", "content": "one"}]}, config)
    second = graph.invoke({"messages": [{"role": "user", "content": "two"}]}, config)
    assert first["messages"][-1]["content"] == "one"
    assert second["messages"][-1]["content"] == "two"
    assert model.calls == 2


def test_stream_emits_values_and_trace() -> None:
    model = FakeModel([{"role": "assistant", "content": "stream"}])
    graph = create_deerflow_agent(model)
    events = list(
        graph.stream(
            {"messages": [{"role": "user", "content": "hi"}]},
            {"configurable": {"thread_id": "stream"}},
        )
    )
    assert any(event[0] == "values" for event in events)
    trace = graph.graph.export_trace()
    assert trace["protocol"] == "reactivegraph.causal-trace.v1"


def test_unsupported_features_fail_closed() -> None:
    with pytest.raises(ValueError, match="does not support.*features"):
        create_deerflow_agent(FakeModel([]), features=object())


def test_tool_schema_and_model_tool_binding_survive_replacement() -> None:
    class BoundModel(FakeModel):
        def bind_tools(self, tools):
            self.bound_tools = tools
            return self

    tool = FakeTool()
    tool.args_schema = {"type": "object", "properties": {"value": {"type": "string"}}}
    model = BoundModel([{"role": "assistant", "content": "schema"}])
    graph = create_deerflow_agent(model, tools=[tool])
    # DeerFlow's real factory binds tool schemas to the model before graph execution.
    assert [bound.name for bound in model.bound_tools] == ["echo"]
    assert model.bound_tools[0].args_schema == tool.args_schema
    assert graph.tools == [tool]


def test_factory_result_is_async_capable() -> None:
    import asyncio

    model = FakeModel([{"role": "assistant", "content": "async"}])
    graph = create_deerflow_agent(model)
    result = asyncio.run(
        graph.ainvoke(
            {"messages": [{"role": "user", "content": "hi"}]},
            {"configurable": {"thread_id": "async"}},
        )
    )
    assert result["messages"][-1]["content"] == "async"


def test_factory_supports_updates_messages_and_custom_stream_events() -> None:
    model = FakeModel([{"role": "assistant", "content": "stream modes"}])
    graph = create_deerflow_agent(model)
    events = list(
        graph.stream(
            {"messages": [{"role": "user", "content": "hi"}]},
            {"configurable": {"thread_id": "stream-modes"}},
            stream_mode=["values", "updates", "messages-tuple"],
        )
    )
    assert any(event[0] == "values" for event in events)
    assert any(event[0] == "updates" for event in events)
    assert any(event[0] == "messages-tuple" for event in events)


def test_thread_state_and_minimal_checkpoint_behavior() -> None:
    model = FakeModel([{"role": "assistant", "content": "first"}, {"role": "assistant", "content": "second"}])
    graph = create_deerflow_agent(model, checkpointer=ReactiveCheckpointSaver())
    config = {"configurable": {"thread_id": "checkpoint"}}
    graph.invoke({"messages": [{"role": "user", "content": "one"}]}, config)
    second = graph.invoke({"messages": [{"role": "user", "content": "two"}]}, config)
    assert [m["content"] for m in second["messages"]] == ["one", "first", "two", "second"]
    assert graph.get_state(config) == second


def test_tool_exception_becomes_recoverable_error_message() -> None:
    class BrokenTool(FakeTool):
        def invoke(self, args):
            raise ValueError("boom")

    model = FakeModel(
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "broken", "name": "echo", "args": {"value": "x"}}],
            },
            {"role": "assistant", "content": "recovered"},
        ]
    )
    graph = create_deerflow_agent(model, tools=[BrokenTool()])
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "break"}]},
        {"configurable": {"thread_id": "tool-error"}},
    )
    tool_message = result["messages"][-2]
    assert tool_message["role"] == "tool"
    assert tool_message["tool_call_id"] == "broken"
    assert tool_message["content"].startswith("Error: Tool 'echo' failed with ValueError: boom.")
    assert "Continue with available context" in tool_message["content"]
    assert result["messages"][-1]["content"] == "recovered"


def test_dangling_tool_call_gets_synthetic_recovery_message() -> None:
    model = FakeModel([{"role": "assistant", "content": "first"}, {"role": "assistant", "content": "done"}])
    graph = create_deerflow_agent(model)
    result = graph.invoke(
        {
            "messages": [
                {"role": "user", "content": "one"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"id": "dangling", "name": "missing", "args": {}}],
                },
                {"role": "user", "content": "two"},
            ]
        },
        {"configurable": {"thread_id": "dangling"}},
    )
    tool_messages = [m for m in result["messages"] if m["role"] == "tool"]
    assert tool_messages
    assert tool_messages[0]["tool_call_id"] == "dangling"
    assert tool_messages[0]["name"] == "missing"
    assert "Tool call could not be executed" in tool_messages[0]["content"]


def test_token_usage_metadata_is_accumulated() -> None:
    class UsageModel(FakeModel):
        def invoke(self, messages):
            return {
                "role": "assistant",
                "content": "usage",
                "usage_metadata": {
                    "input_tokens": 3,
                    "output_tokens": 5,
                    "total_tokens": 8,
                },
            }

    graph = create_deerflow_agent(UsageModel([]))
    first = graph.invoke(
        {"messages": [{"role": "user", "content": "usage"}]},
        {"configurable": {"thread_id": "usage"}},
    )
    second = graph.invoke(
        {"messages": [{"role": "user", "content": "usage"}]},
        {"configurable": {"thread_id": "usage"}},
    )
    assert first["usage_metadata"] == {
        "input_tokens": 3,
        "output_tokens": 5,
        "total_tokens": 8,
    }
    assert second["usage_metadata"] == {
        "input_tokens": 6,
        "output_tokens": 10,
        "total_tokens": 16,
    }


def test_summarization_replaces_long_history_with_summary() -> None:
    class UsageModel(FakeModel):
        def __init__(self):
            super().__init__([])
            self.calls = 0

        def invoke(self, messages):
            self.calls += 1
            if self.calls == 1:
                return {
                    "role": "assistant",
                    "content": "long",
                    "usage_metadata": {
                        "input_tokens": 1000,
                        "output_tokens": 10,
                        "total_tokens": 1010,
                    },
                }
            return {"role": "assistant", "content": "done"}

    class Summarizer:
        def invoke(self, messages):
            return "compressed summary"

    graph = create_deerflow_agent(
        UsageModel(),
        summarizer=Summarizer(),
        summarization_trigger=("messages", 1),
        summarization_keep=("messages", 1),
        checkpointer=ReactiveCheckpointSaver(),
    )
    config = {"configurable": {"thread_id": "summary"}}
    graph.invoke({"messages": [{"role": "user", "content": "one"}]}, config)
    second = graph.invoke({"messages": [{"role": "user", "content": "two"}]}, config)
    assert second["summary_text"] == "compressed summary"
    contents = [message["content"] for message in second["messages"]]
    assert "one" not in contents
    assert "compressed summary" in contents
    assert contents[-3:] == ["compressed summary", "two", "done"]


def test_clarification_returns_structured_question() -> None:
    class ClarifyModel(FakeModel):
        def invoke(self, messages):
            return {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "clarify",
                        "name": "ask_clarification",
                        "args": {"question": "What scope?"},
                    }
                ],
            }

    graph = create_deerflow_agent(ClarifyModel([]))
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "help"}]},
        {"configurable": {"thread_id": "clarify"}},
    )
    assert result["final"]["tool_calls"][0]["name"] == "ask_clarification"
    assert result["final"]["tool_calls"][0]["args"]["question"] == "What scope?"
    assert result["interrupted"] is True


def test_tool_command_updates_state_without_langgraph_import() -> None:
    class Command:
        def __init__(self, update):
            self.update = update

    class CommandTool(FakeTool):
        def invoke(self, args):
            return Command(
                {
                    "messages": [
                        {
                            "role": "tool",
                            "tool_call_id": "command",
                            "name": "echo",
                            "content": "command-result",
                        }
                    ],
                    "command_state": 7,
                }
            )

    model = FakeModel(
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "command", "name": "echo", "args": {}}],
            },
            {"role": "assistant", "content": "done"},
        ]
    )
    graph = create_deerflow_agent(model, tools=[CommandTool()])
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "command"}]},
        {"configurable": {"thread_id": "command"}},
    )
    assert result["command_state"] == 7
    assert result["messages"][-2]["content"] == "command-result"
    assert result["messages"][-1]["content"] == "done"


def test_command_return_direct_stops_after_tool_result() -> None:
    class Command:
        def __init__(self, update):
            self.update = update

    class DirectTool(FakeTool):
        def invoke(self, args):
            return Command(
                {
                    "messages": [
                        {
                            "role": "tool",
                            "tool_call_id": "direct",
                            "name": "echo",
                            "content": "direct-result",
                        }
                    ],
                    "return_direct": True,
                }
            )

    model = FakeModel(
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "direct", "name": "echo", "args": {}}],
            }
        ]
    )
    graph = create_deerflow_agent(model, tools=[DirectTool()])
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "direct"}]},
        {"configurable": {"thread_id": "direct"}},
    )
    assert model.calls == 1
    assert result["return_direct"] is True
    assert result["messages"][-1]["content"] == "direct-result"


def test_deerflow_runtime_stream_mode_compatibility() -> None:
    model = FakeModel([{"role": "assistant", "content": "modes"}])
    graph = create_deerflow_agent(model)
    modes = ["values", "messages-tuple", "updates", "debug", "tasks", "checkpoints"]
    events = list(
        graph.stream(
            {"messages": [{"role": "user", "content": "modes"}]},
            {"configurable": {"thread_id": "all-modes"}},
            stream_mode=modes,
        )
    )
    observed = {"messages-tuple" if mode == "messages" else mode for mode, _ in events}
    assert set(modes) == observed


def test_reactive_runtime_worker_publishes_events_and_terminal_success() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveRunManager, reactive_run_agent

    class WorkerModel(FakeModel):
        def invoke(self, messages):
            return {
                "role": "assistant",
                "content": "worker",
                "usage_metadata": {
                    "input_tokens": 2,
                    "output_tokens": 3,
                    "total_tokens": 5,
                },
            }

    graph = create_deerflow_agent(WorkerModel([]))
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {
            "run_id": "run-1",
            "thread_id": "thread-1",
            "model_name": "fake",
        }
    )

    events = asyncio.run(
        reactive_run_agent(
            bridge=bridge,
            run_manager=manager,
            record=record,
            graph=graph,
            graph_input={"messages": [{"role": "user", "content": "run"}]},
            config={"configurable": {"thread_id": "thread-1"}},
            stream_modes=["values", "updates", "messages-tuple", "debug", "tasks", "checkpoints"],
        )
    )
    assert record.status == "success"
    assert manager.get_run("run-1").status == "success"
    modes = {mode for mode, _ in events}
    assert modes == {
        "metadata",
        "values",
        "updates",
        "messages-tuple",
        "debug",
        "tasks",
        "checkpoints",
        "terminal",
    }
    assert events[-1] == ("terminal", {"status": "success"})


def test_reactive_runtime_worker_cancel_sets_interrupted() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveRunManager, reactive_run_agent

    class SlowModel(FakeModel):
        def invoke(self, messages):
            import time

            time.sleep(0.03)
            return {"role": "assistant", "content": "slow"}

    graph = create_deerflow_agent(SlowModel([]))
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-2", "thread_id": "thread-2", "model_name": "fake"}
    )

    async def cancel_soon():
        await asyncio.sleep(0.005)
        record.abort_event.set()

    async def main():
        worker = asyncio.create_task(
            reactive_run_agent(
                bridge=bridge,
                run_manager=manager,
                record=record,
                graph=graph,
                graph_input={"messages": [{"role": "user", "content": "cancel"}]},
                config={"configurable": {"thread_id": "thread-2"}},
                stream_modes=["values"],
            )
        )
        cancellation = asyncio.create_task(cancel_soon())
        await asyncio.wait([worker, cancellation], timeout=2)
        return await worker

    events = asyncio.run(main())
    assert record.status == "interrupted"
    assert events[-1] == ("terminal", {"status": "interrupted"})


def test_reactive_runtime_worker_error_publishes_error_and_terminal() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveRunManager, reactive_run_agent

    class FailingModel(FakeModel):
        def invoke(self, messages):
            raise RuntimeError("provider unavailable")

    graph = create_deerflow_agent(FailingModel([]))
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-error", "thread_id": "thread-error", "model_name": "fake"}
    )

    async def main():
        with pytest.raises(RuntimeError, match="provider unavailable"):
            await reactive_run_agent(
                bridge=bridge,
                run_manager=manager,
                record=record,
                graph=graph,
                graph_input={"messages": [{"role": "user", "content": "fail"}]},
                config={"configurable": {"thread_id": "thread-error"}},
                stream_modes=["values"],
            )

    asyncio.run(main())
    assert record.status == "error"
    assert record.error == "provider unavailable"
    assert ("error", {"message": "provider unavailable", "name": "RuntimeError"}) in bridge.events
    assert bridge.events[-1] == ("terminal", {"status": "error"})


def test_reactive_runtime_worker_publishes_metadata_first() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveRunManager, reactive_run_agent

    graph = create_deerflow_agent(FakeModel([{"role": "assistant", "content": "meta"}]))
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-meta", "thread_id": "thread-meta", "model_name": "fake"}
    )

    events = asyncio.run(
        reactive_run_agent(
            bridge=bridge,
            run_manager=manager,
            record=record,
            graph=graph,
            graph_input={"messages": [{"role": "user", "content": "meta"}]},
            config={"configurable": {"thread_id": "thread-meta"}},
            stream_modes=["values"],
        )
    )

    assert events[0] == (
        "metadata",
        {"run_id": "run-meta", "thread_id": "thread-meta"},
    )
    assert bridge.events[0] == events[0]
    assert record.status == "success"


def test_reactive_run_bridge_supports_end_signal() -> None:
    from deerflow_reactive import ReactiveRunBridge

    bridge = ReactiveRunBridge()
    bridge.publish("run", "values", {"ok": True})
    bridge.publish_end("run")

    assert bridge.ended == {"run"}
    assert bridge.events == [("values", {"ok": True})]


def test_reactive_runtime_worker_publishes_end_signal() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveRunManager, reactive_run_agent

    graph = create_deerflow_agent(FakeModel([{"role": "assistant", "content": "end"}]))
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-end", "thread_id": "thread-end", "model_name": "fake"}
    )

    asyncio.run(
        reactive_run_agent(
            bridge=bridge,
            run_manager=manager,
            record=record,
            graph=graph,
            graph_input={"messages": [{"role": "user", "content": "end"}]},
            config={"configurable": {"thread_id": "thread-end"}},
            stream_modes=["values"],
        )
    )

    assert bridge.ended == {"run-end"}


def test_reactive_run_bridge_replays_events_then_end() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge

    bridge = ReactiveRunBridge()
    bridge.publish("run", "metadata", {"run_id": "run"})
    bridge.publish("run", "values", {"messages": []})
    bridge.publish_end("run")

    async def collect():
        return [event async for event in bridge.subscribe("run")]

    received = asyncio.run(collect())
    assert [(event.event, event.data) for event in received] == [
        ("metadata", {"run_id": "run"}),
        ("values", {"messages": []}),
        ("__end__", None),
    ]
    assert received[0].id == bridge.event_ids[0]
    assert received[1].id == bridge.event_ids[1]


def test_reactive_run_bridge_live_subscribe_receives_future_events() -> None:
    import asyncio

    from deerflow_reactive import REACTIVE_END, ReactiveRunBridge

    bridge = ReactiveRunBridge()

    async def main():
        received: list[Any] = []

        async def consume():
            async for event in bridge.subscribe("live"):
                received.append(event)

        consumer = asyncio.create_task(consume())
        while not bridge.stream_exists("live"):
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        bridge.publish("live", "metadata", {"run_id": "live"})
        await asyncio.sleep(0)
        bridge.publish("live", "values", {"messages": []})
        bridge.publish_end("live")
        await asyncio.wait_for(consumer, timeout=2)
        return received

    received = asyncio.run(main())
    assert [(event.event, event.data) for event in received] == [
        ("metadata", {"run_id": "live"}),
        ("values", {"messages": []}),
        ("__end__", None),
    ]
    assert received[-1] is REACTIVE_END


def test_reactive_run_bridge_subscribe_heartbeat_when_idle() -> None:
    import asyncio

    from deerflow_reactive import REACTIVE_HEARTBEAT, ReactiveRunBridge

    bridge = ReactiveRunBridge()

    async def main():
        received: list[Any] = []

        async def consume():
            async for event in bridge.subscribe("idle", heartbeat_interval=0.01):
                received.append(event)
                return

        await asyncio.wait_for(consume(), timeout=2)
        return received

    received = asyncio.run(main())
    assert received == [REACTIVE_HEARTBEAT]
    assert received[0].event == "__heartbeat__"


def test_reactive_run_bridge_subscribe_last_event_id_resumes() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge

    bridge = ReactiveRunBridge()
    bridge.publish("resume", "metadata", {"n": 1})
    first = bridge.event_ids[0]
    bridge.publish("resume", "values", {"n": 2})
    bridge.publish_end("resume")

    async def collect():
        return [
            event
            async for event in bridge.subscribe("resume", last_event_id=first)
        ]

    received = asyncio.run(collect())
    assert [(event.event, event.data) for event in received] == [
        ("values", {"n": 2}),
        ("__end__", None),
    ]


def test_reactive_run_bridge_reports_gap_for_expired_cursor() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveStreamGap

    bridge = ReactiveRunBridge(queue_maxsize=2)
    bridge.publish("gap", "values", {"n": 1})
    expired = bridge.event_ids[0]
    bridge.publish("gap", "values", {"n": 2})
    bridge.publish("gap", "values", {"n": 3})
    bridge.publish_end("gap")

    async def collect():
        return [event async for event in bridge.subscribe("gap", last_event_id=expired)]

    received = asyncio.run(collect())
    assert len(received) == 1
    assert isinstance(received[0], ReactiveStreamGap)
    assert received[0].requested_event_id == expired
    assert received[0].earliest_available_event_id == bridge.event_ids[1]
    assert received[0].latest_available_event_id == bridge.event_ids[2]


def test_reactive_run_bridge_gap_when_subscriber_falls_behind() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunBridge, ReactiveStreamGap

    bridge = ReactiveRunBridge(queue_maxsize=1)

    async def main():
        events = []

        async def consume():
            async for event in bridge.subscribe("slow"):
                events.append(event)
                if isinstance(event, ReactiveStreamGap):
                    return

        consumer = asyncio.create_task(consume())
        while not bridge.stream_exists("slow"):
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        # Overflow the retained buffer while the subscriber is parked on its
        # first wake, forcing a gap instead of a silent partial replay.
        bridge.publish("slow", "values", {"n": 1})
        bridge.publish("slow", "values", {"n": 2})
        bridge.publish("slow", "values", {"n": 3})
        await asyncio.wait_for(consumer, timeout=2)
        return events

    events = asyncio.run(main())
    assert any(isinstance(event, ReactiveStreamGap) for event in events)


def test_reactive_runtime_worker_streams_to_live_subscriber() -> None:
    import asyncio

    from deerflow_reactive import REACTIVE_END, ReactiveRunBridge, ReactiveRunManager, reactive_run_agent

    graph = create_deerflow_agent(FakeModel([{"role": "assistant", "content": "live"}]))
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-live", "thread_id": "thread-live", "model_name": "fake"}
    )

    async def main():
        consumed: list[Any] = []

        async def consume():
            async for event in bridge.subscribe("run-live"):
                consumed.append(event)

        subscriber = asyncio.create_task(consume())
        while not bridge.stream_exists("run-live"):
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        await reactive_run_agent(
            bridge=bridge,
            run_manager=manager,
            record=record,
            graph=graph,
            graph_input={"messages": [{"role": "user", "content": "live"}]},
            config={"configurable": {"thread_id": "thread-live"}},
            stream_modes=["values"],
        )
        await asyncio.wait_for(subscriber, timeout=2)
        return consumed

    consumed = asyncio.run(main())
    names = [event.event for event in consumed]
    assert names[0] == "metadata"
    assert names[-2:] == ["terminal", "__end__"]
    assert "values" in names
    assert consumed[-1] is REACTIVE_END
    assert record.status == "success"


def test_reactive_run_event_store_assigns_thread_sequence() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunEventStore

    store = ReactiveRunEventStore()

    async def main():
        first = await store.put(
            thread_id="thread-1",
            run_id="run-1",
            event_type="run.start",
            category="lifecycle",
            content={"ok": True},
        )
        second = await store.put(
            thread_id="thread-1",
            run_id="run-2",
            event_type="run.end",
            category="lifecycle",
        )
        return first, second

    first, second = asyncio.run(main())
    assert first["seq"] == 1
    assert second["seq"] == 2
    assert first["thread_id"] == "thread-1"
    assert first["run_id"] == "run-1"
    assert first["content"] == {"ok": True}
    assert first["metadata"] == {}
    assert first["created_at"]


def test_reactive_run_event_store_batches_and_lists_by_run() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunEventStore

    store = ReactiveRunEventStore()

    async def main():
        await store.put_batch(
            [
                {
                    "thread_id": "thread-1",
                    "run_id": "run-1",
                    "event_type": "run.start",
                    "category": "lifecycle",
                },
                {
                    "thread_id": "thread-1",
                    "run_id": "run-2",
                    "event_type": "run.start",
                    "category": "lifecycle",
                },
            ]
        )
        return (
            await store.list_events("thread-1"),
            await store.list_events("thread-1", run_id="run-1"),
        )

    thread_events, run_events = asyncio.run(main())
    assert [event["seq"] for event in thread_events] == [1, 2]
    assert [event["run_id"] for event in run_events] == ["run-1"]


def test_reactive_run_journal_buffers_until_flush() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunEventStore, ReactiveRunJournal

    store = ReactiveRunEventStore()
    journal = ReactiveRunJournal("run-j", "thread-j", store, flush_threshold=10)
    journal.record(event_type="run.start", category="lifecycle")
    assert store.events == {}
    assert journal.feed_generation == 0

    asyncio.run(journal.flush())
    assert len(store.events["thread-j"]) == 1
    assert store.events["thread-j"][0]["event_type"] == "run.start"
    assert journal.feed_generation == 1


def test_reactive_run_journal_flushes_at_threshold() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunEventStore, ReactiveRunJournal

    store = ReactiveRunEventStore()

    async def main():
        journal = ReactiveRunJournal("run-t", "thread-t", store, flush_threshold=2)
        journal.record(event_type="a", category="lifecycle")
        journal.record(event_type="b", category="lifecycle")
        await journal.flush()
        return journal

    journal = asyncio.run(main())
    assert [event["event_type"] for event in store.events["thread-t"]] == ["a", "b"]
    assert journal.feed_generation >= 1


def test_reactive_run_journal_records_delivery_receipt() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunEventStore, ReactiveRunJournal

    store = ReactiveRunEventStore()
    journal = ReactiveRunJournal("run-d", "thread-d", store)
    assert journal.get_delivery_content() == {"presented": 0, "paths": [], "by_tool": {}}

    journal.record_delivery()
    asyncio.run(journal.flush())

    delivery = [
        event
        for event in store.events["thread-d"]
        if event["event_type"] == "run.delivery"
    ]
    assert len(delivery) == 1
    assert delivery[0]["category"] == "outputs"
    assert delivery[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}


def test_reactive_run_journal_tracks_completion_data() -> None:
    from deerflow_reactive import ReactiveRunEventStore, ReactiveRunJournal

    journal = ReactiveRunJournal("run-c", "thread-c", ReactiveRunEventStore())
    journal.observe_state(
        {
            "messages": [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "world"},
            ],
            "usage_metadata": {
                "input_tokens": 3,
                "output_tokens": 4,
                "total_tokens": 7,
            },
        }
    )
    data = journal.get_completion_data()
    assert data["total_input_tokens"] == 3
    assert data["total_output_tokens"] == 4
    assert data["total_tokens"] == 7
    assert data["message_count"] == 2
    assert data["first_human_message"] == "hello"
    assert data["last_ai_message"] == "world"


def test_reactive_runtime_worker_records_journal_lifecycle_and_delivery() -> None:
    import asyncio

    from deerflow_reactive import (
        ReactiveRunBridge,
        ReactiveRunEventStore,
        ReactiveRunJournal,
        ReactiveRunManager,
        reactive_run_agent,
    )

    store = ReactiveRunEventStore()
    journal = ReactiveRunJournal("run-jw", "thread-jw", store)
    graph = create_deerflow_agent(
        FakeModel(
            [
                {
                    "role": "assistant",
                    "content": "journaled",
                    "usage_metadata": {
                        "input_tokens": 1,
                        "output_tokens": 2,
                        "total_tokens": 3,
                    },
                }
            ]
        )
    )
    bridge = ReactiveRunBridge()
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-jw", "thread_id": "thread-jw", "model_name": "fake"}
    )

    asyncio.run(
        reactive_run_agent(
            bridge=bridge,
            run_manager=manager,
            record=record,
            graph=graph,
            graph_input={"messages": [{"role": "user", "content": "hi"}]},
            config={"configurable": {"thread_id": "thread-jw"}},
            stream_modes=["values"],
            journal=journal,
        )
    )

    records = store.events["thread-jw"]
    event_types = [event["event_type"] for event in records]
    assert event_types[0] == "run.start"
    assert "run.end" in event_types
    assert "run.delivery" in event_types
    completion = journal.get_completion_data()
    assert completion["total_tokens"] == 3
    assert completion["message_count"] == 2
    assert completion["first_human_message"] == "hi"
    assert completion["last_ai_message"] == "journaled"


def test_reactive_runtime_worker_journals_error_terminal_state() -> None:
    import asyncio

    import pytest

    from deerflow_reactive import (
        ReactiveRunBridge,
        ReactiveRunEventStore,
        ReactiveRunJournal,
        ReactiveRunManager,
        reactive_run_agent,
    )

    class FailingModel(FakeModel):
        def invoke(self, messages):
            raise RuntimeError("boom")

    store = ReactiveRunEventStore()
    journal = ReactiveRunJournal("run-je", "thread-je", store)
    manager = ReactiveRunManager()
    record = manager.create_run(
        {"run_id": "run-je", "thread_id": "thread-je", "model_name": "fake"}
    )

    async def main():
        with pytest.raises(RuntimeError, match="boom"):
            await reactive_run_agent(
                bridge=ReactiveRunBridge(),
                run_manager=manager,
                record=record,
                graph=create_deerflow_agent(FailingModel([])),
                graph_input={"messages": [{"role": "user", "content": "x"}]},
                config={"configurable": {"thread_id": "thread-je"}},
                stream_modes=["values"],
                journal=journal,
            )

    asyncio.run(main())
    end = [
        event
        for event in store.events["thread-je"]
        if event["event_type"] == "run.end"
    ]
    assert len(end) == 1
    assert end[0]["content"] == {"status": "error", "error": "boom"}
    assert record.status == "error"


def test_reactive_run_store_put_get_and_thread_paging_matches_upstream() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put(
            "r1",
            thread_id="t1",
            user_id="alice",
            status="success",
            created_at="2024-01-01T00:00:00+00:00",
        )
        await store.put(
            "r2",
            thread_id="t1",
            user_id="alice",
            status="success",
            created_at="2024-01-02T00:00:00+00:00",
        )
        await store.put(
            "r3",
            thread_id="t1",
            user_id="bob",
            status="success",
            created_at="2024-01-03T00:00:00+00:00",
        )
        await store.put(
            "internal",
            thread_id="t1",
            user_id="alice",
            status="pending",
            operation_kind="checkpoint_write",
            created_at="2024-01-04T00:00:00+00:00",
        )
        first = await store.list_by_thread("t1", user_id="alice", limit=1)
        second = await store.list_by_thread(
            "t1",
            user_id="alice",
            limit=1,
            before_created_at=first[0]["created_at"],
            before_run_id=first[0]["run_id"],
        )
        return await store.get("r1", user_id="alice"), first, second

    row, first, second = asyncio.run(main())
    assert row is not None and row["status"] == "success"
    assert [item["run_id"] for item in first] == ["r2"]
    assert [item["run_id"] for item in second] == ["r1"]


def test_reactive_run_store_lifecycle_and_completion_are_monotonic() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put("r1", thread_id="t1", model_name="fake")
        started = await store.start_run("r1")
        progress = await store.update_run_progress(
            "r1", total_tokens=12, lead_agent_tokens=10, subagent_tokens=2
        )
        completed = await store.update_run_completion(
            "r1",
            status="success",
            total_tokens=12,
            lead_agent_tokens=10,
            subagent_tokens=2,
            message_count=2,
        )
        late = await store.update_run_completion(
            "r1", status="error", total_tokens=999, error="late failure"
        )
        return started, progress, completed, late, await store.get("r1")

    started, progress, completed, late, row = asyncio.run(main())
    assert started is True
    assert progress is None
    assert completed is True
    assert late is False
    assert row is not None
    assert row["status"] == "success"
    assert row["total_tokens"] == 12
    assert row["error"] is None


def test_reactive_run_store_aggregates_tokens_by_thread_and_owner() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put("ok", thread_id="t1", user_id="alice", status="running", model_name="m1")
        await store.update_run_completion(
            "ok",
            status="success",
            total_input_tokens=7,
            total_output_tokens=3,
            total_tokens=10,
            lead_agent_tokens=8,
            subagent_tokens=2,
            token_usage_by_model={"m1": {"total_tokens": 10}},
        )
        await store.put("failed", thread_id="t1", user_id="alice", status="running", model_name="m2")
        await store.update_run_completion(
            "failed",
            status="error",
            total_tokens=4,
            lead_agent_tokens=4,
            token_usage_by_model={"m2": {"total_tokens": 4}},
        )
        await store.put("active", thread_id="t1", user_id="alice", status="running", model_name="m1")
        await store.update_run_progress("active", total_tokens=100, lead_agent_tokens=100)
        await store.put("other", thread_id="t1", user_id="bob", status="success")
        return (
            await store.aggregate_tokens_by_thread("t1", user_id="alice"),
            await store.aggregate_tokens_by_thread("t1", user_id="alice", include_active=True),
        )

    completed, active = asyncio.run(main())
    assert completed["total_tokens"] == 14
    assert completed["total_runs"] == 2
    assert completed["by_model"] == {
        "m1": {"tokens": 10, "runs": 1},
        "m2": {"tokens": 4, "runs": 1},
    }
    assert completed["by_caller"] == {"lead_agent": 12, "subagent": 2, "middleware": 0}
    assert active["total_tokens"] == 114
    assert active["total_runs"] == 3


def test_reactive_run_store_cancel_finalize_race_and_lease_renewal() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put(
            "r1",
            thread_id="t1",
            status="running",
            owner_worker_id="worker-a",
            lease_expires_at="2026-01-01T00:00:00+00:00",
        )
        first = await store.request_cancel("r1", action="rollback")
        second = await store.request_cancel("r1", action="interrupt")
        finalized = await store.finalize_if_not_cancelled("r1", status="success")
        renewal = await store.renew_lease(
            "r1",
            owner_worker_id="worker-a",
            lease_expires_at="2026-01-01T00:01:00+00:00",
        )
        return first, second, finalized, renewal, await store.get("r1")

    first, second, finalized, renewal, row = asyncio.run(main())
    assert first == "rollback"
    assert second == "rollback"
    assert finalized.finalized is False
    assert finalized.cancel_action == "rollback"
    assert renewal.renewed is True
    assert renewal.cancel_action == "rollback"
    assert row is not None and row["status"] == "running"


def test_reactive_run_store_rejects_lease_takeover_after_renewal() -> None:
    import asyncio
    from datetime import UTC, datetime, timedelta

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()
    expired = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    fresh = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()

    async def main():
        await store.put(
            "expired",
            thread_id="t1",
            status="running",
            owner_worker_id="dead-worker",
            lease_expires_at=expired,
        )
        await store.put(
            "renewed",
            thread_id="t2",
            status="running",
            owner_worker_id="alive-worker",
            lease_expires_at=expired,
        )
        candidates = await store.list_inflight_with_expired_lease(grace_seconds=10)
        assert [row["run_id"] for row in candidates] == ["expired", "renewed"]
        await store.update_lease(
            "renewed",
            owner_worker_id="alive-worker",
            lease_expires_at=fresh,
        )
        claimed_expired = await store.claim_for_takeover(
            "expired", grace_seconds=10, error="orphaned"
        )
        claimed_renewed = await store.claim_for_takeover(
            "renewed", grace_seconds=10, error="orphaned"
        )
        return claimed_expired, claimed_renewed, await store.get("renewed")

    claimed_expired, claimed_renewed, renewed = asyncio.run(main())
    assert claimed_expired is True
    assert claimed_renewed is False
    assert renewed is not None and renewed["status"] == "running"


def test_reactive_run_manager_persists_worker_terminal_status() -> None:
    import asyncio

    from deerflow_reactive import (
        ReactiveRunBridge,
        ReactiveRunManager,
        ReactiveRunStore,
        reactive_run_agent,
    )

    store = ReactiveRunStore()
    manager = ReactiveRunManager(store=store)
    record = manager.create_run({"run_id": "r1", "thread_id": "t1", "model_name": "fake"})
    graph = create_deerflow_agent(FakeModel([{"role": "assistant", "content": "persisted"}]))

    async def main():
        await reactive_run_agent(
            bridge=ReactiveRunBridge(),
            run_manager=manager,
            record=record,
            graph=graph,
            graph_input={"messages": [{"role": "user", "content": "hi"}]},
            config={"configurable": {"thread_id": "t1"}},
            stream_modes=["values"],
        )
        return await store.get("r1")

    row = asyncio.run(main())
    assert row is not None
    assert row["status"] == "success"
    assert row["thread_id"] == "t1"


def test_reactive_run_store_atomic_reject_and_idempotent_reuse() -> None:
    import asyncio

    from deerflow_reactive import ReactiveConflictError, ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        first, claimed = await store.create_thread_operation_atomic(
            "r1",
            thread_id="t1",
            owner_worker_id="worker-a",
            lease_expires_at=None,
            idempotency_key="request:1",
        )
        reused, reused_claimed = await store.create_thread_operation_atomic(
            "r2",
            thread_id="t1",
            owner_worker_id="worker-b",
            lease_expires_at=None,
            idempotency_key="request:1",
        )
        try:
            await store.create_thread_operation_atomic(
                "r3",
                thread_id="t1",
                owner_worker_id="worker-b",
                lease_expires_at=None,
            )
        except ReactiveConflictError as exc:
            conflict = str(exc)
        else:
            conflict = None
        return first, claimed, reused, reused_claimed, conflict

    first, claimed, reused, reused_claimed, conflict = asyncio.run(main())
    assert first["run_id"] == "r1"
    assert claimed == []
    assert reused["run_id"] == "r1"
    assert reused_claimed == []
    assert conflict == "Thread t1 already has an active run"


def test_reactive_run_store_atomic_interrupt_claims_active_run() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.create_thread_operation_atomic(
            "old",
            thread_id="t1",
            owner_worker_id="worker-a",
            lease_expires_at=None,
        )
        new, claimed = await store.create_thread_operation_atomic(
            "new",
            thread_id="t1",
            owner_worker_id="worker-b",
            lease_expires_at=None,
            multitask_strategy="interrupt",
        )
        return new, claimed, await store.get("old")

    new, claimed, old = asyncio.run(main())
    assert new["run_id"] == "new"
    assert [row["run_id"] for row in claimed] == ["old"]
    assert old is not None and old["status"] == "interrupted"


def test_reactive_run_store_rollback_claim_is_atomic_on_conflict() -> None:
    import asyncio
    from datetime import UTC, datetime, timedelta

    from deerflow_reactive import ReactiveConflictError, ReactiveRunStore

    store = ReactiveRunStore()
    live_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()

    async def main():
        await store.put(
            "foreign",
            thread_id="t1",
            status="running",
            owner_worker_id="worker-a",
            lease_expires_at=live_lease,
        )
        try:
            await store.create_thread_operation_atomic(
                "replacement",
                thread_id="t1",
                owner_worker_id="worker-b",
                lease_expires_at=None,
                multitask_strategy="interrupt",
            )
        except ReactiveConflictError:
            pass
        return await store.get("foreign")

    foreign = asyncio.run(main())
    assert foreign is not None and foreign["status"] == "running"


def test_reactive_run_store_changed_cursor_and_batch_lookup() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put("r1", thread_id="t1", user_id="alice", status="success")
        await store.put(
            "internal",
            thread_id="t1",
            user_id="alice",
            status="pending",
            operation_kind="checkpoint_write",
        )
        await store.put("r2", thread_id="t1", user_id="bob", status="success")
        await store.update_model_name("r1", "new-model")
        first = await store.list_changed(after_change_seq=0, after_run_id="", user_id="alice")
        cursor = (first[-1]["change_seq"], first[-1]["run_id"])
        second = await store.list_changed(
            after_change_seq=cursor[0],
            after_run_id=cursor[1],
            user_id="alice",
        )
        batch = await store.get_many_by_thread(
            "t1", {"r1", "r2", "internal"}, user_id="alice"
        )
        return first, second, batch

    first, second, batch = asyncio.run(main())
    assert [row["run_id"] for row in first] == ["r1"]
    assert second == []
    assert set(batch) == {"r1"}


def test_reactive_run_store_regenerate_visibility_queries() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put("source", thread_id="t1", user_id="alice", status="success")
        await store.put(
            "success-attempt",
            thread_id="t1",
            user_id="alice",
            status="success",
            metadata={"regenerate_from_run_id": "source"},
        )
        await store.put(
            "failed-edit",
            thread_id="t1",
            user_id="alice",
            status="error",
            metadata={"regenerate_from_run_id": "source", "replay_kind": "edit"},
        )
        await store.put(
            "success-edit",
            thread_id="t1",
            user_id="alice",
            status="success",
            metadata={"regenerate_from_run_id": "source", "replay_kind": "edit"},
        )
        await store.put(
            "bob-attempt",
            thread_id="t1",
            user_id="bob",
            status="success",
            metadata={"regenerate_from_run_id": "source", "replay_kind": "edit"},
        )
        return (
            await store.list_successful_regenerate_sources("t1", user_id="alice"),
            await store.list_edit_regenerate_runs("t1", user_id="alice"),
        )

    sources, edits = asyncio.run(main())
    assert sources == {"source"}
    assert [row["run_id"] for row in edits] == ["failed-edit", "success-edit"]


def test_reactive_run_store_delete_by_thread_preserves_internal_operations() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunStore

    store = ReactiveRunStore()

    async def main():
        await store.put("r1", thread_id="t1", user_id="alice", status="success")
        await store.put(
            "reservation",
            thread_id="t1",
            user_id="alice",
            status="pending",
            operation_kind="delete",
        )
        await store.put("r2", thread_id="t1", user_id="bob", status="success")
        removed = await store.delete_by_thread("t1", user_id="alice")
        return removed, await store.get("r1"), await store.get("reservation"), await store.get("r2")

    removed, r1, reservation, r2 = asyncio.run(main())
    assert removed == 1
    assert r1 is None
    assert reservation is not None
    assert r2 is not None


def test_reactive_run_manager_create_or_reject_reuses_idempotency_key() -> None:
    import asyncio

    from deerflow_reactive import ReactiveRunManager, ReactiveRunStore

    store = ReactiveRunStore()
    first_manager = ReactiveRunManager(store=store)
    second_manager = ReactiveRunManager(store=store)

    async def main():
        first = await first_manager.create_or_reject(
            "t1", user_id="alice", idempotency_key="request:1"
        )
        reused = await second_manager.create_or_reject(
            "t1", user_id="alice", idempotency_key="request:1"
        )
        rows = await store.list_by_thread("t1", user_id="alice")
        return first, reused, rows

    first, reused, rows = asyncio.run(main())
    assert reused.run_id == first.run_id
    assert len(rows) == 1
