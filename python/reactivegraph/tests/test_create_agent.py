"""`create_agent` — the LangChain agent-factory contract, owned by ReactiveGraph.

DeerFlow builds its agent graph through `create_agent(model, tools, ...)` and
then relies on graph-level behavior: `.invoke`/`.stream`, `.checkpointer`,
`.store`, `.builder.schemas`, and middleware hooks. This module pins that
contract against our own engine (zero langchain/langgraph imports).

Behaviors that are not implemented must fail closed with a precise message —
never silently degrade.
"""

from __future__ import annotations

from typing import Any

import pytest

from reactivegraph.create_agent import create_agent
from reactivegraph.errors import GraphRecursionError
from reactivegraph.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage


def _runtime(context: Any):
    from reactivegraph.runtime import Runtime

    return Runtime(context=context)


class FakeToolCallingModel:
    """Minimal stand-in for a chat model that can emit tool calls."""

    def __init__(self, responses: list[AIMessage]) -> None:
        self.responses = list(responses)
        self.calls: list[list[object]] = []
        self.bound_tools: list[list[object]] | None = None

    def bind_tools(self, tools, **kwargs):
        self.bound_tools = list(tools)
        return self

    def invoke(self, messages, **kwargs):
        self.calls.append(list(messages))
        if not self.responses:
            raise AssertionError("FakeToolCallingModel ran out of responses")
        return self.responses.pop(0)


def _tool_call_message(name: str, args: dict, call_id: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"name": name, "args": args, "id": call_id, "type": "tool_call"}],
    )


class TestNoToolSingleTurn:
    def test_invoke_returns_messages_with_the_model_reply(self) -> None:
        model = FakeToolCallingModel([AIMessage(content="hello")])
        agent = create_agent(model)
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})
        assert [m.content for m in out["messages"]] == ["hi", "hello"]

    def test_system_prompt_is_prepended_before_the_message_list(self) -> None:
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, system_prompt="be brief")
        agent.invoke({"messages": [HumanMessage(content="hi")]})
        (seen,) = model.calls
        # The model boundary speaks the flat dict shape (see
        # TestForeignModelBoundary): a real chat model cannot coerce our
        # classes, so what the model receives is a dict, not an instance.
        assert seen[0]["type"] == "system"
        assert seen[0]["content"] == "be brief"
        assert seen[1]["type"] == "human"
        assert seen[1]["content"] == "hi"

    def test_default_system_prompt_is_exposed_as_request_system_message(self) -> None:
        """Upstream keeps the static prompt in ``request.system_message``.

        Middleware such as DeerFlow's ``SystemMessageCoalescingMiddleware``
        reads and rewrites that field; flattening the prompt into
        ``request.messages`` before the chain runs would make the field always
        ``None`` and silently discard every middleware override.
        """
        seen: dict[str, Any] = {}

        class Probe(ForeignAgentMiddlewareStandIn):
            name = "system-prompt-probe"

            def wrap_model_call(self, request, handler):
                seen["system_message"] = request.system_message
                seen["messages"] = list(request.messages)
                return handler(request)

        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, system_prompt="be brief", middleware=[Probe()])
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert isinstance(seen["system_message"], SystemMessage)
        assert seen["system_message"].content == "be brief"
        assert [m.type for m in seen["messages"]] == ["human"]

    def test_wrap_model_call_system_message_override_reaches_the_model(self) -> None:
        """A rewritten ``system_message`` must be flattened at the boundary."""

        class Rewriting(ForeignAgentMiddlewareStandIn):
            name = "system-prompt-rewriter"

            def wrap_model_call(self, request, handler):
                return handler(
                    request.override(system_message=SystemMessage(content="rewritten"))
                )

        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, system_prompt="original", middleware=[Rewriting()])
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        (seen,) = model.calls
        assert seen[0]["type"] == "system"
        assert seen[0]["content"] == "rewritten"
        assert [m["type"] for m in seen[1:]] == ["human"]

    def test_wrap_model_call_can_remove_the_default_system_prompt(self) -> None:
        """``override(system_message=None)`` is the documented opt-out."""

        class Dropping(ForeignAgentMiddlewareStandIn):
            name = "system-prompt-dropper"

            def wrap_model_call(self, request, handler):
                return handler(request.override(system_message=None))

        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, system_prompt="original", middleware=[Dropping()])
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        (seen,) = model.calls
        assert [m["type"] for m in seen] == ["human"]

    def test_input_messages_get_ids_before_any_node_sees_them(self) -> None:
        """LangGraph assigns missing ids while initializing the channel.

        DeerFlow's ``ThreadDataMiddleware`` rebuilds the last human message
        (adding ``name="user-input"``) and returns it under the *same id*. On
        ``add_messages`` semantics that is a replacement, not an append. If we
        leave ids as ``None`` until the reducer runs, the rebuilt copy looks
        like a second message and the transcript ends up with ``["hi", "hi"]``.
        """
        ids_seen: list[str | None] = []

        class Probe(ForeignAgentMiddlewareStandIn):
            name = "input-id-probe"

            def before_agent(self, state, runtime):
                ids_seen.append(state["messages"][-1].id)
                return {
                    "messages": [
                        type(state["messages"][-1])(
                            content="hi", id=state["messages"][-1].id, name="user-input"
                        )
                    ]
                }

        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, middleware=[Probe()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert ids_seen and ids_seen[0] is not None
        assert [m.content for m in out["messages"]] == ["hi", "ok"]
        assert out["messages"][0].name == "user-input"

    def test_caller_supplied_ids_are_preserved(self) -> None:
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model)
        out = agent.invoke({"messages": [HumanMessage(content="hi", id="keep-me")]})

        assert out["messages"][0].id == "keep-me"

    def test_graph_exposes_checkpointer_and_store_attributes(self) -> None:
        agent = create_agent(FakeToolCallingModel([AIMessage(content="ok")]))
        assert hasattr(agent, "checkpointer")
        assert hasattr(agent, "store")

    def test_checkpointer_is_passed_through_identity(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        saver = CheckpointStore()
        agent = create_agent(FakeToolCallingModel([AIMessage(content="ok")]), checkpointer=saver)
        assert agent.checkpointer is saver

    def test_invalid_checkpointer_is_rejected_like_upstream(self) -> None:
        """Upstream raises TypeError for a non-saver; silently ignoring it would
        turn every threaded turn into a blank thread."""
        sentinel = object()
        with pytest.raises(
            TypeError,
            match="Invalid checkpointer provided.*Received object",
        ):
            create_agent(
                FakeToolCallingModel([AIMessage(content="ok")]), checkpointer=sentinel
            )

    def test_checkpointer_true_fails_closed_for_a_root_graph(self) -> None:
        """``True`` means "inherit the parent's saver"; there is no parent here.

        Upstream raises on the first run (not at construction), so the stored
        attribute stays ``True`` and the failure surfaces at ``invoke``.
        """
        agent = create_agent(
            FakeToolCallingModel([AIMessage(content="ok")]), checkpointer=True
        )
        assert agent.checkpointer is True
        with pytest.raises(
            RuntimeError, match="checkpointer=True cannot be used for root graphs"
        ):
            agent.invoke({"messages": [HumanMessage(content="one")]})
        with pytest.raises(
            RuntimeError, match="checkpointer=True cannot be used for root graphs"
        ):
            agent.get_state({"configurable": {"thread_id": "t1"}})

    def test_recursion_limit_defaults_to_9999(self) -> None:
        agent = create_agent(FakeToolCallingModel([AIMessage(content="ok")]))
        assert agent.config["recursion_limit"] == 9999


class TestToolLoop:
    def test_model_tool_model_runs_two_model_calls_and_appends_tool_message(self) -> None:
        calls = {"n": 0}

        def add(a: int, b: int) -> int:
            """Add two numbers."""
            calls["n"] += 1
            return a + b

        model = FakeToolCallingModel(
            [
                _tool_call_message("add", {"a": 1, "b": 2}, "c1"),
                AIMessage(content="3"),
            ]
        )
        agent = create_agent(model, tools=[add])
        out = agent.invoke({"messages": [HumanMessage(content="1+2?")]})
        assert len(model.calls) == 2
        assert calls["n"] == 1
        tool_messages = [m for m in out["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_messages) == 1
        assert tool_messages[0].content == "3"
        assert tool_messages[0].tool_call_id == "c1"

    def test_tools_are_bound_to_the_model(self) -> None:
        def noop() -> str:
            """Do nothing."""
            return "ok"

        model = FakeToolCallingModel([AIMessage(content="done")])
        create_agent(model, tools=[noop]).invoke({"messages": [HumanMessage(content="go")]})
        assert model.bound_tools is not None
        assert len(model.bound_tools) == 1


class TestFailClosed:
    def test_unimplemented_arguments_raise_precise_errors(self) -> None:
        model = FakeToolCallingModel([AIMessage(content="ok")])
        with pytest.raises(NotImplementedError, match="response_format"):
            create_agent(model, response_format=dict)
        with pytest.raises(NotImplementedError, match="interrupt_before"):
            create_agent(model, interrupt_before=["model"])
        with pytest.raises(NotImplementedError, match="transformers"):
            create_agent(model, transformers=[])


class _RecordingModel:
    """Chat model fake that records calls and can stream."""

    def __init__(self, responses: list[AIMessage]) -> None:
        self.responses = list(responses)
        self.calls: list[list[object]] = []
        self.bound_tools: list[object] | None = None

    def bind_tools(self, tools, **kwargs):
        self.bound_tools = list(tools)
        return self

    def bind(self, **kwargs):
        return self

    def invoke(self, messages, **kwargs):
        self.calls.append(list(messages))
        if not self.responses:
            raise AssertionError("_RecordingModel ran out of responses")
        return self.responses.pop(0)

    def stream(self, messages, **kwargs):
        self.calls.append(list(messages))
        yield self.responses.pop(0)


def _tool_middleware(name: str, order: list[str]):
    """Build an AgentMiddleware subclass recording every lifecycle hook."""
    from reactivegraph.middleware import AgentMiddleware

    class _Recorder(AgentMiddleware):
        def before_agent(self, state, runtime):
            order.append(f"{name}.before_agent")
            return None

        def before_model(self, state, runtime):
            order.append(f"{name}.before_model")
            return None

        def after_model(self, state, runtime):
            order.append(f"{name}.after_model")
            return None

        def after_agent(self, state, runtime):
            order.append(f"{name}.after_agent")
            return None

    _Recorder.name = name  # type: ignore[attr-defined]
    return _Recorder()


class TestMiddlewareContract:
    def test_hook_order_matches_upstream(self) -> None:
        """before_* run forward, after_* run in reverse (probe-verified)."""
        order: list[str] = []
        agent = create_agent(
            _RecordingModel([AIMessage(content="ok")]),
            middleware=[_tool_middleware("M1", order), _tool_middleware("M2", order)],
        )
        agent.invoke({"messages": [HumanMessage(content="hi")]})
        assert order == [
            "M1.before_agent",
            "M2.before_agent",
            "M1.before_model",
            "M2.before_model",
            "M2.after_model",
            "M1.after_model",
            "M2.after_agent",
            "M1.after_agent",
        ]

    def test_duplicate_middleware_instances_are_rejected(self) -> None:
        """Upstream asserts on duplicate `.name`; we must not silently accept."""
        order: list[str] = []
        mw = _tool_middleware("dup", order)
        with pytest.raises(AssertionError, match="Please remove duplicate middleware instances."):
            create_agent(_RecordingModel([AIMessage(content="ok")]), middleware=[mw, mw])

    def test_middleware_tools_are_collected_into_the_tool_node(self) -> None:
        from reactivegraph.middleware import AgentMiddleware

        def extra(x: str) -> str:
            """Echo."""
            return x

        class _ToolProvider(AgentMiddleware):
            def __init__(self) -> None:
                super().__init__()
                self.tools = [extra]

        agent = create_agent(
            _RecordingModel([AIMessage(content="ok")]), middleware=[_ToolProvider()]
        )
        assert sorted(agent.get_graph().nodes.keys()) == [
            "__end__",
            "__start__",
            "model",
            "tools",
        ]


class TestGraphShape:
    def test_node_names_without_tools_match_upstream(self) -> None:
        agent = create_agent(_RecordingModel([AIMessage(content="ok")]))
        assert sorted(agent.get_graph().nodes.keys()) == ["__end__", "__start__", "model"]

    def test_node_names_with_tools_match_upstream(self) -> None:
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        agent = create_agent(_RecordingModel([AIMessage(content="ok")]), tools=[add])
        assert sorted(agent.get_graph().nodes.keys()) == [
            "__end__",
            "__start__",
            "model",
            "tools",
        ]

    def test_edges_with_middleware_match_upstream_contract(self) -> None:
        """Graph inspection must expose the actual middleware routing surface.

        DeerFlow and its tests reason about node and edge shape, so hiding the
        middleware nodes behind a two-node facade makes the compatibility layer
        look correct while its real runtime topology is different.
        """
        from reactivegraph.middleware import AgentMiddleware, hook_config

        class Before(AgentMiddleware):
            name = "before"

            @hook_config(can_jump_to=["model", "end"])
            def before_model(self, state, runtime):
                return None

        class After(AgentMiddleware):
            name = "after"

            @hook_config(can_jump_to=["model", "tools", "end"])
            def after_model(self, state, runtime):
                return None

        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        agent = create_agent(
            _RecordingModel([AIMessage(content="ok")]),
            tools=[add],
            middleware=[Before(), After()],
        )

        assert sorted(agent.get_graph().nodes) == [
            "__end__",
            "__start__",
            "after.after_model",
            "before.before_model",
            "model",
            "tools",
        ]
        assert {
            (edge.source, edge.target, edge.conditional)
            for edge in agent.get_graph().edges
        } == {
            ("__start__", "before.before_model", False),
            ("before.before_model", "model", True),
            ("before.before_model", "__end__", True),
            ("model", "after.after_model", False),
            ("after.after_model", "tools", True),
            ("after.after_model", "before.before_model", True),
            ("after.after_model", "__end__", True),
            ("tools", "before.before_model", True),
        }


class TestJumpTo:
    """``jump_to`` is an ephemeral routing directive, not persisted state."""

    def test_after_model_can_reenter_model_once(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, hook_config

        class Jumper(AgentMiddleware):
            name = "jumper"

            def __init__(self) -> None:
                super().__init__()
                self.calls = 0
                self.observed_jump_to: list[Any] = []

            @hook_config(can_jump_to=["model"])
            def after_model(self, state, runtime):
                self.calls += 1
                self.observed_jump_to.append(state.get("jump_to"))
                if self.calls == 1:
                    return {"jump_to": "model"}
                return None

        middleware = Jumper()
        model = FakeToolCallingModel(
            [AIMessage(content="first"), AIMessage(content="second")]
        )
        agent = create_agent(model, middleware=[middleware])

        chunks = list(
            agent.stream(
                {"messages": [HumanMessage(content="hi")]},
                stream_mode=["updates", "values"],
            )
        )
        updates = [chunk[1] for chunk in chunks if chunk[0] == "updates"]
        values = [chunk[1] for chunk in chunks if chunk[0] == "values"]

        assert len(model.calls) == 2
        assert middleware.observed_jump_to == [None, None]
        assert [message.content for message in values[-1]["messages"]] == [
            "hi",
            "first",
            "second",
        ]
        assert all("jump_to" not in value for value in values)
        assert all(
            "jump_to" not in update
            for frame in updates
            for update in frame.values()
            if isinstance(update, dict)
        )

    def test_jump_without_declaration_is_ignored(self) -> None:
        from reactivegraph.middleware import AgentMiddleware

        class Undeclared(AgentMiddleware):
            name = "undeclared"

            def after_model(self, state, runtime):
                return {"jump_to": "model"}

        model = FakeToolCallingModel([AIMessage(content="only")])
        agent = create_agent(model, middleware=[Undeclared()])

        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert len(model.calls) == 1
        assert "jump_to" not in out

    def test_after_agent_can_jump_to_model(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, hook_config

        class Jumper(AgentMiddleware):
            name = "jumper"

            def __init__(self) -> None:
                super().__init__()
                self.calls = 0

            @hook_config(can_jump_to=["model"])
            def after_agent(self, state, runtime):
                self.calls += 1
                if self.calls == 1:
                    return {"jump_to": "model"}
                return None

        middleware = Jumper()
        model = FakeToolCallingModel(
            [AIMessage(content="first"), AIMessage(content="second")]
        )
        agent = create_agent(model, middleware=[middleware])

        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert len(model.calls) == 2
        assert middleware.calls == 2
        assert [message.content for message in out["messages"]] == [
            "hi",
            "first",
            "second",
        ]
        assert "jump_to" not in out


class TestRecursionLimit:
    """``recursion_limit`` counts LangGraph *super-steps*, not turns.

    Probe-verified against langgraph 1.2.9: ``create_agent`` compiles one node
    per middleware hook pair, so a turn costs ``before_model nodes + model +
    after_model nodes + tools``; the run needs one more unit for the terminating
    super-step, which is why a graph that executes exactly ``limit`` nodes still
    raises. Exhaustion must surface as ``GraphRecursionError`` *after* the work
    already committed has been streamed, never as a silent truncation.
    """

    @staticmethod
    def _echo_tool():
        def echo(x: str) -> str:
            """Echo the token back."""
            return x

        return echo

    def _two_turn_model(self) -> FakeToolCallingModel:
        return FakeToolCallingModel(
            [
                _tool_call_message("echo", {"x": "hi"}, "c1"),
                AIMessage(content="done"),
            ]
        )

    def test_completes_when_the_limit_covers_every_super_step(self) -> None:
        model = self._two_turn_model()
        agent = create_agent(model, tools=[self._echo_tool()])
        out = agent.invoke(
            {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 4}
        )
        assert len(model.calls) == 2
        assert out["messages"][-1].content == "done"

    def test_exhausted_limit_stops_before_the_next_node(self) -> None:
        """limit=2 runs ``model`` and ``tools``, then refuses the second model."""
        model = self._two_turn_model()
        agent = create_agent(model, tools=[self._echo_tool()])
        with pytest.raises(GraphRecursionError, match="Recursion limit of 2 reached"):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 2}
            )
        assert len(model.calls) == 1

    def test_exactly_limit_nodes_executed_still_raises(self) -> None:
        """The terminating super-step is charged too, as upstream does."""
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model)
        with pytest.raises(GraphRecursionError, match="Recursion limit of 1 reached"):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 1}
            )
        assert len(model.calls) == 1

    @pytest.mark.parametrize("limit", [0, -1, -100])
    def test_limit_below_one_is_rejected(self, limit: int) -> None:
        agent = create_agent(FakeToolCallingModel([AIMessage(content="ok")]))
        with pytest.raises(ValueError, match="recursion_limit must be at least 1"):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]},
                {"recursion_limit": limit},
            )

    def test_run_config_limit_overrides_the_compiled_default(self) -> None:
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model)
        assert agent.config["recursion_limit"] == 9999
        agent.invoke({"messages": [HumanMessage(content="go")]}, {"recursion_limit": 5})
        assert len(model.calls) == 1

    def test_default_limit_lets_a_long_loop_finish(self) -> None:
        responses = [
            _tool_call_message("echo", {"x": str(i)}, f"c{i}") for i in range(20)
        ]
        responses.append(AIMessage(content="done"))
        model = FakeToolCallingModel(responses)
        agent = create_agent(model, tools=[self._echo_tool()])
        out = agent.invoke({"messages": [HumanMessage(content="go")]})
        assert len(model.calls) == 21
        assert out["messages"][-1].content == "done"

    def test_middleware_hook_nodes_are_charged(self) -> None:
        """One ``before_model`` middleware adds one super-step per turn."""
        from reactivegraph.middleware import AgentMiddleware

        class BeforeModel(AgentMiddleware):
            name = "before-model-probe"

            def __init__(self) -> None:
                self.calls = 0

            def before_model(self, state, runtime):
                self.calls += 1
                return None

        probe = BeforeModel()
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, middleware=[probe])
        with pytest.raises(GraphRecursionError):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 2}
            )
        assert (probe.calls, len(model.calls)) == (1, 1)

        model = FakeToolCallingModel([AIMessage(content="ok")])
        probe = BeforeModel()
        agent = create_agent(model, middleware=[probe])
        agent.invoke({"messages": [HumanMessage(content="go")]}, {"recursion_limit": 3})
        assert (probe.calls, len(model.calls)) == (1, 1)

    def test_exhaustion_before_the_model_never_calls_it(self) -> None:
        from reactivegraph.middleware import AgentMiddleware

        class BeforeModel(AgentMiddleware):
            name = "before-model-probe"

            def before_model(self, state, runtime):
                return None

        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, middleware=[BeforeModel()])
        with pytest.raises(GraphRecursionError):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 1}
            )
        assert model.calls == []

    def test_stream_yields_the_partial_state_before_raising(self) -> None:
        """Upstream commits each super-step, so the tool result is visible."""
        model = self._two_turn_model()
        agent = create_agent(model, tools=[self._echo_tool()])
        chunks: list[dict] = []
        with pytest.raises(GraphRecursionError):
            for chunk in agent.stream(
                {"messages": [HumanMessage(content="go")]},
                {"recursion_limit": 2},
                stream_mode="values",
            ):
                chunks.append(chunk)
        last = chunks[-1]
        assert [type(m).__name__ for m in last["messages"]] == [
            "HumanMessage",
            "AIMessage",
            "ToolMessage",
        ]
        assert not [k for k in last if k.startswith("__reactivegraph")]

    def test_run_private_budget_keys_never_leak_into_the_result(self) -> None:
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model)
        out = agent.invoke({"messages": [HumanMessage(content="go")]})
        assert not [k for k in out if k.startswith("__reactivegraph")]

    def test_error_message_matches_upstream_verbatim(self) -> None:
        """DeerFlow's CLI shows this text; a drift would be user-visible."""
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model)
        with pytest.raises(GraphRecursionError) as excinfo:
            agent.invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 1}
            )
        assert str(excinfo.value) == (
            "Recursion limit of 1 reached without hitting a stop condition. You "
            "can increase the limit by setting the `recursion_limit` config key.\n"
            "For troubleshooting, visit: https://docs.langchain.com/oss/python/"
            "langgraph/errors/GRAPH_RECURSION_LIMIT"
        )

    @staticmethod
    def _one_shot_agent() -> Any:
        return create_agent(FakeToolCallingModel([AIMessage(content="ok")]))

    def test_none_limit_keeps_the_compiled_default(self) -> None:
        """``None`` means "use the default", not "unlimited" (upstream parity)."""
        model = FakeToolCallingModel([AIMessage(content="ok")])
        create_agent(model).invoke(
            {"messages": [HumanMessage(content="go")]}, {"recursion_limit": None}
        )
        assert len(model.calls) == 1

    def test_non_numeric_limit_raises_type_error(self) -> None:
        """Upstream raises from the comparison itself, not a custom check."""
        with pytest.raises(TypeError):
            self._one_shot_agent().invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": "3"}
            )

    def test_boolean_limit_compares_as_one(self) -> None:
        """``True`` is an int, so it behaves exactly like ``1``."""
        with pytest.raises(GraphRecursionError, match="Recursion limit of True"):
            self._one_shot_agent().invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": True}
            )

    def test_fractional_limits_compare_against_the_fractional_bound(self) -> None:
        """``step <= stop`` is a float comparison, so 1.5 fails but 2.5 passes."""
        with pytest.raises(GraphRecursionError, match="Recursion limit of 1.5"):
            self._one_shot_agent().invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 1.5}
            )
        model = FakeToolCallingModel([AIMessage(content="ok")])
        create_agent(model).invoke(
            {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 2.5}
        )
        assert len(model.calls) == 1

    def test_limit_above_the_compiled_ceiling_fails_closed(self) -> None:
        """A limit the engine cannot honour must raise, never truncate."""
        from reactivegraph.create_agent import _MAX_RECURSION_LIMIT

        agent = create_agent(FakeToolCallingModel([AIMessage(content="ok")]))
        with pytest.raises(ValueError, match="exceeds ReactiveGraph's compiled ceiling"):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]},
                {"recursion_limit": _MAX_RECURSION_LIMIT + 1},
            )

    def test_intermediate_base_override_still_compiles_a_node(self) -> None:
        """Upstream compares against the *framework* base, not the nearest one.

        A hook defined on a user's intermediate base class is an override even
        though it is the most-base definition in the concrete class's MRO.
        """
        from reactivegraph.middleware import AgentMiddleware

        class SharedBase(AgentMiddleware):
            def before_model(self, state, runtime):
                return None

        class Concrete(SharedBase):
            pass

        # before_model + model + terminating tick = 3, so 2 is one short.
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, middleware=[Concrete()])
        with pytest.raises(GraphRecursionError):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 2}
            )
        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, middleware=[Concrete()])
        agent.invoke({"messages": [HumanMessage(content="go")]}, {"recursion_limit": 3})
        assert len(model.calls) == 1

    def test_bare_base_hooks_do_not_compile_nodes(self) -> None:
        """An untouched ``AgentMiddleware`` contributes no node at all."""
        from reactivegraph.middleware import AgentMiddleware

        class Bare(AgentMiddleware):
            pass

        model = FakeToolCallingModel([AIMessage(content="ok")])
        agent = create_agent(model, middleware=[Bare()])
        # Only the model node and the terminating tick are charged.
        agent.invoke({"messages": [HumanMessage(content="go")]}, {"recursion_limit": 2})

    def test_budget_resets_between_runs_on_the_same_thread(self) -> None:
        """Upstream gives every ``invoke`` a fresh loop, so a second run may
        spend its own allowance even after the first one exhausted its own."""
        model = FakeToolCallingModel(
            [
                _tool_call_message("echo", {"x": "hi"}, "c1"),
                AIMessage(content="done"),
                AIMessage(content="done again"),
            ]
        )
        agent = create_agent(model, tools=[self._echo_tool()])
        config = {"configurable": {"thread_id": "t"}}
        # First run: two model calls + one tool + terminating tick = 4.
        with pytest.raises(GraphRecursionError):
            agent.invoke(
                {"messages": [HumanMessage(content="go")]},
                {**config, "recursion_limit": 3},
            )
        # Second run gets its own budget; a stale exhausted flag would make it
        # raise even though it has steps to spare.
        out = agent.invoke(
            {"messages": [HumanMessage(content="go again")]},
            {**config, "recursion_limit": 3},
        )
        assert out["messages"][-1].content == "done again"

    def test_exhausted_run_checkpoints_the_partial_turn(self) -> None:
        """DeerFlow's executor catches the error and reads the partial state
        back to report ``turn_capped``, so the work must be persisted."""
        from reactivegraph.checkpoint import MemoryCheckpointSaver

        model = self._two_turn_model()
        agent = create_agent(
            model, tools=[self._echo_tool()], checkpointer=MemoryCheckpointSaver()
        )
        config = {"configurable": {"thread_id": "t"}, "recursion_limit": 2}
        with pytest.raises(GraphRecursionError):
            agent.invoke({"messages": [HumanMessage(content="go")]}, config)
        snapshot = agent.get_state(config)
        assert [type(m).__name__ for m in snapshot.values["messages"]] == [
            "HumanMessage",
            "AIMessage",
            "ToolMessage",
        ]
        assert not [k for k in snapshot.values if k.startswith("__reactivegraph")]

    def test_ainvoke_honours_the_run_config_limit(self) -> None:
        import asyncio

        model = self._two_turn_model()
        agent = create_agent(model, tools=[self._echo_tool()])

        async def _run():
            return await agent.ainvoke(
                {"messages": [HumanMessage(content="go")]}, {"recursion_limit": 2}
            )

        with pytest.raises(GraphRecursionError):
            asyncio.run(_run())
        assert len(model.calls) == 1

    def test_astream_honours_the_run_config_limit(self) -> None:
        import asyncio

        model = self._two_turn_model()
        agent = create_agent(model, tools=[self._echo_tool()])

        async def _run():
            chunks = []
            async for chunk in agent.astream(
                {"messages": [HumanMessage(content="go")]},
                {"recursion_limit": 2},
                stream_mode="values",
            ):
                chunks.append(chunk)
            return chunks

        with pytest.raises(GraphRecursionError):
            asyncio.run(_run())
        assert len(model.calls) == 1


class TestStreamModes:
    """DeerFlow drives the graph through ``astream(stream_mode=[...])``.

    ``worker.py`` asks for ``["values", "messages", "custom"]`` and
    ``executor.py`` for ``"values"``; ignoring the argument would silently
    degrade the product surface, so unsupported modes must raise.
    """

    def test_single_mode_updates_yields_node_keyed_dicts(self) -> None:
        """One chunk per node run; the payload is the *delta*, not full state."""
        agent = create_agent(_RecordingModel([AIMessage(content="ok")]))
        chunks = list(
            agent.stream({"messages": [HumanMessage(content="hi")]}, stream_mode="updates")
        )
        assert len(chunks) == 1
        assert set(chunks[0]) == {"model"}
        (only,) = chunks[0]["model"]["messages"]
        assert only.content == "ok"

    def test_updates_reports_tools_node_for_tool_results(self) -> None:
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        model = _RecordingModel(
            [
                _tool_call_message("add", {"a": 1, "b": 2}, "c1"),
                AIMessage(content="3"),
            ]
        )
        chunks = list(
            create_agent(model, tools=[add]).stream(
                {"messages": [HumanMessage(content="q")]}, stream_mode="updates"
            )
        )
        assert [next(iter(c)) for c in chunks] == ["model", "tools", "model"]
        tool_update = chunks[1]["tools"]["messages"]
        assert len(tool_update) == 1
        assert tool_update[0].content == "3"

    def test_multi_mode_yields_mode_chunk_tuples(self) -> None:
        agent = create_agent(_RecordingModel([AIMessage(content="ok")]))
        chunks = list(
            agent.stream(
                {"messages": [HumanMessage(content="hi")]},
                stream_mode=["updates", "values"],
            )
        )
        assert all(isinstance(c, tuple) and len(c) == 2 for c in chunks)
        modes = [c[0] for c in chunks]
        assert "updates" in modes
        assert "values" in modes

    def test_values_snapshots_follow_real_middleware_node_topology(self) -> None:
        """Each middleware hook is its own super-step, so each emits a frame.

        Upstream compiles ``first.before_model -> second.before_model -> model
        -> second.after_model -> first.after_model``. Collapsing those nodes
        into one task loses the intermediate snapshots DeerFlow consumes.
        """
        from reactivegraph.middleware import AgentMiddleware

        class First(AgentMiddleware):
            name = "first"

            def before_model(self, state, runtime):
                return {"trace": [*state.get("trace", []), "first.before_model"]}

            def after_model(self, state, runtime):
                return {"trace": [*state.get("trace", []), "first.after_model"]}

        class Second(AgentMiddleware):
            name = "second"

            def before_model(self, state, runtime):
                return {"trace": [*state.get("trace", []), "second.before_model"]}

            def after_model(self, state, runtime):
                return {"trace": [*state.get("trace", []), "second.after_model"]}

        agent = create_agent(
            _RecordingModel([AIMessage(content="ok")]),
            middleware=[First(), Second()],
        )
        chunks = list(
            agent.stream(
                {"messages": [HumanMessage(content="hi")]}, stream_mode="values"
            )
        )

        assert [chunk.get("trace", []) for chunk in chunks] == [
            [],
            ["first.before_model"],
            ["first.before_model", "second.before_model"],
            ["first.before_model", "second.before_model"],
            ["first.before_model", "second.before_model", "second.after_model"],
            [
                "first.before_model",
                "second.before_model",
                "second.after_model",
                "first.after_model",
            ],
        ]

    def test_noop_middleware_does_not_emit_a_values_snapshot(self) -> None:
        """Private budget writes are not public state changes.

        LangGraph only emits ``values`` after a super-step that wrote a public
        channel. A middleware hook returning ``None`` still advances the
        recursion budget, but must not look like an observable state update to
        DeerFlow's stream consumer.
        """
        from reactivegraph.middleware import AgentMiddleware

        class Noop(AgentMiddleware):
            name = "noop"

            def before_model(self, state, runtime):
                return None

            def after_model(self, state, runtime):
                return None

        agent = create_agent(
            _RecordingModel([AIMessage(content="ok")]),
            middleware=[Noop()],
        )
        chunks = list(
            agent.stream(
                {"messages": [HumanMessage(content="hi")]}, stream_mode="values"
            )
        )

        assert len(chunks) == 2
        assert [message.content for message in chunks[-1]["messages"]] == [
            "hi",
            "ok",
        ]

    def test_default_stream_mode_is_updates(self) -> None:
        agent = create_agent(_RecordingModel([AIMessage(content="ok")]))
        chunks = list(agent.stream({"messages": [HumanMessage(content="hi")]}))
        assert chunks and set(chunks[0]) == {"model"}

    def test_unsupported_stream_mode_raises_instead_of_degrading(self) -> None:
        agent = create_agent(_RecordingModel([AIMessage(content="ok")]))
        with pytest.raises(ValueError, match="bogus"):
            list(agent.stream({"messages": [HumanMessage(content="hi")]}, stream_mode="bogus"))

    def test_astream_accepts_stream_mode_and_context(self) -> None:
        import asyncio

        agent = create_agent(_RecordingModel([AIMessage(content="ok")]), context_schema=dict)

        async def _run():
            return [
                c
                async for c in agent.astream(
                    {"messages": [HumanMessage(content="hi")]},
                    stream_mode="values",
                    context={},
                )
            ]

        chunks = asyncio.run(_run())
        assert chunks
        assert all("messages" in c for c in chunks)


class TestNoFrameworkImports:
    def test_create_agent_module_does_not_import_langchain_or_langgraph(self) -> None:
        import pathlib
        import re

        source = (
            pathlib.Path(__file__).resolve().parents[1]
            / "reactivegraph"
            / "create_agent.py"
        ).read_text()
        assert not re.search(r"^\s*(from|import)\s+(langchain|langgraph)", source, re.M)


class TestForeignModelBoundary:
    """A *real* chat model must be able to read the transcript we hand it.

    LangChain's ``convert_to_messages`` rejects our classes outright
    (``NotImplementedError: Unsupported message type``), so the engine must
    serialize outbound messages into the flat ``{type, **dump}`` dict shape
    LangChain coerces. These tests use a recording stand-in that asserts on the
    boundary payload instead of importing langchain_core.
    """

    class DictOnlyModel:
        """Accepts only the flat dict shape; rejects anything else."""

        def __init__(self, replies: list[object]) -> None:
            self.replies = list(replies)
            self.seen: list[list[object]] = []

        def bind_tools(self, tools, **kwargs):
            return self

        def invoke(self, messages, **kwargs):
            self.seen.append(list(messages))
            for message in messages:
                if not isinstance(message, dict):
                    raise NotImplementedError(
                        f"Unsupported message type: {type(message)}"
                    )
                if "type" not in message:
                    raise ValueError("message dict must carry 'type'")
            return self.replies.pop(0)

    def test_model_receives_flat_dicts_not_engine_objects(self) -> None:
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        model = self.DictOnlyModel([AIMessage(content="hello")])
        agent = create_agent(model)
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        (seen,) = model.seen
        assert [m["type"] for m in seen] == ["human"]
        assert seen[0]["content"] == "hi"

    def test_system_prompt_is_also_a_flat_dict(self) -> None:
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        model = self.DictOnlyModel([AIMessage(content="ok")])
        agent = create_agent(model, system_prompt="be brief")
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        (seen,) = model.seen
        assert [m["type"] for m in seen] == ["system", "human"]

    def test_foreign_messages_returned_by_the_model_are_adopted(self) -> None:
        """A model handing back *its* AIMessage must land in our state."""
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class ForeignReply:
            type = "ai"

            def model_dump(self) -> dict:
                return {
                    "content": "from the model",
                    "additional_kwargs": {},
                    "response_metadata": {},
                    "type": "ai",
                    "name": None,
                    "id": None,
                    "tool_calls": [],
                    "invalid_tool_calls": [],
                }

        model = self.DictOnlyModel([ForeignReply()])
        agent = create_agent(model)
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert isinstance(out["messages"][-1], AIMessage)
        assert out["messages"][-1].content == "from the model"


class ForeignAgentMiddlewareStandIn:
    """Faithful stand-in for ``langchain.agents.middleware.AgentMiddleware``.

    DeerFlow's middleware subclasses *LangChain's* base, not ours. That base
    defines all four wrapper stubs, each raising ``NotImplementedError`` with
    the canonical message; a subclass that overrides only hooks therefore
    inherits raising stubs. Detection must be structural, since identity can
    never match across the two class hierarchies.
    """

    name = "foreign"

    def wrap_model_call(self, request, handler):
        raise NotImplementedError(
            "Synchronous implementation of wrap_model_call is not available. "
            "You are likely encountering this error because you defined only the async version "
            "(awrap_model_call) and invoked your agent in a synchronous context "
            "(e.g., using `stream()` or `invoke()`)."
        )

    async def awrap_model_call(self, request, handler):
        raise NotImplementedError(
            "Asynchronous implementation of awrap_model_call is not available. "
            "You are likely encountering this error because you defined only the sync version "
            "(wrap_model_call) and invoked your agent in an asynchronous context "
            "(e.g., using `astream()` or `ainvoke()`)."
        )

    def wrap_tool_call(self, request, handler):
        raise NotImplementedError(
            "Synchronous implementation of wrap_tool_call is not available. "
            "You are likely encountering this error because you defined only the async version "
            "(awrap_tool_call) and invoked your agent in a synchronous context "
            "(e.g., using `stream()` or `invoke()`)."
        )

    async def awrap_tool_call(self, request, handler):
        raise NotImplementedError(
            "Asynchronous implementation of awrap_tool_call is not available. "
            "You are likely encountering this error because you defined only the sync version "
            "(wrap_tool_call) and invoked your agent in an asynchronous context "
            "(e.g., using `astream()` or `ainvoke()`)."
        )

    def before_model(self, state, runtime):
        return None


class TestHostMiddlewareMessageInjection:
    """Host middleware patches *its own* message objects into the request.

    DeerFlow's ``DanglingToolCallMiddleware`` synthesizes ``ToolMessage``
    instances from the host's class hierarchy and splices them into
    ``request.messages``. The outbound bridge must therefore accept a foreign
    message object, not just our own.
    """

    class ForeignToolMessage:
        type = "tool"

        def __init__(self, content: str, tool_call_id: str) -> None:
            self.content = content
            self.tool_call_id = tool_call_id

        def model_dump(self) -> dict:
            return {
                "content": self.content,
                "additional_kwargs": {},
                "response_metadata": {},
                "type": "tool",
                "name": "x",
                "id": None,
                "tool_call_id": self.tool_call_id,
                "artifact": None,
                "status": "success",
            }

    def test_flat_dict_helper_accepts_a_foreign_message(self) -> None:
        from reactivegraph.messages import message_to_foreign_dict

        flat = message_to_foreign_dict(self.ForeignToolMessage("patched", "c1"))
        assert flat["type"] == "tool"
        assert flat["content"] == "patched"
        assert flat["tool_call_id"] == "c1"

    def test_flat_dict_helper_still_rejects_non_messages(self) -> None:
        from reactivegraph.messages import message_to_foreign_dict

        class NotAMessage:
            pass

        with pytest.raises(TypeError):
            message_to_foreign_dict(NotAMessage())

    def test_middleware_injected_foreign_message_reaches_the_model(self) -> None:
        """The full path: middleware splices, model receives a flat dict."""
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        foreign_tool = self.ForeignToolMessage("patched", "c1")

        class PatchingMiddleware(ForeignAgentMiddlewareStandIn):
            name = "patching"

            def wrap_model_call(self, request, handler):
                return handler(request.override(messages=[*request.messages, foreign_tool]))

        class RecordingModel:
            def __init__(self) -> None:
                self.seen: list[list[object]] = []

            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, **kwargs):
                self.seen.append(list(messages))
                return AIMessage(content="ok")

        model = RecordingModel()
        agent = create_agent(model, middleware=[PatchingMiddleware()])
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        (seen,) = model.seen
        assert [m["type"] for m in seen] == ["human", "tool"]
        assert seen[1]["tool_call_id"] == "c1"

    def test_foreign_messages_returned_by_middleware_are_adopted(self) -> None:
        """``after_model`` updates carrying host messages must be normalized."""
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage, ToolMessage

        foreign_tool = self.ForeignToolMessage("from mw", "c9")

        class InjectingMiddleware(ForeignAgentMiddlewareStandIn):
            name = "injecting"

            def after_model(self, state, runtime):
                return {"messages": [foreign_tool]}

        class PlainModel:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, **kwargs):
                return AIMessage(content="ok")

        agent = create_agent(PlainModel(), middleware=[InjectingMiddleware()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        adopted = [m for m in out["messages"] if type(m).__name__ == "ToolMessage"]
        assert adopted, [type(m).__name__ for m in out["messages"]]
        assert isinstance(adopted[0], ToolMessage)
        assert adopted[0].content == "from mw"


class TestMiddlewareWrapToolCall:
    """``create_agent`` must apply ``wrap_tool_call`` middleware.

    DeerFlow's harness defines ``wrap_tool_call`` in 15 files (measured by
    ``rg -l 'def wrap_tool_call' packages/harness``). Upstream applies them in
    the tools node; ignoring them would silently drop caller-registered
    behaviour, which our fail-closed policy forbids.

    Upstream probe (langchain 1.3.14): a middleware that rewrites the result
    content yields ``ToolMessage('[wrapped]3')``.
    """

    def test_sync_wrap_tool_call_rewrites_the_tool_result(self) -> None:
        from reactivegraph.create_agent import create_agent
        from reactivegraph.middleware import AgentMiddleware

        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        class WrapTool(AgentMiddleware):
            name = "wrap_tool"

            def wrap_tool_call(self, request, handler):
                result = handler(request)
                result.content = f"[wrapped]{result.content}"
                return result

        model = FakeToolCallingModel(
            [_tool_call_message("add", {"a": 1, "b": 2}, "c1"), AIMessage(content="done")]
        )
        agent = create_agent(model, tools=[add], middleware=[WrapTool()])
        out = agent.invoke({"messages": [HumanMessage(content="go")]})

        tool_messages = [m for m in out["messages"] if isinstance(m, ToolMessage)]
        assert len(tool_messages) == 1
        assert tool_messages[0].content == "[wrapped]3"

    def test_wrap_tool_call_receives_the_documented_request_shape(self) -> None:
        from reactivegraph.create_agent import create_agent

        seen: dict[str, Any] = {}

        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        from reactivegraph.middleware import AgentMiddleware

        class Probe(AgentMiddleware):
            name = "probe"

            def wrap_tool_call(self, request, handler):
                seen["type"] = type(request).__name__
                seen["fields"] = sorted(
                    getattr(request, "__dataclass_fields__", {})
                )
                seen["tool_call"] = dict(request.tool_call)
                seen["tool_name"] = getattr(request.tool, "name", None)
                seen["state_has_messages"] = isinstance(request.state, dict) and (
                    "messages" in request.state
                )
                seen["runtime_type"] = type(request.runtime).__name__
                return handler(request)

        model = FakeToolCallingModel(
            [_tool_call_message("add", {"a": 1, "b": 2}, "c1"), AIMessage(content="done")]
        )
        agent = create_agent(model, tools=[add], middleware=[Probe()])
        agent.invoke({"messages": [HumanMessage(content="go")]})

        assert seen["type"] == "ToolCallRequest"
        assert seen["fields"] == ["runtime", "state", "tool", "tool_call"]
        assert seen["tool_call"]["name"] == "add"
        assert seen["tool_call"]["args"] == {"a": 1, "b": 2}
        assert seen["tool_call"]["id"] == "c1"
        assert seen["tool_name"] == "add"
        assert seen["state_has_messages"] is True
        assert seen["runtime_type"] == "ToolRuntime"

    def test_a_foreign_base_tool_wrapper_still_runs(self) -> None:
        """Real DeerFlow middleware subclasses *LangChain's* AgentMiddleware.

        Detection must be structural, exactly as for ``wrap_model_call``.
        """
        from reactivegraph.create_agent import create_agent

        foreign_base = ForeignAgentMiddlewareStandIn

        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        class WrapTool(foreign_base):
            name = "foreign_wrap"

            def wrap_tool_call(self, request, handler):
                result = handler(request)
                result.content = f"<{result.content}>"
                return result

        model = FakeToolCallingModel(
            [_tool_call_message("add", {"a": 1, "b": 2}, "c1"), AIMessage(content="done")]
        )
        agent = create_agent(model, tools=[add], middleware=[WrapTool()])
        out = agent.invoke({"messages": [HumanMessage(content="go")]})

        tool_messages = [m for m in out["messages"] if isinstance(m, ToolMessage)]
        assert [m.content for m in tool_messages] == ["<3>"]

    def test_tool_wrapper_errors_are_not_swallowed_as_tool_messages(self) -> None:
        """Upstream lets ``wrap_tool_call`` exceptions propagate unless
        ``handle_tool_errors`` is configured. Our tools task must not convert
        an engine-level wrapper failure into a fake successful tool result.
        """
        import pytest

        from reactivegraph.create_agent import create_agent

        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        from reactivegraph.middleware import AgentMiddleware

        class Boom(AgentMiddleware):
            name = "boom"

            def wrap_tool_call(self, request, handler):
                raise RuntimeError("middleware exploded")

        model = FakeToolCallingModel(
            [_tool_call_message("add", {"a": 1, "b": 2}, "c1"), AIMessage(content="done")]
        )
        agent = create_agent(model, tools=[add], middleware=[Boom()])
        with pytest.raises(RuntimeError, match="middleware exploded"):
            agent.invoke({"messages": [HumanMessage(content="go")]})


class TestForeignMiddlewareBaseClasses:
    """Host middleware subclasses *its* AgentMiddleware, not ours.

    Our composition step must skip the inherited ``wrap_model_call`` stub, but
    the identity check can only see our own base class. A host middleware that
    only defines state hooks therefore inherits a stub that raises
    ``NotImplementedError`` and would be invoked as if it were an override.

    Upstream's rule is behavioural: only a *user-defined* ``wrap_model_call``
    participates in the chain. The stand-in below mirrors LangChain's shape
    (a base class whose stub raises, subclasses overriding only hooks).
    """

    ForeignAgentMiddleware = ForeignAgentMiddlewareStandIn

    def test_hook_only_foreign_middleware_is_not_treated_as_an_override(self) -> None:
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class HookOnly(self.ForeignAgentMiddleware):
            name = "hook_only"

            def before_model(self, state, runtime):
                return {"messages": [*state.get("messages", []), HumanMessage(content="mw")]}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, **kwargs):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[HookOnly()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert [m.content for m in out["messages"]] == ["hi", "mw", "ok"]

    def test_a_real_override_on_a_foreign_base_still_runs(self) -> None:
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class Overriding(self.ForeignAgentMiddleware):
            name = "overriding"

            def wrap_model_call(self, request, handler):
                response = handler(request)
                response.result[0].content = "wrapped"
                return response

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, **kwargs):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[Overriding()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert out["messages"][-1].content == "wrapped"

    def test_async_only_foreign_middleware_still_raises_in_sync_context(self) -> None:
        """Upstream includes async-only middleware in the *sync* chain so the
        inherited sync stub raises. Skipping it would silently ignore a
        middleware the caller explicitly registered.

        Probe-verified against langchain 1.3.14:
        ``create_agent(Model(), middleware=[AsyncOnly()]).invoke(...)`` raises
        ``NotImplementedError: Synchronous implementation of wrap_model_call is
        not available.``
        """
        import pytest

        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class AsyncOnly(self.ForeignAgentMiddleware):
            name = "async_only"

            async def awrap_model_call(self, request, handler):
                return await handler(request)

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, **kwargs):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[AsyncOnly()])
        with pytest.raises(
            NotImplementedError,
            match="Synchronous implementation of wrap_model_call",
        ):
            agent.invoke({"messages": [HumanMessage(content="hi")]})


class TestMiddlewareRuntime:
    """Middleware hooks receive a live run-scoped runtime, never ``None``.

    Probe-verified against langchain 1.3.14 / langgraph 1.2.9: every hook
    (``before_agent``/``before_model``/``after_model``/``after_agent``) and
    ``request.runtime`` in ``wrap_model_call`` receive a ``Runtime`` whose
    ``context`` is the *same dict object* the caller handed in. DeerFlow
    depends on both facts: 35 middleware modules read ``runtime.context``
    (thread_id, run_id, user_id), and several guard middlewares write
    ``runtime.context["stop_reason"]`` which the run worker reads back after
    the stream ends.
    """

    def _agent_with(self, hook_name: str, record: dict[str, Any]):
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class Probe(ForeignAgentMiddlewareStandIn):
            name = "probe"

        def hook(self, state, runtime):
            record[hook_name] = runtime
            return None

        setattr(Probe, hook_name, hook)
        if hook_name == "before_agent":
            # before_agent alone would otherwise never fire for a single turn.
            pass

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        return create_agent(Model(), middleware=[Probe()]), HumanMessage

    def test_before_agent_receives_a_runtime_with_context(self) -> None:
        record: dict[str, Any] = {}
        agent, make_human_message = self._agent_with("before_agent", record)
        context = {"thread_id": "T1"}
        agent.invoke(
            {"messages": [make_human_message(content="hi")]},
            config={"configurable": {"__pregel_runtime": _runtime(context)}},
        )

        runtime = record["before_agent"]
        assert runtime is not None, "hooks must never receive None"
        assert runtime.context == {"thread_id": "T1"}

    def test_middleware_context_mutation_is_visible_to_the_caller(self) -> None:
        """DeerFlow writes ``runtime.context['stop_reason']`` from a guard
        middleware and the run worker reads it back off the dict it passed in."""
        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class Guard(ForeignAgentMiddlewareStandIn):
            name = "guard"

            def after_model(self, state, runtime):
                runtime.context["stop_reason"] = "loop_capped"
                return None

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        context = {"thread_id": "T1"}
        agent = create_agent(Model(), middleware=[Guard()])
        agent.invoke(
            {"messages": [HumanMessage(content="hi")]},
            config={"configurable": {"__pregel_runtime": _runtime(context)}},
        )

        assert context["stop_reason"] == "loop_capped"

    def test_wrap_model_call_request_carries_state_and_runtime(self) -> None:
        record: dict[str, Any] = {}

        from reactivegraph.create_agent import create_agent
        from reactivegraph.messages import AIMessage, HumanMessage

        class Probe(ForeignAgentMiddlewareStandIn):
            name = "probe"

            def wrap_model_call(self, request, handler):
                record["state"] = request.state
                record["runtime"] = request.runtime
                return handler(request)

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[Probe()])
        agent.invoke(
            {"messages": [HumanMessage(content="hi")]},
            config={"configurable": {"__pregel_runtime": _runtime({"thread_id": "T1"})}},
        )

        assert isinstance(record["state"], dict), type(record["state"]).__name__
        assert record["state"]["messages"][0].content == "hi"
        assert record["runtime"].context == {"thread_id": "T1"}

    def test_hooks_receive_the_runtime_the_caller_pinned(self) -> None:
        """``configurable['__pregel_runtime']`` is how DeerFlow's worker
        installs context when it drives ``astream(config=...)`` directly."""
        record: dict[str, Any] = {}
        agent, make_human_message = self._agent_with("before_model", record)
        pinned = _runtime({"thread_id": "T9"})
        agent.invoke(
            {"messages": [make_human_message(content="hi")]},
            config={"configurable": {"__pregel_runtime": pinned}},
        )

        assert record["before_model"] is pinned


class TestMiddlewareStateWrites:
    """Hook return values are state updates, not scratch values.

    Upstream merges every ``before_*``/``after_*`` return dict into the graph
    state. DeerFlow depends on it: ``ThreadDataMiddleware.before_agent``
    returns ``{"thread_data": {...}}`` and the CLI reads
    ``state["thread_data"]["outputs_path"]`` afterwards.
    """

    def test_before_agent_write_reaches_the_caller(self) -> None:
        from reactivegraph.messages import AIMessage, HumanMessage

        class Writer(ForeignAgentMiddlewareStandIn):
            name = "writer"

            def before_agent(self, state, runtime):
                return {"thread_data": {"outputs_path": "/tmp/out"}}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[Writer()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert out["thread_data"] == {"outputs_path": "/tmp/out"}

    def test_before_model_and_after_hook_writes_reach_the_caller(self) -> None:
        from reactivegraph.messages import AIMessage, HumanMessage

        class Writer(ForeignAgentMiddlewareStandIn):
            name = "writer"

            def before_model(self, state, runtime):
                return {"seen_before_model": True}

            def after_model(self, state, runtime):
                return {"seen_after_model": True}

            def after_agent(self, state, runtime):
                return {"seen_after_agent": True}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[Writer()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert out["seen_before_model"] is True
        assert out["seen_after_model"] is True
        assert out["seen_after_agent"] is True

    def test_before_agent_runs_once_even_when_the_loop_re_enters(self) -> None:
        """``before_agent`` is a per-run hook, not a per-model-call hook."""
        from reactivegraph.messages import AIMessage, HumanMessage
        from reactivegraph.tools import tool

        calls: list[str] = []

        @tool
        def echo(text: str) -> str:
            """Echo text."""
            return text

        class Recorder(ForeignAgentMiddlewareStandIn):
            name = "recorder"

            def before_agent(self, state, runtime):
                calls.append("before_agent")
                return None

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                # The engine hands host models plain dicts (message_to_foreign_dict),
                # which is exactly what a real LangChain model coerces.
                has_tool_result = any(
                    isinstance(m, dict) and m.get("type") == "tool" for m in messages
                )
                if not has_tool_result:
                    return AIMessage(
                        content="",
                        tool_calls=[
                            {"name": "echo", "args": {"text": "x"}, "id": "c1", "type": "tool_call"}
                        ],
                    )
                return AIMessage(content="done")

        agent = create_agent(Model(), tools=[echo], middleware=[Recorder()])
        agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert calls == ["before_agent"]

    def test_tool_sees_state_written_by_a_before_agent_hook(self) -> None:
        """DeerFlow's ``present_file_tool`` reads ``runtime.state["thread_data"]``.

        ``ThreadDataMiddleware.before_agent`` writes that key, then the tool
        runs inside the same graph. Upstream passes the *full* graph state to
        ``ToolNode``; narrowing it to ``{"messages": ...}`` makes the tool
        raise "Thread runtime state is not available".
        """
        from reactivegraph.messages import AIMessage, HumanMessage
        from reactivegraph.middleware import ToolRuntime
        from reactivegraph.tools import tool

        seen: dict[str, Any] = {}

        @tool
        def inspect(runtime: ToolRuntime) -> str:
            """Read thread_data out of the injected runtime state."""
            seen["state"] = runtime.state
            return "ok"

        class ThreadData(ForeignAgentMiddlewareStandIn):
            name = "thread_data"

            def before_agent(self, state, runtime):
                return {"thread_data": {"outputs_path": "/tmp/out"}}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                has_tool_result = any(
                    isinstance(m, dict) and m.get("type") == "tool" for m in messages
                )
                if not has_tool_result:
                    return _tool_call_message("inspect", {}, "c1")
                return AIMessage(content="done")

        agent = create_agent(Model(), tools=[inspect], middleware=[ThreadData()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert seen["state"]["thread_data"] == {"outputs_path": "/tmp/out"}
        tool_messages = [m for m in out["messages"] if isinstance(m, ToolMessage)]
        assert [m.content for m in tool_messages] == ["ok"]


def _merge_tags(left, right):
    return sorted({*(left or ()), *(right or ())})


def _merge_ids(left, right):
    return [*(left or ()), *(right or ())]


class TestSchemaDrivenChannels:
    """The compiled agent exposes the state schema's channel table.

    DeerFlow's checkpoint-mutation path reads ``graph.channels`` to decide
    which fields need ``Overwrite`` wrapping, so a missing table is not a
    cosmetic gap: reducer channels get replaced instead of merged, and a
    per-run cap that counts accumulated entries never fires.
    """

    class _StaticModel:
        def __init__(self, replies: list[Any] | None = None) -> None:
            self._replies = list(replies or [])
            self.seen: list[list[object]] = []

        def bind_tools(self, tools, **kwargs):
            return self

        def invoke(self, messages, **kwargs):
            self.seen.append(list(messages))
            if self._replies:
                return self._replies.pop(0)
            return AIMessage(content="done")

    def test_channels_are_derived_from_the_state_schema(self) -> None:
        from typing import Annotated

        from typing_extensions import TypedDict

        from reactivegraph.message_state import add_messages

        # Functional form: this module uses ``from __future__ import
        # annotations``, so a class body's annotations would be unresolvable
        # strings (upstream LangGraph fails the same way).
        schema = TypedDict(  # noqa: UP013 - local class annotations are unresolvable strings
            "Schema",
            {
                "messages": Annotated[list, add_messages],
                "tags": Annotated[list, _merge_tags],
                "note": str,
            },
        )

        agent = create_agent(self._StaticModel(), state_schema=schema)
        channels = agent.channels

        assert type(channels["messages"]).__name__ == "BinaryOperatorAggregate"
        assert type(channels["tags"]).__name__ == "BinaryOperatorAggregate"
        assert type(channels["note"]).__name__ == "LastValue"
        assert agent.builder.channels is agent.channels

    def test_delta_channel_annotations_are_preserved_verbatim(self) -> None:
        """DeerFlow's delta mode annotates ``messages`` with LangGraph's
        ``DeltaChannel``; the compiled graph must carry that exact object so
        ``isinstance(channel, DeltaChannel)`` holds for the storage layer."""
        from typing import Annotated

        from typing_extensions import TypedDict

        lg_channels = pytest.importorskip("langgraph.channels")

        schema = TypedDict(  # noqa: UP013 - local class annotations are unresolvable strings
            "DeltaSchema",
            {
                "messages": Annotated[
                    list,
                    lg_channels.DeltaChannel(_merge_ids, list, snapshot_frequency=10),
                ]
            },
        )

        agent = create_agent(self._StaticModel(), state_schema=schema)

        assert isinstance(agent.channels["messages"], lg_channels.DeltaChannel)
        assert agent.channels["messages"].snapshot_frequency == 10

    def test_first_values_snapshot_contains_eager_reducer_defaults(self) -> None:
        """Upstream initializes reducer channels before the first node runs.

        DeerFlow reads fields such as ``tags`` and ``todos`` from the first
        ``values`` frame even before a middleware writes them. A missing key
        makes those consumers treat the channel as absent instead of empty.
        """
        from typing import Annotated

        from typing_extensions import TypedDict

        schema = TypedDict(  # noqa: UP013 - local class annotations are unresolvable strings
            "Schema", {"tags": Annotated[list, _merge_tags], "note": str}
        )
        agent = create_agent(self._StaticModel(), state_schema=schema)

        chunks = list(
            agent.stream(
                {"messages": [HumanMessage(content="hi")]}, stream_mode="values"
            )
        )

        assert chunks[0]["tags"] == []

    def test_first_values_snapshot_does_not_overwrite_reducer_input(self) -> None:
        from typing import Annotated

        from typing_extensions import TypedDict

        schema = TypedDict(  # noqa: UP013 - local class annotations are unresolvable strings
            "Schema", {"tags": Annotated[list, _merge_tags], "note": str}
        )
        agent = create_agent(self._StaticModel(), state_schema=schema)

        chunks = list(
            agent.stream(
                {"tags": ["caller"], "messages": [HumanMessage(content="hi")]},
                stream_mode="values",
            )
        )

        assert chunks[0]["tags"] == ["caller"]

    def test_reducer_channels_merge_writes_instead_of_replacing_them(self) -> None:
        """A reducer channel folds every write; last-write-wins would drop the
        entries an earlier model turn already recorded."""
        from typing import Annotated

        from typing_extensions import TypedDict

        schema = TypedDict(  # noqa: UP013 - local class annotations are unresolvable strings
            "Schema", {"ids": Annotated[list, _merge_ids], "note": str}
        )

        class Writer(ForeignAgentMiddlewareStandIn):
            name = "writer"

            def after_model(self, state, runtime):
                return {"ids": ["new"]}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        agent = create_agent(Model(), middleware=[Writer()], state_schema=schema)
        out = agent.invoke({"ids": ["old"], "note": "kept"})

        assert out["ids"] == ["old", "new"]
        assert out["note"] == "kept"


class TestMiddlewareStateThreading:
    """Hook chains thread state, exactly like upstream's node-per-middleware graph.

    Upstream compiles one node per middleware, so each hook observes the state
    its predecessors already produced. Collapsing the chain into a single
    function that hands every hook the same pre-chain snapshot silently breaks
    every middleware that reads what an earlier one wrote.
    """

    def test_after_model_sees_state_written_by_an_earlier_after_model(self) -> None:
        from reactivegraph.messages import AIMessage, HumanMessage

        seen: list[Any] = []

        class First(ForeignAgentMiddlewareStandIn):
            name = "first"

            def after_model(self, state, runtime):
                seen.append(list(state.get("ledger") or []))
                return None

        class Second(ForeignAgentMiddlewareStandIn):
            name = "second"

            def after_model(self, state, runtime):
                return {"ledger": ["one"]}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        # Upstream wires ``model -> last.after_model -> ... -> first.after_model``
        # (factory.py:1738), so the *last* registered hook runs first. ``Second``
        # writes the ledger and ``First``, running after it, must observe it.
        agent = create_agent(Model(), middleware=[First(), Second()])
        out = agent.invoke({"messages": [HumanMessage(content="hi")]})

        assert out["ledger"] == ["one"]
        assert seen == [["one"]]

    def test_after_model_sees_the_model_message_it_must_inspect(self) -> None:
        """The model's reply must already be in the state the hook receives.

        DeerFlow's token-budget middleware strips ``tool_calls`` from the
        ``AIMessage`` in ``after_model``; if the loop still routes on the
        pre-hook message the tool runs anyway and the cap is unenforced.
        """
        from reactivegraph.messages import AIMessage, HumanMessage
        from reactivegraph.tools import tool

        ran: list[str] = []

        @tool
        def bash(command: str) -> str:
            """Run a fake shell command."""
            ran.append(command)
            return "ok"

        class Cap(ForeignAgentMiddlewareStandIn):
            name = "cap"

            def after_model(self, state, runtime):
                last = state["messages"][-1]
                if not getattr(last, "tool_calls", None):
                    return None
                stripped = last.model_copy(update={"tool_calls": []})
                return {"messages": [stripped]}

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                if any(isinstance(m, dict) and m.get("type") == "tool" for m in messages):
                    return AIMessage(content="done")
                return _tool_call_message("bash", {"command": "ls"}, "call-1")

        agent = create_agent(Model(), tools=[bash], middleware=[Cap()])
        out = agent.invoke({"messages": [HumanMessage(content="go")]})

        assert ran == []
        assert [m.content for m in out["messages"] if isinstance(m, ToolMessage)] == []


class TestCheckpointerThreadState:
    """A checkpointer is not a decoration: it is the thread's memory.

    ``create_deerflow_agent(..., checkpointer=InMemorySaver())`` is the product
    configuration. LangGraph's contract is that a second ``invoke`` with the
    same ``configurable.thread_id`` continues from the first invocation's
    state (reducer channels accumulate, last-value channels are inherited), and
    that ``get_state``/``update_state``/``get_state_history`` address that
    thread. Storing the object without wiring it means every turn silently
    starts from a blank thread.
    """

    class _Model:
        def __init__(self) -> None:
            self.received: list[list[object]] = []

        def bind_tools(self, tools, **kwargs):
            return self

        def invoke(self, messages, **kwargs):
            self.received.append(list(messages))
            return AIMessage(content=f"reply-{len(messages)}")

    def test_second_invoke_continues_the_thread(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        model = self._Model()
        agent = create_agent(model, checkpointer=CheckpointStore())
        config = {"configurable": {"thread_id": "t1"}}

        first = agent.invoke({"messages": [HumanMessage(content="one")]}, config)
        second = agent.invoke({"messages": [HumanMessage(content="two")]}, config)

        assert [m.content for m in first["messages"]] == ["one", "reply-1"]
        assert [m.content for m in second["messages"]] == [
            "one",
            "reply-1",
            "two",
            "reply-3",
        ]

    def test_threads_are_isolated(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        agent.invoke(
            {"messages": [HumanMessage(content="one")]},
            {"configurable": {"thread_id": "a"}},
        )
        other = agent.invoke(
            {"messages": [HumanMessage(content="x")]},
            {"configurable": {"thread_id": "b"}},
        )

        assert [m.content for m in other["messages"]] == ["x", "reply-1"]

    def test_get_state_returns_the_head_snapshot(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        config = {"configurable": {"thread_id": "t1"}}
        agent.invoke({"messages": [HumanMessage(content="one")]}, config)

        snapshot = agent.get_state(config)

        assert [m.content for m in snapshot.values["messages"]] == ["one", "reply-1"]
        assert snapshot.next == ()
        assert snapshot.config["configurable"]["checkpoint_id"]
        assert snapshot.metadata["source"] == "loop"

    def test_get_state_history_lists_the_lineage_newest_first(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        config = {"configurable": {"thread_id": "t1"}}
        agent.invoke({"messages": [HumanMessage(content="one")]}, config)
        agent.invoke({"messages": [HumanMessage(content="two")]}, config)

        history = list(agent.get_state_history(config))

        assert len(history) >= 2
        newest = history[0].values["messages"]
        oldest = history[-1].values["messages"]
        assert [m.content for m in newest] == ["one", "reply-1", "two", "reply-3"]
        assert [m.content for m in oldest] == ["one", "reply-1"]

    def test_update_state_appends_through_the_reducer(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        config = {"configurable": {"thread_id": "t1"}}
        agent.invoke({"messages": [HumanMessage(content="one")]}, config)

        updated = agent.update_state(
            config, {"messages": [HumanMessage(content="manual")]}
        )

        assert updated["configurable"]["checkpoint_id"]
        values = agent.get_state(config).values
        assert [m.content for m in values["messages"]] == ["one", "reply-1", "manual"]

    def test_invoke_with_a_checkpointer_requires_a_thread_id(self) -> None:
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        with pytest.raises(ValueError, match="thread_id"):
            agent.invoke({"messages": [HumanMessage(content="one")]})

    def test_state_access_without_a_checkpointer_fails_closed(self) -> None:
        agent = create_agent(self._Model())
        config = {"configurable": {"thread_id": "t1"}}

        for call in (
            lambda: agent.get_state(config),
            lambda: agent.update_state(config, {"messages": []}),
            lambda: list(agent.get_state_history(config)),
        ):
            with pytest.raises(ValueError, match="No checkpointer set"):
                call()

    def test_checkpointer_assigned_after_construction_is_used(self) -> None:
        """Hosts (DeerFlow's run worker) assign ``agent.checkpointer`` late.

        ``CompiledStateGraph`` rebinds its thread adapter on every access for
        exactly this reason; the agent wrapper must do the same, otherwise the
        assignment is silently ignored and every run reports
        ``No checkpointer set``.
        """
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model())
        agent.checkpointer = CheckpointStore()
        config = {"configurable": {"thread_id": "late-1"}}

        first = agent.invoke({"messages": [HumanMessage(content="one")]}, config)
        second = agent.invoke({"messages": [HumanMessage(content="two")]}, config)

        assert [m.content for m in first["messages"]] == ["one", "reply-1"]
        assert [m.content for m in second["messages"]] == [
            "one",
            "reply-1",
            "two",
            "reply-3",
        ]
        snapshot = agent.get_state(config)
        assert [m.content for m in snapshot.values["messages"]] == [
            "one",
            "reply-1",
            "two",
            "reply-3",
        ]

    def test_real_langgraph_in_memory_saver_drives_the_thread(self) -> None:
        """The host passes its *own* saver class; the engine must duck-type it."""
        memory = pytest.importorskip("langgraph.checkpoint.memory")
        saver = memory.InMemorySaver()

        agent = create_agent(self._Model(), checkpointer=saver)
        config = {"configurable": {"thread_id": "lg-1"}}

        agent.invoke({"messages": [HumanMessage(content="one")]}, config)
        second = agent.invoke({"messages": [HumanMessage(content="two")]}, config)

        assert [m.content for m in second["messages"]] == [
            "one",
            "reply-1",
            "two",
            "reply-3",
        ]
        assert saver.get_tuple(config) is not None

    def test_run_time_writes_version_their_channels(self) -> None:
        """A channel changed *during* the run must get a new version.

        LangGraph's ``InMemorySaver`` keys channel blobs by
        ``(thread, ns, channel, version)``. If a channel's value changes but its
        version does not, the new blob is filed under the parent checkpoint's
        key and silently rewrites history in place.
        """
        from typing import Annotated

        from typing_extensions import TypedDict

        from reactivegraph.message_state import add_messages
        from reactivegraph.middleware import AgentMiddleware

        memory = pytest.importorskip("langgraph.checkpoint.memory")
        schema = TypedDict(  # noqa: UP013 - local class annotations are unresolvable strings
            "Schema",
            {
                "messages": Annotated[list, add_messages],
                "note": str,
            },
        )
        seen: list[int] = []

        class Writer(AgentMiddleware):
            state_schema = schema

            def after_model(self, state, runtime):
                seen.append(len(seen) + 1)
                return {"note": f"run-{len(seen)}"}

        model = self._Model()
        agent = create_agent(
            model, middleware=[Writer()], state_schema=schema, checkpointer=memory.InMemorySaver()
        )
        config = {"configurable": {"thread_id": "t1"}}

        agent.invoke({"messages": [HumanMessage(content="one")]}, config)
        agent.invoke({"messages": [HumanMessage(content="two")]}, config)

        history = list(agent.get_state_history(config))
        notes = [snapshot.values.get("note") for snapshot in history]
        assert notes == ["run-2", "run-1"], (
            "the parent checkpoint's channel blob was overwritten in place; "
            f"history reads back as {notes}"
        )

    async def test_aget_state_history_materializes_before_yielding(self) -> None:
        """A real saver's async cursor dies when the consumer stops early.

        LangGraph's own ``aget_state_history`` eagerly drains ``alist`` before
        yielding ("eagerly consume list() to avoid holding up the db cursor").
        Streaming the saver's cursor straight through instead lets a consumer
        ``break`` leave it open; the next statement then dies with
        ``ValueError: Connection closed`` (DeerFlow's
        ``CheckpointStateAccessor.ahistory`` breaks as soon as it has enough).
        """
        import contextlib

        from reactivegraph.checkpoint import CheckpointStore

        closed: list[bool] = []
        records = [
            {"checkpoint": {"channel_values": {"messages": []}, "channel_versions": {}}},
            {"checkpoint": {"channel_values": {"messages": []}, "channel_versions": {}}},
        ]

        class CursorSaver(CheckpointStore):
            async def alist(self, config, **kwargs):
                try:
                    for record in records:
                        yield record
                finally:
                    closed.append(True)

        agent = create_agent(self._Model(), checkpointer=CursorSaver())
        config = {"configurable": {"thread_id": "cursor-1"}}

        with contextlib.suppress(Exception):
            async for _ in agent.aget_state_history(config):
                break

        assert closed == [True], (
            "the saver's async cursor was still open after the consumer broke "
            "out of the history loop"
        )

    def test_checkpointer_false_is_treated_as_absent(self) -> None:
        agent = create_agent(self._Model(), checkpointer=False)

        out = agent.invoke({"messages": [HumanMessage(content="one")]})
        assert [m.content for m in out["messages"]] == ["one", "reply-1"]
        with pytest.raises(ValueError, match="No checkpointer set"):
            agent.get_state({"configurable": {"thread_id": "t1"}})

    def test_update_state_honours_overwrite(self) -> None:
        """DeerFlow wraps replace-style writes (compaction, rollback) in Overwrite.

        A reducer channel must then take the wrapped value verbatim instead of
        folding it, or restoring a rollback point would append the snapshot to
        the live transcript instead of replacing it.
        """
        from reactivegraph.checkpoint import CheckpointStore
        from reactivegraph.types import Overwrite

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        config = {"configurable": {"thread_id": "t1"}}
        agent.invoke({"messages": [HumanMessage(content="one")]}, config)

        agent.update_state(
            config, {"messages": Overwrite([HumanMessage(content="only")])}
        )

        values = agent.get_state(config).values
        assert [m.content for m in values["messages"]] == ["only"]

    def test_checkpoint_metadata_carries_config_metadata(self) -> None:
        """DeerFlow records the thread's agent binding in ``config['metadata']``.

        The checkpoint write merges string/number config metadata into the
        stored metadata (upstream ``get_checkpoint_metadata``), and the thread
        readers rely on reading it back off the snapshot.
        """
        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        config = {
            "configurable": {"thread_id": "t1"},
            "metadata": {"deerflow_agent_name": "research-agent", "attempt": 2},
        }
        agent.invoke({"messages": [HumanMessage(content="one")]}, config)

        metadata = agent.get_state(config).metadata
        assert metadata["deerflow_agent_name"] == "research-agent"
        assert metadata["attempt"] == 2
        assert metadata["source"] == "loop"

    def test_async_thread_state_twins_drive_the_same_thread(self) -> None:
        """The product's worker drives ``astream``/``ainvoke``; state must persist."""
        import asyncio

        from reactivegraph.checkpoint import CheckpointStore

        agent = create_agent(self._Model(), checkpointer=CheckpointStore())
        config = {"configurable": {"thread_id": "t1"}}

        async def _run():
            first = await agent.ainvoke({"messages": [HumanMessage(content="one")]}, config)
            second = await agent.ainvoke({"messages": [HumanMessage(content="two")]}, config)
            snapshot = await agent.aget_state(config)
            return first, second, snapshot

        first, second, snapshot = asyncio.run(_run())
        assert [m.content for m in first["messages"]] == ["one", "reply-1"]
        assert [m.content for m in second["messages"]] == [
            "one",
            "reply-1",
            "two",
            "reply-3",
        ]
        assert [m.content for m in snapshot.values["messages"]] == [
            "one",
            "reply-1",
            "two",
            "reply-3",
        ]


class TestAmbientConfigAcrossModes:
    """``get_config()`` must expose the caller's configurable in every entry point.

    DeerFlow middleware reads ``get_config()["configurable"]["thread_id"]``
    instead of receiving it as an argument (``ThreadDataMiddleware``). The sync
    path publishes the caller's config; the async path must too, or an async
    run fails with "Thread ID is required" even though the caller passed one.
    """

    def _thread_id_probe(self) -> tuple[Any, Any]:
        from reactivegraph.middleware import AgentMiddleware
        from reactivegraph.runtime import get_config

        record: dict[str, Any] = {}

        class Probe(AgentMiddleware):
            def before_agent(self, state, runtime):
                record["thread_id"] = (
                    get_config().get("configurable", {}).get("thread_id")
                )
                return None

        class Model:
            def bind_tools(self, tools, **kwargs):
                return self

            def invoke(self, messages, config=None):
                return AIMessage(content="ok")

        return create_agent(Model(), middleware=[Probe()]), record

    def test_sync_invoke_publishes_configurable(self) -> None:
        agent, record = self._thread_id_probe()
        agent.invoke(
            {"messages": [HumanMessage(content="hi")]},
            {"configurable": {"thread_id": "sync-t"}},
        )
        assert record["thread_id"] == "sync-t"

    def test_async_invoke_publishes_configurable(self) -> None:
        import asyncio

        agent, record = self._thread_id_probe()

        async def _run():
            await agent.ainvoke(
                {"messages": [HumanMessage(content="hi")]},
                {"configurable": {"thread_id": "async-t"}},
            )

        asyncio.run(_run())
        assert record["thread_id"] == "async-t"

    def test_async_stream_publishes_configurable(self) -> None:
        import asyncio

        agent, record = self._thread_id_probe()

        async def _run():
            async for _ in agent.astream(
                {"messages": [HumanMessage(content="hi")]},
                {"configurable": {"thread_id": "stream-t"}},
                stream_mode="values",
            ):
                pass

        asyncio.run(_run())
        assert record["thread_id"] == "stream-t"
