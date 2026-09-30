"""Object-model ``ToolNode`` contract pinned against the upstream engine.

DeerFlow drives every tool call through ``ToolNode``: middleware wraps it with
``wrap_tool_call``, guardrails read ``request.tool_call["args"]``, MCP tools
receive a ``ToolRuntime`` injected by annotation, and built-in tools return
``Command`` objects that must reach the graph scheduler instead of being
wrapped into a ``ToolMessage``.

No test here imports the upstream engine; ``benchmarks/differential/
tool_node_differential.py`` compares the same behaviours against the real
package.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Annotated, Any, Generic, TypeVar

import pytest
from pydantic import BaseModel

from reactivegraph.errors import GraphBubbleUp
from reactivegraph.messages import AIMessage, HumanMessage, RemoveMessage, ToolMessage
from reactivegraph.middleware import ToolCallRequest, ToolRuntime
from reactivegraph.tool_node import (
    INVALID_TOOL_NAME_ERROR_TEMPLATE,
    TOOL_CALL_ERROR_TEMPLATE,
    InjectedState,
    InjectedStore,
    ToolCallWithContext,
    ToolInvocationError,
    ToolNode,
    _get_all_injected_args,
    _infer_handled_types,
    msg_content_output,
    tools_condition,
)
from reactivegraph.tools import BaseTool, InjectedToolArg, tool
from reactivegraph.types import Command, Send


def call(name: str, args: dict[str, Any], call_id: str = "c1") -> dict[str, Any]:
    return {"name": name, "args": args, "id": call_id, "type": "tool_call"}


def ai(*calls: dict[str, Any]) -> AIMessage:
    return AIMessage(content="", tool_calls=list(calls))


class TestInputParsing:
    def test_dict_state_returns_dict_output(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([echo])
        result = node.invoke({"messages": [ai(call("echo", {"x": 1}))]})
        assert result == {"messages": [ToolMessage(content="1", name="echo", tool_call_id="c1")]}

    def test_list_state_returns_list_output(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([echo])
        result = node.invoke([ai(call("echo", {"x": 1}))])
        assert result == [ToolMessage(content="1", name="echo", tool_call_id="c1")]

    def test_direct_tool_calls_are_executed_without_messages(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([echo])
        result = node.invoke([call("echo", {"x": 2})])
        assert result == {"messages": [ToolMessage(content="2", name="echo", tool_call_id="c1")]}

    def test_custom_messages_key(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([echo], messages_key="chat")
        result = node.invoke({"chat": [ai(call("echo", {"x": 3}))]})
        assert result == {"chat": [ToolMessage(content="3", name="echo", tool_call_id="c1")]}

    def test_tool_call_with_context_payload(self) -> None:
        @tool
        def where(state: Annotated[dict, InjectedState]) -> str:
            """Doc."""
            return state["marker"]

        node = ToolNode([where])
        payload: ToolCallWithContext = {
            "tool_call": call("where", {}),
            "__type": "tool_call_with_context",
            "state": {"marker": "here"},
        }
        assert node.invoke(payload) == {
            "messages": [ToolMessage(content="here", name="where", tool_call_id="c1")]
        }

    def test_attribute_state(self) -> None:
        class State(BaseModel):
            messages: list[Any]

        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([echo])
        result = node.invoke(State(messages=[ai(call("echo", {"x": 4}))]))
        assert result == {"messages": [ToolMessage(content="4", name="echo", tool_call_id="c1")]}

    def test_missing_messages_raises(self) -> None:
        node = ToolNode([])
        with pytest.raises(ValueError, match="No message found in input"):
            node.invoke({"other": []})

    def test_missing_ai_message_raises(self) -> None:
        node = ToolNode([])
        with pytest.raises(ValueError, match="No AIMessage found in input"):
            node.invoke({"messages": [HumanMessage(content="hi")]})

    def test_latest_ai_message_wins(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([echo])
        state = {
            "messages": [
                ai(call("echo", {"x": 1}, "old")),
                ToolMessage(content="1", name="echo", tool_call_id="old"),
                ai(call("echo", {"x": 5}, "new")),
            ]
        }
        assert node.invoke(state) == {
            "messages": [ToolMessage(content="5", name="echo", tool_call_id="new")]
        }


class TestConstruction:
    def test_plain_callable_is_coerced(self) -> None:
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        node = ToolNode([add])
        assert set(node.tools_by_name) == {"add"}
        assert isinstance(node.tools_by_name["add"], BaseTool)

    def test_tools_by_name_maps_instances(self) -> None:
        @tool("renamed")
        def original(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([original])
        assert list(node.tools_by_name) == ["renamed"]
        assert node.tools_by_name["renamed"] is original

    def test_name_and_tags_are_exposed(self) -> None:
        node = ToolNode([], name="my-tools", tags=["a"])
        assert node.name == "my-tools"
        assert node.tags == ["a"]


class TestExecution:
    def test_multiple_calls_keep_model_order(self) -> None:
        @tool
        def slow(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([slow])
        calls = [call("slow", {"x": i}, f"c{i}") for i in range(5)]
        result = node.invoke({"messages": [ai(*calls)]})
        assert [m.tool_call_id for m in result["messages"]] == [f"c{i}" for i in range(5)]

    def test_async_batch_keeps_model_order(self) -> None:
        @tool
        async def slow(x: int) -> int:
            """Doc."""
            await asyncio.sleep(0)
            return x

        node = ToolNode([slow])
        calls = [call("slow", {"x": i}, f"c{i}") for i in range(5)]
        result = asyncio.run(node.ainvoke({"messages": [ai(*calls)]}))
        assert [m.tool_call_id for m in result["messages"]] == [f"c{i}" for i in range(5)]

    def test_async_node_executes_async_tool(self) -> None:
        @tool
        async def add(a: int, b: int) -> int:
            """Doc."""
            return a + b

        node = ToolNode([add])
        result = asyncio.run(node.ainvoke({"messages": [ai(call("add", {"a": 1, "b": 2}))]}))
        assert result == {"messages": [ToolMessage(content="3", name="add", tool_call_id="c1")]}

    def test_unknown_tool_produces_error_message(self) -> None:
        node = ToolNode([])
        result = node.invoke({"messages": [ai(call("ghost", {}))]})
        expected = INVALID_TOOL_NAME_ERROR_TEMPLATE.format(
            requested_tool="ghost", available_tools=""
        )
        assert result["messages"][0] == ToolMessage(
            content=expected, name="ghost", tool_call_id="c1", status="error"
        )

    def test_unknown_tool_lists_available_names(self) -> None:
        @tool("alpha")
        def alpha(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([alpha])
        result = node.invoke({"messages": [ai(call("ghost", {}))]})
        assert "alpha" in result["messages"][0].content
        assert result["messages"][0].status == "error"

    def test_non_serializable_output_falls_back_to_str(self) -> None:
        class Weird:
            def __str__(self) -> str:
                return "weird"

            def __iter__(self) -> Any:
                raise TypeError("nope")

        assert msg_content_output(Weird()) == "weird"


class TestErrorHandling:
    def test_default_handler_only_handles_invocation_errors(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise RuntimeError("kaboom")

        node = ToolNode([boom])
        with pytest.raises(RuntimeError, match="kaboom"):
            node.invoke({"messages": [ai(call("boom", {"x": 1}))]})

    def test_default_handler_reports_validation_errors(self) -> None:
        @tool
        def typed(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([typed])
        result = node.invoke({"messages": [ai(call("typed", {"x": "not-an-int"}))]})
        message = result["messages"][0]
        assert message.status == "error"
        assert "Error invoking tool 'typed'" in message.content

    def test_handle_tool_errors_true_uses_default_template(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ValueError("kaboom")

        node = ToolNode([boom], handle_tool_errors=True)
        result = node.invoke({"messages": [ai(call("boom", {"x": 1}))]})
        assert result["messages"][0].content == TOOL_CALL_ERROR_TEMPLATE.format(
            error=repr(ValueError("kaboom"))
        )
        assert result["messages"][0].status == "error"

    def test_handle_tool_errors_string(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ValueError("kaboom")

        node = ToolNode([boom], handle_tool_errors="please retry")
        result = node.invoke({"messages": [ai(call("boom", {"x": 1}))]})
        assert result["messages"][0].content == "please retry"

    def test_handle_tool_errors_callable_receives_exception(self) -> None:
        seen: list[Exception] = []

        def handler(exc: ValueError) -> str:
            seen.append(exc)
            return "handled"

        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ValueError("kaboom")

        node = ToolNode([boom], handle_tool_errors=handler)
        result = node.invoke({"messages": [ai(call("boom", {"x": 1}))]})
        assert result["messages"][0].content == "handled"
        assert isinstance(seen[0], ValueError)

    def test_callable_handler_only_catches_declared_type(self) -> None:
        def handler(exc: KeyError) -> str:
            return "handled"

        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ValueError("kaboom")

        node = ToolNode([boom], handle_tool_errors=handler)
        with pytest.raises(ValueError, match="kaboom"):
            node.invoke({"messages": [ai(call("boom", {"x": 1}))]})

    def test_exception_type_selector(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ValueError("kaboom")

        node = ToolNode([boom], handle_tool_errors=ValueError)
        assert node.invoke({"messages": [ai(call("boom", {"x": 1}))]})["messages"][0].status == (
            "error"
        )

    def test_exception_tuple_selector(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise TypeError("kaboom")

        node = ToolNode([boom], handle_tool_errors=(ValueError, TypeError))
        assert node.invoke({"messages": [ai(call("boom", {"x": 1}))]})["messages"][0].status == (
            "error"
        )

    def test_handle_tool_errors_false_reraises(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ValueError("kaboom")

        node = ToolNode([boom], handle_tool_errors=False)
        with pytest.raises(ValueError, match="kaboom"):
            node.invoke({"messages": [ai(call("boom", {"x": 1}))]})

    def test_infer_handled_types_without_annotation_defaults_to_exception(self) -> None:
        def handler(exc) -> str:  # noqa: ANN001
            return "x"

        assert _infer_handled_types(handler) == (Exception,)

    def test_infer_handled_types_rejects_non_exception_annotation(self) -> None:
        def handler(exc: int) -> str:
            return "x"

        with pytest.raises(ValueError, match="Arbitrary types are not supported"):
            _infer_handled_types(handler)

    def test_infer_handled_types_supports_union(self) -> None:
        def handler(exc: ValueError | TypeError) -> str:
            return "x"

        assert set(_infer_handled_types(handler)) == {ValueError, TypeError}

    def test_graph_bubble_up_is_never_swallowed(self) -> None:
        @tool
        def interrupt_me(x: int) -> int:
            """Doc."""
            raise GraphBubbleUp()

        node = ToolNode([interrupt_me], handle_tool_errors=True)
        with pytest.raises(GraphBubbleUp):
            node.invoke({"messages": [ai(call("interrupt_me", {"x": 1}))]})

    def test_invocation_error_exposes_details(self) -> None:
        @tool
        def typed(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([typed])
        result = node.invoke({"messages": [ai(call("typed", {"x": "bad"}))]})
        content = result["messages"][0].content
        assert content.startswith("Error invoking tool 'typed' with kwargs {'x': 'bad'}")
        assert (
            "Error invoking tool 'typed' with kwargs {'x': 'bad'} with error:"
            in content
        )


class TestWrappers:
    def test_wrap_tool_call_can_short_circuit(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> ToolMessage:
            return ToolMessage(content="cached", tool_call_id=request.tool_call["id"])

        node = ToolNode([echo], wrap_tool_call=wrapper)
        result = node.invoke({"messages": [ai(call("echo", {"x": 1}))]})
        assert result["messages"][0].content == "cached"

    def test_wrap_tool_call_can_override_arguments(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> Any:
            patched = request.override(
                tool_call={**request.tool_call, "args": {"x": 99}},
            )
            return execute(patched)

        node = ToolNode([echo], wrap_tool_call=wrapper)
        result = node.invoke({"messages": [ai(call("echo", {"x": 1}))]})
        assert result["messages"][0].content == "99"

    def test_wrap_tool_call_can_retry(self) -> None:
        attempts: list[int] = []

        @tool
        def flaky(x: int) -> int:
            """Doc."""
            attempts.append(x)
            if len(attempts) < 2:
                raise ValueError("transient")
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> Any:
            try:
                return execute(request)
            except ValueError:
                return execute(request)

        node = ToolNode([flaky], wrap_tool_call=wrapper)
        result = node.invoke({"messages": [ai(call("flaky", {"x": 1}))]})
        assert result["messages"][0].content == "1"
        assert attempts == [1, 1]

    def test_wrapper_exception_is_converted_when_handled(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> Any:
            raise RuntimeError("wrapper blew up")

        node = ToolNode([echo], wrap_tool_call=wrapper, handle_tool_errors=True)
        result = node.invoke({"messages": [ai(call("echo", {"x": 1}))]})
        assert result["messages"][0].status == "error"
        assert "wrapper blew up" in result["messages"][0].content

    def test_wrapper_exception_propagates_when_unhandled(self) -> None:
        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> Any:
            raise RuntimeError("wrapper blew up")

        node = ToolNode([echo], wrap_tool_call=wrapper, handle_tool_errors=False)
        with pytest.raises(RuntimeError, match="wrapper blew up"):
            node.invoke({"messages": [ai(call("echo", {"x": 1}))]})

    def test_async_wrapper_used_by_ainvoke(self) -> None:
        @tool
        async def echo(x: int) -> int:
            """Doc."""
            return x

        async def awrapper(request: ToolCallRequest, execute: Any) -> ToolMessage:
            return ToolMessage(content="async-cached", tool_call_id=request.tool_call["id"])

        node = ToolNode([echo], awrap_tool_call=awrapper)
        result = asyncio.run(node.ainvoke({"messages": [ai(call("echo", {"x": 1}))]}))
        assert result["messages"][0].content == "async-cached"

    def test_async_falls_back_to_sync_wrapper(self) -> None:
        @tool
        async def echo(x: int) -> int:
            """Doc."""
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> ToolMessage:
            return ToolMessage(content="sync-fallback", tool_call_id=request.tool_call["id"])

        node = ToolNode([echo], wrap_tool_call=wrapper)
        result = asyncio.run(node.ainvoke({"messages": [ai(call("echo", {"x": 1}))]}))
        assert result["messages"][0].content == "sync-fallback"

    def test_request_exposes_state_and_runtime(self) -> None:
        seen: dict[str, Any] = {}

        @tool
        def echo(x: int) -> int:
            """Doc."""
            return x

        def wrapper(request: ToolCallRequest, execute: Any) -> Any:
            seen["state"] = request.state
            seen["runtime"] = request.runtime
            return execute(request)

        node = ToolNode([echo], wrap_tool_call=wrapper)
        node.invoke({"messages": [ai(call("echo", {"x": 1}))]})
        assert seen["state"]["messages"][0].tool_calls[0]["name"] == "echo"
        assert isinstance(seen["runtime"], ToolRuntime)
        assert seen["runtime"].tool_call_id == "c1"


class TestInjection:
    def test_tool_call_id_injection(self) -> None:
        from reactivegraph.tools import InjectedToolCallId

        @tool
        def identify(x: int, tool_call_id: Annotated[str, InjectedToolCallId]) -> str:
            """Doc."""
            return tool_call_id

        node = ToolNode([identify])
        result = node.invoke({"messages": [ai(call("identify", {"x": 1}))]})
        assert result["messages"][0].content == "c1"

    def test_runtime_injection_by_name(self) -> None:
        @tool
        def show(runtime: ToolRuntime) -> str:
            """Doc."""
            return str(runtime.tool_call_id)

        node = ToolNode([show])
        result = node.invoke({"messages": [ai(call("show", {}))]})
        assert result["messages"][0].content == "c1"

    def test_runtime_injection_by_annotation(self) -> None:

        @tool
        def show(runtime: Annotated[Any, InjectedToolArg()]) -> str:
            """Doc."""
            return str(runtime.tool_call_id)

        node = ToolNode([show])
        result = node.invoke({"messages": [ai(call("show", {}))]})
        assert result["messages"][0].content == "c1"

    def test_forged_runtime_argument_is_stripped(self) -> None:
        @tool
        def show(runtime: ToolRuntime) -> str:
            """Doc."""
            return str(runtime.tool_call_id)

        node = ToolNode([show])
        result = node.invoke(
            {"messages": [ai(call("show", {"runtime": "forged"}))]}
        )
        assert result["messages"][0].content == "c1"

    def test_injected_state_field(self) -> None:
        @tool
        def read(foo: Annotated[str, InjectedState("foo")]) -> str:
            """Doc."""
            return foo

        node = ToolNode([read])
        result = node.invoke({"messages": [ai(call("read", {}))], "foo": "bar"})
        assert result["messages"][0].content == "bar"

    def test_injected_state_whole_state(self) -> None:
        @tool
        def read(state: Annotated[dict, InjectedState]) -> str:
            """Doc."""
            return state["foo"]

        node = ToolNode([read])
        result = node.invoke({"messages": [ai(call("read", {}))], "foo": "baz"})
        assert result["messages"][0].content == "baz"

    def test_injected_state_list_input_is_wrapped(self) -> None:
        @tool
        def read(messages: Annotated[list, InjectedState("messages")]) -> int:
            """Doc."""
            return len(messages)

        node = ToolNode([read])
        result = node.invoke([ai(call("read", {}))])
        assert result[0].content == "1"

    def test_missing_state_field_raises_key_error(self) -> None:
        @tool
        def read(foo: Annotated[str, InjectedState("foo")]) -> str:
            """Doc."""
            return foo

        node = ToolNode([read], handle_tool_errors=False)
        with pytest.raises(KeyError, match="foo"):
            node.invoke({"messages": [ai(call("read", {}))]})

    def test_optional_state_field_is_left_out(self) -> None:
        @tool
        def read(foo: Annotated[str | None, InjectedState("foo")] = None) -> str:
            """Doc."""
            return str(foo)

        node = ToolNode([read])
        result = node.invoke({"messages": [ai(call("read", {}))]})
        assert result["messages"][0].content == "None"

    def test_missing_store_raises(self) -> None:
        @tool
        def read(store: Annotated[Any, InjectedStore()]) -> str:
            """Doc."""
            return "never"

        node = ToolNode([read], handle_tool_errors=False)
        with pytest.raises(ValueError, match="please compile your graph with a store"):
            node.invoke({"messages": [ai(call("read", {}))]})

    def test_store_is_injected_from_runtime(self) -> None:
        @tool
        def read(store: Annotated[Any, InjectedStore()]) -> str:
            """Doc."""
            return "never"

        sentinel = object()
        node = ToolNode([read])
        tool_runtime = ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=None,
            tool_call_id="c1",
            store=sentinel,
        )
        injected = node._inject_tool_args(call("read", {}), tool_runtime, read)
        assert injected["args"]["store"] is sentinel

    def test_injected_args_are_hidden_from_model_schema(self) -> None:
        @tool
        def read(x: int, state: Annotated[dict, InjectedState]) -> str:
            """Doc."""
            return str(x)

        assert "state" not in read.tool_call_schema.model_fields
        assert _get_all_injected_args(read).state == {"state": None}

    def test_get_all_injected_args_runtime_by_name(self) -> None:
        async def coro(
            runtime: Annotated[object | None, InjectedToolArg()] = None,
            **arguments: object,
        ) -> str:
            return "ok"

        from reactivegraph.tools import StructuredTool

        discovered = StructuredTool(
            name="mcp_tool",
            description="d",
            args_schema={"type": "object", "properties": {}},
            coroutine=coro,
        )
        assert _get_all_injected_args(discovered).runtime == "runtime"

    def test_get_all_injected_args_detects_schema_field(self) -> None:
        @tool
        def read(runtime: ToolRuntime) -> str:
            """Doc."""
            return "x"

        injected = _get_all_injected_args(read)
        assert injected.runtime == "runtime"
        assert "runtime" in injected.all_injected_keys

    def test_caller_forged_injected_state_is_stripped(self) -> None:
        @tool
        def read(foo: Annotated[str, InjectedState("foo")]) -> str:
            """Doc."""
            return foo

        node = ToolNode([read])
        result = node.invoke(
            {"messages": [ai(call("read", {"foo": "forged"}))], "foo": "real"}
        )
        assert result["messages"][0].content == "real"

    def test_validation_error_filters_injected_arguments(self) -> None:
        @tool
        def read(x: int, state: Annotated[dict, InjectedState]) -> str:
            """Doc."""
            return str(x)

        node = ToolNode([read])
        result = node.invoke({"messages": [ai(call("read", {"x": "bad"}))]})
        content = result["messages"][0].content
        assert "x:" in content
        assert "state" not in content


class TestCommandReturns:
    def test_command_dict_update_with_matching_tool_message(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(
                update={"messages": [ToolMessage(content="ok", tool_call_id="c1")]}
            )

        node = ToolNode([jump])
        result = node.invoke({"messages": [ai(call("jump", {"x": 1}))]})
        assert isinstance(result, list)
        command = result[0]
        assert isinstance(command, Command)
        assert command.update["messages"][0].name == "jump"

    def test_command_list_update_for_list_input(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(update=[ToolMessage(content="ok", tool_call_id="c1")])

        node = ToolNode([jump])
        result = node.invoke([ai(call("jump", {"x": 1}))])
        assert isinstance(result[0], Command)
        assert result[0].update[0].name == "jump"

    def test_command_dict_update_rejected_for_list_input(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(
                update={"messages": [ToolMessage(content="ok", tool_call_id="c1")]}
            )

        node = ToolNode([jump], handle_tool_errors=False)
        with pytest.raises(ValueError, match="only when using dict"):
            node.invoke([ai(call("jump", {"x": 1}))])

    def test_command_list_update_rejected_for_dict_input(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(update=[ToolMessage(content="ok", tool_call_id="c1")])

        node = ToolNode([jump], handle_tool_errors=False)
        with pytest.raises(ValueError, match="only when using list of messages"):
            node.invoke({"messages": [ai(call("jump", {"x": 1}))]})

    def test_command_without_terminator_is_rejected(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(update={"messages": [HumanMessage(content="nope")]})

        node = ToolNode([jump], handle_tool_errors=False)
        with pytest.raises(ValueError, match="Every tool call"):
            node.invoke({"messages": [ai(call("jump", {"x": 1}))]})

    def test_command_to_parent_skips_terminator_requirement(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(graph=Command.PARENT, goto="other")

        node = ToolNode([jump])
        result = node.invoke({"messages": [ai(call("jump", {"x": 1}))]})
        assert result[0].graph == Command.PARENT

    def test_remove_all_messages_is_allowed(self) -> None:
        from reactivegraph.constants import REMOVE_ALL_MESSAGES

        @tool
        def clear(x: int) -> Command:
            """Doc."""
            return Command(update={"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES)]})

        node = ToolNode([clear])
        result = node.invoke({"messages": [ai(call("clear", {"x": 1}))]})
        assert isinstance(result[0], Command)

    def test_parent_send_commands_are_collapsed(self) -> None:
        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(
                graph=Command.PARENT,
                goto=[Send("a" if x == 1 else "b", {"x": x})],
            )

        node = ToolNode([jump])
        result = node.invoke(
            {
                "messages": [
                    ai(
                        call("jump", {"x": 1}, "c1"),
                        call("jump", {"x": 2}, "c2"),
                    )
                ]
            }
        )
        assert len(result) == 1
        assert [send.node for send in result[0].goto] == ["a", "b"]

    def test_list_of_plain_values_is_wrapped_by_the_tool_contract(self) -> None:
        @tool
        def bad(x: int) -> list[Any]:
            """Doc."""
            return ["not-a-message"]

        node = ToolNode([bad], handle_tool_errors=False)
        result = node.invoke({"messages": [ai(call("bad", {"x": 1}))]})
        # The tool contract wraps a plain list into ToolMessage content, so
        # ToolNode itself only ever sees a valid ToolMessage here.
        assert result["messages"][0].content == '["not-a-message"]'

    def test_unexpected_return_type_is_rejected(self) -> None:
        @tool
        def bad(x: int) -> int:
            """Doc."""
            return x

        node = ToolNode([bad])
        result = node.invoke({"messages": [ai(call("bad", {"x": 1}))]})
        # A plain int is wrapped by the tool contract into a ToolMessage.
        assert result["messages"][0].content == "1"

    def test_list_terminator_must_match_outer_call(self) -> None:
        @tool
        def jump(x: int) -> list[Any]:
            """Doc."""
            return [
                ToolMessage(content="a", tool_call_id="other"),
                ToolMessage(content="b", tool_call_id="c1"),
            ]

        node = ToolNode([jump])
        result = node.invoke({"messages": [ai(call("jump", {"x": 1}))]})
        assert [m.tool_call_id for m in result["messages"]] == ["other", "c1"]

    def test_list_terminator_count_is_validated(self) -> None:
        @tool
        def jump(x: int) -> list[Any]:
            """Doc."""
            return [
                ToolMessage(content="a", tool_call_id="c1"),
                ToolMessage(content="b", tool_call_id="c1"),
            ]

        node = ToolNode([jump], handle_tool_errors=False)
        with pytest.raises(ValueError, match="expected exactly one terminating"):
            node.invoke({"messages": [ai(call("jump", {"x": 1}))]})


class TestToolsCondition:
    def test_routes_to_tools_when_tool_calls_present(self) -> None:
        state = {"messages": [ai(call("x", {}))]}
        assert tools_condition(state) == "tools"

    def test_ends_when_last_message_has_no_tool_calls(self) -> None:
        state = {"messages": [AIMessage(content="done")]}
        assert tools_condition(state) == "__end__"

    def test_custom_messages_key(self) -> None:
        state = {"chat": [ai(call("x", {}))]}
        assert tools_condition(state, messages_key="chat") == "tools"

    def test_list_state(self) -> None:
        assert tools_condition([ai(call("x", {}))]) == "tools"

    def test_attribute_state(self) -> None:
        class State(BaseModel):
            messages: list[Any]

        assert tools_condition(State(messages=[ai(call("x", {}))])) == "tools"

    def test_missing_messages_raises(self) -> None:
        with pytest.raises(ValueError, match="No messages found"):
            tools_condition({"other": []})


class TestToolInvocationError:
    def test_message_and_attributes(self) -> None:
        from pydantic import ValidationError

        class Model(BaseModel):
            x: int

        try:
            Model(x="bad")
        except ValidationError as exc:
            error = ToolInvocationError("t", exc, {"x": "bad"})
        assert error.tool_name == "t"
        assert error.tool_kwargs == {"x": "bad"}
        assert "Error invoking tool 't'" in error.message

    def test_filtered_errors_are_formatted(self) -> None:
        from pydantic import ValidationError

        class Model(BaseModel):
            x: int

        try:
            Model(x="bad")
        except ValidationError as exc:
            error = ToolInvocationError(
                "t", exc, {"x": "bad"}, filtered_errors=[{"loc": ("x",), "msg": "bad"}]
            )
        assert "x: bad" in error.message


class TestModuleBoundary:
    def test_tool_node_module_does_not_import_upstream(self) -> None:
        import reactivegraph.tool_node as module

        source = inspect.getsource(module)
        assert "langchain" not in source
        assert "langgraph" not in source


class ForeignToolMessage:
    """Stand-in for a host tool's own message class.

    Core tests must not import langchain, so this mirrors the structural shape
    the bridge keys on: a ``type`` string plus a dict-returning ``model_dump``.
    """

    type = "tool"

    def __init__(self, content: str, tool_call_id: str, name: str = "host_tool") -> None:
        self.content = content
        self.tool_call_id = tool_call_id
        self.name = name

    def model_dump(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "additional_kwargs": {},
            "response_metadata": {},
            "type": "tool",
            "name": self.name,
            "id": None,
            "tool_call_id": self.tool_call_id,
            "artifact": None,
            "status": "success",
        }


class ForeignCommand:
    """Stand-in for the host's ``Command`` (structural, not imported)."""

    def __init__(self, update: Any, goto: Any = ()) -> None:
        self.update = update
        self.goto = goto
        self.graph = None
        self.resume = None


@dataclass
class ForeignRuntime:
    """Stand-in for a host runtime dataclass (state/context/config/... shape).

    The real host type is a dataclass too -- measured from
    ``langgraph.prebuilt.tool_node.ToolRuntime`` -- which is what lets pydantic
    build a schema for the annotation without ``arbitrary_types_allowed``.
    """

    state: Any = None
    context: Any = None
    config: Any = None
    stream_writer: Any = None
    tool_call_id: Any = None
    store: Any = None
    tools: Any = None
    execution_info: Any = None
    server_info: Any = None


T_co = TypeVar("T_co")
S_co = TypeVar("S_co")


@dataclass
class ForeignGenericRuntime(Generic[T_co, S_co]):
    """A subscripted generic runtime, as DeerFlow declares it.

    ``ToolRuntime[dict[str, Any], ThreadState]`` is a ``_GenericAlias``: the
    declared object is not a class, but ``get_origin`` recovers the dataclass.
    """

    state: Any = None
    context: Any = None
    config: Any = None
    stream_writer: Any = None
    tool_call_id: Any = None
    store: Any = None
    tools: Any = None
    execution_info: Any = None
    server_info: Any = None


class ForeignTool:
    """A host tool: duck-typed ``.name``/``.invoke``, no shared base class.

    DeerFlow defines every tool with LangChain's ``@tool`` decorator, so the
    engine can never share a class hierarchy with them. Accepting them
    structurally is the only way those tools can run on ReactiveGraph.
    """

    def __init__(self, name: str, fn: Any) -> None:
        self.name = name
        self.description = "host tool"
        self.fn = fn
        self.calls: list[Any] = []

    def get_input_schema(self) -> type[BaseModel]:
        return _foreign_schema_for(self.fn)

    def invoke(self, call: Any, config: Any = None) -> Any:
        self.calls.append(call)
        return self.fn(call)

    async def ainvoke(self, call: Any, config: Any = None) -> Any:
        self.calls.append(call)
        return self.fn(call)


def _foreign_schema_for(fn: Any) -> type[BaseModel]:
    """Build a pydantic schema from a plain function's annotations."""
    import inspect as _inspect

    annotations = {
        name: param.annotation
        for name, param in _inspect.signature(fn).parameters.items()
        if name != "call"
    }
    return type("ForeignSchema", (BaseModel,), {"__annotations__": annotations})


class TestForeignTools:
    """Host tools (LangChain ``@tool`` objects) must run on our ToolNode.

    Measured blast radius in the DeerFlow harness: 65 ``@tool`` definitions, 27
    of them returning ``Command``, 84 taking an injected runtime parameter.
    Without structural acceptance none of them can execute on ReactiveGraph.
    """

    def test_a_foreign_tool_is_accepted_and_returns_a_normalized_message(self) -> None:
        def impl(call: dict[str, Any]) -> Any:
            return ForeignToolMessage("42", call["id"])

        node = ToolNode([ForeignTool("host_tool", impl)])
        result = node.invoke({"messages": [ai(call("host_tool", {"x": 1}))]})

        assert isinstance(result, dict)
        (msg,) = result["messages"]
        assert isinstance(msg, ToolMessage), type(msg).__name__
        assert msg.content == "42"
        assert msg.tool_call_id == "c1"
        assert msg.name == "host_tool"

    def test_a_foreign_tool_returning_a_command_routes_instead_of_stringifying(self) -> None:
        def impl(call: dict[str, Any]) -> Any:
            return ForeignCommand(
                update={"messages": [ForeignToolMessage("routed", call["id"])]}
            )

        node = ToolNode([ForeignTool("host_tool", impl)])
        result = node.invoke({"messages": [ai(call("host_tool", {"x": 1}))]})

        assert isinstance(result, list), result
        (command,) = result
        assert isinstance(command, Command), type(command).__name__
        (msg,) = command.update["messages"]
        assert isinstance(msg, ToolMessage)
        assert msg.content == "routed"
        assert msg.tool_call_id == "c1"

    def test_a_foreign_tool_receives_its_own_declared_runtime_type(self) -> None:
        """A host tool validates the injected runtime against *its* dataclass,
        so we must construct the declared type rather than pass ours."""
        seen: dict[str, Any] = {}

        def impl(call: dict[str, Any]) -> Any:
            runtime = call["args"]["runtime"]
            seen["type"] = type(runtime).__name__
            seen["context"] = runtime.context
            seen["tool_call_id"] = runtime.tool_call_id
            return ForeignToolMessage("ok", call["id"])

        tool = ForeignTool("needs_runtime", impl)
        tool.get_input_schema = lambda: type(
            "RuntimeSchema",
            (BaseModel,),
            {"__annotations__": {"runtime": ForeignRuntime}},
        )
        node = ToolNode([tool])
        result = node.invoke({"messages": [ai(call("needs_runtime", {}))]})

        assert seen["type"] == "ForeignRuntime", seen
        assert seen["tool_call_id"] == "c1"
        assert isinstance(result["messages"][0], ToolMessage)

    def test_a_foreign_tool_with_a_generic_runtime_alias_is_supported(self) -> None:
        """DeerFlow annotates tools ``ToolRuntime[dict[str, Any], ThreadState]``.

        A subscripted generic is not a class, so ``isinstance(declared, type)``
        is False and naive coercion passes our dataclass straight to the host
        tool's pydantic validator, which rejects it. Unwrap to the origin.
        """
        seen: dict[str, Any] = {}

        def impl(call: dict[str, Any]) -> Any:
            runtime = call["args"]["runtime"]
            seen["type"] = type(runtime).__name__
            seen["state"] = runtime.state
            return ForeignToolMessage("ok", call["id"])

        tool = ForeignTool("generic_runtime", impl)
        tool.get_input_schema = lambda: type(
            "RuntimeSchema",
            (BaseModel,),
            {
                "__annotations__": {
                    "runtime": ForeignGenericRuntime[dict, dict],
                }
            },
        )
        node = ToolNode([tool])
        result = node.invoke(
            {"messages": [ai(call("generic_runtime", {}))], "thread_data": {"x": 1}}
        )

        assert seen["type"] == "ForeignGenericRuntime", seen
        assert seen["state"]["thread_data"] == {"x": 1}
        assert isinstance(result["messages"][0], ToolMessage)

    def test_a_foreign_tool_error_still_propagates(self) -> None:
        def impl(call: dict[str, Any]) -> Any:
            raise ValueError("host tool exploded")

        node = ToolNode([ForeignTool("host_tool", impl)])
        with pytest.raises(ValueError, match="host tool exploded"):
            node.invoke({"messages": [ai(call("host_tool", {}))]})
