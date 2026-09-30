"""Tool contract pinned against real ``langchain-core``.

DeerFlow declares 68 ``@tool``-decorated functions and consumes a narrow but
strict surface: the auto-generated ``args_schema`` / ``tool_call_schema``
pydantic models, the OpenAI function schema, injected-argument filtering, and
``func`` / ``coroutine`` forwarding.  These tests pin that surface.

No test here imports LangChain; ``benchmarks/differential/tools_differential.py``
compares the same behaviours against the real upstream package.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import subprocess
import sys
import textwrap
from typing import Annotated

import pytest
from pydantic import BaseModel, ValidationError
from typing_extensions import TypedDict

from reactivegraph.tools import (
    BaseTool,
    InjectedToolArg,
    InjectedToolCallId,
    StructuredTool,
    ToolException,
    convert_to_openai_function,
    convert_to_openai_tool,
    tool,
)


class TestToolDecorator:
    def test_bare_decorator_uses_function_name_and_docstring(self) -> None:
        @tool
        def plain(x: int) -> int:
            """Plain."""
            return x + 1

        assert isinstance(plain, BaseTool)
        assert plain.name == "plain"
        assert plain.description == "Plain."
        assert plain.invoke({"x": 1}) == 2

    def test_explicit_name_overrides_function_name(self) -> None:
        @tool("renamed")
        def original(x: int) -> int:
            """Doc."""
            return x

        assert original.name == "renamed"

    def test_parse_docstring_extracts_summary_and_arg_descriptions(self) -> None:
        @tool(parse_docstring=True)
        def parsed(name: str, count: int = 1) -> str:
            """Greet someone.

            Args:
                name: Who to greet.
                count: How many times.
            """
            return name * count

        assert parsed.description == "Greet someone."
        fields = parsed.tool_call_schema.model_fields
        assert fields["name"].description == "Who to greet."
        assert fields["count"].description == "How many times."

    def test_without_parse_docstring_description_keeps_full_docstring(self) -> None:
        @tool
        def noparse(x: int) -> int:
            """Summary line.

            Args:
                x: the x value.
            """
            return x

        # Upstream keeps the raw docstring verbatim (minus surrounding
        # whitespace); it never strips the "Args:" block when parsing is off.
        expected = textwrap.dedent(noparse.func.__doc__).strip()
        assert "Args:" in expected  # the fixture itself still documents x
        assert noparse.description == expected
        assert noparse.tool_call_schema.model_fields["x"].description is None

    def test_return_direct_flag_is_preserved(self) -> None:
        @tool(return_direct=True)
        def direct(x: int) -> int:
            """Doc."""
            return x

        assert direct.return_direct is True

    def test_default_return_direct_is_false(self) -> None:
        @tool
        def indirect(x: int) -> int:
            """Doc."""
            return x

        assert indirect.return_direct is False


class TestSchemaGeneration:
    def test_args_schema_is_a_pydantic_model(self) -> None:
        @tool
        def f(a: str, b: int = 3) -> None:
            """Doc."""

        schema = f.args_schema
        assert schema is not None
        assert issubclass(schema, BaseModel)
        validated = schema.model_validate({"a": "x"})
        assert validated.a == "x"
        assert validated.b == 3

    def test_optional_default_is_visible_in_model_schema(self) -> None:
        @tool(parse_docstring=True)
        def f(a: str, note: str | None = None) -> None:
            """Doc.

            Args:
                a: The a value.
                note: Optional note.
            """

        params = convert_to_openai_tool(f)["function"]["parameters"]
        assert params["required"] == ["a"]
        assert list(params["properties"]) == ["a", "note"]
        assert params["properties"]["note"]["default"] is None

    def test_model_schema_excludes_runtime_injected_argument(self) -> None:
        from reactivegraph.middleware import ToolRuntime

        @tool(parse_docstring=True)
        def f(a: str, runtime: ToolRuntime) -> str:
            """Doc.

            Args:
                a: The a value.
            """
            return a

        params = convert_to_openai_tool(f)["function"]["parameters"]
        assert list(params["properties"]) == ["a"]
        assert "runtime" not in f.tool_call_schema.model_fields

    def test_model_schema_excludes_injected_tool_call_id(self) -> None:
        @tool(parse_docstring=True)
        def f(q: str, tool_call_id: Annotated[str, InjectedToolCallId]) -> str:
            """Doc.

            Args:
                q: The query.
            """
            return q

        params = convert_to_openai_tool(f)["function"]["parameters"]
        assert list(params["properties"]) == ["q"]
        assert "tool_call_id" not in f.tool_call_schema.model_fields

    def test_injected_tool_call_id_is_filled_from_a_model_tool_call(self) -> None:
        @tool(parse_docstring=True)
        def f(q: str, tool_call_id: Annotated[str, InjectedToolCallId]) -> str:
            """Doc.

            Args:
                q: The query.
            """
            return f"{q}:{tool_call_id}"

        call = {"args": {"q": "x"}, "name": "f", "type": "tool_call", "id": "call-1"}
        result = f.invoke(call)
        assert result.content == "x:call-1"
        assert result.tool_call_id == "call-1"

    def test_args_schema_keeps_injected_fields_for_validation(self) -> None:
        @tool(parse_docstring=True)
        def f(q: str, tool_call_id: Annotated[str, InjectedToolCallId]) -> str:
            """Doc.

            Args:
                q: The query.
            """
            return q

        assert list(f.args_schema.model_fields) == ["q", "tool_call_id"]

    def test_typeddict_argument_becomes_nested_object_schema(self) -> None:
        class Item(TypedDict):
            name: str
            size: int

        @tool(parse_docstring=True)
        def f(item: Item) -> None:
            """Doc.

            Args:
                item: The item.
            """

        params = convert_to_openai_tool(f)["function"]["parameters"]
        assert params["properties"]["item"]["type"] == "object"
        assert set(params["properties"]["item"]["properties"]) == {"name", "size"}

    def test_convert_to_openai_function_is_the_inner_function_object(self) -> None:
        @tool("named")
        def f(x: int) -> int:
            """Doc."""
            return x

        function = convert_to_openai_function(f)
        assert function["name"] == "named"
        assert function["description"] == "Doc."
        assert set(function) == {"name", "description", "parameters"}

    def test_langchain_tool_decorator_excludes_our_tool_runtime(self) -> None:
        """DeerFlow builds its builtins with **langchain_core's** ``@tool``.

        That decorator decides "injected" from its own
        ``_DirectlyInjectedToolArg`` marker. When our ``ToolRuntime`` is not a
        subclass, the runtime stays in ``tool_call_schema`` and
        ``convert_to_openai_tool`` then dies generating a JSON schema for its
        ``stream_writer: Callable`` field — the real
        ``present_files``/``bind_tools`` failure.
        """
        host_tools = pytest.importorskip(
            "langchain_core.tools", reason="langchain_core is the optional comparison target"
        )
        host_tool = host_tools.tool

        from reactivegraph import middleware

        def f(a: str, runtime: middleware.ToolRuntime) -> str:
            """Doc.

            Args:
                a: The a value.
            """
            return a

        f.__annotations__["runtime"] = middleware.ToolRuntime
        f = host_tool(parse_docstring=True)(f)

        assert "runtime" not in f.tool_call_schema.model_fields
        from langchain_core.utils.function_calling import (
            convert_to_openai_tool as host_convert,
        )

        params = host_convert(f)["function"]["parameters"]
        assert list(params["properties"]) == ["a"]

    def test_schema_is_json_serialisable(self) -> None:
        @tool
        def f(a: str) -> None:
            """Doc."""

        json.dumps(convert_to_openai_tool(f))


class TestInvocation:
    def test_sync_invoke_returns_result(self) -> None:
        @tool
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        assert add.invoke({"a": 1, "b": 2}) == 3

    def test_invoke_validates_arguments(self) -> None:
        @tool
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        with pytest.raises(ValidationError):
            add.invoke({"a": "not-an-int", "b": 2})

    def test_tool_exception_is_raised(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Boom."""
            raise ToolException("nope")

        with pytest.raises(ToolException, match="nope"):
            boom.invoke({"x": 1})

    def test_ordinary_exception_is_raised(self) -> None:
        @tool
        def boom(x: int) -> int:
            """Boom."""
            raise ValueError("nope")

        with pytest.raises(ValueError, match="nope"):
            boom.invoke({"x": 1})

    @pytest.mark.asyncio
    async def test_async_tool_is_awaitable(self) -> None:
        @tool
        async def add(a: int, b: int) -> int:
            """Add."""
            await asyncio.sleep(0)
            return a + b

        assert await add.ainvoke({"a": 1, "b": 2}) == 3

    @pytest.mark.asyncio
    async def test_sync_tool_is_callable_through_ainvoke(self) -> None:
        @tool
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        assert await add.ainvoke({"a": 1, "b": 2}) == 3

    def test_async_tool_exposes_coroutine(self) -> None:
        @tool
        async def f(x: int) -> int:
            """Doc."""
            return x

        assert inspect.iscoroutinefunction(f.coroutine)
        assert f.func is None

    def test_sync_tool_exposes_func_and_no_coroutine(self) -> None:
        @tool
        def f(x: int) -> int:
            """Doc."""
            return x

        assert callable(f.func)
        assert f.coroutine is None


class TestStructuredTool:
    def test_constructs_from_explicit_fields(self) -> None:
        def run(a: int) -> int:
            return a * 2

        built = StructuredTool.from_function(func=run, name="double", description="Double.")
        assert built.name == "double"
        assert built.invoke({"a": 3}) == 6

    def test_constructs_with_explicit_args_schema(self) -> None:
        class Args(BaseModel):
            a: int

        def run(a: int) -> int:
            return a + 1

        built = StructuredTool(
            name="inc",
            description="Increment.",
            args_schema=Args,
            func=run,
        )
        assert built.invoke({"a": 1}) == 2
        assert set(convert_to_openai_tool(built)["function"]["parameters"]["properties"]) == {"a"}


class TestInjectedMarkers:
    def test_injected_tool_arg_marks_annotation(self) -> None:
        # Upstream stores the marker *class* in Annotated metadata, so the
        # detection contract is a subclass check, not an isinstance check.
        annotation = Annotated[str, InjectedToolArg]
        assert annotation.__metadata__ == (InjectedToolArg,)
        assert any(
            meta is InjectedToolArg or issubclass(meta, InjectedToolArg)
            for meta in annotation.__metadata__
        )

    def test_injected_tool_call_id_is_an_injected_tool_arg(self) -> None:
        assert issubclass(InjectedToolCallId, InjectedToolArg)


class TestModuleBoundary:
    def test_tools_fall_back_to_plain_pydantic_without_host_base_tool(self) -> None:
        """langchain-core stays optional: the host base is imported lazily.

        The module must import ``langchain_core.tools`` inside
        :func:`_host_base_tool` rather than at module scope, so a host that
        lacks it still gets a working (plain-pydantic) tool surface instead of
        an ImportError. Blocking only ``langchain_core.tools`` exercises the
        fallback without disturbing the rest of the package.
        """
        code = textwrap.dedent(
            """
            import importlib.abc, sys

            class _Blocker(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "langchain_core.tools":
                        raise ImportError("langchain_core.tools is not installed")
                    return None

            sys.meta_path.insert(0, _Blocker())

            import reactivegraph.tools as tools_module

            assert tools_module._host_base_tool() is None
            assert tools_module.HostBaseTool is None

            from reactivegraph.tools import StructuredTool

            def f(a: int, b: int) -> int:
                \"\"\"Add.\"\"\"
                return a + b

            t = StructuredTool.from_function(f)
            result = t.invoke(
                {"args": {"a": 1, "b": 2}, "name": "f", "type": "tool_call", "id": "c1"}
            )
            assert result.content == "3", result
            print("OK")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert "OK" in result.stdout


class TestToolMessageFormatting:
    """Tool outputs returned for a model tool call become ToolMessages."""

    def test_model_tool_call_wraps_result_in_tool_message(self) -> None:
        from reactivegraph.messages import ToolMessage

        @tool
        def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        result = add.invoke(
            {"args": {"a": 1, "b": 2}, "name": "add", "type": "tool_call", "id": "c1"}
        )
        assert result == ToolMessage(content="3", name="add", tool_call_id="c1")
        assert isinstance(result, ToolMessage)

    def test_non_string_content_is_json_stringified(self) -> None:
        from reactivegraph.messages import ToolMessage

        @tool
        def mapping(x: int) -> dict[str, list[int]]:
            """Doc."""
            return {"a": [x]}

        result = mapping.invoke(
            {"args": {"x": 1}, "name": "mapping", "type": "tool_call", "id": "c2"}
        )
        assert result == ToolMessage(content='{"a": [1]}', name="mapping", tool_call_id="c2")

    def test_message_content_blocks_are_preserved(self) -> None:
        from reactivegraph.messages import ToolMessage

        @tool
        def blocks(x: int) -> list[object]:
            """Doc."""
            return [{"type": "text", "text": str(x)}, "raw"]

        result = blocks.invoke(
            {"args": {"x": 4}, "name": "blocks", "type": "tool_call", "id": "c3"}
        )
        assert result == ToolMessage(
            content=[{"type": "text", "text": "4"}, "raw"],
            name="blocks",
            tool_call_id="c3",
        )

    def test_handled_tool_error_gets_error_status(self) -> None:
        from reactivegraph.messages import ToolMessage

        @tool
        def boom(x: int) -> int:
            """Doc."""
            raise ToolException("handled!")

        boom.handle_tool_error = True
        result = boom.invoke(
            {"args": {"x": 1}, "name": "boom", "type": "tool_call", "id": "c4"}
        )
        assert result == ToolMessage(
            content="handled!", name="boom", tool_call_id="c4", status="error"
        )

    @pytest.mark.asyncio
    async def test_async_model_tool_call_wraps_result(self) -> None:
        from reactivegraph.messages import ToolMessage

        @tool
        async def add(a: int, b: int) -> int:
            """Add."""
            return a + b

        result = await add.ainvoke(
            {"args": {"a": 1, "b": 2}, "name": "add", "type": "tool_call", "id": "c5"}
        )
        assert result == ToolMessage(content="3", name="add", tool_call_id="c5")


class TestHostToolOutputInterop:
    """Tools must recognise host-framework tool-output markers as pass-through.

    DeerFlow still constructs ``langgraph.types.Command`` in the gateway and
    some tools return ``langchain_core`` ``ToolMessage`` objects. Both classes
    are virtual members of the host ``ToolOutputMixin``; the engine's marker
    must recognise them, otherwise the tool layer silently stringifies a
    control-flow command into a ``ToolMessage`` and the graph never jumps.
    """

    def test_host_command_return_is_not_wrapped(self) -> None:
        host_types = pytest.importorskip("langgraph.types")

        @tool
        def jump(x: int) -> object:
            """Doc."""
            return host_types.Command(update={"x": x})

        result = jump.invoke(
            {"args": {"x": 1}, "name": "jump", "type": "tool_call", "id": "c1"}
        )
        assert isinstance(result, host_types.Command)
        assert result.update == {"x": 1}

    def test_engine_command_return_is_not_wrapped(self) -> None:
        from reactivegraph.types import Command

        @tool
        def jump(x: int) -> Command:
            """Doc."""
            return Command(update={"x": x})

        result = jump.invoke(
            {"args": {"x": 1}, "name": "jump", "type": "tool_call", "id": "c2"}
        )
        assert isinstance(result, Command)
        assert result.update == {"x": 1}

    def test_host_tool_message_in_list_is_not_wrapped(self) -> None:
        lc = pytest.importorskip("langchain_core.messages")

        @tool
        def emit(x: int) -> list[object]:
            """Doc."""
            return [lc.ToolMessage(content="ok", tool_call_id="c3")]

        result = emit.invoke(
            {"args": {"x": 1}, "name": "emit", "type": "tool_call", "id": "c3"}
        )
        assert isinstance(result, list)
        assert isinstance(result[0], lc.ToolMessage)
        assert result[0].content == "ok"


class TestHostToolNodeInterop:
    """Our tools must be usable by the *host* ``ToolNode``.

    ``langchain.agents.create_agent`` collects ``middleware.tools`` and hands
    them to ``langgraph.prebuilt.ToolNode``, which does
    ``isinstance(tool, langchain_core.tools.BaseTool)`` and, when false, tries
    to re-coerce the object through ``create_tool``. A structurally identical
    but unrelated class therefore dies with
    ``ValueError: The first argument must be a string or a callable…``.

    Measured on the real DeerFlow suite: every TodoMiddleware graph test fails
    this way. The fix is the same one already used for messages and
    middleware — inherit the host class when it is importable.
    """

    def test_our_tool_is_a_host_base_tool_instance(self) -> None:
        host_tools = pytest.importorskip("langchain_core.tools")

        @tool
        def ping(x: int) -> int:
            """Ping."""
            return x

        assert isinstance(ping, host_tools.BaseTool)

    def test_host_tool_node_accepts_our_tool_without_recoercion(self) -> None:
        pytest.importorskip("langgraph.prebuilt")
        from langgraph.prebuilt import ToolNode

        @tool
        def ping(x: int) -> int:
            """Ping."""
            return x + 1

        from langchain_core.messages import AIMessage as HostAIMessage
        from langgraph.graph import END, START, StateGraph

        node = ToolNode([ping])
        # ToolNode re-coerces anything that is not a host BaseTool, which is
        # where the real DeerFlow failures came from; identity proves it kept
        # our object as-is.
        assert node.tools_by_name["ping"] is ping

        builder = StateGraph(dict)
        builder.add_node("tools", node)
        builder.add_edge(START, "tools")
        builder.add_edge("tools", END)
        graph = builder.compile()
        result = graph.invoke(
            {
                "messages": [
                    HostAIMessage(
                        content="",
                        tool_calls=[{"name": "ping", "args": {"x": 1}, "id": "c1"}],
                    )
                ]
            }
        )
        [message] = result["messages"]
        assert message.content == "2"
