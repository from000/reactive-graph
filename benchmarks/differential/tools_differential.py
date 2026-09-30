#!/usr/bin/env python3
"""Differential contract check: ReactiveGraph tools vs upstream LangChain.

Runs in a single interpreter that has BOTH the native ``reactivegraph`` source
tree on ``sys.path`` and the real DeerFlow dependency set installed
(``langchain==1.3.14``, ``langchain-core==1.4.9``, ``langgraph==1.2.9``).

Each probe performs the same operation on both sides and compares the
normalised observable result.  A probe that cannot be compared is reported as
``SKIP`` with the reason, never silently as a pass.

Usage:
    cd "$DEERFLOW_UPSTREAM_ROOT/backend"
    PYTHONPATH=<reactivegraph-source> .venv/bin/python <this-script>
"""

import asyncio
import json
import sys
from collections.abc import Callable
from typing import Annotated, Any

from typing_extensions import TypedDict

FAILURES: list[str] = []
CHECKS = 0


def record(name: str, ours: Any, theirs: Any) -> None:
    global CHECKS
    CHECKS += 1
    if ours != theirs:
        FAILURES.append(f"{name}\n    ours  ={ours!r}\n    theirs={theirs!r}")
        print(f"FAIL {name}")
    else:
        print(f"ok   {name}  ({ours!r})")


def skip(name: str, reason: str) -> None:
    print(f"SKIP {name}  ({reason})")


def error_of(fn: Callable[[], Any]) -> str:
    try:
        fn()
    except BaseException as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"
    return "NO ERROR"


def build(module: Any) -> dict[str, Any]:
    """Build the same tool set from whichever framework's primitives."""
    tool = module.tool
    structured_tool = module.StructuredTool
    injected_tool_call_id = module.InjectedToolCallId
    runtime_type = module.ToolRuntime

    built: dict[str, Any] = {}

    @tool
    def plain(x: int) -> int:
        """Plain."""
        return x + 1

    built["plain"] = plain

    @tool("renamed")
    def original(x: int) -> int:
        """Doc."""
        return x

    built["renamed"] = original

    @tool(parse_docstring=True)
    def parsed(name: str, count: int = 1) -> str:
        """Greet someone.

        Args:
            name: Who to greet.
            count: How many times.
        """
        return name * count

    built["parsed"] = parsed

    @tool(parse_docstring=True)
    def optional(a: str, note: str | None = None) -> None:
        """Doc.

        Args:
            a: The a value.
            note: Optional note.
        """

    built["optional"] = optional

    @tool(parse_docstring=True)
    def injected(q: str, tool_call_id: Annotated[str, injected_tool_call_id]) -> str:
        """Doc.

        Args:
            q: The query.
        """
        return q

    built["injected"] = injected

    @tool(parse_docstring=True)
    def runtime_arg(a: str, runtime: runtime_type) -> str:
        """Doc.

        Args:
            a: The a value.
        """
        return a

    built["runtime_arg"] = runtime_arg

    @tool
    def annotated(a: Annotated[str, "The a value."], b: int = 2) -> None:
        """Doc."""

    built["annotated"] = annotated

    class Item(TypedDict):
        name: str
        size: int

    @tool(parse_docstring=True)
    def nested(item: Item) -> None:
        """Doc.

        Args:
            item: The item.
        """

    built["nested"] = nested

    @tool
    def boom(x: int) -> int:
        """Boom."""
        raise module.ToolException("nope")

    built["boom"] = boom

    @tool
    async def async_add(a: int, b: int) -> int:
        """Add."""
        return a + b

    built["async_add"] = async_add

    @tool(return_direct=True)
    def direct(x: int) -> int:
        """Doc."""
        return x

    built["direct"] = direct

    @tool
    def handled(x: int) -> int:
        """Doc."""
        raise module.ToolException("handled!")

    handled.handle_tool_error = True
    built["handled"] = handled

    @tool
    def invalid(a: int) -> int:
        """Doc."""
        return a

    invalid.handle_validation_error = True
    built["invalid"] = invalid

    @tool(response_format="content_and_artifact")
    def artifact(x: int) -> int:
        """Doc."""
        return x

    built["artifact"] = artifact

    def double(a: int) -> int:
        return a * 2

    built["structured"] = structured_tool.from_function(
        func=double, name="double", description="Double."
    )

    def undoc(a: int) -> int:
        return a

    built["undocumented"] = undoc
    return built


def schema_of(tool: Any) -> dict[str, Any]:
    schema = tool.tool_call_schema
    if isinstance(schema, dict):
        return schema
    return schema.model_json_schema()


def describe(tool: Any) -> dict[str, Any]:
    return {
        "name": tool.name,
        "description": tool.description,
        "return_direct": tool.return_direct,
        "args_schema_fields": list(tool.args_schema.model_fields),
        "tool_call_fields": list(tool.tool_call_schema.model_fields),
        "schema": schema_of(tool),
        "func": tool.func is not None,
        "coroutine": tool.coroutine is not None,
    }


def main() -> int:
    import langchain_core.tools as theirs
    import langchain_core.utils.function_calling as their_convert
    import reactivegraph.tools as ours

    theirs.convert_to_openai_function = their_convert.convert_to_openai_function
    theirs.convert_to_openai_tool = their_convert.convert_to_openai_tool

    theirs_module = type(sys)("theirs_tools")
    theirs_module.tool = theirs.tool
    theirs_module.StructuredTool = theirs.StructuredTool
    theirs_module.InjectedToolCallId = theirs.InjectedToolCallId
    theirs_module.ToolException = theirs.ToolException
    from langgraph.prebuilt import ToolRuntime as TheirRuntime

    theirs_module.ToolRuntime = TheirRuntime

    ours_module = type(sys)("ours_tools")
    ours_module.tool = ours.tool
    ours_module.StructuredTool = ours.StructuredTool
    ours_module.InjectedToolCallId = ours.InjectedToolCallId
    ours_module.ToolException = ours.ToolException
    ours_module.ToolRuntime = ours.ToolRuntime

    theirs_built = build(theirs_module)
    ours_built = build(ours_module)

    for key in ("plain", "renamed", "parsed", "optional", "injected", "runtime_arg"):
        record(f"tool.{key}.describe", describe(ours_built[key]), describe(theirs_built[key]))

    record(
        "tool.annotated.schema",
        schema_of(ours_built["annotated"]),
        schema_of(theirs_built["annotated"]),
    )
    record(
        "tool.nested.schema",
        schema_of(ours_built["nested"]),
        schema_of(theirs_built["nested"]),
    )
    record(
        "tool.direct.return_direct",
        ours_built["direct"].return_direct,
        theirs_built["direct"].return_direct,
    )
    record(
        "tool.plain.return_direct",
        ours_built["plain"].return_direct,
        theirs_built["plain"].return_direct,
    )
    record(
        "tool.parsed.default_docstring_retention",
        ours_built["parsed"].description,
        theirs_built["parsed"].description,
    )

    record(
        "convert_to_openai_function.parsed",
        ours.convert_to_openai_function(ours_built["parsed"]),
        theirs.convert_to_openai_function(theirs_built["parsed"]),
    )
    record(
        "convert_to_openai_tool.parsed",
        ours.convert_to_openai_tool(ours_built["parsed"]),
        theirs.convert_to_openai_tool(theirs_built["parsed"]),
    )
    record(
        "convert_to_openai_tool.strict",
        ours.convert_to_openai_tool(ours_built["parsed"], strict=True),
        theirs.convert_to_openai_tool(theirs_built["parsed"], strict=True),
    )
    record(
        "convert_to_openai_tool.dict_passthrough",
        ours.convert_to_openai_tool({"type": "web_search"}),
        theirs.convert_to_openai_tool({"type": "web_search"}),
    )
    record(
        "convert_to_openai_function.keys",
        sorted(ours.convert_to_openai_function(ours_built["plain"])),
        sorted(theirs.convert_to_openai_function(theirs_built["plain"])),
    )
    record(
        "schema.json_serialisable",
        json.dumps(ours.convert_to_openai_tool(ours_built["nested"]), sort_keys=True),
        json.dumps(theirs.convert_to_openai_tool(theirs_built["nested"]), sort_keys=True),
    )

    record(
        "invoke.plain",
        ours_built["plain"].invoke({"x": 1}),
        theirs_built["plain"].invoke({"x": 1}),
    )
    record(
        "invoke.validation_error",
        error_of(lambda: ours_built["plain"].invoke({"x": "nope"})).split("(")[0],
        error_of(lambda: theirs_built["plain"].invoke({"x": "nope"})).split("(")[0],
    )
    record(
        "invoke.tool_exception",
        error_of(lambda: ours_built["boom"].invoke({"x": 1})),
        error_of(lambda: theirs_built["boom"].invoke({"x": 1})),
    )
    record(
        "invoke.handle_tool_error",
        ours_built["handled"].invoke({"x": 1}),
        theirs_built["handled"].invoke({"x": 1}),
    )
    record(
        "invoke.handle_validation_error",
        type(ours_built["invalid"].invoke({"a": "x"})).__name__,
        type(theirs_built["invalid"].invoke({"a": "x"})).__name__,
    )
    record(
        "invoke.content_and_artifact_error",
        error_of(lambda: ours_built["artifact"].invoke({"x": 1})),
        error_of(lambda: theirs_built["artifact"].invoke({"x": 1})),
    )
    record(
        "invoke.ainvoke_sync_tool",
        asyncio.run(ours_built["plain"].ainvoke({"x": 1})),
        asyncio.run(theirs_built["plain"].ainvoke({"x": 1})),
    )
    record(
        "invoke.ainvoke_async_tool",
        asyncio.run(ours_built["async_add"].ainvoke({"a": 1, "b": 2})),
        asyncio.run(theirs_built["async_add"].ainvoke({"a": 1, "b": 2})),
    )
    record(
        "invoke.injected_tool_call_id_missing",
        error_of(lambda: ours_built["injected"].invoke({"q": "x"})),
        error_of(lambda: theirs_built["injected"].invoke({"q": "x"})),
    )
    def normalized_output(value: Any) -> Any:
        # Each engine owns its own message class, so compare the observable
        # class name and serialized fields instead of Python object identity.
        if hasattr(value, "model_dump"):
            return (type(value).__name__, value.model_dump())
        return value

    record(
        "invoke.injected_tool_call_id_present",
        normalized_output(
            ours_built["injected"].invoke(
                {"args": {"q": "x"}, "name": "injected", "type": "tool_call", "id": "c1"}
            )
        ),
        normalized_output(
            theirs_built["injected"].invoke(
                {"args": {"q": "x"}, "name": "injected", "type": "tool_call", "id": "c1"}
            )
        ),
    )
    record(
        "invoke.injected_args_keys",
        sorted(ours_built["injected"]._injected_args_keys),
        sorted(theirs_built["injected"]._injected_args_keys),
    )
    record(
        "invoke.runtime_arg_injected_keys",
        sorted(ours_built["runtime_arg"]._injected_args_keys),
        sorted(theirs_built["runtime_arg"]._injected_args_keys),
    )
    record(
        "invoke.structured_from_function",
        ours_built["structured"].invoke({"a": 3}),
        theirs_built["structured"].invoke({"a": 3}),
    )
    record(
        "invoke.args_property",
        ours_built["parsed"].args,
        theirs_built["parsed"].args,
    )
    record(
        "invoke.is_single_input",
        (ours_built["parsed"].is_single_input, ours_built["plain"].is_single_input),
        (theirs_built["parsed"].is_single_input, theirs_built["plain"].is_single_input),
    )

    record(
        "structured.undocumented_error",
        error_of(
            lambda: ours.StructuredTool.from_function(func=ours_built["undocumented"])
        ),
        error_of(
            lambda: theirs.StructuredTool.from_function(func=theirs_built["undocumented"])
        ),
    )

    from pydantic import BaseModel

    class Args(BaseModel):
        a: int

    def inc(a: int) -> int:
        return a + 1

    ours_explicit = ours.StructuredTool(
        name="inc", description="Increment.", args_schema=Args, func=inc
    )
    theirs_explicit = theirs.StructuredTool(
        name="inc", description="Increment.", args_schema=Args, func=inc
    )
    record(
        "structured.explicit_args_schema",
        (ours_explicit.invoke({"a": 1}), schema_of(ours_explicit)),
        (theirs_explicit.invoke({"a": 1}), schema_of(theirs_explicit)),
    )

    record(
        "injected.issubclass_relationship",
        issubclass(ours.InjectedToolCallId, ours.InjectedToolArg),
        issubclass(theirs.InjectedToolCallId, theirs.InjectedToolArg),
    )
    required_public_names = {
        "BaseTool",
        "InjectedToolArg",
        "InjectedToolCallId",
        "StructuredTool",
        "ToolException",
        "tool",
    }
    record(
        "module.__all__.present",
        sorted(required_public_names - set(ours.__all__)),
        sorted(required_public_names - set(theirs.__all__)),
    )

    print()
    print(f"checks={CHECKS} failures={len(FAILURES)}")
    for failure in FAILURES:
        print(failure)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
