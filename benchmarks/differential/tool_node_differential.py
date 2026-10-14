#!/usr/bin/env python3
"""Differential contract check: ReactiveGraph ToolNode vs upstream LangGraph.

Runs in one interpreter that has both the native ``reactivegraph`` source tree
on ``sys.path`` and the real DeerFlow dependency set installed
(``langchain-core==1.4.9``, ``langgraph==1.2.9``).

Every probe drives both ToolNodes through ``_func``/``_afunc`` with an explicit
config and runtime, which is the same entry point a compiled graph uses. Outputs
are normalised to framework-independent structures (message fields, Command
graph/goto/update) so the comparison is about behaviour, not object identity.

A probe that cannot be compared is reported as ``SKIP`` with the reason, never
silently as a pass.

Usage:
    cd "$DEERFLOW_UPSTREAM_ROOT/backend"
    PYTHONPATH=<reactivegraph-source> .venv/bin/python <this-script>
"""

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Annotated, Any

FAILURES: list[str] = []
CHECKS = 0


def record(name: str, ours: Any, theirs: Any) -> None:
    global CHECKS
    CHECKS += 1
    if ours != theirs:
        FAILURES.append(f"{name}\n    ours  ={ours!r}\n    theirs={theirs!r}")
        print(f"FAIL {name}")
        print(f"     ours  ={ours!r}")
        print(f"     theirs={theirs!r}")
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


def outcome_of(fn: Callable[[], Any]) -> dict[str, Any]:
    """Capture either the normalised value or the raised error, uniformly."""
    try:
        return {"status": "ok", "value": fn()}
    except BaseException as exc:  # noqa: BLE001
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}


def call(name: str, args: dict[str, Any], call_id: str = "c1") -> dict[str, Any]:
    return {"name": name, "args": args, "id": call_id, "type": "tool_call"}


def normalize(value: Any) -> Any:
    """Reduce either framework's output to plain comparable data."""
    if isinstance(value, dict):
        if value.get("__type") == "tool_call_with_context":
            return {k: normalize(v) for k, v in value.items()}
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if hasattr(value, "tool_call_id") and hasattr(value, "content"):
        return {
            "kind": "ToolMessage",
            "content": value.content,
            "name": getattr(value, "name", None),
            "tool_call_id": value.tool_call_id,
            "status": getattr(value, "status", None),
        }
    if type(value).__name__ == "Command" and hasattr(value, "update"):
        return {
            "kind": "Command",
            "graph": value.graph,
            "goto": normalize(value.goto),
            "update": normalize(value.update),
        }
    if type(value).__name__ == "Send" and hasattr(value, "node"):
        return {"kind": "Send", "node": value.node, "arg": normalize(value.arg)}
    if hasattr(value, "model_dump"):
        return {"kind": type(value).__name__, "data": normalize(value.model_dump())}
    return value


class Side:
    """One framework's ToolNode plus the primitives used to build probes."""

    def __init__(self, module: Any, tool_node_cls: Any, runtime_cls: Any) -> None:
        self.m = module
        self.ToolNode = tool_node_cls
        self.Runtime = runtime_cls

    def run(self, node: Any, state: Any, config: dict[str, Any] | None = None) -> Any:
        return normalize(node._func(state, config or {}, self.Runtime()))

    async def arun(
        self, node: Any, state: Any, config: dict[str, Any] | None = None
    ) -> Any:
        return normalize(await node._afunc(state, config or {}, self.Runtime()))


def build_tools(m: Any, runtime_cls: Any) -> dict[str, Any]:
    tool = m.tool
    injected_tool_call_id = m.InjectedToolCallId
    injected_state = m.InjectedState
    injected_store = m.InjectedStore
    tool_exception = m.ToolException
    built: dict[str, Any] = {}

    @tool
    def echo(x: int) -> int:
        """Echo."""
        return x

    built["echo"] = echo

    @tool
    def concat(a: str, b: str) -> str:
        """Concat."""
        return a + b

    built["concat"] = concat

    @tool
    async def aecho(x: int) -> int:
        """Async echo."""
        return x

    built["aecho"] = aecho

    @tool
    def typed(x: int) -> int:
        """Typed."""
        return x

    built["typed"] = typed

    @tool
    def boom(x: int) -> int:
        """Boom."""
        raise ValueError("kaboom")

    built["boom"] = boom

    @tool
    def typed_boom(x: int) -> int:
        """Typed boom."""
        raise TypeError("type kaboom")

    built["typed_boom"] = typed_boom

    @tool
    def tool_exc(x: int) -> int:
        """Tool exception."""
        raise tool_exception("tool kaboom")

    built["tool_exc"] = tool_exc

    @tool
    def state_field(foo: Annotated[str, injected_state("foo")]) -> str:
        """State field."""
        return foo

    built["state_field"] = state_field

    @tool
    def state_whole(state: Annotated[dict, injected_state]) -> str:
        """Whole state."""
        return state["foo"]

    built["state_whole"] = state_whole

    @tool
    def store_arg(store: Annotated[Any, injected_store]) -> str:
        """Store."""
        return "used"

    built["store_arg"] = store_arg

    @tool
    def runtime_arg(runtime: runtime_cls) -> str:
        """Runtime."""
        return str(runtime.tool_call_id)

    built["runtime_arg"] = runtime_arg

    @tool
    def tool_runtime_arg(
        x: int, runtime: m.ToolRuntime
    ) -> str:
        """Real ToolRuntime injection."""
        return f"{runtime.tool_call_id}|{runtime.state.get('foo')}"

    built["tool_runtime_arg"] = tool_runtime_arg

    @tool
    def injected_id(
        x: int, tool_call_id: Annotated[str, injected_tool_call_id]
    ) -> str:
        """Injected id."""
        return tool_call_id

    built["injected_id"] = injected_id

    @tool
    def jump(x: int) -> m.Command:
        """Jump."""
        return m.Command(update={"messages": [m.ToolMessage("ok", tool_call_id="c1")]})

    built["jump"] = jump

    @tool
    def jump_list(x: int) -> m.Command:
        """Jump with list update."""
        return m.Command(update=[m.ToolMessage("ok", tool_call_id="c1")])

    built["jump_list"] = jump_list

    @tool
    def no_terminator(x: int) -> m.Command:
        """Command without a matching ToolMessage."""
        return m.Command(update={"messages": [m.HumanMessage("nope")]})

    built["no_terminator"] = no_terminator

    @tool
    def parent_go(x: int) -> m.Command:
        """Parent command."""
        return m.Command(
            graph=m.Command.PARENT,
            goto=[m.Send("a" if x == 1 else "b", {"x": x})],
        )

    built["parent_go"] = parent_go

    @tool
    def multi(x: int) -> list[Any]:
        """Two messages with exactly one matching terminator."""
        return [
            m.ToolMessage("other", tool_call_id="other"),
            m.ToolMessage("mine", tool_call_id="c1"),
        ]

    built["multi"] = multi

    @tool
    def multi_bad(x: int) -> list[Any]:
        """Two messages with two matching terminators."""
        return [
            m.ToolMessage("a", tool_call_id="c1"),
            m.ToolMessage("b", tool_call_id="c1"),
        ]

    built["multi_bad"] = multi_bad

    @tool
    def plain_list(x: int) -> list[Any]:
        """Plain list, wrapped by the tool contract."""
        return ["not-a-message"]

    built["plain_list"] = plain_list

    @tool
    def clear_all(x: int) -> m.Command:
        """Remove all messages."""
        return m.Command(update={"messages": [m.RemoveMessage(id=m.REMOVE_ALL_MESSAGES)]})

    built["clear_all"] = clear_all

    return built


def message(m: Any, calls: list[dict[str, Any]]) -> Any:
    return m.AIMessage(content="", tool_calls=calls)


def main() -> int:
    import langchain_core.messages as their_messages
    import langchain_core.tools as their_tools
    import langgraph.prebuilt as their_prebuilt
    import langgraph.types as their_types
    import reactivegraph.constants as our_constants
    import reactivegraph.messages as our_messages
    import reactivegraph.middleware as our_middleware
    import reactivegraph.tool_node as our_module
    import reactivegraph.tools as our_tools
    import reactivegraph.types as our_types
    from langgraph.graph.message import REMOVE_ALL_MESSAGES as THEIR_REMOVE_ALL
    from langgraph.prebuilt import tool_node as their_tool_node_module
    from langgraph.runtime import Runtime as TheirRuntime
    from reactivegraph.runtime import Runtime as OurRuntime

    theirs = Side(their_tools, their_prebuilt.ToolNode, TheirRuntime)
    ours = Side(
        SimpleNamespace(**{
            "tool": our_tools.tool,
                "InjectedToolCallId": our_tools.InjectedToolCallId,
                "InjectedState": our_module.InjectedState,
                "InjectedStore": our_module.InjectedStore,
                "ToolException": our_tools.ToolException,
            "ToolRuntime": our_middleware.ToolRuntime,
                "Command": our_types.Command,
                "ToolMessage": our_messages.ToolMessage,
                "HumanMessage": our_messages.HumanMessage,
                "RemoveMessage": our_messages.RemoveMessage,
                "REMOVE_ALL_MESSAGES": our_constants.REMOVE_ALL_MESSAGES,
                "Send": our_types.Send,
                "AIMessage": our_messages.AIMessage,
            },
        ),
        our_module.ToolNode,
        OurRuntime,
    )

    their_module = SimpleNamespace(**{
            "tool": their_tools.tool,
            "InjectedToolCallId": their_tools.InjectedToolCallId,
            "InjectedState": their_prebuilt.InjectedState,
            "InjectedStore": their_prebuilt.InjectedStore,
            "ToolException": their_tools.ToolException,
            "ToolRuntime": their_prebuilt.ToolRuntime,
            "Command": their_types.Command,
            "ToolMessage": their_messages.ToolMessage,
            "HumanMessage": their_messages.HumanMessage,
            "RemoveMessage": their_messages.RemoveMessage,
            "REMOVE_ALL_MESSAGES": THEIR_REMOVE_ALL,
            "Send": their_types.Send,
            "AIMessage": their_messages.AIMessage,
        },
    )

    ours_built = build_tools(ours.m, OurRuntime)
    their_tools_built = build_tools(their_module, TheirRuntime)

    # ---- input shapes -------------------------------------------------
    for name, state in (
        (
            "dict",
            lambda m, t: {
                "messages": [
                    m.AIMessage(content="", tool_calls=[call("echo", {"x": 1})])
                ]
            },
        ),
        ("list", lambda m, t: [m.AIMessage(content="", tool_calls=[call("echo", {"x": 1})])]),
        ("bare_tool_calls", lambda m, t: [call("echo", {"x": 1})]),
    ):
        record(
            f"input.{name}",
            ours.run(ours.ToolNode([ours_built["echo"]]), state(ours.m, ours_built)),
            theirs.run(
                theirs.ToolNode([their_tools_built["echo"]]),
                state(their_module, their_tools_built),
            ),
        )

    record(
        "input.custom_messages_key",
        ours.run(
            ours.ToolNode([ours_built["echo"]], messages_key="chat"),
            {"chat": [ours.m.AIMessage(content="", tool_calls=[call("echo", {"x": 2})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["echo"]], messages_key="chat"),
            {"chat": [their_module.AIMessage(content="", tool_calls=[call("echo", {"x": 2})])]},
        ),
    )

    record(
        "input.no_messages_error",
        error_of(lambda: ours.run(ours.ToolNode([ours_built["echo"]]), {"other": []})),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode([their_tools_built["echo"]]), {"other": []}
            )
        ),
    )

    # ---- execution ----------------------------------------------------
    record(
        "exec.batch_order",
        ours.run(
            ours.ToolNode([ours_built["echo"]]),
            {
                "messages": [
                    ours.m.AIMessage(
                        content="",
                        tool_calls=[
                            call("echo", {"x": 3}, "c3"),
                            call("echo", {"x": 1}, "c1"),
                            call("echo", {"x": 2}, "c2"),
                        ],
                    )
                ]
            },
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["echo"]]),
            {
                "messages": [
                    their_module.AIMessage(
                        content="",
                        tool_calls=[
                            call("echo", {"x": 3}, "c3"),
                            call("echo", {"x": 1}, "c1"),
                            call("echo", {"x": 2}, "c2"),
                        ],
                    )
                ]
            },
        ),
    )
    record(
        "exec.async",
        asyncio.run(
            ours.arun(
                ours.ToolNode([ours_built["aecho"]]),
                {"messages": [ours.m.AIMessage(content="", tool_calls=[call("aecho", {"x": 5})])]},
            )
        ),
        asyncio.run(
            theirs.arun(
                theirs.ToolNode([their_tools_built["aecho"]]),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("aecho", {"x": 5})]
                        )
                    ]
                },
            )
        ),
    )
    record(
        "exec.unknown_tool",
        ours.run(
            ours.ToolNode([ours_built["echo"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("missing", {})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["echo"]]),
            {"messages": [their_module.AIMessage(content="", tool_calls=[call("missing", {})])]},
        ),
    )

    # ---- error policy -------------------------------------------------
    def policy_probe(policy_name: str, policy_ours: Any, policy_theirs: Any) -> None:
        record(
            f"errors.{policy_name}.value_error",
            outcome_of(
                lambda: ours.run(
                    ours.ToolNode([ours_built["boom"]], handle_tool_errors=policy_ours),
                    {
                        "messages": [
                            ours.m.AIMessage(content="", tool_calls=[call("boom", {"x": 1})])]},
                )
            ),
            outcome_of(
                lambda: theirs.run(
                    theirs.ToolNode(
                        [their_tools_built["boom"]], handle_tool_errors=policy_theirs
                    ),
                    {
                        "messages": [
                            their_module.AIMessage(
                                content="", tool_calls=[call("boom", {"x": 1})]
                            )
                        ]
                    },
                )
            ),
        )

    policy_probe(
        "default",
        our_module._default_handle_tool_errors,
        their_tool_node_module._default_handle_tool_errors,
    )
    policy_probe("true", True, True)
    policy_probe("string", "please retry", "please retry")
    policy_probe("type", ValueError, ValueError)
    policy_probe("tuple", (ValueError, TypeError), (ValueError, TypeError))

    record(
        "errors.false.reraise",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["boom"]], handle_tool_errors=False),
                {"messages": [ours.m.AIMessage(content="", tool_calls=[call("boom", {"x": 1})])]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode(
                    [their_tools_built["boom"]], handle_tool_errors=False
                ),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("boom", {"x": 1})]
                        )
                    ]
                },
            )
        ),
    )
    record(
        "errors.type_mismatch.reraise",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["typed_boom"]], handle_tool_errors=ValueError),
                {
                    "messages": [
                        ours.m.AIMessage(content="", tool_calls=[call("typed_boom", {"x": 1})])]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode(
                    [their_tools_built["typed_boom"]], handle_tool_errors=ValueError
                ),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("typed_boom", {"x": 1})]
                        )
                    ]
                },
            )
        ),
    )

    def handler(exc: ValueError) -> str:
        return f"handled: {exc}"

    policy_probe("callable", handler, handler)

    record(
        "errors.infer_union",
        tuple(
            sorted(
                t.__name__
                for t in our_module._infer_handled_types(
                    lambda exc: "x"  # type: ignore[arg-type,return-value]
                )
            )
        ),
        tuple(
            sorted(
                t.__name__
                for t in their_tool_node_module._infer_handled_types(
                    lambda exc: "x"  # type: ignore[arg-type,return-value]
                )
            )
        ),
    )

    def union_handler(exc: ValueError | TypeError) -> str:
        return "x"

    record(
        "errors.infer_union_types",
        tuple(sorted(t.__name__ for t in our_module._infer_handled_types(union_handler))),
        tuple(
            sorted(
                t.__name__ for t in their_tool_node_module._infer_handled_types(union_handler)
            )
        ),
    )

    def bad_handler(exc: int) -> str:
        return "x"

    record(
        "errors.infer_arbitrary_type_error",
        error_of(lambda: our_module._infer_handled_types(bad_handler)),
        error_of(lambda: their_tool_node_module._infer_handled_types(bad_handler)),
    )

    # ---- injection ----------------------------------------------------
    record(
        "inject.state_field",
        ours.run(
            ours.ToolNode([ours_built["state_field"]]),
            {
                "messages": [ours.m.AIMessage(content="", tool_calls=[call("state_field", {})])],
                "foo": "bar",
            },
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["state_field"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("state_field", {})])
                ],
                "foo": "bar",
            },
        ),
    )
    record(
        "inject.state_whole",
        ours.run(
            ours.ToolNode([ours_built["state_whole"]]),
            {
                "messages": [ours.m.AIMessage(content="", tool_calls=[call("state_whole", {})])],
                "foo": "baz",
            },
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["state_whole"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("state_whole", {})])
                ],
                "foo": "baz",
            },
        ),
    )
    record(
        "inject.store_missing_error",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["store_arg"]]),
                {"messages": [ours.m.AIMessage(content="", tool_calls=[call("store_arg", {})])]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode([their_tools_built["store_arg"]]),
                {
                    "messages": [
                        their_module.AIMessage(content="", tool_calls=[call("store_arg", {})])
                    ]
                },
            )
        ),
    )
    record(
        "inject.runtime_by_name",
        ours.run(
            ours.ToolNode([ours_built["runtime_arg"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("runtime_arg", {})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["runtime_arg"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("runtime_arg", {})])
                ]
            },
        ),
    )
    record(
        "inject.forged_runtime_stripped",
        ours.run(
            ours.ToolNode([ours_built["runtime_arg"]]),
            {
                "messages": [
                    ours.m.AIMessage(
                        content="", tool_calls=[call("runtime_arg", {"runtime": "forged"})]
                    )
                ]
            },
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["runtime_arg"]]),
            {
                "messages": [
                    their_module.AIMessage(
                        content="", tool_calls=[call("runtime_arg", {"runtime": "forged"})]
                    )
                ]
            },
        ),
    )
    record(
        "inject.tool_runtime_real",
        ours.run(
            ours.ToolNode([ours_built["tool_runtime_arg"]]),
            {
                "messages": [
                    ours.m.AIMessage(content="", tool_calls=[call("tool_runtime_arg", {"x": 1})])
                ],
                "foo": "bar",
            },
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["tool_runtime_arg"]]),
            {
                "messages": [
                    their_module.AIMessage(
                        content="", tool_calls=[call("tool_runtime_arg", {"x": 1})]
                    )
                ],
                "foo": "bar",
            },
        ),
    )
    record(
        "inject.tool_call_id",
        ours.run(
            ours.ToolNode([ours_built["injected_id"]]),
            {
                "messages": [
                    ours.m.AIMessage(content="", tool_calls=[call("injected_id", {"x": 1})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["injected_id"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("injected_id", {"x": 1})])
                ]
            },
        ),
    )

    # ---- validation errors -------------------------------------------
    record(
        "validation.invocation_error",
        ours.run(
            ours.ToolNode([ours_built["typed"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("typed", {"x": "bad"})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["typed"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("typed", {"x": "bad"})])
                ]
            },
        ),
    )

    # ---- Command handling --------------------------------------------
    record(
        "command.dict_update",
        ours.run(
            ours.ToolNode([ours_built["jump"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("jump", {"x": 1})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["jump"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("jump", {"x": 1})])
                ]
            },
        ),
    )
    record(
        "command.list_update_on_dict_input",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["jump_list"]], handle_tool_errors=False),
                {
                    "messages": [
                        ours.m.AIMessage(content="", tool_calls=[call("jump_list", {"x": 1})])]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode(
                    [their_tools_built["jump_list"]], handle_tool_errors=False
                ),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("jump_list", {"x": 1})]
                        )
                    ]
                },
            )
        ),
    )
    record(
        "command.missing_terminator",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["no_terminator"]], handle_tool_errors=False),
                {
                    "messages": [
                        ours.m.AIMessage(
                            content="", tool_calls=[call("no_terminator", {"x": 1})]
                        )
                    ]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode(
                    [their_tools_built["no_terminator"]], handle_tool_errors=False
                ),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("no_terminator", {"x": 1})]
                        )
                    ]
                },
            )
        ),
    )
    record(
        "command.parent_go_collapsed",
        ours.run(
            ours.ToolNode([ours_built["parent_go"]]),
            {
                "messages": [
                    ours.m.AIMessage(
                        content="",
                        tool_calls=[
                            call("parent_go", {"x": 1}, "c1"),
                            call("parent_go", {"x": 2}, "c2"),
                        ],
                    )
                ]
            },
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["parent_go"]]),
            {
                "messages": [
                    their_module.AIMessage(
                        content="",
                        tool_calls=[
                            call("parent_go", {"x": 1}, "c1"),
                            call("parent_go", {"x": 2}, "c2"),
                        ],
                    )
                ]
            },
        ),
    )
    record(
        "command.clear_all",
        ours.run(
            ours.ToolNode([ours_built["clear_all"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("clear_all", {"x": 1})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["clear_all"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("clear_all", {"x": 1})])
                ]
            },
        ),
    )
    record(
        "command.multi_terminator_ok",
        ours.run(
            ours.ToolNode([ours_built["multi"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("multi", {"x": 1})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["multi"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("multi", {"x": 1})])
                ]
            },
        ),
    )
    record(
        "command.multi_terminator_rejected",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["multi_bad"]], handle_tool_errors=False),
                {
                    "messages": [
                        ours.m.AIMessage(content="", tool_calls=[call("multi_bad", {"x": 1})])]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode(
                    [their_tools_built["multi_bad"]], handle_tool_errors=False
                ),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("multi_bad", {"x": 1})]
                        )
                    ]
                },
            )
        ),
    )
    record(
        "command.plain_list_wrapped",
        ours.run(
            ours.ToolNode([ours_built["plain_list"]]),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("plain_list", {"x": 1})])]},
        ),
        theirs.run(
            theirs.ToolNode([their_tools_built["plain_list"]]),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("plain_list", {"x": 1})])
                ]
            },
        ),
    )

    # ---- wrappers -----------------------------------------------------
    def sync_wrapper(request: Any, handler: Callable[..., Any]) -> Any:
        result = handler(request)
        return ours.m.ToolMessage(
            f"wrapped:{result.content}",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
        )

    def their_sync_wrapper(request: Any, handler: Callable[..., Any]) -> Any:
        result = handler(request)
        return their_module.ToolMessage(
            f"wrapped:{result.content}",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
        )

    record(
        "wrapper.sync",
        ours.run(
            ours.ToolNode([ours_built["echo"]], wrap_tool_call=sync_wrapper),
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("echo", {"x": 7})])]},
        ),
        theirs.run(
            theirs.ToolNode(
                [their_tools_built["echo"]], wrap_tool_call=their_sync_wrapper
            ),
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("echo", {"x": 7})])
                ]
            },
        ),
    )

    async def async_wrapper(request: Any, handler: Callable[..., Any]) -> Any:
        result = await handler(request)
        return ours.m.ToolMessage(
            f"async:{result.content}",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
        )

    async def their_async_wrapper(request: Any, handler: Callable[..., Any]) -> Any:
        result = await handler(request)
        return their_module.ToolMessage(
            f"async:{result.content}",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
        )

    record(
        "wrapper.async",
        asyncio.run(
            ours.arun(
                ours.ToolNode([ours_built["echo"]], awrap_tool_call=async_wrapper),
                {"messages": [ours.m.AIMessage(content="", tool_calls=[call("echo", {"x": 8})])]},
            )
        ),
        asyncio.run(
            theirs.arun(
                theirs.ToolNode(
                    [their_tools_built["echo"]], awrap_tool_call=their_async_wrapper
                ),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("echo", {"x": 8})]
                        )
                    ]
                },
            )
        ),
    )

    # ---- middleware request surface -----------------------------------
    def sync_override(request: Any, handler: Callable[..., Any]) -> Any:
        seen: dict[str, Any] = {}
        seen["tool_name"] = request.tool_call["name"]
        seen["has_runtime"] = request.runtime is not None
        seen["state_foo"] = (request.state or {}).get("foo")
        patched = request.override(
            tool_call={
                "name": "concat",
                "args": {"a": "OVERRIDDEN", "b": "!"},
                "id": request.tool_call["id"],
                "type": "tool_call",
            }
        )
        result = handler(patched)
        return ours.m.ToolMessage(
            f"seen={sorted(seen.items())}|{result.content}",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
        )

    def their_sync_override(request: Any, handler: Callable[..., Any]) -> Any:
        seen: dict[str, Any] = {}
        seen["tool_name"] = request.tool_call["name"]
        seen["has_runtime"] = request.runtime is not None
        seen["state_foo"] = (request.state or {}).get("foo")
        patched = request.override(
            tool_call={
                "name": "concat",
                "args": {"a": "OVERRIDDEN", "b": "!"},
                "id": request.tool_call["id"],
                "type": "tool_call",
            }
        )
        result = handler(patched)
        return their_module.ToolMessage(
            f"seen={sorted(seen.items())}|{result.content}",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
        )

    record(
        "wrapper.request_override_and_runtime",
        ours.run(
            ours.ToolNode([ours_built["concat"]], wrap_tool_call=sync_override),
            {
                "messages": [
                    ours.m.AIMessage(
                        content="", tool_calls=[call("concat", {"a": "x", "b": "y"})]
                    )
                ],
                "foo": "bar",
            },
        ),
        theirs.run(
            theirs.ToolNode(
                [their_tools_built["concat"]], wrap_tool_call=their_sync_override
            ),
            {
                "messages": [
                    their_module.AIMessage(
                        content="", tool_calls=[call("concat", {"a": "x", "b": "y"})]
                    )
                ],
                "foo": "bar",
            },
        ),
    )

    # ---- constructor validation ----------------------------------------
    record(
        "ctor.missing_tools_keyword_error",
        error_of(lambda: ours.ToolNode(handle_tool_errors=True)),
        error_of(lambda: theirs.ToolNode(handle_tool_errors=True)),
    )
    record(
        "ctor.duplicate_tool_name_accepted",
        error_of(
            lambda: ours.ToolNode([ours_built["echo"], ours_built["echo"]])
        ),
        error_of(
            lambda: theirs.ToolNode([their_tools_built["echo"], their_tools_built["echo"]])
        ),
    )
    record(
        "ctor.bad_error_policy",
        error_of(
            lambda: ours.run(
                ours.ToolNode([ours_built["boom"]], handle_tool_errors=3.5),
                {"messages": [ours.m.AIMessage(content="", tool_calls=[call("boom", {"x": 1})])]},
            )
        ),
        error_of(
            lambda: theirs.run(
                theirs.ToolNode([their_tools_built["boom"]], handle_tool_errors=3.5),
                {
                    "messages": [
                        their_module.AIMessage(
                            content="", tool_calls=[call("boom", {"x": 1})]
                        )
                    ]
                },
            )
        ),
    )
    # ---- helpers ------------------------------------------------------
    record(
        "tools_condition.with_calls",
        our_module.tools_condition(
            {"messages": [ours.m.AIMessage(content="", tool_calls=[call("echo", {})])]}
        ),
        their_tool_node_module.tools_condition(
            {
                "messages": [
                    their_module.AIMessage(content="", tool_calls=[call("echo", {})])
                ]
            }
        ),
    )
    record(
        "tools_condition.without_calls",
        our_module.tools_condition({"messages": [ours.m.AIMessage(content="done")]}),
        their_tool_node_module.tools_condition(
            {"messages": [their_module.AIMessage(content="done")]}
        ),
    )
    record(
        "tools_condition.missing_messages",
        error_of(lambda: our_module.tools_condition({"other": []})),
        error_of(lambda: their_tool_node_module.tools_condition({"other": []})),
    )
    for value in ("plain", 3, {"a": 1}, ["x"], ["not-a-message"], [{"type": "text", "text": "hi"}]):
        record(
            f"msg_content_output.{value!r}",
            our_module.msg_content_output(value),
            their_tool_node_module.msg_content_output(value),
        )

    print()
    print("parity baseline: langchain-core 1.4.9 / langgraph 1.2.9 / cpython 3.12.11")
    if FAILURES:
        print(f"{len(FAILURES)} FAILURES out of {CHECKS} checks")
        for failure in FAILURES:
            print(failure)
        return 1
    print(f"all {CHECKS} checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
