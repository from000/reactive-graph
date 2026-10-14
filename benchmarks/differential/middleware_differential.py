#!/usr/bin/env python3
"""Differential contract check: ReactiveGraph middleware vs upstream LangChain.

Runs in a single interpreter that has BOTH the native ``reactivegraph`` source
tree on ``sys.path`` and the real DeerFlow dependency set installed
(``langchain==1.3.14``, ``langchain-core==1.4.9``, ``langgraph==1.2.9``).

Each probe performs the same operation on both sides and compares the
normalised observable result.  A probe that cannot be compared is reported as
``SKIP`` with the reason, never silently as a pass.

Usage:
    cd "$DEERFLOW_UPSTREAM_ROOT/backend"
    PYTHONPATH=<reactivegraph-source> .venv/bin/python \
        <this-script>
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import sys
from collections.abc import Callable
from typing import Any

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


def model_request_probes() -> None:
    from langchain.agents.middleware import ModelRequest as Theirs
    from reactivegraph.middleware import ModelRequest as Ours

    ours = Ours(model="m", messages=[])
    theirs = Theirs(model="m", messages=[])
    record("ModelRequest.defaults", dataclasses.asdict(ours), dataclasses.asdict(theirs))
    record(
        "ModelRequest.field_order",
        [f.name for f in dataclasses.fields(Ours)],
        [f.name for f in dataclasses.fields(Theirs)],
    )
    record(
        "ModelRequest.is_dataclass",
        dataclasses.is_dataclass(Ours),
        dataclasses.is_dataclass(Theirs),
    )

    for label, kwargs in (
        ("system_prompt", {"system_prompt": "be concise"}),
        ("system_message", None),
    ):
        if kwargs is None:
            from langchain_core.messages import SystemMessage as TheirMsg
            from reactivegraph.messages import SystemMessage as OurMsg

            o = Ours(model="m", messages=[], system_message=OurMsg("x"))
            t = Theirs(model="m", messages=[], system_message=TheirMsg("x"))
            record(
                f"ModelRequest.{label}",
                (type(o.system_message).__name__, o.system_message.content, o.system_prompt),
                (type(t.system_message).__name__, t.system_message.content, t.system_prompt),
            )
        else:
            o = Ours(model="m", messages=[], **kwargs)
            t = Theirs(model="m", messages=[], **kwargs)
            record(
                f"ModelRequest.{label}",
                (type(o.system_message).__name__, o.system_message.content, o.system_prompt),
                (type(t.system_message).__name__, t.system_message.content, t.system_prompt),
            )

    def both_error(fn: Callable[[], Any]) -> str:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            return f"{type(exc).__name__}"
        return "NO ERROR"

    from langchain_core.messages import SystemMessage as TheirMsg
    from reactivegraph.messages import SystemMessage as OurMsg

    record(
        "ModelRequest.both_prompts_error",
        both_error(
            lambda: Ours(model="m", messages=[], system_prompt="a", system_message=OurMsg("b"))
        ),
        both_error(
            lambda: Theirs(model="m", messages=[], system_prompt="a", system_message=TheirMsg("b"))
        ),
    )

    o = Ours(model="m", messages=[])
    t = Theirs(model="m", messages=[])
    o.messages = ["new"]
    t.messages = ["new"]
    record("ModelRequest.assignment", (o.messages, t.messages), (["new"], ["new"]))

    patched_o = Ours(model="m", messages=[]).override(system_prompt="p")
    patched_t = Theirs(model="m", messages=[]).override(system_prompt="p")
    record(
        "ModelRequest.override.system_prompt",
        (type(patched_o.system_message).__name__, patched_o.system_message.content),
        (type(patched_t.system_message).__name__, patched_t.system_message.content),
    )


def state_probes() -> None:
    import langchain.agents.middleware.types as theirs_types
    from reactivegraph import middleware as ours_mod

    for name in ("AgentState", "InputAgentState", "OutputAgentState"):
        o = getattr(ours_mod, name)
        t = getattr(theirs_types, name)
        record(
            f"{name}.keys",
            (sorted(o.__required_keys__), sorted(o.__optional_keys__)),
            (sorted(t.__required_keys__), sorted(t.__optional_keys__)),
        )


def middleware_defaults_probes() -> None:
    from langchain.agents.middleware import AgentMiddleware as Theirs
    from reactivegraph.middleware import AgentMiddleware as Ours

    o = Ours()
    t = Theirs()
    record("AgentMiddleware.name", o.name, t.name)
    record("AgentMiddleware.transformers", tuple(o.transformers), tuple(t.transformers))
    record("AgentMiddleware.has_tools_class", hasattr(Ours, "tools"), hasattr(Theirs, "tools"))
    record("AgentMiddleware.has_tools_instance", hasattr(o, "tools"), hasattr(t, "tools"))
    record(
        "AgentMiddleware.state_schema_name",
        o.state_schema.__name__,
        t.state_schema.__name__,
    )

    state = {"messages": []}
    for hook in ("before_agent", "before_model", "after_model", "after_agent"):
        record(
            f"AgentMiddleware.{hook}",
            getattr(o, hook)(state, None),
            getattr(t, hook)(state, None),
        )


def decorator_probes() -> None:
    import langchain.agents.middleware as theirs_mw
    import reactivegraph.middleware as ours_mw

    for decorator in ("before_model", "after_model", "before_agent", "after_agent"):
        def make(side: Any, _hook_name: str = decorator) -> tuple[str, str, Any, Any]:
            @side(state_schema=dict)
            def hook(state: Any, runtime: Any) -> dict[str, int]:
                return {"n": 1}

            return (
                type(hook).__name__,
                hook.state_schema.__name__,
                getattr(type(hook), _hook_name)(hook, {"messages": []}, None),
                tuple(hook.tools),
            )

        record(
            f"@{decorator}",
            make(getattr(ours_mw, decorator)),
            make(getattr(theirs_mw, decorator)),
        )

    def named(side: Any) -> tuple[str, str]:
        @side(name="Custom")
        def hook(state: Any, runtime: Any) -> None:
            return None

        return type(hook).__name__, hook.name

    record("@before_model(name=)", named(ours_mw.before_model), named(theirs_mw.before_model))

    def can_jump(side: Any) -> Any:
        @side(can_jump_to=["end"])
        def hook(state: Any, runtime: Any) -> None:
            return None

        return getattr(type(hook).before_model, "__can_jump_to__", None)

    record(
        "@before_model(can_jump_to=)",
        can_jump(ours_mw.before_model),
        can_jump(theirs_mw.before_model),
    )

    def async_side(side: Any) -> tuple[bool, bool]:
        @side
        async def hook(state: Any, runtime: Any) -> None:
            return None

        return (
            inspect.iscoroutinefunction(hook.abefore_model),
            hook.before_model({"messages": []}, None) is None,
        )

    record(
        "@before_model(async)",
        async_side(ours_mw.before_model),
        async_side(theirs_mw.before_model),
    )

    def hook_config_side(side: Any) -> Any:
        @side(can_jump_to=["tools", "end"])
        def hook(state: Any, runtime: Any) -> None:
            return None

        return getattr(hook, "__can_jump_to__", None)

    record(
        "@hook_config(can_jump_to=)",
        hook_config_side(ours_mw.hook_config),
        hook_config_side(theirs_mw.hook_config),
    )


def wrap_defaults_probes() -> None:
    from langchain.agents.middleware import AgentMiddleware as Theirs
    from langchain.agents.middleware import ModelRequest as TheirReq
    from reactivegraph.middleware import AgentMiddleware as Ours
    from reactivegraph.middleware import ModelRequest as OurReq

    def msg(fn: Callable[[], Any]) -> str:
        try:
            fn()
        except NotImplementedError as exc:
            return str(exc)
        return "NO ERROR"

    o, t = Ours(), Theirs()
    record(
        "wrap_model_call.sync_default_message",
        msg(lambda: o.wrap_model_call(OurReq(model="m", messages=[]), lambda r: None)),
        msg(lambda: t.wrap_model_call(TheirReq(model="m", messages=[]), lambda r: None)),
    )
    record(
        "wrap_tool_call.sync_default_message",
        msg(lambda: o.wrap_tool_call(None, lambda r: None)),
        msg(lambda: t.wrap_tool_call(None, lambda r: None)),
    )


def tool_call_request_probes() -> None:
    from langgraph.prebuilt.tool_node import ToolCallRequest as Theirs
    from reactivegraph.middleware import ToolCallRequest as Ours

    o_fields = [f.name for f in dataclasses.fields(Ours)]
    t_fields = [f.name for f in dataclasses.fields(Theirs)]
    record("ToolCallRequest.field_order", o_fields, t_fields)

    o = Ours(tool_call={"name": "x", "args": {}, "id": "1"}, tool=None, state={}, runtime=None)  # type: ignore[arg-type]
    t = Theirs(tool_call={"name": "x", "args": {}, "id": "1"}, tool=None, state={}, runtime=None)  # type: ignore[arg-type]

    o2 = o.override(tool_call={"name": "y", "args": {}, "id": "1"})
    t2 = t.override(tool_call={"name": "y", "args": {}, "id": "1"})
    record("ToolCallRequest.override", o2.tool_call["name"], t2.tool_call["name"])
    record("ToolCallRequest.override_is_new", o2 is not o, t2 is not t)


def tool_runtime_probes() -> None:
    from langgraph.prebuilt.tool_node import ToolRuntime as Theirs
    from reactivegraph.middleware import ToolRuntime as Ours

    o_fields = [f.name for f in dataclasses.fields(Ours)]
    t_fields = [f.name for f in dataclasses.fields(Theirs)]
    record("ToolRuntime.field_order", o_fields, t_fields)

    sentinel = {
        "state": {},
        "context": None,
        "config": {},
        "stream_writer": None,
        "tool_call_id": "1",
        "store": None,
    }
    o = Ours(**sentinel)  # type: ignore[arg-type]
    t = Theirs(**sentinel)  # type: ignore[arg-type]
    record(
        "ToolRuntime.emit_output_delta_without_writer",
        o.emit_output_delta("d"),
        t.emit_output_delta("d"),
    )
    record("ToolRuntime.default_tools", o.tools, t.tools)
    record("ToolRuntime.default_execution_info", o.execution_info, t.execution_info)
    record("ToolRuntime.default_server_info", o.server_info, t.server_info)

    # The upstream writer lives on a module-level ContextVar that the tool
    # execution runtime binds per call.  Both sides must observe the same
    # "no writer -> no-op, bound writer -> forwarded, unbound -> no-op" cycle.
    import reactivegraph.middleware as our_mw
    from langgraph.pregel._tools import _tool_call_writer as their_writer_var

    our_seen: list[Any] = []
    their_seen: list[Any] = []

    def our_writer(delta: Any) -> None:
        our_seen.append(delta)

    def their_writer(delta: Any) -> None:
        their_seen.append(delta)

    with our_mw.tool_call_writer(our_writer):
        o.emit_output_delta("bound")
    token = their_writer_var.set(their_writer)
    try:
        t.emit_output_delta("bound")
    finally:
        their_writer_var.reset(token)
    record("ToolRuntime.emit_output_delta_bound", our_seen, their_seen)

    our_seen.clear()
    their_seen.clear()
    o.emit_output_delta("after")
    t.emit_output_delta("after")
    record("ToolRuntime.emit_output_delta_after_unbind", our_seen, their_seen)


def main() -> int:
    print(f"python={sys.version.split()[0]}")
    probes = (
        model_request_probes,
        state_probes,
        middleware_defaults_probes,
        decorator_probes,
        wrap_defaults_probes,
        tool_call_request_probes,
        tool_runtime_probes,
    )
    for probe in probes:
        print(f"\n== {probe.__name__} ==")
        probe()

    print(f"\nchecks={CHECKS} failures={len(FAILURES)}")
    if FAILURES:
        print("\n".join(FAILURES))
        return 1
    print(json.dumps({"checks": CHECKS, "failures": 0}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
