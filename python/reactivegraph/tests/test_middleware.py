"""Middleware contract pinned against real ``langchain``/``langgraph``.

DeerFlow has 47 imports from ``langchain.agents.middleware`` and 19 from its
``types`` module.  These tests pin the observable contract rather than the
upstream implementation: request back-compat, hook decorators, middleware
metadata, and the tool request/runtime shapes consumed by the harness.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect

import pytest


class TestModelRequest:
    def test_constructor_defaults_match_upstream(self) -> None:
        from reactivegraph.middleware import ModelRequest

        request = ModelRequest(model="m", messages=[])
        assert request.model == "m"
        assert request.messages == []
        assert request.system_message is None
        assert request.tool_choice is None
        assert request.tools == []
        assert request.response_format is None
        assert request.state == {"messages": []}
        assert request.runtime is None
        assert request.model_settings == {}

    def test_system_prompt_is_an_alias_for_system_message(self) -> None:
        from reactivegraph.messages import SystemMessage
        from reactivegraph.middleware import ModelRequest

        request = ModelRequest(model="m", messages=[], system_prompt="be concise")
        assert isinstance(request.system_message, SystemMessage)
        assert request.system_message.content == "be concise"
        assert request.system_prompt == "be concise"

    def test_both_system_prompt_and_system_message_raise(self) -> None:
        from reactivegraph.messages import SystemMessage
        from reactivegraph.middleware import ModelRequest

        with pytest.raises(ValueError, match="Cannot specify both"):
            ModelRequest(
                model="m",
                messages=[],
                system_prompt="a",
                system_message=SystemMessage("b"),
            )

    def test_direct_assignment_warns_and_still_sets(self) -> None:
        from reactivegraph.middleware import ModelRequest

        request = ModelRequest(model="m", messages=[])
        with pytest.warns(DeprecationWarning, match="Direct attribute assignment"):
            request.messages = ["new"]
        assert request.messages == ["new"]

    def test_system_prompt_assignment_converts_to_message(self) -> None:
        from reactivegraph.messages import SystemMessage
        from reactivegraph.middleware import ModelRequest

        request = ModelRequest(model="m", messages=[])
        with pytest.warns(DeprecationWarning, match="system_prompt"):
            request.system_prompt = "new prompt"
        assert isinstance(request.system_message, SystemMessage)
        assert request.system_message.content == "new prompt"

    def test_override_system_prompt_converts(self) -> None:
        from reactivegraph.messages import SystemMessage
        from reactivegraph.middleware import ModelRequest

        request = ModelRequest(model="m", messages=[])
        patched = request.override(system_prompt="new")
        assert isinstance(patched.system_message, SystemMessage)
        assert patched.system_message.content == "new"
        assert request.system_message is None

    def test_override_rejects_both_prompt_forms(self) -> None:
        from reactivegraph.messages import SystemMessage
        from reactivegraph.middleware import ModelRequest

        request = ModelRequest(model="m", messages=[])
        with pytest.raises(ValueError, match="Cannot specify both"):
            request.override(system_prompt="a", system_message=SystemMessage("b"))

    def test_dataclass_fields_match_upstream_order(self) -> None:
        from reactivegraph.middleware import ModelRequest

        assert [f.name for f in dataclasses.fields(ModelRequest)] == [
            "model",
            "messages",
            "system_message",
            "tool_choice",
            "tools",
            "response_format",
            "state",
            "runtime",
            "model_settings",
        ]


class TestResponses:
    def test_model_response_defaults(self) -> None:
        from reactivegraph.messages import AIMessage
        from reactivegraph.middleware import ModelResponse

        response = ModelResponse(result=[AIMessage("ok")])
        assert response.structured_response is None

    def test_extended_response_defaults(self) -> None:
        from reactivegraph.messages import AIMessage
        from reactivegraph.middleware import ExtendedModelResponse, ModelResponse

        extended = ExtendedModelResponse(model_response=ModelResponse([AIMessage("ok")]))
        assert extended.command is None

    def test_schema_omission_sentinels(self) -> None:
        from reactivegraph.middleware import (
            OmitFromInput,
            OmitFromOutput,
            OmitFromSchema,
            PrivateStateAttr,
        )

        assert OmitFromSchema() == OmitFromSchema(input=True, output=True)
        assert OmitFromInput == OmitFromSchema(input=True, output=False)
        assert OmitFromOutput == OmitFromSchema(input=False, output=True)
        assert PrivateStateAttr == OmitFromSchema(input=True, output=True)

    def test_agent_state_keys_match_upstream(self) -> None:
        from reactivegraph.middleware import AgentState, InputAgentState, OutputAgentState

        # Verified against langchain 1.3.14 with `from __future__ import annotations`:
        # upstream reports every AgentState key as required.
        assert AgentState.__required_keys__ == frozenset(
            {"messages", "jump_to", "structured_response"}
        )
        assert AgentState.__optional_keys__ == frozenset()
        assert InputAgentState.__required_keys__ == frozenset({"messages"})
        assert InputAgentState.__optional_keys__ == frozenset()
        assert OutputAgentState.__required_keys__ == frozenset({"messages", "structured_response"})
        assert OutputAgentState.__optional_keys__ == frozenset()


class TestAgentMiddleware:
    def test_defaults_match_upstream(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, AgentState

        middleware = AgentMiddleware()
        assert middleware.name == "AgentMiddleware"
        assert middleware.state_schema is not AgentState
        assert middleware.state_schema.__name__ == "_DefaultAgentState"
        assert middleware.transformers == ()
        assert not hasattr(AgentMiddleware, "tools")
        assert not hasattr(middleware, "tools")

    def test_default_hooks_return_none(self) -> None:
        from reactivegraph.middleware import AgentMiddleware

        middleware = AgentMiddleware()
        assert middleware.before_agent({"messages": []}, None) is None
        assert middleware.before_model({"messages": []}, None) is None
        assert middleware.after_model({"messages": []}, None) is None
        assert middleware.after_agent({"messages": []}, None) is None

    @pytest.mark.asyncio
    async def test_default_async_hooks_return_none(self) -> None:
        from reactivegraph.middleware import AgentMiddleware

        middleware = AgentMiddleware()
        assert await middleware.abefore_agent({"messages": []}, None) is None
        assert await middleware.abefore_model({"messages": []}, None) is None
        assert await middleware.aafter_model({"messages": []}, None) is None
        assert await middleware.aafter_agent({"messages": []}, None) is None

    def test_sync_wrap_defaults_fail_closed_with_upstream_message(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, ModelRequest

        middleware = AgentMiddleware()
        request = ModelRequest(model="m", messages=[])
        with pytest.raises(
            NotImplementedError, match="Synchronous implementation of wrap_model_call"
        ):
            middleware.wrap_model_call(request, lambda r: None)
        with pytest.raises(
            NotImplementedError, match="Synchronous implementation of wrap_tool_call"
        ):
            middleware.wrap_tool_call(None, lambda r: None)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_async_wrap_defaults_fail_closed_with_upstream_message(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, ModelRequest

        async def handler(_request: object) -> object:
            return object()

        middleware = AgentMiddleware()
        request = ModelRequest(model="m", messages=[])
        with pytest.raises(
            NotImplementedError, match="Asynchronous implementation of awrap_model_call"
        ):
            await middleware.awrap_model_call(request, handler)  # type: ignore[arg-type]
        with pytest.raises(
            NotImplementedError, match="Asynchronous implementation of awrap_tool_call"
        ):
            await middleware.awrap_tool_call(None, handler)  # type: ignore[arg-type]


class TestHookDecorators:
    def test_before_model_sync_creates_instance(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, AgentState, before_model

        @before_model
        def hook(state: AgentState, runtime: object) -> dict[str, int]:
            return {"n": 1}

        assert isinstance(hook, AgentMiddleware)
        assert type(hook).__name__ == "hook"
        assert hook.state_schema is AgentState
        assert hook.before_model({"messages": []}, None) == {"n": 1}  # type: ignore[arg-type]

    def test_before_model_async_sets_only_async_hook(self) -> None:
        from reactivegraph.middleware import AgentMiddleware, before_model

        @before_model(can_jump_to=["end"])
        async def hook(state: object, runtime: object) -> None:
            return None

        assert isinstance(hook, AgentMiddleware)
        assert inspect.iscoroutinefunction(hook.abefore_model)
        assert hook.before_model({"messages": []}, None) is None
        assert getattr(type(hook).abefore_model, "__can_jump_to__", None) == ["end"]

    @pytest.mark.asyncio
    async def test_before_model_async_runs(self) -> None:
        from reactivegraph.middleware import before_model

        @before_model
        async def hook(state: object, runtime: object) -> dict[str, str]:
            await asyncio.sleep(0)
            return {"ok": "yes"}

        assert await hook.abefore_model({"messages": []}, None) == {"ok": "yes"}

    def test_all_hook_decorator_names_and_state_schema(self) -> None:
        from reactivegraph.middleware import (
            after_agent,
            after_model,
            before_agent,
            before_model,
        )

        class CustomState(dict):
            pass

        for decorator, method_name in (
            (before_agent, "before_agent"),
            (before_model, "before_model"),
            (after_model, "after_model"),
            (after_agent, "after_agent"),
        ):
            def hook(state: object, runtime: object) -> dict[str, int]:
                return {"x": 1}

            made = decorator(hook, state_schema=CustomState, name="Named")
            assert made.state_schema is CustomState
            assert type(made).__name__ == "Named"
            assert getattr(made, method_name)({"messages": []}, None) == {"x": 1}

    def test_can_jump_to_metadata_from_hook_config(self) -> None:
        from reactivegraph.middleware import before_model, hook_config

        @before_model
        @hook_config(can_jump_to=["tools", "end"])
        def hook(state: object, runtime: object) -> None:
            return None

        assert getattr(type(hook).before_model, "__can_jump_to__", None) == ["tools", "end"]

    def test_hook_config_without_destinations_adds_no_metadata(self) -> None:
        from reactivegraph.middleware import hook_config

        @hook_config()
        def hook() -> None:
            return None

        assert not hasattr(hook, "__can_jump_to__")

    def test_dynamic_prompt_sync_and_async_paths(self) -> None:
        from reactivegraph.messages import AIMessage, SystemMessage
        from reactivegraph.middleware import ModelRequest, ModelResponse, dynamic_prompt

        @dynamic_prompt
        def sync_prompt(request: ModelRequest) -> SystemMessage:
            return SystemMessage("sync prompt")

        seen: list[ModelRequest] = []

        def sync_handler(request: ModelRequest) -> ModelResponse:
            seen.append(request)
            return ModelResponse([AIMessage("ok")])

        assert sync_prompt.wrap_model_call(ModelRequest(model="m", messages=[]), sync_handler)
        assert seen[0].system_message is not None
        assert seen[0].system_message.content == "sync prompt"

        @dynamic_prompt
        async def async_prompt(request: ModelRequest) -> str:
            return "async prompt"

        async def main() -> None:
            captured: list[ModelRequest] = []

            async def async_handler(request: ModelRequest) -> ModelResponse:
                captured.append(request)
                return ModelResponse([AIMessage("ok")])

            await async_prompt.awrap_model_call(ModelRequest(model="m", messages=[]), async_handler)
            assert captured[0].system_message is not None
            assert captured[0].system_message.content == "async prompt"

        asyncio.run(main())

    def test_wrap_model_call_decorator_metadata(self) -> None:
        from reactivegraph.middleware import wrap_model_call

        @wrap_model_call(name="Wrapper")
        def hook(request: object, handler: object) -> object:
            return handler(request)  # type: ignore[operator]

        assert type(hook).__name__ == "Wrapper"
        assert hook.wrap_model_call(object(), lambda request: "ok") == "ok"

    def test_wrap_tool_call_decorator_sync_and_async(self) -> None:
        from reactivegraph.middleware import wrap_tool_call

        @wrap_tool_call
        def sync_hook(request: object, handler: object) -> object:
            return handler(request)  # type: ignore[operator]

        @wrap_tool_call
        async def async_hook(request: object, handler: object) -> object:
            return await handler(request)  # type: ignore[operator]

        assert sync_hook.wrap_tool_call("r", lambda request: "ok") == "ok"

        async def main() -> None:
            async def handler(request: object) -> object:
                return "async-ok"

            assert await async_hook.awrap_tool_call("r", handler) == "async-ok"

        asyncio.run(main())


class TestToolCallRequest:
    def test_override_returns_replacement(self) -> None:
        from reactivegraph.middleware import ToolCallRequest

        request = ToolCallRequest(
            tool_call={"name": "t", "args": {"x": 1}, "id": "c1"},
            tool=None,
            state={"messages": []},
            runtime=None,  # type: ignore[arg-type]
        )
        patched = request.override(state={"messages": ["new"]})
        assert patched is not request
        assert patched.state == {"messages": ["new"]}
        assert request.state == {"messages": []}

    def test_direct_assignment_warns_and_still_sets(self) -> None:
        from reactivegraph.middleware import ToolCallRequest

        request = ToolCallRequest(
            tool_call={"name": "t", "args": {}, "id": "c1"},
            tool=None,
            state={},
            runtime=None,  # type: ignore[arg-type]
        )
        with pytest.warns(DeprecationWarning, match="ToolCallRequest"):
            request.state = {"changed": True}
        assert request.state == {"changed": True}

    def test_fields_match_upstream(self) -> None:
        from reactivegraph.middleware import ToolCallRequest

        assert [f.name for f in dataclasses.fields(ToolCallRequest)] == [
            "tool_call",
            "tool",
            "state",
            "runtime",
        ]


class TestToolRuntime:
    def test_fields_and_defaults(self) -> None:
        from reactivegraph.middleware import ToolRuntime

        runtime = ToolRuntime(
            state={"messages": []},
            context={"user": "u"},
            config={"configurable": {}},
            stream_writer=None,
            tool_call_id="c1",
            store=None,
        )
        assert runtime.tools == []
        assert runtime.execution_info is None
        assert runtime.server_info is None

    def test_emit_output_delta_without_bound_writer_is_noop(self) -> None:
        from reactivegraph.middleware import ToolRuntime

        runtime = ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=lambda _delta: pytest.fail("instance writer must not be used"),
            tool_call_id="c1",
            store=None,
        )
        assert runtime.emit_output_delta("delta") is None

    def test_emit_output_delta_reads_per_tool_call_writer(self) -> None:
        from reactivegraph.middleware import ToolRuntime, tool_call_writer

        runtime = ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=None,
            tool_call_id="c1",
            store=None,
        )
        seen: list[object] = []
        with tool_call_writer(seen.append):
            runtime.emit_output_delta({"partial": True})
        assert seen == [{"partial": True}]

    def test_tool_call_writer_is_reset_after_block(self) -> None:
        from reactivegraph.middleware import ToolRuntime, tool_call_writer

        runtime = ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=None,
            tool_call_id="c1",
            store=None,
        )
        with tool_call_writer(lambda _delta: None):
            pass
        assert runtime.emit_output_delta("after") is None


class TestHostToolRuntimeInterop:
    """Hosts that validate an injected runtime against *their* ``ToolRuntime``.

    LangChain validates an injected ``runtime`` argument against the tool's
    declared annotation. ``langchain.tools.ToolRuntime`` is
    ``langgraph.prebuilt.tool_node.ToolRuntime``; when a host tool is declared
    against it and the engine injects our dataclass, pydantic raises
    ``ValidationError`` before the tool body runs. Subclassing the host class
    (when importable) keeps both the host validator and the host ``isinstance``
    checks true, exactly like ``Command``/``Overwrite``.
    """

    def test_engine_tool_runtime_is_a_host_tool_runtime(self) -> None:
        host = pytest.importorskip("langgraph.prebuilt.tool_node")
        from reactivegraph.middleware import ToolRuntime

        assert issubclass(ToolRuntime, host.ToolRuntime)
        assert isinstance(
            ToolRuntime(
                state={},
                context=None,
                config={},
                stream_writer=None,
                tool_call_id="c1",
                store=None,
            ),
            host.ToolRuntime,
        )

    def test_host_tool_runtime_is_accepted_by_a_host_tool_validator(self) -> None:
        """The measured DeerFlow failure: pydantic rejected our runtime."""
        host_tools = pytest.importorskip("langchain.tools")
        host_prebuilt = pytest.importorskip("langgraph.prebuilt.tool_node")
        from reactivegraph.middleware import ToolRuntime

        host_runtime = host_prebuilt.ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=None,
            tool_call_id="c1",
            store=None,
        )
        assert isinstance(host_runtime, ToolRuntime)

        def f(a: str, runtime: host_tools.ToolRuntime) -> str:
            """Doc.

            Args:
                a: the value.
            """
            return a

        f.__annotations__["runtime"] = host_tools.ToolRuntime
        tool = pytest.importorskip("langchain_core.tools").tool(parse_docstring=True)(f)
        # A host runtime instance must pass the tool's own args validator.
        assert tool.args_schema.model_validate({"a": "x", "runtime": host_runtime})

    def test_host_tool_runtime_satisfies_the_engine_annotation(self) -> None:
        """The reverse direction, needed by DeerFlow's own tests.

        ``tests/test_sandbox_middleware.py`` constructs the *host* runtime and
        passes it as the ``runtime`` argument of an engine-annotated tool; the
        tool's pydantic validator must accept it.
        """
        host_tools = pytest.importorskip("langchain.tools")
        from reactivegraph.middleware import ToolRuntime

        host_runtime = host_tools.ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=None,
            tool_call_id="c1",
            store=None,
        )
        assert isinstance(host_runtime, ToolRuntime)
        assert issubclass(host_tools.ToolRuntime, ToolRuntime)

    def test_engine_tool_runtime_fields_are_unchanged(self) -> None:
        from reactivegraph.middleware import ToolRuntime

        runtime = ToolRuntime(
            state={},
            context=None,
            config={},
            stream_writer=None,
            tool_call_id="c1",
            store=None,
        )
        assert runtime.tools == []
        assert runtime.execution_info is None
        assert runtime.server_info is None


class TestImportsWithoutLangChain:
    def test_middleware_module_has_no_module_scope_upstream_import(self) -> None:
        """The optional bridge must stay lazy.

        A plain ``"langchain" not in source`` scan cannot survive the bridge
        (which imports the host base inside a ``try``), so the real rule is
        pinned instead: no *module-scope* import of either package, which is
        what would make the engine require them.
        """
        import ast

        import reactivegraph.middleware as middleware

        tree = ast.parse(inspect.getsource(middleware))
        offenders: list[str] = []
        for node in tree.body:
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                if module.split(".")[0] in {"langchain", "langgraph"}:
                    offenders.append(module)
        assert offenders == []

    def test_middleware_module_imports_with_upstream_uninstalled(self) -> None:
        """The source scan above is a proxy; this is the behaviour it stands for.

        Both ``langchain`` and ``langgraph`` are blocked, so the optional
        identity bridge must fall back to the native hook definitions rather
        than turning them into a hard dependency.
        """
        import subprocess
        import sys
        import textwrap

        code = textwrap.dedent(
            """
            import importlib.abc, sys

            class _Blocker(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname.split(".")[0] in ("langchain", "langgraph"):
                        raise ImportError(fullname + " is not installed")
                    return None

            sys.meta_path.insert(0, _Blocker())

            from reactivegraph.middleware import AgentMiddleware

            class Hook(AgentMiddleware):
                def after_model(self, state, runtime):
                    return {"seen": True}

            assert Hook().after_model({}, None) == {"seen": True}
            print("OK")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert "OK" in result.stdout


# Upstream's ``create_agent`` decides whether a middleware participates in a
# chain with a *function identity* test against its own base:
#
#     m.__class__.wrap_tool_call is not AgentMiddleware.wrap_tool_call
#
# A middleware that inherits from ReactiveGraph's base therefore looks like it
# overrides every hook, and upstream calls the inherited stub, which raises
# ``NotImplementedError``. Two bridges fix that without giving up our own base
# class: our base adopts the host's hook *function objects* (so an unoverridden
# hook is identical), and the host base is registered as a virtual subclass (so
# middleware written against LangChain satisfies our ``issubclass`` checks).
_MIDDLEWARE_HOOKS = (
    "before_agent",
    "abefore_agent",
    "before_model",
    "abefore_model",
    "after_model",
    "aafter_model",
    "after_agent",
    "aafter_agent",
    "wrap_model_call",
    "awrap_model_call",
    "wrap_tool_call",
    "awrap_tool_call",
)


class TestHostMiddlewareInterop:
    def test_inherited_hooks_are_the_host_function_objects(self) -> None:
        host = pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.middleware import AgentMiddleware

        for hook in _MIDDLEWARE_HOOKS:
            assert getattr(AgentMiddleware, hook) is getattr(host.AgentMiddleware, hook), hook

    def test_host_subclasses_are_recognised_by_the_engine(self) -> None:
        host = pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.middleware import AgentMiddleware

        class HostMiddleware(host.AgentMiddleware):
            pass

        assert issubclass(HostMiddleware, AgentMiddleware)
        assert isinstance(HostMiddleware(), AgentMiddleware)

    def test_engine_base_is_a_real_subclass_of_the_host_base(self) -> None:
        host = pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.middleware import AgentMiddleware

        assert issubclass(AgentMiddleware, host.AgentMiddleware)
        assert isinstance(AgentMiddleware(), host.AgentMiddleware)

    def test_super_hook_reaches_a_monkeypatched_host_hook(self) -> None:
        """DeerFlow middleware calls ``super().abefore_agent``.

        The engine's base used to *copy* the host's hook function objects onto
        itself, which shadowed the host attribute. A host-side monkeypatch (and
        any host override installed later) then never reached ``super()``; the
        measured fork failure was
        ``assert None == {'delegated': True}`` in
        ``test_abefore_agent_delegates_to_super_when_not_acquiring``. Real
        inheritance leaves the host attribute in the lookup path.
        """
        host = pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.middleware import AgentMiddleware

        calls: list[tuple[object, object]] = []

        async def fake(self, state, runtime):
            calls.append((state, runtime))
            return {"delegated": True}

        original = host.AgentMiddleware.abefore_agent
        host.AgentMiddleware.abefore_agent = fake
        try:

            class Child(AgentMiddleware):
                async def abefore_agent(self, state, runtime):  # type: ignore[override]
                    return await super().abefore_agent(state, runtime)

            result = asyncio.run(Child().abefore_agent({}, None))
        finally:
            host.AgentMiddleware.abefore_agent = original

        assert result == {"delegated": True}
        assert calls == [({}, None)]

    def test_bridge_does_not_widen_sibling_subclasses(self) -> None:
        """Only the framework base is widened.

        If the bridge leaked into every subclass, ``isinstance(x, SomeSibling)``
        would be true for unrelated host middleware, and the harness's anchor
        resolution (``isinstance(m, anchor)``) would match the wrong object.
        """
        host = pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.middleware import AgentMiddleware

        class Ours(AgentMiddleware):
            pass

        class Sibling(AgentMiddleware):
            pass

        assert isinstance(Ours(), AgentMiddleware)
        assert not isinstance(Ours(), Sibling)
        assert not issubclass(Ours, Sibling)
        assert not issubclass(host.AgentMiddleware, Sibling)
        assert not issubclass(host.AgentMiddleware, Ours)

    def test_upstream_sees_inherited_hooks_as_not_overridden(self) -> None:
        host = pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.middleware import AgentMiddleware

        class Ours(AgentMiddleware):
            def after_model(self, state, runtime):  # type: ignore[override]
                return None

        middleware = Ours()
        for hook in _MIDDLEWARE_HOOKS:
            if hook == "after_model":
                continue
            assert getattr(type(middleware), hook) is getattr(host.AgentMiddleware, hook), hook
        assert type(middleware).after_model is not host.AgentMiddleware.after_model

    def test_engine_never_presents_a_stub_as_a_real_override(self) -> None:
        """The engine's own participation rule must survive the bridge."""
        pytest.importorskip("langchain.agents.middleware")
        from reactivegraph.create_agent import _overrides_hook
        from reactivegraph.middleware import AgentMiddleware

        class Plain(AgentMiddleware):
            pass

        class Hooked(AgentMiddleware):
            def wrap_tool_call(self, request, handler):  # type: ignore[override]
                return handler(request)

        assert _overrides_hook(Plain, "wrap_tool_call") is False
        assert _overrides_hook(Hooked, "wrap_tool_call") is True
