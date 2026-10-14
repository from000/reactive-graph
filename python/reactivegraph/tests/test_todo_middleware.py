"""``TodoListMiddleware``: the planning surface hosts subclass.

DeerFlow's ``TodoMiddleware`` subclasses LangChain's ``TodoListMiddleware`` and
reads its ``Todo`` type. Porting it is required to run the lead agent without
LangChain: the base class, the state schema, and the ``write_todos`` tool must
all come from the engine while preserving upstream behaviour.
"""

from __future__ import annotations

import asyncio

from reactivegraph.messages import AIMessage, HumanMessage, SystemMessage
from reactivegraph.middleware import AgentMiddleware, ModelRequest, ModelResponse
from reactivegraph.todo import (
    WRITE_TODOS_SYSTEM_PROMPT,
    WRITE_TODOS_TOOL_DESCRIPTION,
    PlanningState,
    Todo,
    TodoListMiddleware,
    WriteTodosInput,
    write_todos,
)


def test_middleware_is_an_agent_middleware_with_the_planning_state() -> None:
    mw = TodoListMiddleware()
    assert isinstance(mw, AgentMiddleware)
    assert mw.state_schema is PlanningState
    assert [tool.name for tool in mw.tools] == ["write_todos"]
    assert mw.system_prompt == WRITE_TODOS_SYSTEM_PROMPT
    assert mw.tool_description == WRITE_TODOS_TOOL_DESCRIPTION


def test_custom_prompts_are_honoured() -> None:
    mw = TodoListMiddleware(system_prompt="SYS", tool_description="DESC")
    assert mw.system_prompt == "SYS"
    assert mw.tools[0].description == "DESC"


def test_write_todos_tool_updates_state_and_emits_a_tool_message() -> None:
    todos: list[Todo] = [{"content": "a", "status": "in_progress"}]
    command = write_todos.func(todos, tool_call_id="c1") if write_todos.func else None
    assert command is not None
    assert command.update["todos"] == todos
    assert command.update["messages"][0].tool_call_id == "c1"


def test_wrap_model_call_appends_the_system_prompt() -> None:
    mw = TodoListMiddleware()
    seen: list[ModelRequest] = []

    def handler(request: ModelRequest) -> ModelResponse:
        seen.append(request)
        return ModelResponse(result=[AIMessage(content="ok")])

    request = ModelRequest(
        model=None, messages=[HumanMessage(content="hi")], system_message=None
    )
    mw.wrap_model_call(request, handler)
    assert seen[0].system_message is not None
    assert WRITE_TODOS_SYSTEM_PROMPT in seen[0].system_message.text


def test_wrap_model_call_appends_to_an_existing_system_message() -> None:
    mw = TodoListMiddleware(system_prompt="EXTRA")
    seen: list[ModelRequest] = []

    def handler(request: ModelRequest) -> ModelResponse:
        seen.append(request)
        return ModelResponse(result=[])

    request = ModelRequest(
        model=None,
        messages=[],
        system_message=SystemMessage(content="BASE"),
    )
    mw.wrap_model_call(request, handler)
    assert "BASE" in seen[0].system_message.text
    assert "EXTRA" in seen[0].system_message.text


def test_parallel_write_todos_calls_are_rejected() -> None:
    mw = TodoListMiddleware()
    state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "write_todos", "args": {}, "id": "c1", "type": "tool_call"},
                    {"name": "write_todos", "args": {}, "id": "c2", "type": "tool_call"},
                ],
            )
        ]
    }
    update = mw.after_model(state, None)
    assert update is not None
    assert [m.tool_call_id for m in update["messages"]] == ["c1", "c2"]
    assert all(m.status == "error" for m in update["messages"])


def test_a_single_write_todos_call_is_allowed() -> None:
    mw = TodoListMiddleware()
    state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "write_todos", "args": {}, "id": "c1", "type": "tool_call"}
                ],
            )
        ]
    }
    assert mw.after_model(state, None) is None


def test_async_hooks_match_sync() -> None:
    mw = TodoListMiddleware()
    state = {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "write_todos", "args": {}, "id": "c1", "type": "tool_call"},
                    {"name": "write_todos", "args": {}, "id": "c2", "type": "tool_call"},
                ],
            )
        ]
    }
    assert asyncio.run(mw.aafter_model(state, None)) is not None

    seen: list[ModelRequest] = []

    async def handler(request: ModelRequest) -> ModelResponse:
        seen.append(request)
        return ModelResponse(result=[])

    request = ModelRequest(model=None, messages=[], system_message=None)
    asyncio.run(mw.awrap_model_call(request, handler))
    assert seen[0].system_message is not None


def test_write_todos_input_schema_is_exposed() -> None:
    fields = WriteTodosInput.model_fields
    assert "todos" in fields


def test_planning_state_declares_todos() -> None:
    assert "todos" in PlanningState.__annotations__
