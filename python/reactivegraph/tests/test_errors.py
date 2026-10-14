"""Control-flow exceptions and error codes, pinned against real LangGraph.

Middleware catches ``GraphBubbleUp`` to let engine control flow (interrupts,
parent commands, drains) pass through untouched while still wrapping ordinary
tool/model failures. That only works if the hierarchy is exactly right:
``GraphInterrupt`` and ``ParentCommand`` are bubble-up signals, while
``InvalidUpdateError`` and ``GraphRecursionError`` are ordinary errors.
"""

from __future__ import annotations

import pytest

from reactivegraph.errors import (
    ErrorCode,
    GraphBubbleUp,
    GraphInterrupt,
    GraphRecursionError,
    InvalidUpdateError,
    ParentCommand,
    create_error_message,
)
from reactivegraph.types import Command


class TestHierarchy:
    def test_bubble_up_is_the_control_flow_root(self) -> None:
        assert issubclass(GraphInterrupt, GraphBubbleUp)
        assert issubclass(ParentCommand, GraphBubbleUp)

    def test_recursion_error_is_a_recursion_error(self) -> None:
        # Callers catch ``RecursionError`` to detect the step budget.
        assert issubclass(GraphRecursionError, RecursionError)
        assert not issubclass(GraphRecursionError, GraphBubbleUp)

    def test_invalid_update_is_not_control_flow(self) -> None:
        assert issubclass(InvalidUpdateError, Exception)
        assert not issubclass(InvalidUpdateError, GraphBubbleUp)

    def test_invalid_update_is_the_host_class_when_langgraph_is_installed(self) -> None:
        """Hosts catch their *own* ``InvalidUpdateError`` by identity.

        DeerFlow's ``checkpoint_patches`` test (and its callers) wrap reducer
        failures in ``pytest.raises(langgraph.errors.InvalidUpdateError)``; an
        unrelated engine class escapes that guard.
        """
        host_errors = pytest.importorskip("langgraph.errors")

        assert issubclass(InvalidUpdateError, host_errors.InvalidUpdateError)
        assert isinstance(InvalidUpdateError("boom"), host_errors.InvalidUpdateError)

    def test_bubble_up_family_matches_the_host_classes(self) -> None:
        """Host middleware catches its own ``GraphBubbleUp`` by identity.

        DeerFlow providers (guardrails, sanitization, llm-error handling) raise
        ``langgraph.errors.GraphBubbleUp`` from inside engine middleware hooks,
        which re-raise *our* ``GraphBubbleUp``. If the two are unrelated the
        signal is swallowed and the run continues as if nothing happened —
        measured as ~20 failures across the fork's middleware suites.
        """
        host_errors = pytest.importorskip("langgraph.errors")

        for name in (
            "GraphBubbleUp",
            "GraphInterrupt",
            "ParentCommand",
            "GraphRecursionError",
        ):
            ours = globals()[name]
            host = getattr(host_errors, name)
            # Identity, not merely subclassing: ``except Ours`` does *not*
            # catch a host instance, so the two spellings must name the same
            # object for middleware to re-raise the signal correctly.
            assert ours is host, name

    def test_bubble_up_is_not_caught_as_ordinary_value_error(self) -> None:
        assert not issubclass(GraphBubbleUp, ValueError)


class TestErrorCode:
    def test_values_match_upstream(self) -> None:
        assert ErrorCode.GRAPH_RECURSION_LIMIT.value == "GRAPH_RECURSION_LIMIT"
        assert ErrorCode.INVALID_CONCURRENT_GRAPH_UPDATE.value == "INVALID_CONCURRENT_GRAPH_UPDATE"
        assert ErrorCode.INVALID_GRAPH_NODE_RETURN_VALUE.value == "INVALID_GRAPH_NODE_RETURN_VALUE"
        assert ErrorCode.MULTIPLE_SUBGRAPHS.value == "MULTIPLE_SUBGRAPHS"
        assert ErrorCode.INVALID_CHAT_HISTORY.value == "INVALID_CHAT_HISTORY"

    def test_create_error_message_appends_code(self) -> None:
        message = create_error_message(
            message="Can receive only one Overwrite value per super-step.",
            error_code=ErrorCode.INVALID_CONCURRENT_GRAPH_UPDATE,
        )
        assert message.startswith("Can receive only one Overwrite value per super-step.\n")
        assert "INVALID_CONCURRENT_GRAPH_UPDATE" in message


class TestGraphInterrupt:
    def test_carries_interrupt_list(self) -> None:
        interrupt = GraphInterrupt([{"id": "i1", "value": {"q": "?"}}])
        assert interrupt.args == ([{"id": "i1", "value": {"q": "?"}}],)

    def test_defaults_to_empty(self) -> None:
        assert GraphInterrupt().args == ((),)


class TestParentCommand:
    def test_wraps_the_command(self) -> None:
        command = Command(goto="parent_node")
        error = ParentCommand(command)
        assert error.args == (command,)
