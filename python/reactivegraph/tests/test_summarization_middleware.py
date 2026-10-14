"""``SummarizationMiddleware``: the compaction surface hosts subclass.

DeerFlow's ``DeerFlowSummarizationMiddleware`` subclasses this to add
pre-compression hooks and multi-model fallback. The parent used to come from
``langchain.agents.middleware``, which imports LangGraph. The engine now owns
it, and these tests pin both the surface DeerFlow consumes and behavioural
parity with upstream.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from reactivegraph.messages import AIMessage, HumanMessage, ToolMessage
from reactivegraph.middleware import AgentMiddleware
from reactivegraph.summarization import (
    DEFAULT_SUMMARY_PROMPT,
    SummarizationMiddleware,
)


class _Model:
    """Minimal chat model: enough for summarization and profile inspection."""

    _llm_type = "fake"
    profile = None

    def __init__(self) -> None:
        self.calls: list[str] = []

    def invoke(self, prompt, config=None):
        self.calls.append(prompt if isinstance(prompt, str) else str(prompt))
        return AIMessage(content="SUMMARY")

    async def ainvoke(self, prompt, config=None):
        return self.invoke(prompt, config)

    def with_retry(self, **kwargs):
        return self


def _messages(count: int) -> list:
    return [
        HumanMessage(content=f"m{i}") if i % 2 == 0 else AIMessage(content=f"m{i}")
        for i in range(count)
    ]


def test_defaults_match_upstream() -> None:
    mw = SummarizationMiddleware(_Model())
    assert mw.keep == ("messages", 20)
    assert mw.trim_tokens_to_summarize == 4000
    assert mw.summary_prompt == DEFAULT_SUMMARY_PROMPT
    assert isinstance(mw, AgentMiddleware)
    assert mw.trigger is None


def test_deprecated_parameters_still_map_onto_the_new_ones() -> None:
    with pytest.warns(DeprecationWarning):
        mw = SummarizationMiddleware(_Model(), max_tokens_before_summary=1234)
    assert mw.trigger == ("tokens", 1234)

    with pytest.warns(DeprecationWarning):
        mw2 = SummarizationMiddleware(_Model(), messages_to_keep=5)
    assert mw2.keep == ("messages", 5)


def test_no_trigger_means_no_summarization() -> None:
    mw = SummarizationMiddleware(_Model())
    assert mw.before_model({"messages": _messages(50)}, None) is None


def test_message_count_trigger_summarizes_and_keeps_the_tail() -> None:
    model = _Model()
    mw = SummarizationMiddleware(model, trigger=("messages", 10), keep=("messages", 4))
    update = mw.before_model({"messages": _messages(12)}, None)
    assert update is not None
    assert model.calls, "the summary model must have been invoked"
    messages = update["messages"]
    assert messages[0].__class__.__name__ == "RemoveMessage"
    assert "summary of the conversation" in messages[1].content
    assert len(messages) == 6  # remove-all + summary + 4 preserved


def test_cutoff_does_not_split_a_tool_call_pair() -> None:
    ai = AIMessage(
        content="",
        tool_calls=[{"name": "t", "args": {}, "id": "c1", "type": "tool_call"}],
    )
    tool = ToolMessage(content="r", tool_call_id="c1")
    messages = [HumanMessage(content="a"), ai, tool, HumanMessage(content="b")]
    mw = SummarizationMiddleware(_Model(), trigger=("messages", 1), keep=("messages", 2))
    assert mw._determine_cutoff_index(messages) == 1


def test_invalid_trigger_config_fails_closed() -> None:
    with pytest.raises(ValueError):
        SummarizationMiddleware(_Model(), trigger=("bogus", 1))
    with pytest.raises(ValueError):
        SummarizationMiddleware(_Model(), trigger=("messages", 0))
    with pytest.raises(ValueError):
        SummarizationMiddleware(_Model(), keep=("fraction", 2.0))
    with pytest.raises(ValueError):
        SummarizationMiddleware(_Model(), trigger={"tokens": True})


def test_fractional_limits_require_a_model_profile() -> None:
    with pytest.raises(ValueError, match="Model profile information is required"):
        SummarizationMiddleware(_Model(), trigger=("fraction", 0.5))


async def test_async_summarization_matches_sync() -> None:
    mw = SummarizationMiddleware(_Model(), trigger=("messages", 5), keep=("messages", 2))
    update = await mw.abefore_model({"messages": _messages(8)}, None)
    assert update is not None
    assert len(update["messages"]) == 4


def test_message_ids_are_backfilled() -> None:
    messages = _messages(3)
    assert all(m.id is None for m in messages)
    SummarizationMiddleware._ensure_message_ids(messages)
    assert all(m.id is not None for m in messages)
    assert len({m.id for m in messages}) == 3


def test_differential_parity_with_upstream() -> None:
    """Same inputs, same cutoff/trigger decisions as LangChain's class."""
    upstream = pytest.importorskip(
        "langchain.agents.middleware.summarization",
        reason="langchain is the optional comparison target",
    )
    from langchain_core import messages as lc_messages

    ours = SummarizationMiddleware(_Model(), trigger=("messages", 10), keep=("messages", 4))
    theirs = upstream.SummarizationMiddleware(
        _Model(), trigger=("messages", 10), keep=("messages", 4)
    )
    assert ours.keep == theirs.keep
    assert ours.trim_tokens_to_summarize == theirs.trim_tokens_to_summarize
    assert ours.summary_prompt == theirs.summary_prompt

    # Feed the *same* message objects to both implementations: the engine must
    # interoperate with langchain_core messages, not only its native ones.
    shared_msgs = [
        lc_messages.HumanMessage(content=f"m{i}")
        if i % 2 == 0
        else lc_messages.AIMessage(content=f"m{i}")
        for i in range(30)
    ]
    assert ours._determine_cutoff_index(shared_msgs) == theirs._determine_cutoff_index(
        shared_msgs
    )
    assert ours._should_summarize(shared_msgs, 10) == theirs._should_summarize(shared_msgs, 10)
    ours_to_summarize, ours_preserved = ours._partition_messages(shared_msgs, 7)
    theirs_to_summarize, theirs_preserved = theirs._partition_messages(shared_msgs, 7)
    assert ours_to_summarize is not None and theirs_to_summarize is not None
    assert ours_preserved == theirs_preserved
    assert ours_preserved is not None and len(ours_preserved) == 23
    assert [m.content for m in ours_preserved] == [m.content for m in shared_msgs[7:]]


def test_imports_without_langchain_or_langgraph() -> None:
    code = textwrap.dedent(
        """
        import importlib.abc, sys

        class _Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split(".")[0] in {"langchain", "langchain_core", "langgraph"}:
                    raise ImportError(f"{fullname} is not installed")
                return None

        sys.meta_path.insert(0, _Blocker())
        import reactivegraph
        from reactivegraph.summarization import SummarizationMiddleware, DEFAULT_SUMMARY_PROMPT
        assert reactivegraph.__version__
        assert "messages" in DEFAULT_SUMMARY_PROMPT
        print("OK")
        """
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-3000:]
    assert "OK" in result.stdout
