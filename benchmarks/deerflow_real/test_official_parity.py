"""Parity tests against the real bytedance/deer-flow factory.

The upstream checkout is an external, test-only fixture. If it is not present
locally, these tests are skipped rather than silently claiming parity.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from upstream_fixture import require_upstream_backend, upstream_available

HERE = Path(__file__).resolve().parent


@pytest.fixture(scope="module")
def upstream_env() -> dict[str, str]:
    if not upstream_available():
        pytest.skip("real bytedance/deer-flow checkout not available")
    return os.environ.copy()


def _run_upstream(code: str) -> Any:
    deerflow = require_upstream_backend()
    result = subprocess.run(
        [str(deerflow / ".venv" / "bin" / "python"), "-c", code],
        cwd=deerflow,
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "PYTHONPATH": str(deerflow / "packages" / "harness"),
        },
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.mark.skipif(not upstream_available(), reason="real deer-flow checkout not available")
def test_official_factory_minimal_turn_returns_assistant_message() -> None:
    code = """
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from deerflow.agents.factory import create_deerflow_agent
class M(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs): return self
g = create_deerflow_agent(M(responses=[AIMessage(id='official', content='done')]))
out = g.invoke({'messages':[{'role':'user','content':'hi'}]}, {'configurable':{'thread_id':'parity'}})
print(out['messages'][-1].content)
"""
    assert _run_upstream(code).strip() == "done"


@pytest.mark.skipif(not upstream_available(), reason="real deer-flow checkout not available")
def test_reactive_factory_matches_official_minimal_semantics() -> None:
    from deerflow_reactive import create_deerflow_agent

    class Model:
        def invoke(self, messages):
            return {"role": "assistant", "content": "done"}

    graph = create_deerflow_agent(Model())
    result = graph.invoke(
        {"messages": [{"role": "user", "content": "hi"}]},
        {"configurable": {"thread_id": "parity"}},
    )
    assert result["messages"][-1]["content"] == "done"
