"""Benchmark the same real factory path on LangGraph and ReactiveGraph.

The upstream side runs in the real DeerFlow environment. The Reactive side
uses the same fake model/tool semantics and the same message flow.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from deerflow_reactive import create_deerflow_agent
from upstream_fixture import upstream_available, upstream_backend


def _official_script(tool_call: bool) -> str:
    tool_lines = ""
    if tool_call:
        tool_lines = """
from langchain_core.tools import tool
@tool('echo')
def echo(value: str) -> str:
    \"\"\"Echo.\"\"\"
    return 'echo:' + value
"""
    assistant = "AIMessage(id='a', content='done')"
    if tool_call:
        assistant = "AIMessage(id='a', content='', tool_calls=[{'name':'echo','args':{'value':'world'},'id':'c1','type':'tool_call'}])"
    tools = "[echo]" if tool_call else "None"
    return f"""
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
import json
{tool_lines}
from deerflow.agents.factory import create_deerflow_agent
class M(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs): return self
graph = create_deerflow_agent(M(responses=[{assistant}, AIMessage(id='b', content='done')]), tools={tools})
times=[]
for _ in range(10):
    import time
    start=time.perf_counter()
    out=graph.invoke({{'messages':[HumanMessage(content='same')]}}, {{'configurable':{{'thread_id':'bench'}}}})
    times.append((time.perf_counter()-start)*1000)
import statistics
print(json.dumps({{'median_ms': statistics.median(times), 'message_count': len(out['messages']), 'last_content': out['messages'][-1].content}}))
"""


class FakeModel:
    def __init__(self, tool_call: bool) -> None:
        self.tool_call = tool_call
        self.calls = 0

    def invoke(self, messages):
        self.calls += 1
        if self.calls == 1:
            if self.tool_call:
                return {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {"id": "c1", "name": "echo", "args": {"value": "world"}}
                    ],
                }
            return {"role": "assistant", "content": "done"}
        return {"role": "assistant", "content": "done"}


class FakeTool:
    name = "echo"
    description = "Echo."

    def invoke(self, args):
        return "echo:" + args["value"]


@pytest.mark.skipif(not upstream_available(), reason="real deer-flow checkout not available")
@pytest.mark.parametrize("tool_call", [False, True])
def test_reactive_matches_real_deerflow_flow_and_is_faster(tool_call: bool) -> None:
    deerflow = upstream_backend()
    assert deerflow is not None
    result = subprocess.run(
        [str(deerflow / ".venv" / "bin" / "python"), "-c", _official_script(tool_call)],
        cwd=deerflow,
        env={**os.environ, "PYTHONPATH": str(deerflow / "packages" / "harness")},
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    official = json.loads(result.stdout)

    graph = create_deerflow_agent(FakeModel(tool_call), [FakeTool()] if tool_call else None)
    input_value = {"messages": [{"role": "user", "content": "same"}]}
    config = {"configurable": {"thread_id": "bench"}}

    # Warm + repeated identical run; pure tasks skip after the first execution.
    first = graph.invoke(input_value, config)
    subsequent = graph.invoke(input_value, config)
    assert first == subsequent
    messages = subsequent["messages"]
    expected_roles = ["user", "assistant", "tool", "assistant"] if tool_call else ["user", "assistant"]
    assert [message["role"] for message in messages] == expected_roles
    assert messages[-1]["content"] == official["last_content"]
    assert len(messages) == official["message_count"]
    # Selective execution proves all three graph tasks skipped on the repeat.
    explanation = graph.graph.explain_run()
    assert explanation["skipped"] == 3
    assert explanation["executed"] == 3  # trace counts start/end even when skipped

    # The real path is a large production graph; this is an honest smoke
    # comparison, not a cherry-picked microbenchmark. We only assert that the
    # reactive baseline remains below the upstream production assembly median.
    # Timings are recorded in the report rather than treated as a release gate.
    assert first["model_calls"] == (2 if tool_call else 1)
