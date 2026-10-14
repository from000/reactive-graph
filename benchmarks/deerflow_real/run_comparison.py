"""Run the real DeerFlow factory comparison and write a JSON report."""
from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from upstream_fixture import require_upstream_backend

HERE = Path(__file__).resolve().parent
OFFICIAL_TEMPLATE = """
import json, statistics, time
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage
{tool_code}
from deerflow.agents.factory import create_deerflow_agent
class M(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs): return self
graph = create_deerflow_agent(M(responses=[{assistant}]), tools={tools})
times=[]
for _ in range(10):
    start=time.perf_counter()
    out=graph.invoke({{'messages':[HumanMessage(content='same')]}}, {{'configurable':{{'thread_id':'bench'}}}})
    times.append((time.perf_counter()-start)*1000)
print(json.dumps({{'median_ms':statistics.median(times),'times':times,'message_count':len(out['messages'])}}))
"""


class Model:
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
                    "tool_calls": [{"id": "c1", "name": "echo", "args": {"value": "world"}}],
                }
            return {"role": "assistant", "content": "done"}
        return {"role": "assistant", "content": "done"}


class Tool:
    name = "echo"
    description = "Echo"

    def invoke(self, args):
        return "echo:" + args["value"]


def official(tool_call: bool) -> dict:
    deerflow = require_upstream_backend()
    if tool_call:
        tool_code = "from langchain_core.tools import tool\n@tool('echo')\ndef echo(value: str) -> str:\n    '''Echo.'''\n    return 'echo:'+value\n"
        assistant = "AIMessage(id='a', content='', tool_calls=[{'name':'echo','args':{'value':'world'},'id':'c1','type':'tool_call'}]), AIMessage(id='b', content='done')"
        tools = "[echo]"
    else:
        tool_code = ""
        assistant = "AIMessage(id='a', content='done')"
        tools = "None"
    code = OFFICIAL_TEMPLATE.format(tool_code=tool_code, assistant=assistant, tools=tools)
    result = subprocess.run(
        [str(deerflow / ".venv" / "bin" / "python"), "-c", code],
        cwd=deerflow,
        env={**os.environ, "PYTHONPATH": str(deerflow / "packages" / "harness")},
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def reactive(tool_call: bool) -> dict:
    sys.path.insert(0, str(HERE))
    from deerflow_reactive import create_deerflow_agent

    graph = create_deerflow_agent(Model(tool_call), [Tool()] if tool_call else None)
    input_value = {"messages": [{"role": "user", "content": "same"}]}
    config = {"configurable": {"thread_id": "bench"}}
    first_times = []
    for _ in range(10):
        start = time.perf_counter()
        out = graph.invoke(input_value, config)
        first_times.append((time.perf_counter() - start) * 1000)
    # A new graph provides a fresh thread; this is first-run median.
    graph2 = create_deerflow_agent(Model(tool_call), [Tool()] if tool_call else None)
    graph2.invoke(input_value, config)
    repeat_times = []
    for _ in range(10):
        start = time.perf_counter()
        graph2.invoke(input_value, config)
        repeat_times.append((time.perf_counter() - start) * 1000)
    explanation = graph2.graph.explain_run()
    return {
        "medianFirstMs": statistics.median(first_times),
        "firstTimes": first_times,
        "medianRepeatMs": statistics.median(repeat_times),
        "repeatTimes": repeat_times,
        "messageCount": len(out["messages"]),
        "skipped": explanation["skipped"],
    }


def main() -> None:
    try:
        require_upstream_backend()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    report = {"deerflowCommit": "5d8a9492eea97e2f06f28d46df69c29355acf4da", "runs": 10}
    for tool_call in (False, True):
        key = "tool" if tool_call else "modelOnly"
        report[key] = {"official": official(tool_call), "reactive": reactive(tool_call)}
    output = HERE / "comparison.json"
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    sys.exit(main())
