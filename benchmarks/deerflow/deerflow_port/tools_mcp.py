"""链路 4：tools + MCP（内置工具 + OpenAPI 发现的 MCP 式工具）。

对照 deer-flow v2 `tools/builtins/`（12 个内置工具：present_file / task_tool /
view_image 等任务型工具）与 `mcp/`（client + task_tool_caller——外部 MCP 服务
经工具暴露给 agent）：本模块的 Toolkit 同时注册内置 @tool 与 `from_openapi`
发现的 MCP 式工具；agent 图内 run_tool 任务按剧本（tool_call）分发调用，
工具结果回填 state 并由 answer 任务引用进最终输出。
"""

from __future__ import annotations

from typing import Any

from reactivechain import Toolkit, calculator, from_openapi, tool
from reactivegraph import GraphBuilder, ReactiveGraph


@tool
def search_docs(query: str) -> str:
    """本地文档检索（deer-flow builtins 检索类工具语义）。"""
    return f"docs:{query}"


class ToolsAgent:
    """工具集 + 挂 Driver 的 agent 图（host=None 时仅构造面/工具集）。"""

    def __init__(self, toolkit: Toolkit, graph: ReactiveGraph | None) -> None:
        self.toolkit = toolkit
        self._graph = graph

    def invoke(self, event: str, payload: dict[str, Any], **kw: Any) -> Any:
        assert self._graph is not None, "host=None 构造面无 invoke"
        return self._graph.invoke(event, payload, **kw)

    def stream(self, event: str, payload: dict[str, Any], **kw: Any) -> Any:
        assert self._graph is not None, "host=None 构造面无 stream"
        return self._graph.stream(event, payload, **kw)


def build_tools_agent(
    host,
    calls: dict[str, int] | None = None,
    openapi_spec: dict[str, Any] | None = None,
    base_url: str | None = None,
    graph_id: str = "df_tools",
) -> ToolsAgent:
    """构造带内置 + MCP 式工具的 agent。

    - `openapi_spec`/`base_url`：from_openapi 发现 MCP 式外部工具（deer-flow
      mcp client 语义）；缺省仅内置工具。
    - `calls`：可选计数表（tool/answer）。
    - 返回 `ToolsAgent`（.toolkit 供注册断言；.invoke 走真实 Driver）。
    """
    counters: dict[str, int] = calls if calls is not None else {}

    mcp_by_name: dict[str, Any] = {}
    if openapi_spec is not None and base_url is not None:
        mcp_by_name = {t.name: t for t in from_openapi(openapi_spec, base_url=base_url)}

    toolkit = Toolkit([calculator, search_docs, *mcp_by_name.values()])
    by_name = {t.name: t for t in toolkit}

    if host is None:
        return ToolsAgent(toolkit, None)

    def run_tool(s: dict[str, Any]) -> dict[str, Any]:
        counters["tool"] = counters.get("tool", 0) + 1
        step = s["tool_call"]
        t = by_name[step["name"]]
        res = t.invoke({"tool_call": {"id": "c", "name": step["name"],
                                      "arguments": step.get("arguments", {})}})
        return {"tool_result": res["tool_result"]}

    def answer(s: dict[str, Any]) -> dict[str, Any]:
        counters["answer"] = counters.get("answer", 0) + 1
        return {"out": f"answered-with:{s['tool_result']}"}

    def build(b: GraphBuilder) -> None:
        b.task("run_tool", fn=run_tool, kind="effect",
               on=("run",), reads=("tool_call",), writes=("tool_result",))
        b.task("answer", fn=answer, kind="pure",
               on=("run_tool:written",), reads=("tool_result",), writes=("out",))

    graph = ReactiveGraph.build(build, host=host, graph_id=graph_id)
    return ToolsAgent(toolkit, graph)