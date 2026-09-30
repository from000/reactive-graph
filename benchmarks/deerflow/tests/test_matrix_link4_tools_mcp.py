"""链路 4：tools + MCP（内置工具 + OpenAPI 发现的 MCP 式工具）。

对照 deer-flow v2 `tools/builtins/`（12 内置工具）与 `mcp/`（client +
task_tool_caller——外部服务经工具暴露给 agent）：agent 图内含 run_tool 任务，
按剧本分发调用 Toolkit（内置 @tool + from_openapi 发现的 MCP 式工具），
工具结果回填 state 并影响最终输出。
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from deerflow_port.tools_mcp import build_tools_agent
from reactivegraph import DriverHost

_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "demo", "version": "1"},
    "paths": {
        "/items": {
            "get": {
                "operationId": "get_item",
                "responses": {"200": {"description": "ok"}},
            }
        }
    },
}


class _ApiHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — stdlib 命名
        body = b'{"item": "book"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:  # noqa: ANN401
        pass


@pytest.fixture
def mcp_server():
    """本地 mock OpenAPI server（模拟 MCP 式外部服务）。"""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ApiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()


def test_link4_toolkit_registers_builtin_and_mcp(mcp_server: str) -> None:
    """Toolkit 注册内置 @tool + from_openapi 发现的 MCP 式工具（共 3 个）。"""
    tk = build_tools_agent(None, calls={}, graph_id="df_tools_dry",
                           openapi_spec=_SPEC, base_url=mcp_server).toolkit
    names = {t.name for t in tk}
    assert names == {"calculator", "search_docs", "get_item"}


def test_link4_agent_calls_builtin_tool(df_host: DriverHost, mcp_server: str) -> None:
    """agent 任务调用内置工具，结果回填 state 并影响最终输出。"""
    calls: dict[str, int] = {}
    g = build_tools_agent(df_host, calls=calls, graph_id="df_tools_1",
                          openapi_spec=_SPEC, base_url=mcp_server)
    out = g.invoke("run", {"tool_call": {"name": "calculator",
                                         "arguments": {"expression": "1+2*3"}}})
    assert out["out"].startswith("answered-with:")
    assert "7" in out["out"]
    assert calls["tool"] == 1 and calls["answer"] == 1


def test_link4_agent_calls_mcp_tool(df_host: DriverHost, mcp_server: str) -> None:
    """agent 任务调用 MCP 式工具（from_openapi 发现），外部服务结果进入输出。"""
    calls: dict[str, int] = {}
    g = build_tools_agent(df_host, calls=calls, graph_id="df_tools_2",
                          openapi_spec=_SPEC, base_url=mcp_server)
    out = g.invoke("run", {"tool_call": {"name": "get_item", "arguments": {}}})
    assert "book" in out["out"]