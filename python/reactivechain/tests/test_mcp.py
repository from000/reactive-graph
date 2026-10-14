"""MCP adapter tests (protocol-shaped, no third-party dependency)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from reactivechain.mcp import MCPClient, from_mcp


class _MCPHandler(BaseHTTPRequestHandler):
    server_version = "MCP/1.0"

    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        message = json.loads(raw or b"{}")
        if message.get("method") == "initialize":
            result = {
                "protocolVersion": "1.0",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "mock", "version": "1.0"},
            }
        elif message.get("method") == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "search",
                        "description": "Search documents",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"query": {"type": "string"}},
                            "required": ["query"],
                        },
                        "_meta": {
                            "reactivegraph": {
                                "kind": "effect",
                                "reads": ["query"],
                                "writes": ["results"],
                                "receipt": True,
                            }
                        },
                    }
                ]
            }
        elif message.get("method") == "tools/call":
            if message.get("params", {}).get("name") != "search":
                self.send_response(404)
                self.end_headers()
                return
            args = message.get("params", {}).get("arguments", {})
            result = {
                "content": [{"type": "text", "text": f"found:{args.get('query', '')}"}],
                "structuredContent": {
                    "results": [f"found:{args.get('query', '')}"],
                    "receipt": f"mcp:{args.get('query', '')}",
                },
            }
        else:
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps({"jsonrpc": "2.0", "id": message.get("id"), "result": result})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload.encode())

    def log_message(self, *args: Any) -> None:
        return


@pytest.fixture()
def mcp_server() -> str:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _MCPHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()


def test_from_mcp_imports_reactive_tool_specs(mcp_server: str) -> None:
    tools = from_mcp(mcp_server)
    assert len(tools) == 1
    tool = tools[0]
    assert tool.name == "search"
    assert tool.description == "Search documents"
    assert tool.args_schema == {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
    }
    assert tool.kind == "effect"
    assert tool.reads == {"query"}
    assert tool.writes == {"results"}
    assert tool.receipt is True


def test_mcp_tool_call_maps_content_and_artifact(mcp_server: str) -> None:
    (tool,) = from_mcp(mcp_server)
    output = tool.invoke({"tool_call": {"id": "c1", "arguments": {"query": "rgp"}}})
    assert output["tool_result"] == "found:rgp"
    assert output["tool_artifact"] == {
        "results": ["found:rgp"],
        "receipt": "mcp:rgp",
    }


def test_mcp_client_rejects_protocol_error(mcp_server: str) -> None:
    client = MCPClient(mcp_server)
    with pytest.raises(ValueError, match="MCP error"):
        client.call("missing", {})


def test_mcp_client_sends_authorization_header() -> None:
    class _AuthMCPHandler(_MCPHandler):
        seen_auth: list[str] = []

        def do_POST(self) -> None:
            type(self).seen_auth.append(self.headers.get("Authorization", ""))
            super().do_POST()

    server = ThreadingHTTPServer(("127.0.0.1", 0), _AuthMCPHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        client = MCPClient(base, headers={"Authorization": "Bearer mcp-token"})
        client.initialize()
        assert _AuthMCPHandler.seen_auth[-1] == "Bearer mcp-token"
    finally:
        server.shutdown()
        thread.join()
