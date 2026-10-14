"""MCPServer exposes ReactiveChain tools over MCP JSON-RPC.

The round trip is the point: an MCP client discovering tools from our server
must see the same ``kind``/``reads``/``writes``/``receipt`` metadata that
``MCPTool`` reads back, so a ReactiveGraph tool does not degrade into an opaque
effect just because it travelled through the protocol.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from reactivechain.mcp import MCPClient, MCPServer, MCPTool
from reactivechain.tools import tool


@tool(reads={"query"}, writes={"answer"}, kind="pure", receipt=False)
def lookup(query: str) -> str:
    """Look something up."""
    return f"found:{query}"


@tool(reads={"x"}, writes={"y"}, kind="effect")
def bump(x: int) -> int:
    """Increment a value."""
    return x + 1


class TestServerProtocol:
    def test_initialize_reports_capabilities(self) -> None:
        server = MCPServer([lookup])
        response = server.handle(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        )
        assert response is not None and "result" in response
        assert response["result"]["serverInfo"]["name"] == "reactivechain"
        assert "tools" in response["result"]["capabilities"]

    def test_tools_list_returns_manifest(self) -> None:
        server = MCPServer([lookup, bump])
        response = server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        names = [t["name"] for t in response["result"]["tools"]]
        assert names == ["lookup", "bump"]

    def test_manifest_preserves_reactive_metadata(self) -> None:
        server = MCPServer([lookup])
        entry = server.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})[
            "result"
        ]["tools"][0]
        meta = entry["_meta"]["reactivegraph"]
        assert meta["kind"] == "pure"
        assert meta["reads"] == ["query"]
        assert meta["writes"] == ["answer"]

    def test_tools_call_executes_the_tool(self) -> None:
        server = MCPServer([lookup])
        response = server.handle(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "lookup", "arguments": {"query": "rust"}},
            }
        )
        result = response["result"]
        assert "found:rust" in result["content"][0]["text"]

    def test_unknown_tool_is_an_error_not_an_exception(self) -> None:
        server = MCPServer([lookup])
        response = server.handle(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "nope", "arguments": {}},
            }
        )
        assert "error" in response
        assert "nope" in response["error"]["message"]

    def test_notification_is_not_answered(self) -> None:
        server = MCPServer([lookup])
        # No "id" -> JSON-RPC notification, must not produce a response.
        assert server.handle({"jsonrpc": "2.0", "method": "tools/list"}) is None

    def test_invalid_envelope_is_rejected(self) -> None:
        server = MCPServer([lookup])
        response = server.handle({"id": 1, "method": "tools/list"})
        assert response["error"]["code"] == -32600


class _RpcHandler(BaseHTTPRequestHandler):
    server_instance: MCPServer

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("Content-Length", "0"))
        message = json.loads(self.rfile.read(length) or b"{}")
        response = self.server_instance.handle(message)
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, *args: Any) -> None:
        return


class TestEndToEndRoundTrip:
    """Our server answered by our own client, over real HTTP."""

    def test_discover_and_call_through_http(self) -> None:
        server_impl = MCPServer([lookup, bump])

        class Handler(_RpcHandler):
            server_instance = server_impl

        httpd = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            client = MCPClient(f"http://127.0.0.1:{httpd.server_port}/mcp")
            tools = client.list_tools()
            assert {t["name"] for t in tools} == {"lookup", "bump"}

            # Metadata survives the trip: MCPTool reads _meta.reactivegraph.
            discovered = {t["name"]: MCPTool(client, t) for t in tools}
            assert discovered["lookup"].kind == "pure"
            assert discovered["lookup"].reads == {"query"}
            assert discovered["bump"].kind == "effect"

            result = client.call("lookup", {"query": "python"})
            assert "found:python" in result["content"][0]["text"]
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)
