"""OpenAPI 适配器测试：spec → 工具段（生态独立，纯 stdlib，本地 mock 服务）。"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from reactivechain import BaseTool
from reactivechain.openapi import from_openapi

SPEC: dict = {
    "openapi": "3.0.0",
    "info": {"title": "demo", "version": "1.0.0"},
    "paths": {
        "/items/{item_id}": {
            "get": {
                "operationId": "get_item",
                "summary": "获取条目",
                "parameters": [
                    {
                        "name": "item_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    },
                    {"name": "verbose", "in": "query", "schema": {"type": "boolean"}},
                ],
                "responses": {"200": {"description": "ok"}},
            }
        },
        "/items": {
            "post": {
                "operationId": "create_item",
                "summary": "创建条目",
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"name": {"type": "string"}},
                                "required": ["name"],
                            }
                        }
                    },
                },
            }
        },
    },
}


class _Handler(BaseHTTPRequestHandler):
    seen: list[dict] = []

    def log_message(self, *args):  # silence
        pass

    def do_GET(self) -> None:  # noqa: N802
        # /items/{id}?verbose=..
        parts = self.path.split("?", 1)
        path = parts[0]
        query = {}
        if len(parts) > 1:
            for kv in parts[1].split("&"):
                k, _, v = kv.partition("=")
                query[k] = v
        item_id = path.rsplit("/", 1)[-1]
        body = {"id": int(item_id), "verbose": query.get("verbose") == "true"}
        self._json(200, body)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers["Content-Length"])
        data = json.loads(self.rfile.read(length).decode("utf-8"))
        type(self).seen.append(data)
        self._json(201, {"created": data.get("name")})

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture(scope="module")
def api_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_from_openapi_creates_tools(api_server) -> None:
    tools = from_openapi(SPEC, base_url=api_server)
    assert len(tools) == 2
    by_name = {t.name: t for t in tools}
    assert set(by_name) == {"get_item", "create_item"}

    get_item = by_name["get_item"]
    schema = get_item.to_schema()["function"]["parameters"]
    assert set(schema["properties"]) == {"item_id", "verbose"}
    assert schema["required"] == ["item_id"]  # path 参数必填；query 默认可选
    assert schema["properties"]["item_id"]["type"] == "integer"


def test_openapi_get_invoke(api_server) -> None:
    tools = {t.name: t for t in from_openapi(SPEC, base_url=api_server)}
    out = tools["get_item"].invoke(
        {"tool_call": {"id": "c1", "arguments": {"item_id": 7, "verbose": True}}}
    )
    assert out["tool_result"] == {"id": 7, "verbose": True}
    assert out["tool_call_id"] == "c1"


def test_openapi_post_invoke(api_server) -> None:
    _Handler.seen.clear()
    tools = {t.name: t for t in from_openapi(SPEC, base_url=api_server)}
    out = tools["create_item"].invoke(
        {"tool_call": {"id": "c2", "arguments": {"name": "书"}}}
    )
    assert out["tool_result"] == {"created": "书"}
    assert _Handler.seen[-1] == {"name": "书"}  # POST body 原样传递


def test_from_openapi_rejects_invalid_spec() -> None:
    with pytest.raises(ValueError, match="OpenAPI"):
        from_openapi({}, base_url="http://x")  # type: ignore[arg-type]


def test_tools_are_basetool_instances() -> None:
    tools = from_openapi(SPEC, base_url="http://127.0.0.1:1")
    assert all(isinstance(t, BaseTool) for t in tools)


def test_openapi_auth_header_is_sent() -> None:
    class _AuthHandler(_Handler):
        seen_headers: list[str] = []

        def do_GET(self) -> None:
            type(self).seen_headers.append(self.headers.get("Authorization", ""))
            super().do_GET()

    server = ThreadingHTTPServer(("127.0.0.1", 0), _AuthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        tools = from_openapi(SPEC, base_url=base, headers={"Authorization": "Bearer test"})
        tool = tools[0]
        tool.invoke({"tool_call": {"id": "c1", "arguments": {"item_id": 1}}})
        assert _AuthHandler.seen_headers[-1] == "Bearer test"
    finally:
        server.shutdown()
        thread.join()


def test_openapi_error_becomes_agent_recoverable_string() -> None:
    class _ErrorHandler(_Handler):
        def do_GET(self) -> None:
            self._json(403, {"error": "denied"})

    server = ThreadingHTTPServer(("127.0.0.1", 0), _ErrorHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        tool = from_openapi(SPEC, base_url=base)[0]
        output = tool.invoke({"tool_call": {"id": "c1", "arguments": {"item_id": 1}}})
        assert output["tool_result"].startswith("Error: ValueError: HTTP 403")
    finally:
        server.shutdown()
        thread.join()
