"""MCP adapter: JSON-RPC tool discovery and calls as ReactiveChain tools.

The adapter follows the industry-standard MCP transport shape (JSON-RPC 2.0
messages over HTTP) without requiring a third-party SDK. Tool metadata under
``_meta.reactivegraph`` maps directly to ReactiveGraph's kind/reads/writes/
receipt contract.
"""

from __future__ import annotations

import itertools
import json
import urllib.error
import urllib.request
from collections.abc import Sequence
from typing import Any

from .tools import BaseTool, ToolResult

__all__ = ("MCPClient", "MCPServer", "MCPTool", "from_mcp")


class MCPClient:
    """Minimal MCP JSON-RPC client with initialize/list/call."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 30.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialise the MCPClient."""
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout_s = timeout_s
        self._ids = itertools.count(1)
        self._next_id: int | None = None

    def _request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Issue one request and return its decoded payload."""
        request_id = next(self._ids)
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        req = urllib.request.Request(
            self.base_url,
            data=json.dumps(message).encode("utf-8"),
            headers={"Content-Type": "application/json", **self.headers},
            method="POST",
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(req, timeout=self.timeout_s) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:200]
            raise ValueError(f"MCP error: HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise ValueError(f"MCP request failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError("MCP error: non-JSON response") from exc
        if payload.get("error"):
            raise ValueError(f"MCP error: {payload['error']}")
        return payload.get("result")

    def initialize(self) -> Any:
        return self._request(
            "initialize",
            {
                "protocolVersion": "1.0",
                "capabilities": {},
                "clientInfo": {"name": "reactivechain", "version": "0.1.0"},
            },
        )

    def list_tools(self) -> list[dict[str, Any]]:
        return self._request("tools/list", {}).get("tools", [])

    def call(self, name: str, arguments: dict[str, Any]) -> Any:
        return self._request("tools/call", {"name": name, "arguments": arguments})


class MCPTool(BaseTool):
    """A discovered MCP tool mapped to ReactiveChain's native tool protocol."""

    def __init__(self, client: MCPClient, tool: dict[str, Any]) -> None:
        """Initialise the MCPTool."""
        self.client = client
        self.name = str(tool.get("name") or "")
        if not self.name:
            raise ValueError("MCP tool name must be non-empty")
        self.description = str(tool.get("description") or "")
        self.args_schema = tool.get("inputSchema") or {
            "type": "object",
            "properties": {},
            "required": [],
        }
        reactive = (tool.get("_meta") or {}).get("reactivegraph") or {}
        self.kind = str(reactive.get("kind") or "effect")
        self._reads = set(reactive.get("reads") or [])
        self._writes = set(reactive.get("writes") or [])
        self.receipt = bool(reactive.get("receipt", False))

    def _run(self, **kwargs: Any) -> Any:
        """Execute the tool call for one invocation."""
        result = self.client.call(self.name, kwargs)
        if isinstance(result, dict) and "structuredContent" in result:
            structured = dict(result["structuredContent"])
            receipt = structured.pop("receipt", None)
            artifact = dict(structured)
            if receipt is not None:
                artifact["receipt"] = receipt
            return ToolResult(
                content=_text_content(result),
                artifact=artifact,
                patches=[
                    {"path": [key], "operation": "set", "value": value}
                    for key, value in structured.items()
                ],
            )
        return result


def _text_content(result: Any) -> str:
    if not isinstance(result, dict):
        return str(result)
    parts = result.get("content") or []
    if not isinstance(parts, list):
        return ""
    return "".join(str(item.get("text", "")) for item in parts if isinstance(item, dict))


def from_mcp(
    base_url: str,
    *,
    timeout_s: float = 30.0,
    headers: dict[str, str] | None = None,
) -> list[BaseTool]:
    """Discover MCP tools and return native ReactiveChain tools."""
    client = MCPClient(base_url, timeout_s=timeout_s, headers=headers)
    client.initialize()
    return [MCPTool(client, tool) for tool in client.list_tools()]


class MCPServer:
    """Expose ReactiveChain tools as an MCP endpoint.

    The counterpart to :class:`MCPClient`: this answers ``initialize``,
    ``tools/list`` and ``tools/call`` over the same JSON-RPC 2.0 shape, so any
    MCP-speaking agent can drive ReactiveGraph tools.

    Tool metadata is mirrored back under ``_meta.reactivegraph`` — the same
    key :class:`MCPTool` reads — which means a round trip through the protocol
    preserves ``kind``/``reads``/``writes``/``receipt`` instead of flattening
    every tool into an opaque effect.
    """

    def __init__(self, tools: Sequence[BaseTool], *, name: str = "reactivechain") -> None:
        """Initialise the MCPServer with the tools to expose."""
        self.name = name
        self._tools: dict[str, BaseTool] = {}
        for tool in tools:
            tool_name = getattr(tool, "name", None)
            if not tool_name:
                raise ValueError("MCP-exposed tools must have a non-empty name")
            self._tools[str(tool_name)] = tool

    @property
    def tools(self) -> list[BaseTool]:
        """The exposed tools, in registration order."""
        return list(self._tools.values())

    def tool_manifest(self) -> list[dict[str, Any]]:
        """MCP ``tools/list`` payload entries for every exposed tool."""
        manifest: list[dict[str, Any]] = []
        for tool in self._tools.values():
            entry: dict[str, Any] = {
                "name": tool.name,
                "description": tool.description or "",
                "inputSchema": tool.args_schema
                or {"type": "object", "properties": {}, "required": []},
            }
            reactive = {
                "kind": getattr(tool, "kind", "effect"),
                "reads": sorted(getattr(tool, "reads", set()) or ()),
                "writes": sorted(getattr(tool, "writes", set()) or ()),
            }
            if getattr(tool, "receipt", False):
                reactive["receipt"] = True
            entry["_meta"] = {"reactivegraph": reactive}
            manifest.append(entry)
        return manifest

    def handle(self, message: Any) -> dict[str, Any] | None:
        """Answer one JSON-RPC 2.0 message.

        Returns the response envelope, or ``None`` for a notification (a
        request without an ``id``), which by protocol must not be answered.
        """
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return _mcp_error(None, -32600, "invalid request: expected JSON-RPC 2.0")
        method = message.get("method")
        request_id = message.get("id")
        is_notification = "id" not in message
        try:
            result = self._dispatch(str(method), message.get("params") or {})
        except KeyError as exc:
            return None if is_notification else _mcp_error(
                request_id, -32602, f"unknown tool: {exc.args[0]}"
            )
        except ValueError as exc:
            return None if is_notification else _mcp_error(request_id, -32602, str(exc))
        except Exception as exc:  # noqa: BLE001 - a tool failure is reported, not raised
            return None if is_notification else _mcp_error(
                request_id, -32603, f"{type(exc).__name__}: {exc}"
            )
        if is_notification:
            return None
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _dispatch(self, method: str, params: dict[str, Any]) -> Any:
        if method == "initialize":
            return {
                "protocolVersion": "1.0",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": self.name, "version": "0.1.0"},
            }
        if method in {"tools/list", "notifications/initialized"}:
            return {"tools": self.tool_manifest()}
        if method == "tools/call":
            name = params.get("name")
            if not name:
                raise ValueError("tools/call requires a tool name")
            tool = self._tools[str(name)]
            arguments = params.get("arguments") or {}
            if not isinstance(arguments, dict):
                raise ValueError("tools/call arguments must be an object")
            return _tool_result_to_mcp(tool, arguments)
        raise ValueError(f"unsupported method: {method}")


def _tool_result_to_mcp(tool: BaseTool, arguments: dict[str, Any]) -> dict[str, Any]:
    """Run *tool* and shape its output as an MCP ``tools/call`` result."""
    output = tool.invoke({"tool_call": {"id": "", "arguments": arguments}})
    structured: dict[str, Any] = {}
    text_parts: list[str] = []

    if isinstance(output, dict):
        if "tool_result" in output:
            payload = output["tool_result"]
            if isinstance(payload, dict):
                structured.update(payload)
            else:
                text_parts.append(str(payload))
        artifact = output.get("tool_artifact")
        if isinstance(artifact, dict):
            for key, value in artifact.items():
                if key != "receipt":
                    structured[key] = value
            if "receipt" in artifact:
                structured["receipt"] = artifact["receipt"]
        for key, value in output.items():
            if key not in {"tool_result", "tool_artifact"} and not str(key).startswith("__"):
                structured.setdefault(str(key), value)
    else:
        text_parts.append(str(output))

    if not text_parts:
        text_parts.append(json.dumps(structured, ensure_ascii=False, default=str))
    result: dict[str, Any] = {"content": [{"type": "text", "text": "".join(text_parts)}]}
    if structured:
        result["structuredContent"] = structured
    return result


def _mcp_error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
