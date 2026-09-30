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
from typing import Any

from .tools import BaseTool, ToolResult

__all__ = ("MCPClient", "MCPTool", "from_mcp")


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
