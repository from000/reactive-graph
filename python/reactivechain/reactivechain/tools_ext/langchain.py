"""Optional adapter for LangChain-compatible tools.

This module is not imported by the ReactiveChain core. Install `langchain-core`
separately if you want to reuse an existing LangChain tool.
"""

from __future__ import annotations

from typing import Any

from reactivechain.tools import BaseTool, ToolResult


def from_langchain_tool(tool: Any) -> BaseTool:
    """Adapt a LangChain-compatible tool to the ReactiveChain tool protocol."""
    invoke = getattr(tool, "invoke", None)
    if not callable(invoke):
        raise TypeError("LangChain tool must expose callable invoke()")

    def invoke_args(args: dict[str, Any]) -> Any:
        return invoke(args)

    class _AdaptedTool(BaseTool):
        name = str(getattr(tool, "name", tool.__class__.__name__))
        description = str(getattr(tool, "description", ""))
        args_schema = getattr(tool, "args_schema", None)
        kind = "opaque"
        return_direct = bool(getattr(tool, "return_direct", False))

        def _run(self, **kwargs: Any) -> Any:
            result = invoke_args(kwargs)
            response_format = getattr(tool, "response_format", "content")
            if response_format == "content_and_artifact" and isinstance(result, tuple):
                content, artifact = result
                return ToolResult(content=content, artifact=artifact)
            return result

    return _AdaptedTool()


def from_langchain_message(message: Any) -> Any:
    """Adapt a LangChain-compatible message to ReactiveChain's native message.

    The adapter recognizes the common upstream `type` values (`human`, `ai`,
    `system`, `tool`) and maps tool call `args` to native `arguments`. It does
    not import or depend on the upstream package.
    """
    from reactivechain.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

    message_type = str(getattr(message, "type", getattr(message, "role", "")))
    content = getattr(message, "content", "")
    if message_type in ("human", "user"):
        return HumanMessage(str(content))
    if message_type in ("ai", "assistant"):
        tool_calls = [
            {
                "id": call.get("id", ""),
                "name": call.get("name", ""),
                "arguments": call.get("args", call.get("arguments", {})),
            }
            for call in getattr(message, "tool_calls", []) or []
            if isinstance(call, dict)
        ]
        return AIMessage(str(content), tool_calls=tool_calls)
    if message_type == "tool":
        return ToolMessage(str(content), tool_call_id=getattr(message, "tool_call_id", None))
    if message_type in ("system",):
        return SystemMessage(str(content))
    raise TypeError(f"unsupported LangChain message type: {message_type!r}")
