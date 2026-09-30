"""The engine must drive real langchain-core ecosystem objects.

`create_agent(model: Any)` is duck-typed and `ModelRequest.model` is
langchain-core's own `BaseChatModel`, so a `ChatOpenAI`/`ChatAnthropic`
instance should work exactly like a native fake. This file pins that as a
*tested* guarantee rather than an accident: the model below is the real
`langchain_openai.ChatOpenAI`, driven against a local HTTP server that speaks
the OpenAI chat-completions wire format.

Install with: `uv sync --extra langchain-ecosystem`.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

pytest.importorskip("langchain_openai", reason="needs the langchain-ecosystem extra")

from langchain_openai import ChatOpenAI  # noqa: E402

from reactivegraph import create_agent  # noqa: E402


class _CompletionsHandler(BaseHTTPRequestHandler):
    """Minimal OpenAI-compatible /chat/completions endpoint."""

    received: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        type(self).received.append(body)
        payload = {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 0,
            "model": body.get("model", "gpt-4o-mini"),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "echo-from-openai"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
        }
        encoded = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, *args: Any) -> None:  # silence test output
        return


@pytest.fixture()
def openai_compatible_server() -> Any:
    """A local server speaking the OpenAI wire format; yields its base URL."""
    _CompletionsHandler.received = []
    server = HTTPServer(("127.0.0.1", 0), _CompletionsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_create_agent_drives_real_chatopenai(openai_compatible_server: str) -> None:
    """A real ChatOpenAI instance drives a full agent turn on our engine."""
    model = ChatOpenAI(
        model="gpt-4o-mini",
        api_key="test-key",  # noqa: S106 - obvious placeholder for a local server
        base_url=openai_compatible_server,
    )

    agent = create_agent(model, tools=[])
    out = agent.invoke({"messages": [("user", "hi")]})

    messages = out.get("messages", [])
    assert messages, "agent returned no messages"
    assert messages[-1].content == "echo-from-openai"

    # The provider actually received a well-formed chat-completions request.
    assert _CompletionsHandler.received, "ChatOpenAI never called the endpoint"
    request = _CompletionsHandler.received[-1]
    assert request["model"] == "gpt-4o-mini"
    assert any(
        m.get("role") == "user" and m.get("content") == "hi"
        for m in request["messages"]
    ), f"user turn not forwarded: {request['messages']}"


def test_message_ids_survive_the_langchain_boundary(openai_compatible_server: str) -> None:
    """langchain-core message objects round-trip through the engine intact.

    `add_messages`-style id-based replacement is a LangGraph contract; a host
    that mixes langchain-core messages with engine state must keep observing it.
    """
    from langchain_core.messages import AIMessage, HumanMessage

    model = ChatOpenAI(
        model="gpt-4o-mini",
        api_key="test-key",  # noqa: S106
        base_url=openai_compatible_server,
    )
    agent = create_agent(model, tools=[])

    history = [
        HumanMessage(content="first", id="h1"),
        AIMessage(content="first-reply", id="a1"),
    ]
    out = agent.invoke({"messages": [*history, ("user", "second")]})

    ids = [m.id for m in out["messages"] if getattr(m, "id", None)]
    assert "h1" in ids and "a1" in ids, f"langchain-core ids lost: {ids}"
