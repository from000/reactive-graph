"""M2：模型层测试——FakeLLM 回路 + OpenAICompatChatModel 本地 mock 端点。"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from reactivechain import ReactiveChainError
from reactivechain.llm import FakeLLM, OpenAICompatChatModel
from reactivechain.messages import AIMessage
from reactivechain.prompt import ChatPromptTemplate


class _Handler(BaseHTTPRequestHandler):
    """OpenAI 兼容 mock：/chat/completions，支持 stream（SSE）与 tool_calls。"""

    seen: list[dict] = []

    def log_message(self, *args):  # silence
        pass

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        type(self).seen.append(body)
        if body.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for tok in ["Hello", ", ", "world"]:
                chunk = {"choices": [{"delta": {"content": tok}}]}
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.write(b"data: [DONE]\n\n")
            return
        if body.get("tools"):
            # 要求调用 calc 工具
            msg = {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "calc", "arguments": "{}"},
                    }
                ],
            }
        else:
            msg = {"role": "assistant", "content": "mock 回复"}
        payload = {"choices": [{"message": msg, "finish_reason": "stop"}]}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())


@pytest.fixture(scope="module")
def mock_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _Handler.seen = []
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_fake_llm_invoke() -> None:
    llm = FakeLLM(["你好", "世界"])
    assert llm.invoke({"messages": []})["output"] == "你好"
    assert llm.invoke({"messages": []})["output"] == "世界"


def test_fake_llm_stream() -> None:
    llm = FakeLLM(["a b c"])
    chunks = list(llm.stream({"messages": []}))
    text = "".join(c["chunk"] for c in chunks)
    assert text == "a b c "


def test_fake_llm_prompt_input() -> None:
    llm = FakeLLM(["ok"], input_key="prompt")
    out = llm.invoke({"prompt": "hi"})
    assert out["output"] == "ok"
    assert llm.calls[-1] == [{"role": "user", "content": "hi"}]


def test_fake_llm_exhausted_hint() -> None:
    llm = FakeLLM([])
    with pytest.raises(ReactiveChainError, match="用完所有响应"):
        llm.invoke({"messages": []})


def test_llm_missing_input_hint() -> None:
    llm = FakeLLM(["x"])
    with pytest.raises(ReactiveChainError, match="缺少输入键"):
        llm.invoke({})


def test_openai_compat_invoke(mock_server) -> None:
    llm = OpenAICompatChatModel(mock_server, api_key="test-key", model="mock-model")
    out = llm.invoke({"messages": [{"role": "user", "content": "hi"}]})
    assert out["output"] == "mock 回复"
    sent = _Handler.seen[-1]
    assert sent["model"] == "mock-model"
    assert sent["messages"] == [{"role": "user", "content": "hi"}]


def test_openai_compat_stream(mock_server) -> None:
    llm = OpenAICompatChatModel(mock_server, api_key="test-key", model="mock-model")
    text = "".join(c["chunk"] for c in llm.stream({"messages": []}))
    assert text == "Hello, world"
    assert _Handler.seen[-1]["stream"] is True


def test_openai_compat_bind_tools(mock_server) -> None:
    llm = OpenAICompatChatModel(mock_server, api_key="test-key", model="mock-model")
    tool = {
        "type": "function",
        "function": {
            "name": "calc",
            "description": "计算",
            "parameters": {"type": "object", "properties": {}},
        },
    }
    llm.bind_tools([tool])
    out = llm.invoke({"messages": [{"role": "user", "content": "算一下"}]})
    assert out["output"] == ""  # tool calling 时 content 为空
    msg = AIMessage.from_api_message(out["llm_message"])
    assert msg.tool_calls[0]["function"]["name"] == "calc"


def test_openai_compat_missing_key_hint(mock_server) -> None:
    llm = OpenAICompatChatModel(mock_server, api_key=None, env_key="RC_NO_SUCH_ENV")
    with pytest.raises(ReactiveChainError, match="未配置 api_key"):
        llm.invoke({"messages": []})


def test_chat_prompt_to_llm_pipeline() -> None:
    tmpl = ChatPromptTemplate([("system", "你是助手"), ("human", "{q}")])
    llm = FakeLLM(["好的"])
    chain = tmpl | llm
    out = chain.invoke({"q": "你好"})
    assert out["output"] == "好的"
    assert llm.calls[-1] == [
        {"role": "system", "content": "你是助手"},
        {"role": "user", "content": "你好"},
    ]


# -- C：Anthropic 兼容模型（纯 stdlib HTTP + SSE，本地 mock） -------------------


class _AnthropicHandler(BaseHTTPRequestHandler):
    """Anthropic /v1/messages mock：JSON（非流式）+ SSE（流式）+ 401 路径。"""

    seen: list[dict] = []
    attempts: list[int] = []

    def log_message(self, *args):  # silence
        pass

    def do_POST(self) -> None:  # noqa: N802
        type(self).attempts.append(1)
        if self.path == "/v1/messages/401":
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b'{"error": {"message": "invalid api key"}}')
            return
        if self.headers.get("x-api-key") != "test-key":
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b'{"error": {"message": "bad key"}}')
            return
        assert self.headers.get("anthropic-version")
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        type(self).seen.append(body)
        if body.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            events = [
                {"type": "message_start", "message": {"id": "msg_1", "content": []}},
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                },
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "你好"},
                },
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "，Anthropic"},
                },
                {"type": "content_block_stop", "index": 0},
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn"},
                    "usage": {"output_tokens": 7},
                },
                {"type": "message_stop"},
            ]
            for ev in events:
                self.wfile.write(f"event: {ev['type']}\ndata: {json.dumps(ev)}\n\n".encode())
            return
        data = {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "你好，Anthropic"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 5, "output_tokens": 7},
        }
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


@pytest.fixture(scope="module")
def anthropic_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _AnthropicHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_anthropic_invoke_json(anthropic_server) -> None:
    from reactivechain.llm import AnthropicCompatChatModel

    _AnthropicHandler.seen.clear()
    llm = AnthropicCompatChatModel(
        anthropic_server, api_key="test-key", model="claude-test", max_tokens=64
    )
    out = llm.invoke(
        {
            "messages": [
                {"role": "system", "content": "你是助手"},
                {"role": "user", "content": "你好"},
            ]
        }
    )
    assert out["output"] == "你好，Anthropic"
    body = _AnthropicHandler.seen[-1]
    assert body["system"] == "你是助手"  # system 消息拆到顶层字段
    assert body["messages"] == [{"role": "user", "content": "你好"}]
    assert body["model"] == "claude-test"
    assert body["max_tokens"] == 64


def test_anthropic_stream_sse(anthropic_server) -> None:
    from reactivechain.llm import AnthropicCompatChatModel

    _AnthropicHandler.seen.clear()
    llm = AnthropicCompatChatModel(anthropic_server, api_key="test-key", model="claude-test")
    chunks = [c["chunk"] for c in llm.stream({"messages": [{"role": "user", "content": "hi"}]})]
    assert "".join(chunks) == "你好，Anthropic"
    assert _AnthropicHandler.seen[-1]["stream"] is True


def test_anthropic_rejects_4xx_without_retry(anthropic_server) -> None:
    from reactivechain.llm import AnthropicCompatChatModel

    _AnthropicHandler.attempts.clear()
    # 错误 key → handler 返回 401；4xx 应直接抛错且不重试。
    llm = AnthropicCompatChatModel(
        anthropic_server, api_key="wrong-key", model="claude-test"
    )
    with pytest.raises(ReactiveChainError, match="401"):
        llm.invoke({"messages": [{"role": "user", "content": "hi"}]})
    assert len(_AnthropicHandler.attempts) == 1  # 4xx 不重试