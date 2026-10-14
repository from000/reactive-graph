"""DeerFlow-style workbench backend for the native ReactiveGraph API.

Zero-dependency single file: stdlib ``http.server`` + a real bundled Node
Driver (RGP/1) behind every endpoint. The browser talks REST/SSE; the server
talks to the engine. Nine of the twelve capability cards exercise real engine
paths; recursion-limit / dot-export / vector-search are engine-native (TS,
covered by driver tests) with no Python binding yet — those cards state that
honestly instead of faking it.

Run:
    uv run --directory examples/deerflow python frontend/server.py [--port 8000]
"""

from __future__ import annotations

import json
import os
import re
import shutil
import ssl
import sys
import tempfile
import time
import urllib.request
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONTEND = Path(__file__).resolve().parent

# -- engine wiring ---------------------------------------------------------

HOST: Any = None  # lazy DriverHost (one per server process)
G_IDS: dict[str, str] = {}
_TMP_DIR: str | None = None


def _dist_path() -> Path:
    dist = REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
    if not dist.exists():
        raise RuntimeError(
            "bundled Driver not found — run `pnpm --filter @reactivegraph/driver build` first "
            "(dist/main.js)"
        )
    return dist


def _host() -> Any:
    global HOST, _TMP_DIR
    if HOST is not None:
        return HOST
    from reactivegraph.host import DriverHost

    node = shutil.which("node")
    if node is None:
        raise RuntimeError("node not found — required for the real Driver")
    _TMP_DIR = tempfile.mkdtemp(prefix="rgp-frontend-")
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(_dist_path())
    env["REACTIVEGRAPH_NODE_BIN"] = node
    env["REACTIVEGRAPH_DB"] = str(Path(_TMP_DIR) / "rgp.db")  # durable checkpoints
    import sys as _sys

    print("[server] starting DriverHost...", flush=True, file=_sys.stderr)
    host = DriverHost(env=env)
    host.start()
    print("[server] DriverHost started, handshaking...", flush=True, file=_sys.stderr)
    host.handshake()
    print("[server] handshake OK", flush=True, file=_sys.stderr)
    HOST = host
    return host


def _shutdown() -> None:
    global HOST, _TMP_DIR
    if HOST is not None:
        try:
            HOST.close()
        except Exception as exc:  # noqa: BLE001 - best-effort close
            import sys as _sys

            print(f"[server] host close error: {exc}", file=_sys.stderr)
        HOST = None
    if _TMP_DIR:
        import shutil as _sh

        _sh.rmtree(_TMP_DIR, ignore_errors=True)
        _TMP_DIR = None


class Interrupt(Exception):
    """Raised by a task to park the run for human-in-the-loop (error.type
    ``Interrupt`` maps to the Driver's interrupt/resume path)."""


def _graph(build_fn: Callable[[Any], None], graph_id: str) -> Any:
    """Compile a scenario graph once per process, then reuse the instance."""
    if graph_id not in G_IDS:
        from reactivegraph import GraphBuilder, ReactiveGraph

        def _b(builder: GraphBuilder) -> None:
            build_fn(builder)

        g = ReactiveGraph.build(_b, host=_host(), graph_id=graph_id)
        g._ensure_compiled()
        G_IDS[graph_id] = g
    return G_IDS[graph_id]


# -- scenario graphs -------------------------------------------------------


def build_research(b: Any) -> None:
    """Conversation scenario: generator token stream + extract + computed."""

    def llm(state: Any) -> Any:
        question = str(state.get("q", ""))
        for token in question.split():
            yield token + " "
        return {
            "patches": [
                {"path": ["answer"], "operation": "set", "value": f"researching: {question}"}
            ],
            "writes": ["answer"],
            "return_value": f"researching: {question}",
            "external_receipts": [],
        }

    def extract(state: Any) -> dict:
        return {"topics": [w.strip(" ,.") for w in str(state.get("q", "")).split()[:3]]}

    b.task("llm", fn=llm).on("run", "llm")
    b.task("extract", fn=extract).on("run", "extract")
    b.computed(
        "summary",
        lambda state: f"topics: {', '.join(state.get('topics', []))}",
        reads=("topics",),
    )


def build_scope(b: Any) -> None:
    """Multi-tenant counters via task scope: tenantA and tenantB write the same
    key without colliding (state.tenantA.count / state.tenantB.count)."""

    def bump_a(state: Any) -> dict:
        return {"count": state.get("a", 1)}

    def bump_b(state: Any) -> dict:
        return {"count": state.get("b", 2)}

    b.task("bump_a", fn=bump_a, scope="tenantA").on("run", "bump_a")
    b.task("bump_b", fn=bump_b, scope="tenantB").on("run", "bump_b")


def build_human(b: Any) -> None:
    """Human-in-the-loop: 'run' parks on Interrupt; 'resume' continues with
    the human answer (finalize runs only on the resume route)."""

    def human_step(state: Any) -> None:
        raise Interrupt(f"approval needed for: {state.get('q', '')}")

    def finalize(state: Any) -> dict:
        return {"decision": state.get("answer", "approved"), "done": True}

    b.task("human_step", fn=human_step).on("run", "human_step")
    b.task("finalize", fn=finalize).on("resume", "finalize")


# -- AGNES real-model helpers --------------------------------------------------
# AGNES is an OpenAI-compatible gateway: URL/model/key come from the
# environment (AGNES_API_KEY, CUSTOM_LLM_URL, CUSTOM_MODEL). No third-party
# dependency — plain stdlib urllib.

def _agnes_config() -> dict:
    cfg = {
        "key": os.environ.get("AGNES_API_KEY", ""),
        "url": os.environ.get("CUSTOM_LLM_URL", ""),
        "model": os.environ.get("CUSTOM_MODEL", ""),
    }
    if not cfg["key"] or not cfg["url"] or not cfg["model"]:
        raise RuntimeError(
            "AGNES not configured — set AGNES_API_KEY, CUSTOM_LLM_URL, "
            "CUSTOM_MODEL (e.g. export CUSTOM_LLM_URL=https://apihub.agnes-ai.com/v1 "
            "CUSTOM_MODEL=agnes-2.5-flash)"
        )
    return cfg


def agnes_stream(messages: list[dict], tools: list[dict] | None = None) -> Iterator[str]:
    """Stream tokens from AGNES (chat.completions stream=true). Parses SSE
    lines (`data: {...}` / `data: [DONE]`) and yields content deltas."""
    cfg = _agnes_config()
    wire_messages: list[dict] = []
    for msg in messages:
        m = dict(msg)
        if m.get("tool_calls"):
            m["tool_calls"] = _to_agnes_tool_calls(m["tool_calls"])
        wire_messages.append(m)
    body: dict = {"model": cfg["model"], "messages": wire_messages, "stream": True}
    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"
    req = urllib.request.Request(
        f"{cfg['url'].rstrip('/')}/chat/completions",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg['key']}",
            "Accept": "text/event-stream",
            **_UA,
        },
    )
    with _open_with_retry(req, timeout=120) as r:
        for raw in r:
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            delta = (obj.get("choices") or [{}])[0].get("delta") or {}
            content = delta.get("content")
            if content:
                yield content


def _to_agnes_tool_calls(calls: list) -> list:
    """Engine shape {id,name,arguments} -> AGNES native {id,type,function}."""
    out = []
    for tc in calls:
        arguments = tc.get("arguments")
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {"value": arguments}
        out.append(
            {
                "id": tc.get("id", ""),
                "type": "function",
                "function": {
                    "name": tc.get("name", ""),
                    "arguments": json.dumps(arguments or {}),
                },
            }
        )
    return out


_UA = {"User-Agent": "ReactiveGraph-workbench/0.1"}
_SSL_CTX = ssl.create_default_context()


def _open_with_retry(req: urllib.request.Request, timeout: int, attempts: int = 3) -> Any:
    """Open the request with an explicit SSL context and up to `attempts`
    tries (intermittent gateway EOFs on macOS Python are real; retry the
    connection, not mid-stream bytes)."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            return urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX)
        except Exception as exc:  # noqa: BLE001 - connection-level retry
            last = exc
            if i < attempts - 1:
                time.sleep(0.5 * (i + 1))
    assert last is not None
    raise last


def agnes_completion(messages: list[dict], tools: list[dict] | None = None) -> dict:
    """One OpenAI-compatible chat completion call; returns the assistant
    message dict (content / engine-shape tool_calls)."""
    cfg = _agnes_config()
    # Re-serialize assistant tool_calls to the native shape for the gateway.
    wire_messages: list[dict] = []
    for msg in messages:
        m = dict(msg)
        if m.get("tool_calls"):
            m["tool_calls"] = _to_agnes_tool_calls(m["tool_calls"])
        wire_messages.append(m)
    body: dict = {"model": cfg["model"], "messages": wire_messages}
    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"
    req = urllib.request.Request(
        f"{cfg['url'].rstrip('/')}/chat/completions",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg['key']}",
            **_UA,
        },
    )
    with _open_with_retry(req, timeout=60) as r:
        data = json.loads(r.read())
    choice = data["choices"][0]
    message = choice["message"]
    # Normalize tool_calls to the engine shape {id, name, arguments(dict)}:
    # AGNES returns {id, type, function:{name, arguments: json-string}}.
    if message.get("tool_calls"):
        calls = []
        for tc in message["tool_calls"]:
            fn = tc.get("function") or {}
            arguments = fn.get("arguments")
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except json.JSONDecodeError:
                    arguments = {"value": arguments}
            calls.append(
                {"id": tc.get("id", ""), "name": fn.get("name", ""), "arguments": arguments}
            )
        message["tool_calls"] = calls
    return message


def agnes_model(tools_specs: list[dict] | None = None):
    """A model callable for create_react_agent backed by AGNES."""
    specs = tools_specs or []

    def _model(messages: list[dict]) -> dict:
        return agnes_completion(messages, tools=specs or None)

    return _model


# -- handlers --------------------------------------------------------------


def _json(data: Any, status: int = 200) -> tuple[bytes, int, str]:
    return (json.dumps(data).encode(), status, "application/json")


def _error(message: str, status: int = 400) -> tuple[bytes, int, str]:
    # 错误回显截断（security-review LOW）：防上游 URL/内部片段泄露
    return _json({"error": str(message)[:200]}, status)


def api_invoke(body: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    state = g.invoke("run", {"q": body.get("q", "hello")})
    return _json({"state": state})


def api_stream(query: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")

    def _events() -> list[dict]:
        return [ev for ev in g.stream("run", {"q": query.get("q", "Reactive Graph")})]

    return _json({"events": _events()})


def api_computed(query: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    g.invoke("run", {"q": "reactive execution graph"})
    state = g.get_state()
    return _json({"state": state})


def api_scope(query: dict) -> tuple[bytes, int, str]:
    g = _graph(build_scope, "scope")
    g.invoke("run", {"a": 1, "b": 2})
    state = g.get_state()
    return _json(
        {
            "tenantA": state.get("tenantA"),
            "tenantB": state.get("tenantB"),
            "note": "same key 'count', different scopes, no collision",
        }
    )


def api_checkpoint(body: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    g.invoke("run", {"q": "durable checkpoint"})
    cps = g.list_checkpoints()
    return _json({"checkpoints": cps})


def api_checkpoints(query: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    return _json({"checkpoints": g.list_checkpoints()})


def api_restore(body: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    cp_id = body.get("checkpoint_id")
    if not cp_id:
        return _error("checkpoint_id required")
    restored = g.restore_thread(checkpoint_id=cp_id)
    return _json({"restored": restored})


def api_resume(body: dict) -> tuple[bytes, int, str]:
    _graph(build_human, "human")  # ensure compiled
    host = _host()
    result = (
        host.run(
            {"q": body.get("q", "deploy?")},
            graph_id="human",
            event="run",
        )
        or {}
    )
    if not result.get("interrupted"):
        return _json({"interrupted": False, "result": result})
    run_id = result.get("runId")
    resumed = host.resume(run_id, {"answer": body.get("answer", "approved")})
    return _json({"runId": run_id, "interrupted": True, "resumed": resumed})


def api_agent(body: dict) -> tuple[bytes, int, str]:
    from reactivegraph import ToolSpec, create_react_agent

    def get_weather(city: str) -> str:
        return f"{city}: 20C, sunny"

    agent = create_react_agent(
        lambda messages: (
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "c1", "name": "weather", "arguments": {"city": "Paris"}}],
            }
            if len(messages) == 1
            else {"role": "assistant", "content": "The weather in Paris is 20C and sunny."}
        ),
        [ToolSpec(name="weather", fn=get_weather)],
    )
    question = body.get("question", "weather in Paris?")
    out = agent.invoke([{"role": "user", "content": question}])
    return _json({"messages": out["messages"]})


_KEY_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def _safe_seg(value: object, default: str) -> str:
    """store 命名空间/key 白名单：非法或超长回退默认（防命名空间碰撞注入，
    security-review MEDIUM-2）。"""
    if isinstance(value, str) and _KEY_RE.fullmatch(value):
        return value
    return default


def api_store_get(query: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    ns = _safe_seg(query.get("namespace"), "docs")
    key = _safe_seg(query.get("key"), "k")
    value = g.store_get([ns], key)
    return _json({"value": value})


def api_store_put(body: dict) -> tuple[bytes, int, str]:
    g = _graph(build_research, "research")
    ns = _safe_seg(body.get("namespace"), "docs")
    key = _safe_seg(body.get("key"), "k")
    g.store_put([ns], key, body.get("value"))
    return _json({"ok": True})


def api_chat(body: dict) -> tuple[bytes, int, str]:
    """Real LLM: one AGNES completion (non-streaming; the frontend renders the
    full text as a typewriter with an honest 'model returned at once' note)."""
    try:
        message = agnes_completion([{"role": "user", "content": body.get("q", "你好")}])
    except Exception as exc:  # noqa: BLE001 - surface gateway errors
        return _error(f"AGNES call failed: {exc}", 502)
    return _json(
        {
            "content": message.get("content") or "",
            "model": os.environ.get("CUSTOM_MODEL", "agnes"),
            "note": "model returned the full text at once; the frontend splits it for display",
        }
    )


def _ngram_vec(text: str, size: int = 3) -> list[float]:
    """Deterministic example embedding: character n-gram bag-of-words over a
    fixed alphabet (honest: a demo embedding, not a learned model)."""
    import re as _re

    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 "
    vocab: dict[str, int] = {}
    normalized = _re.sub(r"[^a-z0-9 ]", "", text.lower())
    padded = " " + normalized + " "
    for i in range(len(padded) - size + 1):
        gram = padded[i : i + size]
        vocab[gram] = vocab.get(gram, 0) + 1
    vec = [0.0] * (26 + 10 + 1)
    for gram, count in vocab.items():
        for ch in gram:
            idx = alphabet.find(ch)
            if idx >= 0:
                vec[idx] += count
    norm = sum(v * v for v in vec) ** 0.5 or 1.0
    return [round(v / norm, 6) for v in vec]


def api_agent_real(body: dict) -> tuple[bytes, int, str]:
    """Real agent loop: AGNES model picks among REAL tools — weather lookup,
    a safe calculator, and knowledge retrieval over the engine's vector
    store (VECTOR_SEARCH)."""
    from reactivegraph import ToolSpec, create_react_agent

    host = _host()
    # seed a tiny "knowledge base" with deterministic n-gram embeddings
    kb = {
        "ReactiveGraph is an event-driven reactive execution engine.",
        "The Driver executes tasks over the RGP/1 protocol.",
        "Time travel restores a thread from an old checkpoint.",
    }
    for i, text in enumerate(kb):
        host.vector_upsert(_ngram_vec(text), namespace=["kb"], id=f"kb{i}", metadata={"text": text})

    def get_weather(city: str) -> str:
        return f"{city}: 20C, sunny (real tool)"

    def calculator(expr: str) -> str:
        """Safe four-op calculator: digits/operators only, empty builtins."""
        if not isinstance(expr, str) or not re.fullmatch(r"[\d+\-*/().\s]+", expr):
            return "invalid expression"
        if len(expr) > 128:
            # 幂爆炸 DoS 收敛（security-review MEDIUM-3）
            return "expression too long"
        try:
            return str(eval(expr, {"__builtins__": {}}, {}))
        except Exception as exc:  # noqa: BLE001
            return f"error: {exc}"

    def search_knowledge(query: str) -> str:
        hits = host.vector_search(_ngram_vec(query), namespace=["kb"], limit=2)
        if not hits:
            return "no knowledge found"
        return " | ".join(str(h.get("metadata", {}).get("text", h.get("id"))) for h in hits)

    tools = [
        ToolSpec(name="weather", fn=get_weather, description="get weather for a city"),
        ToolSpec(name="calculator", fn=calculator, description="evaluate a simple arithmetic expression"),
        ToolSpec(name="search_knowledge", fn=search_knowledge, description="search the knowledge base"),
    ]
    specs = [
        {
            "type": "function",
            "function": {
                "name": "weather",
                "description": "get weather for a city",
                "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "calculator",
                "description": "evaluate a simple arithmetic expression like '2+3*4'",
                "parameters": {"type": "object", "properties": {"expr": {"type": "string"}}, "required": ["expr"]},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_knowledge",
                "description": "search the knowledge base",
                "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
            },
        },
    ]
    try:
        agent = create_react_agent(agnes_model(specs), tools)
        out = agent.invoke([{"role": "user", "content": body.get("question", "weather in Paris?")}])
    except Exception as exc:  # noqa: BLE001 - surface gateway errors
        return _error(f"AGNES agent failed: {exc}", 502)
    return _json({"messages": out["messages"], "model": os.environ.get("CUSTOM_MODEL", "agnes")})


def api_recursion(query: dict) -> tuple[bytes, int, str]:
    """Real engine path: a 10-task run with config.recursionLimit=3 exceeds the
    limit -> RecursionLimitError (with Hint) surfaces through RGP/1."""

    def build_many(b: Any) -> None:
        for i in range(10):
            b.task(f"t{i}", fn=(lambda x, i=i: {"v": i})).on("run", f"t{i}")

    _graph(build_many, "recursion")
    try:
        _host().run({}, graph_id="recursion", event="run", config={"recursionLimit": 3})
        return _json({"error": "expected RecursionLimitError but the run succeeded"})
    except Exception as exc:  # noqa: BLE001 - surface the real engine error
        return _json({"raised": type(exc).__name__, "message": str(exc)})


def api_dot(query: dict) -> tuple[bytes, int, str]:
    """Real engine path: EXPORT_DOT round trip returns Graphviz DOT for the
    compiled research graph (graph-as-code inspection)."""
    _graph(build_research, "research")  # ensure compiled
    dot = _host().export_dot("research")
    return _json({"dot": dot})


def api_vector(query: dict) -> tuple[bytes, int, str]:
    """Real engine path: upsert three points, then a cosine nearest-neighbour
    search returns them ranked (VECTOR_UPSERT / VECTOR_SEARCH over RGP/1)."""
    host = _host()
    host.vector_upsert([1, 0, 0], namespace=["demo"], id="alpha", metadata={"label": "alpha"})
    host.vector_upsert([0, 1, 0], namespace=["demo"], id="beta", metadata={"label": "beta"})
    host.vector_upsert([0.9, 0.1, 0], namespace=["demo"], id="gamma", metadata={"label": "gamma"})
    results = host.vector_search([0.95, 0.05, 0], namespace=["demo"], limit=3)
    return _json({"results": results})


ROUTES: dict[str, dict[str, Callable[..., tuple[bytes, int, str]]]] = {
    "/api/invoke": {"POST": api_invoke},
    "/api/stream": {"GET": api_stream},
    "/api/computed": {"GET": api_computed},
    "/api/scope": {"GET": api_scope},
    "/api/checkpoint": {"POST": api_checkpoint},
    "/api/checkpoints": {"GET": api_checkpoints},
    "/api/restore": {"POST": api_restore},
    "/api/resume": {"POST": api_resume},
    "/api/agent": {"POST": api_agent},
    "/api/store": {"GET": api_store_get, "POST": api_store_put},
    "/api/chat": {"POST": api_chat},
    "/api/agent-real": {"POST": api_agent_real},
    "/api/recursion": {"GET": api_recursion},
    "/api/dot": {"GET": api_dot},
    "/api/vector": {"GET": api_vector},
}

STATIC: dict[str, str] = {"": "index.html", "app.js": "app.js", "style.css": "style.css"}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:  # quieter
        return

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/chat-stream":
            self._stream_chat(parse_qs(parsed.query).get("q", ["hello"])[0])
            return
        route = ROUTES.get(parsed.path)
        if route and "GET" in route:
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            try:
                body, status, ctype = route["GET"](query)
            except Exception as exc:  # noqa: BLE001
                body, status, ctype = _error(f"{type(exc).__name__}: {exc}", 500)
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        # static
        name = parsed.path.lstrip("/") or "index.html"
        if name in STATIC.values() or name in STATIC:
            path = FRONTEND / (STATIC.get(name, name))
            if path.exists():
                ctype = (
                    "text/javascript"
                    if path.suffix == ".js"
                    else "text/css"
                    if path.suffix == ".css"
                    else "text/html"
                )
                data = path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
        self.send_error(404)

    def _stream_chat(self, q: str) -> None:
        """Server-sent events: one `data: {"token": ...}` per AGNES token."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            for token in agnes_stream([{"role": "user", "content": q}]):
                frame = f"data: {json.dumps({'token': token})}\n\n".encode()
                self.wfile.write(frame)
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except Exception as exc:  # noqa: BLE001 - report as an SSE error event
            # 错误回显截断（security-review LOW）
            self.wfile.write(
                f"data: {json.dumps({'error': str(exc)[:200]})}\n\n".encode()
            )
            self.wfile.flush()

    def do_POST(self) -> None:
        # CSRF/伪造请求收敛（security-review MEDIUM-1）：仅接受 JSON 请求体；
        # 跨源浏览器请求（Origin 非本机）拒绝——同机恶意网页不能以 text/plain
        # 触发默认参数执行（消耗 AGNES key / 写 store）。
        ctype = self.headers.get("Content-Type", "")
        if "application/json" not in ctype:
            self._error_json(415, "Content-Type must be application/json")
            return
        origin = self.headers.get("Origin")
        if origin:
            hostname = urlparse(origin).hostname
            if hostname not in ("127.0.0.1", "localhost"):
                self._error_json(403, "cross-origin request rejected")
                return
        parsed = urlparse(self.path)
        route = ROUTES.get(parsed.path)
        if not route or "POST" not in route:
            self.send_error(404)
            return
        body = self._read_body()
        try:
            data, status, ctype = route["POST"](body)
        except Exception as exc:  # noqa: BLE001
            data, status, ctype = _error(f"{type(exc).__name__}: {exc}", 500)
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error_json(self, status: int, message: str) -> None:
        body, _, ctype = _error(message, status)
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve(port: int = 8000) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"DeerFlow-style workbench on http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        _shutdown()


def main() -> None:
    port = 8000
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    serve(port)


if __name__ == "__main__":
    main()
