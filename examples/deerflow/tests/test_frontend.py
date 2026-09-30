"""End-to-end tests for the DeerFlow-style workbench (frontend/server.py).

Every capability endpoint is exercised against a REAL bundled Driver (the
server spawns a DriverHost per process). Skipped cleanly when node or the
bundled driver dist are unavailable.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]  # reactive-graph/ (deerflow -> examples -> reactive-graph)

# The pytest process inherits the macOS system proxy (urllib.getproxies() ->
# http://127.0.0.1:7892) and proxy_bypass("127.0.0.1") is False, so urlopen
# would send every localhost request to that proxy (502/timeout). Disable
# proxying for the workbench tests; the server is reached directly.
urllib.request.install_opener(urllib.request.build_opener(urllib.request.ProxyHandler({})))

needs_agnes = pytest.mark.skipif(
    os.environ.get("AGNES_API_KEY") is None,
    reason="AGNES_API_KEY not set (real-model tests)",
)

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None
    or not (REPO_ROOT / "packages" / "driver" / "dist" / "main.js").exists(),
    reason="bundled Driver (node + packages/driver/dist) not available",
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestWorkbench:
    @classmethod
    def setup_class(cls) -> None:
        port = _free_port()
        server = Path(__file__).resolve().parents[1] / "frontend" / "server.py"
        env = dict(os.environ)
        env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(
            REPO_ROOT / "packages" / "driver" / "dist" / "main.js"
        )
        env["REACTIVEGRAPH_NODE_BIN"] = shutil.which("node") or ""
        cls.proc = subprocess.Popen(
            [sys.executable, str(server), "--port", str(port)],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                probe = urllib.request.Request(
                    cls.base + "/api/invoke", data=b"{}",
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(probe, timeout=2)
                return
            except Exception:  # noqa: BLE001 - server still starting
                time.sleep(0.3)
        cls.proc.terminate()
        raise RuntimeError("workbench server did not start in time")

    @classmethod
    def teardown_class(cls) -> None:
        cls.proc.terminate()
        try:
            cls.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            cls.proc.kill()

    def _get(self, path: str) -> dict:
        with urllib.request.urlopen(self.base + path, timeout=30) as r:
            return json.loads(r.read())

    def _post(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    # -- capability endpoints -------------------------------------------------

    def test_invoke_event_routing(self) -> None:
        out = self._post("/api/invoke", {"q": "hello world"})
        assert out["state"]["answer"] == "researching: hello world"
        assert out["state"]["topics"] == ["hello", "world"]

    def test_stream_token_messages(self) -> None:
        out = self._get("/api/stream?q=token+flow")
        events = out["events"]
        assert any(e["eventType"] == "messages" for e in events)
        tokens = [
            e["payload"]["message"]["content"] for e in events if e["eventType"] == "messages"
        ]
        assert "".join(tokens) == "token flow "
        assert any(e["eventType"] == "values" for e in events)

    def test_computed(self) -> None:
        out = self._get("/api/computed")
        assert "topics" in out["state"]

    def test_scope_isolation(self) -> None:
        out = self._get("/api/scope")
        assert out["tenantA"] == {"count": 1}
        assert out["tenantB"] == {"count": 2}

    def test_checkpoint_and_list(self) -> None:
        out = self._post("/api/checkpoint", {})
        assert "checkpoints" in out
        listed = self._get("/api/checkpoints")
        assert listed["checkpoints"]

    def test_restore_time_travel(self) -> None:
        self._post("/api/checkpoint", {})
        listed = self._get("/api/checkpoints")
        cp = listed["checkpoints"][-1]["checkpointId"]
        out = self._post("/api/restore", {"checkpoint_id": cp})
        assert "restored" in out and "state" in out["restored"]

    def test_resume_human_in_the_loop(self) -> None:
        out = self._post("/api/resume", {"q": "deploy?", "answer": "approved"})
        assert out["interrupted"] is True
        assert out["resumed"]["decision"] == "approved"
        assert out["resumed"]["done"] is True

    def test_prebuilt_agent_tool_call(self) -> None:
        out = self._post("/api/agent", {"question": "weather?"})
        messages = out["messages"]
        roles = [m["role"] for m in messages]
        assert "tool" in roles and messages[-1]["role"] == "assistant"

    def test_store_roundtrip(self) -> None:
        self._post("/api/store", {"key": "k-e2e", "value": {"v": 42}})
        out = self._get("/api/store?key=k-e2e")
        assert out["value"] == {"v": 42}

    def test_engine_native_cards_are_honest(self) -> None:
        # recursion limit now reaches the real engine via config.recursionLimit
        out = self._get("/api/recursion")
        assert "RecursionLimit" in out["raised"] or "recursion" in out["message"].lower()
        # dot export now reaches the real engine (EXPORT_DOT round trip)
        out = self._get("/api/dot")
        assert "digraph" in out["dot"]
        # vector search now reaches the real engine (VECTOR_UPSERT/SEARCH)
        out = self._get("/api/vector")
        assert [r["id"] for r in out["results"]] == ["alpha", "gamma", "beta"]

    def test_index_page_and_static(self) -> None:
        with urllib.request.urlopen(self.base + "/", timeout=10) as r:
            html = r.read().decode()
        assert "能力实验室" in html and "app.js" in html

    @needs_agnes
    def test_real_llm_chat(self) -> None:
        out = self._post("/api/chat", {"q": "Reply with exactly: pong"})
        assert out["content"] and "pong" in out["content"].lower()
        assert out["model"]

    @needs_agnes
    def test_real_llm_sse_token_stream(self) -> None:
        with urllib.request.urlopen(
            self.base + "/api/chat-stream?q=Reply+with+exactly:+pong", timeout=120
        ) as r:
            frames = []
            for raw in r:
                line = raw.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                if "[DONE]" in line:
                    break
                frames.append(json.loads(line[5:].strip()))
        tokens = [f["token"] for f in frames if "token" in f]
        # real streaming: more than one token frame, and the joined text answers
        assert len(tokens) > 1
        assert "pong" in "".join(tokens).lower()

    @needs_agnes
    def test_real_llm_agent_tool_call_loop(self) -> None:
        out = self._post("/api/agent-real", {"question": "What is the weather in Paris?"})
        messages = out["messages"]
        roles = [m["role"] for m in messages]
        assert "tool" in roles and messages[-1]["role"] == "assistant"
        tool_msg = next(m for m in messages if m["role"] == "tool")
        assert "20C" in tool_msg["content"] or "sunny" in tool_msg["content"].lower()





