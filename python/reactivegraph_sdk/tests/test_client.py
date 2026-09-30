"""SDK client integration tests (Task 11 / D.9.100)."""

from __future__ import annotations

from typing import Any

from reactivegraph.protocol import (
    FrameDecoder,
    decode_value,
    encode_frame,
    encode_value,
    is_envelope,
)

from reactivegraph_sdk import AsyncReactiveGraphClient, ReactiveGraphClient
from reactivegraph_sdk.client import InMemoryDuplex, _env_dict


class TestServer:
    """In-process RGP/1 server responding to SDK requests."""

    def __init__(self, duplex: InMemoryDuplex) -> None:
        self.decoder = FrameDecoder()
        self.duplex = duplex
        self.data: dict[str, Any] = {"assistants": [], "threads": {}, "crons": [], "store": {}}
        duplex.on_data(self._on_data)

    def _respond(self, req: dict, payload: Any) -> None:
        resp = _env_dict(req["id"], "response", req["method"], payload)
        self.duplex.send(encode_frame(encode_value(resp)))

    def _on_data(self, data: bytes) -> None:
        for frame in self.decoder.push(data):
            value = decode_value(frame)
            if not is_envelope(value):
                continue
            req = value
            self._handle(req)

    def _handle(self, req: dict) -> None:
        if req["method"] == "STORE_OP":
            op = (req["payload"] or {}).get("op")
            self._store_op(req, op)
        elif req["method"] == "RUN":
            p = req["payload"] or {}
            self._respond(req, {"run_id": "run-1", "output": p.get("input"), "events": [{"event": "values", "data": p.get("input")}]})
        elif req["method"] == "GET_STATE":
            self._respond(req, {"run_id": (req["payload"] or {}).get("run_id"), "values": {}})
        elif req["method"] == "CANCEL":
            self._respond(req, {"cancelled": (req["payload"] or {}).get("run_id")})
        else:
            self._respond(req, {"ok": True})

    def _store_op(self, req: dict, op: str) -> None:
        p = req["payload"] or {}
        if op == "create_assistant":
            a = {"assistant_id": f"asst-{len(self.data['assistants'])}", "graph_id": p.get("graph_id"), "config": p.get("config")}
            self.data["assistants"].append(a)
            self._respond(req, a)
        elif op == "list_assistants":
            self._respond(req, self.data["assistants"])
        elif op == "create_thread":
            t = {"thread_id": p.get("thread_id") or f"thread-{len(self.data['threads'])}"}
            self.data["threads"][t["thread_id"]] = t
            self._respond(req, t)
        elif op == "get_thread":
            self._respond(req, self.data["threads"].get(p.get("thread_id"), {}))
        elif op == "list_threads":
            self._respond(req, list(self.data["threads"].values()))
        elif op == "put":
            self.data["store"][(p.get("namespace"), p.get("key"))] = p.get("value")
            self._respond(req, {"ok": True})
        elif op == "get":
            self._respond(req, {"value": self.data["store"].get((p.get("namespace"), p.get("key")))})
        elif op == "create_cron":
            c = {"cron_id": f"cron-{len(self.data['crons'])}", "graph_id": p.get("graph_id"), "schedule": p.get("schedule")}
            self.data["crons"].append(c)
            self._respond(req, c)
        elif op == "list_crons":
            self._respond(req, self.data["crons"])
        else:
            self._respond(req, {"ok": True})


def make_pair() -> tuple[InMemoryDuplex, InMemoryDuplex, TestServer]:
    server = InMemoryDuplex()
    client = InMemoryDuplex()
    server.connect(client)
    ts = TestServer(server)
    return server, client, ts


class TestSyncClient:
    def test_assistant_and_thread_lifecycle(self) -> None:
        _, client_d, _ = make_pair()
        client = ReactiveGraphClient(client_d)
        try:
            asst = client.create_assistant(graph_id="agent")
            assert asst["graph_id"] == "agent"
            assert client.assistants() == [asst]

            thread = client.create_thread(thread_id="t1")
            assert thread["thread_id"] == "t1"
            assert client.get_thread("t1") == thread
        finally:
            client.close()

    def test_run_and_store(self) -> None:
        _, client_d, _ = make_pair()
        client = ReactiveGraphClient(client_d)
        try:
            run = client.run(graph_id="agent", thread_id="t1", input={"msg": "hi"})
            assert run["run_id"] == "run-1"
            assert run["output"] == {"msg": "hi"}

            client.store("docs", "k1", {"v": 1})
            assert client.get_store_item("docs", "k1")["value"] == {"v": 1}
        finally:
            client.close()

    def test_crons(self) -> None:
        _, client_d, _ = make_pair()
        client = ReactiveGraphClient(client_d)
        try:
            cron = client.create_cron(graph_id="agent", schedule="0 * * * *")
            assert cron["schedule"] == "0 * * * *"
            assert client.crons() == [cron]
        finally:
            client.close()

    def test_cancel_run(self) -> None:
        _, client_d, _ = make_pair()
        client = ReactiveGraphClient(client_d)
        try:
            res = client.cancel_run("run-9")
            assert res["cancelled"] == "run-9"
        finally:
            client.close()

    def test_resume_run(self) -> None:
        _, client_d, _ = make_pair()
        client = ReactiveGraphClient(client_d)
        try:
            res = client.resume(run_id="run-1", interrupt_response="yes", thread_id="t1")
            assert res["ok"] is True
        finally:
            client.close()


class TestAsyncClient:
    async def test_async_surface(self) -> None:
        _, client_d, _ = make_pair()
        client = AsyncReactiveGraphClient(client_d)
        try:
            asst = await client.create_assistant(graph_id="agent")
            assert asst["graph_id"] == "agent"
            thread = await client.create_thread(thread_id="t2")
            assert thread["thread_id"] == "t2"
            run = await client.run(graph_id="agent", thread_id="t2", input={"x": 1})
            assert run["output"] == {"x": 1}
            events = [ev async for ev in client.stream(graph_id="agent", thread_id="t2", input={"x": 2})]
            assert any(ev.get("event") == "values" for ev in events)
        finally:
            await client.aclose()