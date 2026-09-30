"""ReactiveGraph Python SDK client over RGP/1 (Task 11 / D.9.100).

A duplex-shaped transport (in-process for integration tests; a WebSocket/HTTP2
adapter in production) carries RGP/1 envelopes. The client multiplexes
requests by id, matching upstream langgraph-sdk semantics for assistants,
threads, runs, crons, and the store.
"""

from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import AsyncIterator
from typing import Any

from reactivegraph.protocol import (
    FrameDecoder,
    decode_value,
    encode_frame,
    encode_value,
    is_envelope,
    make_id,
)
from typing_extensions import Self


class Duplex:
    """Byte-level transport contract (mirrors the JS gateway Duplex)."""

    def send(self, data: bytes) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    def close(self, err: Exception | None = None) -> None:  # pragma: no cover
        raise NotImplementedError

    def on_data(self, callback: Any) -> None:  # pragma: no cover
        raise NotImplementedError

    def on_close(self, callback: Any) -> None:  # pragma: no cover
        raise NotImplementedError


class InMemoryDuplex(Duplex):
    """In-memory duplex pair for local integration and SDK tests."""

    def __init__(self, other: InMemoryDuplex | None = None) -> None:
        self.other = other
        self._data: Any = None
        self._close: Any = None
        self._closed = False

    def connect(self, other: InMemoryDuplex) -> None:
        self.other = other
        other.other = self

    def send(self, data: bytes) -> None:
        if self._closed or self.other is None:
            return
        self.other._ingest(data)

    def _ingest(self, data: bytes) -> None:
        if self._data:
            self._data(data)

    def on_data(self, callback: Any) -> None:
        self._data = callback

    def on_close(self, callback: Any) -> None:
        self._close = callback

    def close(self, err: Exception | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        if self._close:
            self._close(err)
        if self.other and not self.other._closed:
            self.other.close(err)


def _env_dict(id: str, kind: str, method: str, payload: Any) -> dict:
    """RGP/1 envelope as a plain dict (matches host.py's wire encoding)."""
    return {"version": 1, "id": id, "kind": kind, "method": method, "payload": payload}


#: sentinel: omit api_key to auto-load from the environment (upstream semantics)
_AUTO = object()


def _resolve_api_key(api_key: Any = _AUTO) -> str | None:
    """Resolve the API key following upstream env fallback semantics.

    * explicit string -> use it;
    * ``None`` -> skip env entirely (no auth);
    * omitted (default sentinel) -> auto-load LANGGRAPH_API_KEY then
      LANGSMITH_API_KEY then LANGCHAIN_API_KEY.
    """
    if api_key is not None and api_key is not _AUTO:
        return str(api_key)
    if api_key is _AUTO:
        for var in ("LANGGRAPH_API_KEY", "LANGSMITH_API_KEY", "LANGCHAIN_API_KEY"):
            value = os.environ.get(var)
            if value:
                return value
        return None
    return None


class _ResponseWaiter:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Any = None
        self.error: Exception | None = None


class ReactiveGraphClient:
    """Synchronous client. `transport` is a connected Duplex speaking RGP/1.

    Authentication mirrors upstream langgraph-sdk: `api_key` may be a string,
    ``None`` (skip env loading) or omitted (auto-load from LANGGRAPH_API_KEY,
    then LANGSMITH_API_KEY, then LANGCHAIN_API_KEY). Keys are carried on the
    wire in the request envelope's ``auth`` field for the gateway to validate.
    """

    def __init__(self, transport: Duplex, *, timeout_s: float = 30.0, api_key: str | None = None, headers: dict | None = None) -> None:
        self._transport = transport
        self._timeout_s = timeout_s
        self._api_key = _resolve_api_key(api_key)
        self._headers = headers or {}
        self._decoder = FrameDecoder()
        self._pending: dict[str, _ResponseWaiter] = {}
        self._lock = threading.Lock()
        self._closed = False
        transport.on_data(self._on_data)
        transport.on_close(lambda err: self._fail_all(err))

    @property
    def auth(self) -> dict:
        """The auth envelope attached to every request (key + extra headers)."""
        out: dict = {"headers": self._headers}
        if self._api_key is not None:
            out["api_key"] = self._api_key
        return out

    # -- low-level request -----------------------------------------------------

    def _on_data(self, data: bytes) -> None:
        for frame in self._decoder.push(data):
            value = decode_value(frame)
            if not is_envelope(value):
                continue
            env = value
            if env["kind"] == "response":
                with self._lock:
                    waiter = self._pending.pop(env["id"], None)
                if waiter:
                    payload = env["payload"] or {}
                    if isinstance(payload, dict) and "error" in payload:
                        err = payload["error"]
                        waiter.error = RuntimeError(f"{err.get('type', 'Error')}: {err.get('message', 'unknown')}")
                    else:
                        waiter.result = payload
                    waiter.event.set()

    def _fail_all(self, err: Exception | None) -> None:
        with self._lock:
            pending = list(self._pending.items())
            self._pending.clear()
        failure = err or ConnectionError("transport closed")
        for _rid, waiter in pending:
            waiter.error = failure
            waiter.event.set()

    def request(self, method: str, payload: Any) -> Any:
        if self._closed:
            raise RuntimeError("client closed")
        rid = make_id()
        # carry auth (api_key + extra headers) on the request for gateway validation
        request_payload = payload
        auth = self.auth
        if auth:
            if isinstance(request_payload, dict):
                request_payload = {**request_payload, "auth": auth}
            else:
                request_payload = {"value": request_payload, "auth": auth}
        env = _env_dict(rid, "request", method, request_payload)
        waiter = _ResponseWaiter()
        with self._lock:
            self._pending[rid] = waiter
        self._transport.send(encode_frame(encode_value(env)))
        if not waiter.event.wait(self._timeout_s):
            with self._lock:
                self._pending.pop(rid, None)
            raise TimeoutError(f"request {method} timed out after {self._timeout_s}s")
        if waiter.error:
            raise waiter.error
        return waiter.result

    # -- high-level API (upstream langgraph-sdk surface) ------------------------

    def assistants(self) -> list[dict]:
        return self.request("STORE_OP", {"op": "list_assistants"}) or []

    def create_assistant(self, *, graph_id: str, config: dict | None = None, **kwargs: Any) -> dict:
        return self.request("STORE_OP", {"op": "create_assistant", "graph_id": graph_id, "config": config or {}, **kwargs}) or {}

    def threads(self) -> list[dict]:
        return self.request("STORE_OP", {"op": "list_threads"}) or []

    def create_thread(self, *, thread_id: str | None = None) -> dict:
        return self.request("STORE_OP", {"op": "create_thread", "thread_id": thread_id}) or {}

    def get_thread(self, thread_id: str) -> dict:
        return self.request("STORE_OP", {"op": "get_thread", "thread_id": thread_id}) or {}

    def run(self, *, graph_id: str, thread_id: str, input: Any = None, config: dict | None = None, **kwargs: Any) -> dict:
        return self.request("RUN", {"graph_id": graph_id, "thread_id": thread_id, "input": input, "config": config or {}, **kwargs}) or {}

    def resume(
        self, *, run_id: str, interrupt_response: Any, thread_id: str | None = None, **kwargs: Any
    ) -> dict:
        """RESUME: continue a previously interrupted run with a response."""
        return self.request(
            "RESUME",
            {"run_id": run_id, "interrupt_response": interrupt_response, "thread_id": thread_id, **kwargs},
        ) or {}

    def get_run(self, run_id: str) -> dict:
        return self.request("GET_STATE", {"run_id": run_id}) or {}

    def cancel_run(self, run_id: str) -> dict:
        return self.request("CANCEL", {"run_id": run_id}) or {}

    def store(self, namespace: str, key: str, value: Any) -> dict:
        return self.request("STORE_OP", {"op": "put", "namespace": namespace, "key": key, "value": value}) or {}

    def get_store_item(self, namespace: str, key: str) -> dict:
        return self.request("STORE_OP", {"op": "get", "namespace": namespace, "key": key}) or {}

    def crons(self) -> list[dict]:
        return self.request("STORE_OP", {"op": "list_crons"}) or []

    def create_cron(self, *, graph_id: str, schedule: str, input: Any = None, **kwargs: Any) -> dict:
        return self.request("STORE_OP", {"op": "create_cron", "graph_id": graph_id, "schedule": schedule, "input": input, **kwargs}) or {}

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._fail_all(None)
        self._transport.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncReactiveGraphClient:
    """Asynchronous client sharing the sync core (runs the transport on the
    event loop via threads; sufficient for the in-process integration suites).
    """

    def __init__(self, transport: Duplex, *, timeout_s: float = 30.0, api_key: str | None = None, headers: dict | None = None) -> None:
        self._sync = ReactiveGraphClient(transport, timeout_s=timeout_s, api_key=api_key, headers=headers)

    async def request(self, method: str, payload: Any) -> Any:
        return await asyncio.to_thread(self._sync.request, method, payload)

    async def assistants(self) -> list[dict]:
        return await self.request("STORE_OP", {"op": "list_assistants"})

    async def create_assistant(self, *, graph_id: str, config: dict | None = None, **kwargs: Any) -> dict:
        return await self.request("STORE_OP", {"op": "create_assistant", "graph_id": graph_id, "config": config or {}, **kwargs})

    async def threads(self) -> list[dict]:
        return await self.request("STORE_OP", {"op": "list_threads"})

    async def create_thread(self, *, thread_id: str | None = None) -> dict:
        return await self.request("STORE_OP", {"op": "create_thread", "thread_id": thread_id})

    async def get_thread(self, thread_id: str) -> dict:
        return await self.request("STORE_OP", {"op": "get_thread", "thread_id": thread_id})

    async def run(self, *, graph_id: str, thread_id: str, input: Any = None, config: dict | None = None, **kwargs: Any) -> dict:
        return await self.request("RUN", {"graph_id": graph_id, "thread_id": thread_id, "input": input, "config": config or {}, **kwargs})

    async def get_run(self, run_id: str) -> dict:
        return await self.request("GET_STATE", {"run_id": run_id})

    async def cancel_run(self, run_id: str) -> dict:
        return await self.request("CANCEL", {"run_id": run_id})

    async def store(self, namespace: str, key: str, value: Any) -> dict:
        return await self.request("STORE_OP", {"op": "put", "namespace": namespace, "key": key, "value": value})

    async def get_store_item(self, namespace: str, key: str) -> dict:
        return await self.request("STORE_OP", {"op": "get", "namespace": namespace, "key": key})

    async def crons(self) -> list[dict]:
        return await self.request("STORE_OP", {"op": "list_crons"})

    async def create_cron(self, *, graph_id: str, schedule: str, input: Any = None, **kwargs: Any) -> dict:
        return await self.request("STORE_OP", {"op": "create_cron", "graph_id": graph_id, "schedule": schedule, "input": input, **kwargs})

    async def stream(self, *, graph_id: str, thread_id: str, input: Any = None) -> AsyncIterator[dict]:
        """Yield STREAM_EVENT events replayed oldest-first (SSE-shaped)."""
        events: list[dict] = []
        run = await self.request("RUN", {"graph_id": graph_id, "thread_id": thread_id, "input": input, "stream": True})
        run_id = run.get("run_id") if isinstance(run, dict) else None
        if run_id:
            events = run.get("events") or [{"event": "values", "data": run.get("output")}]
        for ev in events:
            yield ev

    async def aclose(self) -> None:
        self._sync.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()