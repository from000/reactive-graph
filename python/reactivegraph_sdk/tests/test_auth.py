"""SDK auth tests (sdk-py-auth)."""

from __future__ import annotations

import pytest
from test_client import make_pair  # type: ignore[import-not-found]

from reactivegraph_sdk import ReactiveGraphClient
from reactivegraph_sdk.client import _resolve_api_key


class TestApiKeyResolution:
    def test_explicit_key_wins(self) -> None:
        assert _resolve_api_key("sk-explicit") == "sk-explicit"

    def test_none_skips_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LANGGRAPH_API_KEY", "sk-env")
        assert _resolve_api_key(None) is None

    def test_env_fallback_order(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LANGGRAPH_API_KEY", raising=False)
        monkeypatch.setenv("LANGSMITH_API_KEY", "sk-smith")
        monkeypatch.setenv("LANGCHAIN_API_KEY", "sk-chain")
        # omitted argument -> auto-load from env in upstream order
        assert _resolve_api_key() == "sk-smith"

    def test_omitted_auto_loads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LANGGRAPH_API_KEY", "sk-auto")
        assert _resolve_api_key() == "sk-auto"


class TestAuthOnWire:
    def test_api_key_carried_in_request(self) -> None:
        _server, client_d, ts = make_pair()
        seen: list[dict] = []

        original = ts._handle

        def spy(req: dict) -> None:
            if req["method"] != "STORE_OP" or (req["payload"] or {}).get("op") != "get_thread":
                return original(req)
            seen.append(req["payload"])
            return original(req)

        ts._handle = spy  # type: ignore[method-assign]
        client = ReactiveGraphClient(client_d, api_key="sk-test")
        try:
            client.get_thread("t1")
        finally:
            client.close()
        assert seen and seen[0].get("auth", {}).get("api_key") == "sk-test"

    def test_headers_carried_in_request(self) -> None:
        _server, client_d, ts = make_pair()
        seen: list[dict] = []

        original = ts._handle

        def spy(req: dict) -> None:
            if (req["payload"] or {}).get("op") == "list_threads":
                seen.append(req["payload"])
            return original(req)

        ts._handle = spy  # type: ignore[method-assign]
        client = ReactiveGraphClient(client_d, headers={"x-tenant": "acme"})
        try:
            client.threads()
        finally:
            client.close()
        assert seen and seen[0].get("auth", {}).get("headers") == {"x-tenant": "acme"}