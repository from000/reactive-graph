"""reactivegraph_sdk — Python remote SDK (Task 11 / D.9.100).

Speaks RGP/1 over a duplex transport (WebSocket/HTTP2 adapter, or the
in-process gateway transport used by the integration suites). Mirrors the
upstream langgraph-sdk surface: assistants, threads, runs, crons, store,
stream. Sync and async facades share one request/response core.
"""

__version__ = "0.1.0"

from reactivegraph_sdk.client import AsyncReactiveGraphClient, ReactiveGraphClient

__all__ = ("AsyncReactiveGraphClient", "ReactiveGraphClient")