"""Test-session isolation for the Driver's ambient backend configuration.

``DriverHost`` seeds its child environment from ``os.environ``, and the Driver
treats ``REACTIVEGRAPH_PG_DSN`` / ``REACTIVEGRAPH_REDIS_URL`` as live runtime
configuration (see ``packages/driver/src/main.ts`` ``buildPersistence``).
A developer or CI job that exports one of those variables therefore silently
re-points *every* Driver-backed test at that backend, which makes unrelated
suites fail in ways that look like concurrency or state bugs.

The autouse fixture below removes them for the duration of each test. Tests
that actually want a backend still get one, because they pass it explicitly:
``DriverHost(db_path=...)``, ``DriverHost(pg_dsn=...)``, or via their own
``env=`` mapping. Tests that need to know whether a real Postgres is available
read the test-only opt-in ``REACTIVEGRAPH_TEST_PG_DSN``, which the Driver
ignores by design.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

# Runtime configuration the Driver reads straight out of the environment.
_DRIVER_BACKEND_VARS = (
    "REACTIVEGRAPH_DB",
    "REACTIVEGRAPH_PG_DSN",
    "REACTIVEGRAPH_REDIS_URL",
)


@pytest.fixture(autouse=True)
def _isolate_driver_backend_env() -> Iterator[None]:
    saved = {name: os.environ[name] for name in _DRIVER_BACKEND_VARS if name in os.environ}
    for name in _DRIVER_BACKEND_VARS:
        os.environ.pop(name, None)
    try:
        yield
    finally:
        for name in _DRIVER_BACKEND_VARS:
            os.environ.pop(name, None)
        os.environ.update(saved)
