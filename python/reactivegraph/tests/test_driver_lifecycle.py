"""Driver lifecycle tests: handshake, startup/shutdown, crash propagation,
cancellation, and Driver-to-Python task callback (Task 4 / D.2)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from reactivegraph.host import DriverError, DriverHost, sanitize_traceback

TESTS_DIR = Path(__file__).resolve().parent
FAKE_DRIVER = TESTS_DIR / "fake_driver.mjs"
REPO_ROOT = Path(__file__).resolve().parents[3]


def env_with_fake(**overrides: str) -> dict:
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(FAKE_DRIVER)
    env.update(overrides)
    return env


def real_driver_env(node_bin: str) -> dict:
    env = dict(os.environ)
    env["REACTIVEGRAPH_DRIVER_ENTRY"] = str(REPO_ROOT / "packages" / "driver" / "dist" / "main.js")
    env["REACTIVEGRAPH_NODE_BIN"] = node_bin
    return env


def compile_task_graph(host: DriverHost, task_id: str, callback_id: str,
                       kind: str = "effect", graph_id: str = "graph",
                       callback_ids: list[str] | None = None) -> str:
    """Compile a single-task graph whose body runs as the host callback."""
    spec: dict = {
        "id": graph_id,
        "tasks": [{"id": task_id, "kind": kind, "callbackId": callback_id}],
        "routes": [{"event": "run", "taskId": task_id}],
        "scopes": [],
    }
    if callback_ids is not None:
        spec["callbackIds"] = callback_ids
    return host.compile_graph(spec)


def test_sanitized_traceback_shape() -> None:
    try:
        raise ValueError("boom")
    except ValueError as exc:
        meta = sanitize_traceback(exc)
    assert meta["type"] == "ValueError"
    assert meta["message"] == "boom"
    assert meta["file"] is not None
    assert isinstance(meta["line"], int)


@pytest.mark.parametrize("mismatch", ["0", "1"])
def test_handshake(mismatch: str) -> None:
    host = DriverHost(env=env_with_fake(FAKE_DRIVER_MISMATCH=mismatch))
    host.start()
    if mismatch == "1":
        with pytest.raises(DriverError, match="no common protocol version"):
            host.handshake()
    else:
        result = host.handshake()
        assert result["protocolVersions"] == [1]
    host.close()


@pytest.mark.parametrize("mode", ["echo", "callback"])
def test_run_with_callback_and_echo(tmp_path: Path, mode: str) -> None:
    host = DriverHost(
        env=real_driver_env(node_bin=os.environ.get("REACTIVEGRAPH_NODE_BIN", "node"))
        if mode == "callback"
        else env_with_fake(FAKE_DRIVER_ECHO="1", FAKE_DRIVER_MISMATCH="0"),
    )

    def double(x: dict) -> dict:
        # TaskSuccess shape: patches are committed by the Driver (RGP/1 §6).
        value = {"doubled": x["n"] * 2}
        return {
            "reads": [],
            "patches": [{"path": ["doubled"], "operation": "set", "value": value["doubled"]}],
            "writes": ["doubled"],
            "return_value": value,
            "external_receipts": [],
        }

    if mode == "callback":
        host.register_callback("default", double)
        host.start()
        host.handshake()
        compile_task_graph(host, "double", "default")
        result = host.run({"n": 21}, graph_id="graph")
        assert result["state"]["doubled"] == 42
        host.close()
    else:
        host.start()
        host.handshake()
        result = host.run({"n": 21}, callback_id="unused")
        assert result["state"] == {"n": 21}
        host.close()


def test_callback_exception_carries_sanitized_traceback() -> None:
    node_bin = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    host = DriverHost(env=real_driver_env(node_bin=node_bin))

    def failing(_x: dict) -> dict:
        raise RuntimeError("task exploded")

    host.register_callback("default", failing)
    host.start()
    host.handshake()
    compile_task_graph(host, "fail", "default")
    with pytest.raises(DriverError, match="task exploded"):
        host.run({"n": 1}, graph_id="graph")
    host.close()


def test_driver_crash_propagates_to_pending_requests() -> None:
    host = DriverHost(env=env_with_fake(FAKE_DRIVER_CRASH="1"))
    host.start()
    # fake driver crashes on first frame
    with pytest.raises(DriverError):
        host.request("DRIVER_HELLO", {"sdkVersion": "x", "protocolVersions": [1]}, timeout_ms=5000)
    host.close()


def test_unknown_callback_rejected() -> None:
    node_bin = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    host = DriverHost(env=real_driver_env(node_bin=node_bin))
    host.start()
    host.handshake()
    # COMPILE_GRAPH validates callbackIds: an unregistered callback fails at
    # compile time, before any run starts.
    with pytest.raises(DriverError, match="unregistered callback"):
        compile_task_graph(host, "t", "nope", graph_id="g-bad", callback_ids=["default"])
    host.close()


def test_close_is_idempotent_and_no_orphans() -> None:
    node_bin = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    host = DriverHost(env=real_driver_env(node_bin=node_bin))
    host.start()
    host.handshake()
    child = host._process
    assert child is not None and child.poll() is None
    host.close()
    host.close()  # second close is a no-op
    assert child.poll() is not None  # child exited (not leaked)


def test_concurrent_run_multiplexing() -> None:
    node_bin = os.environ.get("REACTIVEGRAPH_NODE_BIN", "node")
    host = DriverHost(env=real_driver_env(node_bin=node_bin))

    def ident(x: dict) -> dict:
        return {
            "reads": [],
            "patches": [{"path": ["seen"], "operation": "set", "value": x["n"]}],
            "writes": ["seen"],
            "return_value": {"seen": x["n"]},
            "external_receipts": [],
        }

    host.register_callback("default", ident)
    host.start()
    host.handshake()
    compile_task_graph(host, "ident", "default")
    # concurrent requests (sequential here; session multiplexing asserted in TS tests)
    r1 = host.run({"n": 1}, graph_id="graph")
    r2 = host.run({"n": 2}, graph_id="graph")
    assert r1["state"]["seen"] == 1
    assert r2["state"]["seen"] == 2
    host.close()


def test_request_after_close_rejected() -> None:
    host = DriverHost(env=env_with_fake())
    host.start()
    host.close()
    with pytest.raises(DriverError, match="closed"):
        host.request("RUN", {}, timeout_ms=1000)


def test_default_driver_entry_detection() -> None:
    # When no env override and repo checkout present, the bundled entry is found.
    os.environ.pop("REACTIVEGRAPH_DRIVER_ENTRY", None)
    from reactivegraph.host import default_driver_entry

    entry = default_driver_entry()
    assert Path(entry).name == "main.js"