"""Ownership/lease subset derived from real upstream tests.

Upstream sources:
* ``backend/tests/test_multi_worker_run_ownership.py`` (96 tests)
* ``backend/tests/test_run_worker_rollback.py`` (109 tests)

Ported here: durable thread-operation reservation, CancelOutcome semantics,
lease-aware reconciliation, worker identity, and retry-after computation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from deerflow_reactive import (
    CancelOutcome,
    ReactiveConflictError,
    ReactiveRunManager,
    ReactiveRunStore,
    compute_retry_after,
    generate_worker_id,
)


def _lease(seconds: int = 30) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


def _expired(seconds: int = 60) -> str:
    return (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()


def _manager(store=None, *, worker_id="worker-a", grace_seconds=10, heartbeat_enabled=True) -> ReactiveRunManager:
    return ReactiveRunManager(
        store=store or ReactiveRunStore(),
        worker_id=worker_id,
        grace_seconds=grace_seconds,
        heartbeat_enabled=heartbeat_enabled,
    )


# ---------------------------------------------------------------------------
# Worker identity
# ---------------------------------------------------------------------------


def test_worker_id_is_generated_and_unique() -> None:
    first = generate_worker_id()
    second = generate_worker_id()
    assert first != second
    assert ":" in first


def test_two_managers_have_different_default_ids() -> None:
    assert ReactiveRunManager().worker_id != ReactiveRunManager().worker_id


# ---------------------------------------------------------------------------
# reserve_thread_operation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reservation_rejects_non_owning_worker_while_run_active() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    active = await owner.create_or_reject("thread-1", lease_expires_at=_lease())
    owner.set_status(active.run_id, "running")

    with pytest.raises(ReactiveConflictError, match="active run"):
        async with _manager(store, worker_id="worker-b").reserve_thread_operation("thread-1", kind="checkpoint_write"):
            pytest.fail("reservation must not be acquired")


@pytest.mark.anyio
async def test_reservation_blocks_new_runs_until_released() -> None:
    store = ReactiveRunStore()
    writer = _manager(store, worker_id="worker-a")
    runner = _manager(store, worker_id="worker-b")

    async with writer.reserve_thread_operation("thread-1", kind="checkpoint_write"):
        inflight = await store.list_inflight()
        assert len(inflight) == 1
        assert inflight[0]["operation_kind"] == "checkpoint_write"
        with pytest.raises(ReactiveConflictError, match="checkpoint write"):
            await runner.create_or_reject("thread-1", multitask_strategy="interrupt")

    assert await store.list_inflight() == []
    assert await store.list_by_thread("thread-1") == []
    admitted = await runner.create_or_reject("thread-1")
    assert admitted is not None


@pytest.mark.anyio
async def test_reservation_rejects_run_kind() -> None:
    with pytest.raises(ValueError, match="create_or_reject"):
        async with _manager().reserve_thread_operation("t", kind="run"):
            pytest.fail("run kind must be rejected")


@pytest.mark.anyio
async def test_interrupt_reclaims_expired_checkpoint_write_reservation() -> None:
    """A dead checkpoint writer must not wait for periodic reconciliation.

    Mirrors upstream ``test_interrupt_reclaims_expired_checkpoint_write_reservation``:
    an expired non-run reservation is claimed by the interrupting run.
    """
    store = ReactiveRunStore()
    row, _ = await store.create_thread_operation_atomic(
        "stale-reservation",
        thread_id="thread-1",
        owner_worker_id="dead-worker",
        lease_expires_at=_expired(30),
        operation_kind="checkpoint_write",
    )
    assert row["status"] == "pending"

    admitted = await _manager(store, worker_id="worker-b", grace_seconds=10).create_or_reject(
        "thread-1", multitask_strategy="interrupt"
    )
    assert admitted is not None
    stale = await store.get("stale-reservation")
    assert stale is not None
    assert stale["status"] == "interrupted"
    assert stale["owner_worker_id"] == "worker-b"


# ---------------------------------------------------------------------------
# cancel / CancelOutcome
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_local_run_succeeds() -> None:
    manager = _manager()
    record = await manager.create_or_reject("t", lease_expires_at=_lease())
    manager.set_status(record.run_id, "running")
    assert await manager.cancel(record.run_id) is CancelOutcome.cancelled
    assert record.abort_event.is_set()


@pytest.mark.anyio
async def test_cancel_unknown_run_returns_unknown() -> None:
    assert await _manager().cancel("nope") is CancelOutcome.unknown


@pytest.mark.anyio
async def test_cancel_is_idempotent() -> None:
    manager = _manager()
    record = await manager.create_or_reject("t", lease_expires_at=_lease())
    manager.set_status(record.run_id, "running")
    await manager.cancel(record.run_id)
    assert await manager.cancel(record.run_id) is CancelOutcome.cancelled


@pytest.mark.anyio
async def test_cancel_terminal_run_is_not_cancellable() -> None:
    manager = _manager(heartbeat_enabled=False)
    record = await manager.create_or_reject("t", lease_expires_at=_lease())
    manager.set_status(record.run_id, "success")
    assert await manager.cancel(record.run_id) is CancelOutcome.not_cancellable


@pytest.mark.anyio
async def test_cancel_store_only_run_without_heartbeat_is_not_active_locally() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_lease())
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b", heartbeat_enabled=False)
    assert await peer.cancel(record.run_id) is CancelOutcome.not_active_locally


@pytest.mark.anyio
async def test_cancel_non_owner_with_valid_lease_requests_and_owner_observes() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_lease())
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b")
    assert await peer.cancel(record.run_id) is CancelOutcome.requested

    row = await store.get(record.run_id)
    assert row is not None
    assert row["cancel_action"] == "interrupt"
    assert row["cancel_requested_at"] is not None

    # The owner observes the durable request on its next heartbeat.
    observed = await owner.heartbeat()
    assert record.run_id in observed
    assert record.abort_event.is_set()


@pytest.mark.anyio
async def test_cancel_non_owner_with_expired_lease_takes_over() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_expired())
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b")
    assert await peer.cancel(record.run_id) is CancelOutcome.taken_over
    row = await store.get(record.run_id)
    assert row is not None
    assert row["status"] == "error"


@pytest.mark.anyio
async def test_cancel_takeover_respects_grace_seconds() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_expired(5))
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b", grace_seconds=60)
    assert await peer.cancel(record.run_id) is CancelOutcome.requested


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reconciliation_claims_expired_lease_runs() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_expired())
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b")
    recovered = await peer.reconcile_orphaned_inflight_runs(error="worker lost")
    assert [r.run_id for r in recovered] == [record.run_id]
    row = await store.get(record.run_id)
    assert row is not None
    assert row["status"] == "error"
    assert row["error"] == "worker lost"


@pytest.mark.anyio
async def test_reconciliation_skips_active_lease_runs() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_lease())
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b")
    assert await peer.reconcile_orphaned_inflight_runs(error="boom") == []
    row = await store.get(record.run_id)
    assert row is not None
    assert row["status"] == "running"


@pytest.mark.anyio
async def test_reconciliation_skips_locally_active_runs() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=_expired())
    owner.set_status(record.run_id, "running")

    # The same worker still tracks the run as live, so it must not self-reap.
    assert await owner.reconcile_orphaned_inflight_runs(error="boom") == []


@pytest.mark.anyio
async def test_reconciliation_returns_empty_when_no_orphans() -> None:
    assert await _manager().reconcile_orphaned_inflight_runs(error="boom") == []


@pytest.mark.anyio
async def test_reconciliation_claims_null_lease_runs() -> None:
    store = ReactiveRunStore()
    owner = _manager(store, worker_id="worker-a")
    record = await owner.create_or_reject("t", lease_expires_at=None)
    owner.set_status(record.run_id, "running")

    peer = _manager(store, worker_id="worker-b")
    recovered = await peer.reconcile_orphaned_inflight_runs(error="legacy")
    assert [r.run_id for r in recovered] == [record.run_id]


@pytest.mark.anyio
async def test_reconciliation_releases_expired_internal_operation() -> None:
    store = ReactiveRunStore()
    row, _ = await store.create_thread_operation_atomic(
        "op-1",
        thread_id="t",
        owner_worker_id="worker-a",
        lease_expires_at=_expired(),
        operation_kind="checkpoint_write",
    )
    assert row["status"] == "pending"

    recovered = await _manager(store, worker_id="worker-b").reconcile_orphaned_inflight_runs(error="boom")
    assert recovered == []
    assert await store.list_inflight() == []


# ---------------------------------------------------------------------------
# retry-after
# ---------------------------------------------------------------------------


def test_compute_retry_after_null_lease_returns_none() -> None:
    assert compute_retry_after(None) is None


def test_compute_retry_after_unparseable_returns_none() -> None:
    assert compute_retry_after("not-a-date") is None


def test_compute_retry_after_normal_returns_positive_seconds() -> None:
    value = compute_retry_after(_lease(30))
    assert value is not None
    assert 0 < value <= 30
