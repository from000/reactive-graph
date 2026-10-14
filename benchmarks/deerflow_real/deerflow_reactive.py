"""ReactiveGraph replacement for DeerFlow's minimal create_deerflow_agent path.

The factory mirrors the public arguments of DeerFlow 2.1's
``deerflow.agents.factory.create_deerflow_agent`` for the no-feature baseline.
It intentionally accepts LangChain-shaped objects only through their public
``invoke``/``name`` surface and never imports LangChain/LangGraph here.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import re
import socket
import time
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from reactivegraph import GraphBuilder, ReactiveGraph
from reactivegraph.checkpoint import (
    CheckpointStore,
    CheckpointTuple,
    MemoryCheckpointSaver,
    get_checkpointer,
    reset_checkpointer,
)

__all__ = (
    "REACTIVE_END",
    "REACTIVE_HEARTBEAT",
    "CancelOutcome",
    "ReactiveCheckpointSaver",
    "ReactiveCheckpointStore",
    "ReactiveCheckpointTuple",
    "ReactiveConflictError",
    "ReactiveRunBridge",
    "ReactiveRunEventStore",
    "ReactiveRunJournal",
    "ReactiveRunManager",
    "ReactiveRunRecord",
    "ReactiveRunStore",
    "ReactiveStreamEvent",
    "ReactiveStreamGap",
    "compute_retry_after",
    "create_deerflow_agent",
    "generate_worker_id",
    "get_checkpointer",
    "reactive_run_agent",
    "reset_checkpointer",
)


def _message_content(message: dict[str, Any]) -> Any:
    return message.get("content", "")


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                text = item.get("text", item.get("content", ""))
                if text is not None:
                    parts.append(str(text))
            elif value is not None:
                parts.append(str(item))
        return "".join(parts)
    return str(value or "")


def _model_input(messages: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(message) for message in messages]


def _invoke_model(model: Any, messages: list[dict[str, Any]]) -> dict[str, Any]:
    # DeerFlow model mocks and real LangChain chat models support invoke.
    result = model.invoke(messages)
    if isinstance(result, dict):
        return dict(result)
    # Some fake models return a plain response string.
    if isinstance(result, str):
        return {"role": "assistant", "content": result}
    content = getattr(result, "content", None)
    if content is not None:
        out: dict[str, Any] = {"role": "assistant", "content": content}
        tool_calls = getattr(result, "tool_calls", None)
        if tool_calls:
            out["tool_calls"] = list(tool_calls)
        return out
    raise TypeError(f"unsupported model result {type(result).__name__}")


def _invoke_tool(tool: Any, arguments: dict[str, Any]) -> Any:
    # LangChain-compatible tools expose invoke; pass a JSON-schema object.
    return tool.invoke(arguments)


# Checkpoint storage lives in the core package (reactivegraph.checkpoint) so
# the semantics are shared and tested once. These aliases keep the DeerFlow port
# readable under its upstream names.
ReactiveCheckpointTuple = CheckpointTuple
ReactiveCheckpointStore = CheckpointStore
ReactiveCheckpointSaver = MemoryCheckpointSaver




@dataclass
class _ReactiveDeerFlowGraph:
    graph: ReactiveGraph
    model: Any
    tools: list[Any]
    checkpointer: Any | None = None
    summarizer: Any | None = None
    summarization_trigger: tuple[str, int] | None = None
    summarization_keep: tuple[str, int] | None = None
    usage_totals: dict[str, int] = field(default_factory=lambda: {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0})

    async def ainvoke(self, input_value: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
        return await asyncio.to_thread(self.invoke, input_value, config)

    def invoke(self, input_value: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
        thread = ((config or {}).get("configurable") or {}).get("thread_id", "default")
        prior_tuple = self._load_checkpoint(config) if self.checkpointer is not None else None
        prior = dict(prior_tuple.checkpoint.get("channel_values", {})) if prior_tuple is not None else {}
        prior_messages = list(prior.get("messages", []))
        input_messages = list(input_value.get("messages", []))
        # LangGraph checkpointer semantics append new user turns but do not
        # duplicate the exact turn on a retry.
        if input_messages and prior_messages[:len(input_messages)] == input_messages:
            messages = prior_messages
        else:
            messages = prior_messages + input_messages
        summary = None
        if self.summarizer is not None:
            messages, summary = self._maybe_summarize(messages, prior)
            if summary is not None:
                messages = [{"role": "user", "content": summary}, *messages]
        payload = {
            "messages": messages,
            "system_prompt": input_value.get("system_prompt", prior.get("system_prompt", "")),
        }
        state = self.graph.invoke("run", payload)
        assistant = state.get("assistant") or {}
        usage = assistant.get("usage_metadata") or {}
        if usage:
            self.usage_totals["input_tokens"] += int(usage.get("input_tokens", 0))
            self.usage_totals["output_tokens"] += int(usage.get("output_tokens", 0))
            self.usage_totals["total_tokens"] += int(usage.get("total_tokens", 0))
        state["usage_metadata"] = dict(self.usage_totals)
        state["interrupted"] = bool(state.get("interrupted"))
        if summary is not None:
            state["summary_text"] = summary
        if self.checkpointer is not None:
            self._save_checkpoint(config, prior_tuple, state, thread)
        return state

    def _load_checkpoint(self, config: dict[str, Any] | None) -> Any | None:
        """Read the latest checkpoint tuple, tolerating an unconfigured thread."""
        configurable = dict((config or {}).get("configurable") or {})
        if not configurable.get("thread_id"):
            configurable["thread_id"] = "default"
        probe = {**(config or {}), "configurable": configurable}
        return self.checkpointer.get_tuple(probe)

    def _save_checkpoint(self, config: dict[str, Any] | None, prior_tuple: Any | None, state: dict[str, Any], thread: str) -> None:
        """Append one immutable checkpoint, chaining to the prior head."""
        configurable = dict((config or {}).get("configurable") or {})
        configurable["thread_id"] = thread
        configurable.pop("checkpoint_id", None)
        parent_config = (
            copy.deepcopy(prior_tuple.config)
            if prior_tuple is not None
            else {"configurable": dict(configurable)}
        )
        saver = self.checkpointer
        new_id = saver.new_checkpoint_id() if hasattr(saver, "new_checkpoint_id") else f"cp-{time.time_ns()}"
        checkpoint = {
            "v": 1,
            "id": new_id,
            "ts": datetime.now(UTC).isoformat(),
            "channel_values": copy.deepcopy(state),
            "channel_versions": {key: 1 for key in state},
            "versions_seen": {},
        }
        saver.put(parent_config, checkpoint, {"source": "loop", "step": 1}, dict(checkpoint["channel_versions"]))

    def get_state(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.checkpointer is None:
            return {}
        record = self._load_checkpoint(config)
        if record is None:
            return {}
        return dict(record.checkpoint.get("channel_values", {}))

    def _maybe_summarize(self, messages: list[dict[str, Any]], prior: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
        summarizer = self.summarizer
        if summarizer is None or self.summarization_trigger is None:
            return messages, None
        trigger_field, threshold = self.summarization_trigger
        if trigger_field != "messages" or len(messages) < threshold:
            return messages, prior.get("summary_text")
        summary = summarizer.invoke(messages)
        keep_field, keep_count = self.summarization_keep or ("messages", 2)
        kept = messages[-keep_count:] if keep_field == "messages" else messages[-2:]
        return [{**message, "summary_text": summary} if False else message for message in kept], summary

    def stream(
        self,
        input_value: dict[str, Any],
        config: dict[str, Any] | None = None,
        *,
        stream_mode: list[str] | str | None = None,
    ):
        thread = ((config or {}).get("configurable") or {}).get("thread_id", "default")
        payload = {
            "messages": list(input_value.get("messages", [])),
            "system_prompt": input_value.get("system_prompt", ""),
        }
        raw_modes = (
            list(stream_mode)
            if isinstance(stream_mode, list)
            else [stream_mode] if isinstance(stream_mode, str) else ["values"]
        )
        # DeerFlow's public runtime mode is `messages-tuple`; LangGraph maps it
        # to `messages`. Accept both at this native boundary.
        modes = ["messages" if mode == "messages-tuple" else mode for mode in raw_modes]
        messages_mode = "messages-tuple" if "messages-tuple" in raw_modes else "messages"
        latest_state: dict[str, Any] = {}
        for event in self.graph.stream("run", payload, thread_id=thread, trace=True):
            event_type = event.get("eventType")
            task = str(event.get("task") or "")
            if event_type == "values":
                latest_state = dict(event.get("payload", {}).get("state", {}))
            if event_type == "values" and "values" in modes:
                yield "values", latest_state
            if event_type == "messages" and "messages" in modes:
                yield messages_mode, event.get("payload", {}).get("message", event.get("payload"))
            if event_type == "task_end":
                if "messages" in modes and task == "final":
                    state = event.get("writes", {})
                    messages = state.get("messages", [])
                    if messages:
                        yield messages_mode, messages[-1]
                if "updates" in modes:
                    yield "updates", {task: event.get("writes", {})}
                if "tasks" in modes:
                    yield "tasks", {"id": task, "status": "completed"}
                if "debug" in modes:
                    yield "debug", {
                        "type": "task_end",
                        "task": task,
                        "writes": event.get("writes", {}),
                    }
                if "checkpoints" in modes and task == "final":
                    yield "checkpoints", latest_state
            if event_type == "task_start":
                if "tasks" in modes:
                    yield "tasks", {"id": task, "status": "running"}
                if "debug" in modes:
                    yield "debug", {"type": "task_start", "task": task}
            if event_type == "custom" and "custom" in modes:
                yield "custom", event.get("payload", {})



class CancelOutcome(str, Enum):
    """Result of :meth:`ReactiveRunManager.cancel`, mirroring upstream values.

    ``str`` mixin keeps JSON/equality ergonomics; enum identity keeps
    ``outcome is CancelOutcome.cancelled`` comparisons exact.
    """

    cancelled = "cancelled"
    requested = "requested"
    taken_over = "taken_over"
    lease_valid_elsewhere = "lease_valid_elsewhere"
    not_cancellable = "not_cancellable"
    not_active_locally = "not_active_locally"
    unknown = "unknown"


def generate_worker_id() -> str:
    """Unique worker identifier: ``hostname:hex_uuid`` (upstream format)."""
    return f"{socket.gethostname()}:{uuid.uuid4().hex}"


def compute_retry_after(lease_expires_at: str | None, *, grace_seconds: int = 0) -> int | None:
    """Seconds until *lease_expires_at*, or ``None`` when unknown/unparseable.

    Mirrors upstream ``_compute_retry_after``: callers surface this as an HTTP
    ``Retry-After`` hint when a peer still owns a valid lease.
    """
    if not lease_expires_at:
        return None
    try:
        deadline = datetime.fromisoformat(str(lease_expires_at))
    except (TypeError, ValueError):
        return None
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=UTC)
    remaining = (deadline - datetime.now(UTC)).total_seconds() + grace_seconds
    return max(1, int(remaining)) if remaining > 0 else 1


@dataclass
class ReactiveRunRecord:
    run_id: str
    thread_id: str
    model_name: str = "default"
    status: str = "running"
    error: str | None = None
    abort_action: str | None = None
    abort_event: asyncio.Event = field(default_factory=asyncio.Event)


class ReactiveConflictError(RuntimeError):
    """Raised when a thread already has an active run under reject admission."""


class ReactiveRunStore:
    """In-memory subset of DeerFlow's ``MemoryRunStore``.

    This mirrors the durable run metadata surface used by ``RunManager`` and
    the runtime worker: idempotent snapshot writes, newest-first thread paging,
    monotonic completion, cancellation races, and lease ownership.
    """

    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}
        self._runs_by_thread: dict[str, dict[str, None]] = {}
        self._change_seq = 0

    def _mark_changed(self, row: dict[str, Any]) -> None:
        self._change_seq += 1
        row["change_seq"] = self._change_seq

    def _put_sync(
        self,
        run_id: str,
        *,
        thread_id: str,
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        status: str = "pending",
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        error: str | None = None,
        stop_reason: str | None = None,
        created_at: str | None = None,
        owner_worker_id: str | None = None,
        lease_expires_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        """Synchronous snapshot write used by the in-process RunManager."""
        now = datetime.now(UTC).isoformat()
        existing = self._runs.get(run_id)
        self._runs[run_id] = {
            "run_id": run_id,
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "status": status,
            "operation_kind": operation_kind,
            "multitask_strategy": multitask_strategy,
            "metadata": metadata or {},
            "kwargs": kwargs or {},
            "error": error,
            "stop_reason": stop_reason,
            "created_at": created_at or now,
            "updated_at": now,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
            "idempotency_key": idempotency_key,
            "cancel_action": existing.get("cancel_action") if existing else None,
            "cancel_requested_at": existing.get("cancel_requested_at") if existing else None,
        }
        self._mark_changed(self._runs[run_id])
        self._runs_by_thread.setdefault(thread_id, {})[run_id] = None

    async def put(
        self,
        run_id: str,
        *,
        thread_id: str,
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        status: str = "pending",
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        error: str | None = None,
        stop_reason: str | None = None,
        created_at: str | None = None,
        owner_worker_id: str | None = None,
        lease_expires_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        self._put_sync(
            run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            user_id=user_id,
            model_name=model_name,
            status=status,
            operation_kind=operation_kind,
            multitask_strategy=multitask_strategy,
            metadata=metadata,
            kwargs=kwargs,
            error=error,
            stop_reason=stop_reason,
            created_at=created_at,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
            idempotency_key=idempotency_key,
        )

    async def get(self, run_id: str, *, user_id: str | None = None) -> dict[str, Any] | None:
        row = self._runs.get(run_id)
        if row is None or (user_id is not None and row.get("user_id") != user_id):
            return None
        return row

    async def list_by_thread(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
        limit: int = 100,
        before_created_at: str | None = None,
        before_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            row
            for run_id in self._runs_by_thread.get(thread_id, {})
            if (row := self._runs.get(run_id)) is not None
            and row.get("operation_kind", "run") == "run"
            and (user_id is None or row.get("user_id") == user_id)
            and self._is_before_cursor(
                row,
                before_created_at=before_created_at,
                before_run_id=before_run_id,
            )
        ]
        rows.sort(
            key=lambda row: self._sort_key(row["created_at"], row["run_id"]),
            reverse=True,
        )
        return rows[:limit]

    async def list_changed(
        self,
        *,
        after_change_seq: int,
        after_run_id: str,
        user_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        cursor = (after_change_seq, after_run_id)
        rows = [
            row
            for row in self._runs.values()
            if row.get("operation_kind", "run") == "run"
            and (user_id is None or row.get("user_id") == user_id)
            and (int(row.get("change_seq") or 0), row["run_id"]) > cursor
        ]
        rows.sort(key=lambda row: (int(row.get("change_seq") or 0), row["run_id"]))
        return rows[:limit]

    async def get_many_by_thread(
        self,
        thread_id: str,
        run_ids: set[str],
        *,
        user_id: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        return {
            run_id: row
            for run_id in self._runs_by_thread.get(thread_id, {})
            if run_id in run_ids
            and (row := self._runs.get(run_id)) is not None
            and row.get("operation_kind", "run") == "run"
            and (user_id is None or row.get("user_id") == user_id)
        }

    async def list_successful_regenerate_sources(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
    ) -> set[str]:
        sources: set[str] = set()
        for run_id in self._runs_by_thread.get(thread_id, {}):
            row = self._runs.get(run_id)
            if (
                row is None
                or row.get("operation_kind", "run") != "run"
                or row.get("status") != "success"
                or (user_id is not None and row.get("user_id") != user_id)
            ):
                continue
            source = (row.get("metadata") or {}).get("regenerate_from_run_id")
            if isinstance(source, str) and source:
                sources.add(source)
        return sources

    async def list_edit_regenerate_runs(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for run_id in self._runs_by_thread.get(thread_id, {}):
            row = self._runs.get(run_id)
            if row is None or (user_id is not None and row.get("user_id") != user_id):
                continue
            metadata = row.get("metadata") or {}
            source = metadata.get("regenerate_from_run_id")
            if metadata.get("replay_kind") == "edit" and isinstance(source, str) and source:
                rows.append(row)
        rows.sort(key=lambda row: row["created_at"])
        return rows

    async def delete_by_thread(self, thread_id: str, *, user_id: str | None = None) -> int:
        removed = 0
        for run_id in list(self._runs_by_thread.get(thread_id, {})):
            row = self._runs.get(run_id)
            if (
                row is None
                or row.get("operation_kind", "run") != "run"
                or (user_id is not None and row.get("user_id") != user_id)
            ):
                continue
            await self.delete(run_id, user_id=user_id)
            removed += 1
        return removed

    async def update_status(
        self,
        run_id: str,
        status: str,
        *,
        error: str | None = None,
        stop_reason: str | None = None,
        abort_action: str | None = None,
        owner_worker_id: str | None = None,
    ) -> bool:
        row = self._runs.get(run_id)
        if row is None or row["status"] not in ("pending", "running", "interrupted"):
            return False
        if owner_worker_id is not None and row.get("owner_worker_id") not in (None, owner_worker_id):
            return False
        row["status"] = status
        if error is not None:
            row["error"] = error
        if stop_reason is not None:
            row["stop_reason"] = stop_reason
        if abort_action is not None:
            row["abort_action"] = abort_action
        row["updated_at"] = datetime.now(UTC).isoformat()
        self._mark_changed(row)
        return True

    async def start_run(self, run_id: str) -> bool:
        row = self._runs.get(run_id)
        if row is None or row["status"] != "pending":
            return False
        row["status"] = "running"
        row["updated_at"] = datetime.now(UTC).isoformat()
        self._mark_changed(row)
        return True

    async def update_model_name(self, run_id: str, model_name: str | None) -> None:
        row = self._runs.get(run_id)
        if row is not None:
            row["model_name"] = model_name
            row["updated_at"] = datetime.now(UTC).isoformat()
            self._mark_changed(row)

    async def delete(self, run_id: str, *, user_id: str | None = None) -> None:
        row = self._runs.get(run_id)
        if row is None or (user_id is not None and row.get("user_id") != user_id):
            return
        self._runs.pop(run_id, None)
        bucket = self._runs_by_thread.get(row["thread_id"])
        if bucket is not None:
            bucket.pop(run_id, None)
            if not bucket:
                self._runs_by_thread.pop(row["thread_id"], None)

    async def update_run_completion(
        self,
        run_id: str,
        *,
        status: str,
        total_input_tokens: int = 0,
        total_output_tokens: int = 0,
        total_tokens: int = 0,
        llm_call_count: int = 0,
        lead_agent_tokens: int = 0,
        subagent_tokens: int = 0,
        middleware_tokens: int = 0,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int = 0,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
        error: str | None = None,
    ) -> bool:
        row = self._runs.get(run_id)
        if row is None:
            return False
        allowed_sources = {"pending", "running", status}
        if status == "error":
            allowed_sources.add("interrupted")
        if row.get("status") not in allowed_sources:
            return False
        row["status"] = status
        values = {
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "llm_call_count": llm_call_count,
            "lead_agent_tokens": lead_agent_tokens,
            "subagent_tokens": subagent_tokens,
            "middleware_tokens": middleware_tokens,
            "token_usage_by_model": token_usage_by_model,
            "message_count": message_count,
            "last_ai_message": last_ai_message,
            "first_human_message": first_human_message,
            "error": error,
        }
        for key, value in values.items():
            if value is not None:
                row[key] = value
        row["updated_at"] = datetime.now(UTC).isoformat()
        self._mark_changed(row)
        return True

    async def update_run_progress(self, run_id: str, **kwargs: Any) -> None:
        row = self._runs.get(run_id)
        if row is None or row.get("status") != "running":
            return
        for key, value in kwargs.items():
            if value is not None:
                row[key] = value
        row["updated_at"] = datetime.now(UTC).isoformat()

    async def list_pending(self, *, before: str | None = None) -> list[dict[str, Any]]:
        cutoff = before or datetime.now(UTC).isoformat()
        rows = [
            row
            for row in self._runs.values()
            if row.get("operation_kind", "run") == "run"
            and row["status"] == "pending"
            and row["created_at"] <= cutoff
        ]
        rows.sort(key=lambda row: row["created_at"])
        return rows

    async def list_inflight(self, *, before: str | None = None) -> list[dict[str, Any]]:
        cutoff = before or datetime.now(UTC).isoformat()
        rows = [
            row
            for row in self._runs.values()
            if row["status"] in ("pending", "running") and row["created_at"] <= cutoff
        ]
        rows.sort(key=lambda row: row["created_at"])
        return rows

    async def aggregate_tokens_by_thread(
        self,
        thread_id: str,
        *,
        include_active: bool = False,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        statuses = ("success", "error", "running") if include_active else ("success", "error")
        rows = [
            row
            for run_id in self._runs_by_thread.get(thread_id, {})
            if (row := self._runs.get(run_id)) is not None
            and row.get("operation_kind", "run") == "run"
            and row.get("status") in statuses
            and (user_id is None or row.get("user_id") == user_id)
        ]
        by_model: dict[str, dict[str, int]] = {}
        for row in rows:
            usage_by_model = row.get("token_usage_by_model") or {}
            if usage_by_model:
                for model, usage in usage_by_model.items():
                    entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                    entry["tokens"] += usage.get("total_tokens", 0)
                    entry["runs"] += 1
            else:
                model = row.get("model_name") or "unknown"
                entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                entry["tokens"] += row.get("total_tokens", 0)
                entry["runs"] += 1
        return {
            "total_tokens": sum(row.get("total_tokens", 0) for row in rows),
            "total_input_tokens": sum(row.get("total_input_tokens", 0) for row in rows),
            "total_output_tokens": sum(row.get("total_output_tokens", 0) for row in rows),
            "total_runs": len(rows),
            "by_model": by_model,
            "by_caller": {
                "lead_agent": sum(row.get("lead_agent_tokens", 0) for row in rows),
                "subagent": sum(row.get("subagent_tokens", 0) for row in rows),
                "middleware": sum(row.get("middleware_tokens", 0) for row in rows),
            },
        }

    async def update_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> bool:
        row = self._runs.get(run_id)
        if row is None or row.get("status") not in ("pending", "running"):
            return False
        if row.get("owner_worker_id") != owner_worker_id:
            return False
        row["lease_expires_at"] = lease_expires_at
        row["updated_at"] = datetime.now(UTC).isoformat()
        return True

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> _ReactiveLeaseRenewal:
        renewed = await self.update_lease(
            run_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
        )
        row = self._runs.get(run_id)
        return _ReactiveLeaseRenewal(
            renewed=renewed,
            cancel_action=row.get("cancel_action") if renewed and row is not None else None,
        )

    async def request_cancel(self, run_id: str, *, action: str) -> str | None:
        if action not in ("interrupt", "rollback"):
            raise ValueError(f"Unsupported cancellation action: {action}")
        row = self._runs.get(run_id)
        if row is None or row.get("status") not in ("pending", "running"):
            return None
        if row.get("cancel_action") is None:
            row["cancel_action"] = action
            row["cancel_requested_at"] = datetime.now(UTC).isoformat()
        row["updated_at"] = datetime.now(UTC).isoformat()
        self._mark_changed(row)
        return row["cancel_action"]

    async def finalize_if_not_cancelled(
        self,
        run_id: str,
        *,
        status: str,
        error: str | None = None,
        stop_reason: str | None = None,
    ) -> _ReactiveStatusFinalization:
        row = self._runs.get(run_id)
        if row is None:
            return _ReactiveStatusFinalization(finalized=False)
        if row.get("cancel_action") is not None:
            return _ReactiveStatusFinalization(
                finalized=False,
                cancel_action=row["cancel_action"],
            )
        if row.get("status") not in ("pending", "running"):
            return _ReactiveStatusFinalization(finalized=False)
        row["status"] = status
        if error is not None:
            row["error"] = error
        if stop_reason is not None:
            row["stop_reason"] = stop_reason
        row["updated_at"] = datetime.now(UTC).isoformat()
        self._mark_changed(row)
        return _ReactiveStatusFinalization(finalized=True)

    async def claim_for_takeover(
        self,
        run_id: str,
        *,
        grace_seconds: int,
        error: str,
        stop_reason: str | None = None,
        owner_worker_id: str | None = None,
    ) -> bool:
        row = self._runs.get(run_id)
        if row is None or row.get("status") not in ("pending", "running"):
            return False
        lease = row.get("lease_expires_at")
        if lease is not None and not self._lease_expired(lease, grace_seconds=grace_seconds):
            return False
        if owner_worker_id is not None:
            row["owner_worker_id"] = owner_worker_id
        row["status"] = "error"
        row["error"] = error
        if stop_reason is not None:
            row["stop_reason"] = stop_reason
        row["updated_at"] = datetime.now(UTC).isoformat()
        self._mark_changed(row)
        return True

    async def list_inflight_with_expired_lease(
        self,
        *,
        before: str | None = None,
        grace_seconds: int = 10,
    ) -> list[dict[str, Any]]:
        cutoff = self._parse_time(before) if before else datetime.now(UTC)
        rows = []
        for row in self._runs.values():
            if row.get("status") not in ("pending", "running"):
                continue
            created = self._parse_time(row.get("created_at"))
            if created is None or created > cutoff:
                continue
            lease = row.get("lease_expires_at")
            if lease is None or self._lease_expired(lease, grace_seconds=grace_seconds):
                rows.append(row)
        rows.sort(key=lambda row: row["created_at"])
        return rows

    async def create_thread_operation_atomic(
        self,
        run_id: str,
        *,
        thread_id: str,
        owner_worker_id: str,
        lease_expires_at: str | None,
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        created_at: str | None = None,
        grace_seconds: int = 10,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if idempotency_key is not None:
            for existing in self._runs.values():
                if existing.get("idempotency_key") == idempotency_key:
                    return existing, []

        now = datetime.now(UTC).isoformat()
        if multitask_strategy == "reject":
            for row in self._runs.values():
                if row["thread_id"] == thread_id and row.get("status") in ("pending", "running"):
                    raise ReactiveConflictError(
                        f"Thread {thread_id} already has an active run"
                    )

        claimed: list[dict[str, Any]] = []
        change_seq: int | None = None
        if multitask_strategy in ("interrupt", "rollback"):
            candidates: list[dict[str, Any]] = []
            for row in self._runs.values():
                if row["thread_id"] != thread_id:
                    continue
                if row.get("status") not in ("pending", "running"):
                    continue
                lease = row.get("lease_expires_at")
                if (
                    lease is not None
                    and not self._lease_expired(lease, grace_seconds=grace_seconds)
                    and row.get("owner_worker_id") != owner_worker_id
                ):
                    kind = row.get("operation_kind")
                    if kind and kind != "run":
                        raise ReactiveConflictError(f"Thread {thread_id} has an active checkpoint write")
                    raise ReactiveConflictError(
                        f"Thread {thread_id} already has an active run owned by another worker"
                    )
                candidates.append(row)
            change_seq = self._next_change_seq()
            for row in candidates:
                row["status"] = "interrupted"
                row["error"] = "Cancelled by newer run"
                row["owner_worker_id"] = owner_worker_id
                row["updated_at"] = now
                row["change_seq"] = change_seq
                claimed.append(row)

        row = {
            "run_id": run_id,
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "status": "pending",
            "operation_kind": operation_kind,
            "multitask_strategy": multitask_strategy,
            "metadata": metadata or {},
            "kwargs": kwargs or {},
            "error": None,
            "stop_reason": None,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
            "idempotency_key": idempotency_key,
            "cancel_action": None,
            "cancel_requested_at": None,
            "created_at": created_at or now,
            "updated_at": now,
        }
        row["change_seq"] = change_seq if change_seq is not None else self._next_change_seq()
        self._runs[run_id] = row
        self._runs_by_thread.setdefault(thread_id, {})[run_id] = None
        return row, claimed

    def _next_change_seq(self) -> int:
        self._change_seq += 1
        return self._change_seq

    @staticmethod
    def _parse_time(value: object) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @classmethod
    def _sort_key(cls, created_at: object, run_id: str) -> tuple[datetime, str]:
        return (cls._parse_time(created_at) or datetime.min.replace(tzinfo=UTC), run_id)

    @classmethod
    def _is_before_cursor(
        cls,
        row: dict[str, Any],
        *,
        before_created_at: str | None,
        before_run_id: str | None,
    ) -> bool:
        if not before_created_at or not before_run_id:
            return True
        return cls._sort_key(row["created_at"], row["run_id"]) < cls._sort_key(
            before_created_at,
            before_run_id,
        )

    @classmethod
    def _lease_expired(cls, value: str, *, grace_seconds: int) -> bool:
        lease = cls._parse_time(value)
        if lease is None:
            return True
        return lease < datetime.now(UTC) - timedelta(seconds=grace_seconds)


@dataclass(frozen=True)
class _ReactiveLeaseRenewal:
    renewed: bool
    cancel_action: str | None = None


@dataclass(frozen=True)
class _ReactiveStatusFinalization:
    finalized: bool
    cancel_action: str | None = None


class ReactiveRunManager:
    """In-memory subset of DeerFlow ``RunManager`` lifecycle + ownership state."""

    def __init__(
        self,
        store: ReactiveRunStore | None = None,
        *,
        worker_id: str | None = None,
        grace_seconds: int = 10,
        heartbeat_enabled: bool = False,
    ) -> None:
        self.runs: dict[str, ReactiveRunRecord] = {}
        self._store = store
        self._worker_id = worker_id or generate_worker_id()
        self._grace_seconds = grace_seconds
        self._heartbeat_enabled = heartbeat_enabled

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def heartbeat_enabled(self) -> bool:
        return self._heartbeat_enabled

    @property
    def grace_seconds(self) -> int:
        return self._grace_seconds

    def create_run(self, fields: dict[str, Any]) -> ReactiveRunRecord:
        record = ReactiveRunRecord(
            run_id=fields["run_id"],
            thread_id=fields["thread_id"],
            model_name=fields.get("model_name", "default"),
        )
        self.runs[record.run_id] = record
        if self._store is not None:
            self._store._put_sync(
                record.run_id,
                thread_id=record.thread_id,
                assistant_id=fields.get("assistant_id"),
                user_id=fields.get("user_id"),
                model_name=record.model_name,
                status="running",
                operation_kind=fields.get("operation_kind", "run"),
                multitask_strategy=fields.get("multitask_strategy", "reject"),
                metadata=fields.get("metadata"),
                kwargs=fields.get("kwargs"),
                created_at=fields.get("created_at"),
                owner_worker_id=fields.get("owner_worker_id"),
                lease_expires_at=fields.get("lease_expires_at"),
                idempotency_key=fields.get("idempotency_key"),
            )
        return record

    def get_run(self, run_id: str) -> ReactiveRunRecord:
        return self.runs[run_id]

    async def create_or_reject(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        multitask_strategy: str = "reject",
        owner_worker_id: str | None = None,
        lease_expires_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> ReactiveRunRecord:
        if self._store is None:
            raise RuntimeError("create_or_reject requires a ReactiveRunStore")
        run_id = f"run-{len(self._store._runs) + 1}"
        row, _ = await self._store.create_thread_operation_atomic(
            run_id,
            thread_id=thread_id,
            owner_worker_id=owner_worker_id or self._worker_id,
            lease_expires_at=lease_expires_at,
            multitask_strategy=multitask_strategy,
            user_id=user_id,
            model_name=model_name,
            metadata=metadata,
            kwargs=kwargs,
            idempotency_key=idempotency_key,
        )
        existing = self.runs.get(row["run_id"])
        if existing is not None:
            return existing
        record = ReactiveRunRecord(
            run_id=row["run_id"],
            thread_id=row["thread_id"],
            model_name=row.get("model_name") or "default",
        )
        self.runs[record.run_id] = record
        return record

    def set_status(self, run_id: str, status: str, error: str | None = None, **_: Any) -> None:
        record = self.runs[run_id]
        record.status = status
        record.error = error
        if self._store is not None:
            row = self._store._runs.get(run_id)
            if row is not None:
                row["status"] = status
                if error is not None:
                    row["error"] = error
                row["updated_at"] = datetime.now(UTC).isoformat()
                self._store._mark_changed(row)

    # -- ownership: durable reservation ------------------------------------

    @contextlib.asynccontextmanager
    async def reserve_thread_operation(
        self,
        thread_id: str,
        *,
        kind: str,
        user_id: str | None = None,
    ) -> AsyncIterator[None]:
        """Hold exclusive durable admission for a non-run thread operation.

        Mirrors upstream: the reservation is a short-lived pending row, so the
        same durable uniqueness constraint that guards ``create_or_reject``
        closes both sides of the cross-worker race.
        """
        if self._store is None:
            raise RuntimeError("reserve_thread_operation requires a ReactiveRunStore")
        if kind == "run":
            raise ValueError("Normal runs must be admitted with create_or_reject()")
        run_id = f"op-{uuid.uuid4().hex}"
        row, _ = await self._store.create_thread_operation_atomic(
            run_id,
            thread_id=thread_id,
            owner_worker_id=self._worker_id,
            lease_expires_at=(datetime.now(UTC) + timedelta(seconds=self._grace_seconds)).isoformat(),
            operation_kind=kind,
            multitask_strategy="reject",
            user_id=user_id,
        )
        try:
            yield
        finally:
            if row.get("operation_kind") != "run":
                await self._store.delete(row["run_id"])

    # -- ownership: cancellation -------------------------------------------

    async def cancel(self, run_id: str, *, action: str = "interrupt") -> CancelOutcome:
        """Request cancellation, mirroring upstream ``RunManager.cancel``.

        Local runs abort in-memory. Peer-owned runs with a still-valid lease get
        a durable request the owner observes on its next heartbeat; expired or
        null leases are taken over and marked ``error``.
        """
        record = self.runs.get(run_id)
        if record is not None:
            if record.status == "interrupted":
                return CancelOutcome.cancelled
            if record.status not in ("pending", "running"):
                return CancelOutcome.not_cancellable
            if self._store is not None and self._heartbeat_enabled:
                outcome, winning = await self._request_durable_cancel(run_id, action)
                if outcome is CancelOutcome.requested:
                    action = winning or action
                elif outcome is not CancelOutcome.unknown:
                    return outcome
            record.abort_action = action
            record.abort_event.set()
            record.status = "interrupted"
            if self._store is not None:
                await self._persist_status(run_id, "interrupted", abort_action=action)
            return CancelOutcome.cancelled

        if not self._heartbeat_enabled or self._store is None:
            return CancelOutcome.unknown if self._store is None else CancelOutcome.not_active_locally

        row = await self._store.get(run_id)
        if row is None:
            return CancelOutcome.unknown
        status = row.get("status")
        if status == "interrupted":
            return CancelOutcome.cancelled
        if status not in ("pending", "running"):
            return CancelOutcome.not_cancellable
        lease = row.get("lease_expires_at")
        owner = row.get("owner_worker_id")
        if owner in (None, self._worker_id) or self._store._lease_expired(lease, grace_seconds=self._grace_seconds):
            claimed = await self._store.claim_for_takeover(
                run_id,
                grace_seconds=self._grace_seconds,
                error="Cancelled by peer after lease expiry",
                owner_worker_id=self._worker_id,
            )
            if claimed:
                return CancelOutcome.taken_over
            return CancelOutcome.not_cancellable
        outcome, _ = await self._request_durable_cancel(run_id, action)
        return outcome

    async def _request_durable_cancel(self, run_id: str, action: str) -> tuple[CancelOutcome, str | None]:
        try:
            winning = await self._store.request_cancel(run_id, action=action)
        except Exception:  # noqa: BLE001 - durable cancel is best effort
            return CancelOutcome.unknown, None
        if winning is None:
            return CancelOutcome.not_cancellable, None
        row = await self._store.get(run_id)
        if row is not None and row.get("owner_worker_id") not in (None, self._worker_id):
            return CancelOutcome.requested, winning
        return CancelOutcome.requested, winning

    async def _persist_status(self, run_id: str, status: str, *, abort_action: str | None = None) -> bool:
        if self._store is None:
            return False
        return await self._store.update_status(run_id, status, abort_action=abort_action)

    # -- ownership: heartbeat ----------------------------------------------

    async def heartbeat(self) -> list[str]:
        """Renew this worker's leases and apply durable cancel requests.

        Returns the ids of locally-owned runs whose durable cancellation was
        observed, matching upstream heartbeat behaviour.
        """
        if self._store is None:
            return []
        observed: list[str] = []
        lease = (datetime.now(UTC) + timedelta(seconds=self._grace_seconds * 3)).isoformat()
        for run_id, record in list(self.runs.items()):
            if record.status not in ("pending", "running"):
                continue
            renewal = await self._store.renew_lease(run_id, owner_worker_id=self._worker_id, lease_expires_at=lease)
            if not renewal.renewed:
                continue
            if renewal.cancel_action is None:
                continue
            if record.abort_event.is_set():
                continue
            record.abort_action = renewal.cancel_action or "interrupt"
            record.abort_event.set()
            record.status = "interrupted"
            await self._persist_status(run_id, "interrupted", abort_action=record.abort_action)
            observed.append(run_id)
        return observed

    # -- ownership: reconciliation -----------------------------------------

    async def reconcile_orphaned_inflight_runs(
        self,
        *,
        error: str,
        before: str | None = None,
        stop_reason: str | None = None,
    ) -> list[ReactiveRunRecord]:
        """Reclaim runs whose lease expired, mirroring upstream reconciliation.

        A candidate scan is only an optimization: each row is claimed with a
        lease-aware conditional update, so a heartbeat renewal after the scan
        always wins over reconciliation.
        """
        if self._store is None:
            return []
        rows = await self._store.list_inflight_with_expired_lease(before=before, grace_seconds=self._grace_seconds)
        recovered: list[ReactiveRunRecord] = []
        for row in rows:
            run_id = row["run_id"]
            if row.get("operation_kind") != "run":
                # Internal reservations are released, not reported as runs.
                if await self._store.claim_for_takeover(
                    run_id,
                    grace_seconds=self._grace_seconds,
                    error=error,
                    stop_reason=stop_reason,
                    owner_worker_id=self._worker_id,
                ):
                    await self._store.delete(run_id)
                continue
            live = self.runs.get(run_id)
            if live is not None and live.status in ("pending", "running"):
                continue
            claimed = await self._store.claim_for_takeover(
                run_id,
                grace_seconds=self._grace_seconds,
                error=error,
                stop_reason=stop_reason,
                owner_worker_id=self._worker_id,
            )
            if not claimed:
                continue
            record = self.runs.get(run_id) or ReactiveRunRecord(
                run_id=run_id,
                thread_id=row["thread_id"],
                model_name=row.get("model_name") or "default",
            )
            record.status = "error"
            record.error = error
            self.runs[run_id] = record
            recovered.append(record)
        return recovered


class ReactiveRunEventStore:
    """In-memory run event store mirroring DeerFlow's ``MemoryRunEventStore``.

    Events carry a per-thread monotonic ``seq`` so readers can page a stable
    order. ``put_batch`` assigns one sequence per event, matching upstream.
    """

    def __init__(self) -> None:
        self.events: dict[str, list[dict[str, Any]]] = {}
        self._seq_counters: dict[str, int] = {}

    def _next_seq(self, thread_id: str) -> int:
        value = self._seq_counters.get(thread_id, 0) + 1
        self._seq_counters[thread_id] = value
        return value

    def _put_one(
        self,
        *,
        thread_id: str,
        run_id: str,
        event_type: str,
        category: str,
        content: Any = "",
        metadata: dict[str, Any] | None = None,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        record = {
            "thread_id": thread_id,
            "run_id": run_id,
            "event_type": event_type,
            "category": category,
            "content": content,
            "metadata": metadata or {},
            "seq": self._next_seq(thread_id),
            "created_at": created_at or datetime.now(UTC).isoformat(),
        }
        self.events.setdefault(thread_id, []).append(record)
        return record

    async def put(
        self,
        *,
        thread_id: str,
        run_id: str,
        event_type: str,
        category: str,
        content: Any = "",
        metadata: dict[str, Any] | None = None,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        return self._put_one(
            thread_id=thread_id,
            run_id=run_id,
            event_type=event_type,
            category=category,
            content=content,
            metadata=metadata,
            created_at=created_at,
        )

    async def put_batch(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            self._put_one(
                thread_id=event["thread_id"],
                run_id=event["run_id"],
                event_type=event["event_type"],
                category=event["category"],
                content=event.get("content", ""),
                metadata=event.get("metadata"),
                created_at=event.get("created_at"),
            )
            for event in events
        ]

    async def list_events(
        self,
        thread_id: str,
        *,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        records = list(self.events.get(thread_id, []))
        if run_id is not None:
            records = [record for record in records if record["run_id"] == run_id]
        return records


class ReactiveRunJournal:
    """Minimal port of DeerFlow's ``RunJournal``.

    Buffers event records and flushes them to a
    :class:`ReactiveRunEventStore` in batches. Unflushed events stay buffered
    (as upstream does without a running loop) and are written by ``flush()``
    or ``close()``.
    """

    def __init__(
        self,
        run_id: str,
        thread_id: str,
        event_store: ReactiveRunEventStore,
        *,
        flush_threshold: int = 20,
    ) -> None:
        self.run_id = run_id
        self.thread_id = thread_id
        self._store = event_store
        self._flush_threshold = max(1, flush_threshold)
        self._buffer: list[dict[str, Any]] = []
        self._closed = False
        self._feed_generation = 0
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._total_tokens = 0
        self._llm_call_count = 0
        self._message_count = 0
        self._last_ai_message: str | None = None
        self._first_human_message: str | None = None
        self._produced_artifacts: list[tuple[str, str | None]] = []
        self._seen_artifact_keys: set[tuple[str, str | None]] = set()
        self._last_observed_signature: tuple[Any, ...] | None = None

    def _make_event(
        self,
        *,
        event_type: str,
        category: str,
        content: Any = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "thread_id": self.thread_id,
            "run_id": self.run_id,
            "event_type": event_type,
            "category": category,
            "content": content,
            "metadata": metadata or {},
            "created_at": datetime.now(UTC).isoformat(),
        }

    def record(
        self,
        *,
        event_type: str,
        category: str,
        content: Any = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self._closed:
            return
        self._buffer.append(
            self._make_event(
                event_type=event_type,
                category=category,
                content=content,
                metadata=metadata,
            )
        )
        if len(self._buffer) >= self._flush_threshold:
            self._flush_sync()

    def _flush_sync(self) -> None:
        """Schedule an async flush when a loop is running, else keep buffering."""
        if not self._buffer:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        batch = self._buffer[:]
        del self._buffer[:]
        loop.create_task(self._flush_async(batch))

    async def _flush_async(self, batch: list[dict[str, Any]]) -> None:
        try:
            await self._store.put_batch(batch)
            self._feed_generation += 1
        except Exception:  # noqa: BLE001 - mirror upstream: requeue and retry
            self._buffer = batch + self._buffer

    async def flush(self) -> None:
        if self._closed:
            return
        while self._buffer:
            batch = self._buffer[: self._flush_threshold]
            del self._buffer[: self._flush_threshold]
            try:
                await self._store.put_batch(batch)
                self._feed_generation += 1
            except Exception:
                self._buffer = batch + self._buffer
                raise

    async def close(self, *, flush: bool = True) -> None:
        if self._closed:
            return
        if flush:
            await self.flush()
        self._closed = True

    def observe_state(self, state: dict[str, Any]) -> None:
        """Accumulate completion facts from one cumulative graph state.

        DeerFlow's production journal accumulates through LangChain callbacks;
        this port observes the final state once per run. Usage may live at the
        top level (upstream ``AgentState``) or under ``assistant`` (the
        ReactiveGraph task output), so both shapes are accepted.
        """
        messages = [
            message for message in state.get("messages", []) if isinstance(message, dict)
        ]
        usage = state.get("usage_metadata") or {}
        if not usage:
            assistant = state.get("assistant")
            if isinstance(assistant, dict):
                usage = assistant.get("usage_metadata") or {}
        signature = (
            len(messages),
            self._last_ai_message,
            int(usage.get("input_tokens", 0)),
            int(usage.get("output_tokens", 0)),
            int(usage.get("total_tokens", 0)),
        )
        if signature == self._last_observed_signature:
            return
        self._last_observed_signature = signature
        self._total_input_tokens += int(usage.get("input_tokens", 0))
        self._total_output_tokens += int(usage.get("output_tokens", 0))
        self._total_tokens += int(usage.get("total_tokens", 0))
        for message in messages:
            role = message.get("role")
            content = _as_text(message.get("content"))
            self._message_count += 1
            if role == "user" and self._first_human_message is None:
                self._first_human_message = content
            elif role == "assistant":
                self._last_ai_message = content
                self._llm_call_count += 1

    def record_artifact(self, path: str, tool_name: str | None = None) -> None:
        key = (path, tool_name)
        if key in self._seen_artifact_keys:
            return
        self._seen_artifact_keys.add(key)
        self._produced_artifacts.append(key)

    def get_delivery_content(self) -> dict[str, Any]:
        by_tool: dict[str, list[str]] = {}
        paths: list[str] = []
        for path, tool_name in self._produced_artifacts:
            paths.append(path)
            if tool_name:
                by_tool.setdefault(tool_name, []).append(path)
        return {"presented": len(paths), "paths": paths, "by_tool": by_tool}

    def record_delivery(self) -> None:
        self.record(
            event_type="run.delivery",
            category="outputs",
            content=self.get_delivery_content(),
        )

    def get_completion_data(self) -> dict[str, Any]:
        return {
            "total_input_tokens": self._total_input_tokens,
            "total_output_tokens": self._total_output_tokens,
            "total_tokens": self._total_tokens,
            "llm_call_count": self._llm_call_count,
            "message_count": self._message_count,
            "last_ai_message": self._last_ai_message,
            "first_human_message": self._first_human_message,
        }

    @property
    def feed_generation(self) -> int:
        return self._feed_generation


@dataclass(frozen=True)
class ReactiveStreamEvent:
    """One bridge event, mirroring DeerFlow's ``StreamEvent`` shape."""

    id: str
    event: str
    data: Any


@dataclass(frozen=True)
class ReactiveStreamGap:
    """A subscriber cursor can no longer be replayed completely."""

    requested_event_id: str | None
    earliest_available_event_id: str | None
    latest_available_event_id: str | None


REACTIVE_HEARTBEAT = ReactiveStreamEvent(id="", event="__heartbeat__", data=None)
REACTIVE_END = ReactiveStreamEvent(id="", event="__end__", data=None)
_REACTIVE_STREAM_ID_RE = re.compile(r"\d+-(\d+)")


@dataclass
class _ReactiveRunStream:
    events: list[ReactiveStreamEvent] = field(default_factory=list)
    # One wake event per active subscriber. ``publish`` is synchronous, so it
    # sets these instead of notifying an ``asyncio.Condition``.
    waiters: set[asyncio.Event] = field(default_factory=set)
    ended: bool = False
    start_offset: int = 0


class ReactiveRunBridge:
    """In-process stream bridge mirroring DeerFlow's MemoryStreamBridge.

    Producers publish events under a run id; subscribers replay retained
    history, follow live events, receive heartbeats while idle, and observe a
    terminal ``__end__`` sentinel. A bounded buffer reports
    :class:`ReactiveStreamGap` when a reconnect cursor is too old to replay.
    """

    def __init__(self, *, queue_maxsize: int = 256, heartbeat_interval: float = 15.0) -> None:
        self._maxsize = max(1, queue_maxsize)
        self._heartbeat_interval = heartbeat_interval
        self._streams: dict[str, _ReactiveRunStream] = {}
        self._counters: dict[str, int] = {}
        # Flat observability log, kept for worker-level assertions.
        self.events: list[tuple[str, Any]] = []
        self.event_ids: list[str] = []
        self.ended: set[str] = set()

    def _get_or_create_stream(self, run_id: str) -> _ReactiveRunStream:
        if run_id not in self._streams:
            self._streams[run_id] = _ReactiveRunStream()
            self._counters[run_id] = 0
        return self._streams[run_id]

    def _next_id(self, run_id: str) -> str:
        self._counters[run_id] = self._counters.get(run_id, 0) + 1
        return f"{int(time.time() * 1000)}-{self._counters[run_id] - 1}"

    @staticmethod
    def _parse_event_seq(event_id: str) -> int | None:
        match = _REACTIVE_STREAM_ID_RE.fullmatch(event_id)
        return int(match.group(1)) if match else None

    @staticmethod
    def _make_gap(stream: _ReactiveRunStream, requested_event_id: str | None) -> ReactiveStreamGap:
        return ReactiveStreamGap(
            requested_event_id=requested_event_id,
            earliest_available_event_id=stream.events[0].id if stream.events else None,
            latest_available_event_id=stream.events[-1].id if stream.events else None,
        )

    def _resolve_start_offset(
        self,
        stream: _ReactiveRunStream,
        last_event_id: str | None,
    ) -> int | ReactiveStreamGap:
        if last_event_id is None:
            return stream.start_offset
        seq = self._parse_event_seq(last_event_id)
        if seq is not None:
            if seq < stream.start_offset:
                return self._make_gap(stream, last_event_id)
            local_index = seq - stream.start_offset
            if 0 <= local_index < len(stream.events) and stream.events[local_index].id == last_event_id:
                return stream.start_offset + local_index + 1
        # Unknown ids at or above the watermark replay from the earliest
        # retained event, matching upstream's conservative legacy behavior.
        return stream.start_offset

    def stream_exists(self, run_id: str) -> bool:
        return run_id in self._streams

    def publish(self, run_id: str, mode: str, payload: Any) -> None:
        stream = self._get_or_create_stream(run_id)
        entry = ReactiveStreamEvent(id=self._next_id(run_id), event=mode, data=payload)
        stream.events.append(entry)
        if len(stream.events) > self._maxsize:
            overflow = len(stream.events) - self._maxsize
            del stream.events[:overflow]
            stream.start_offset += overflow
        for waiter in stream.waiters:
            waiter.set()
        self.events.append((mode, payload))
        self.event_ids.append(entry.id)

    def publish_end(self, run_id: str) -> None:
        stream = self._get_or_create_stream(run_id)
        stream.ended = True
        for waiter in stream.waiters:
            waiter.set()
        self.ended.add(run_id)

    async def subscribe(
        self,
        run_id: str,
        *,
        last_event_id: str | None = None,
        heartbeat_interval: float | None = None,
    ) -> AsyncIterator[ReactiveStreamEvent | ReactiveStreamGap]:
        interval = (
            self._heartbeat_interval if heartbeat_interval is None else heartbeat_interval
        )
        stream = self._get_or_create_stream(run_id)
        start = self._resolve_start_offset(stream, last_event_id)
        if isinstance(start, ReactiveStreamGap):
            yield start
            return
        next_offset = start
        cursor_event_id = last_event_id
        while True:
            if next_offset < stream.start_offset:
                yield self._make_gap(stream, cursor_event_id)
                return
            local_index = next_offset - stream.start_offset
            if 0 <= local_index < len(stream.events):
                entry = stream.events[local_index]
                next_offset += 1
                cursor_event_id = entry.id
                yield entry
                continue
            if stream.ended:
                yield REACTIVE_END
                return
            # Register the waiter and re-check in the same synchronous step so
            # no publish can slip between the buffer check and the wait.
            wake = asyncio.Event()
            stream.waiters.add(wake)
            try:
                await asyncio.wait_for(wake.wait(), timeout=interval)
            except TimeoutError:
                yield REACTIVE_HEARTBEAT
            finally:
                stream.waiters.discard(wake)

    async def cleanup(self, run_id: str, *, delay: float = 0) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        self._streams.pop(run_id, None)
        self._counters.pop(run_id, None)

    async def close(self) -> None:
        self._streams.clear()
        self._counters.clear()


async def reactive_run_agent(
    *,
    bridge: ReactiveRunBridge,
    run_manager: ReactiveRunManager,
    record: ReactiveRunRecord,
    graph: _ReactiveDeerFlowGraph,
    graph_input: dict[str, Any],
    config: dict[str, Any],
    stream_modes: list[str] | None = None,
    journal: ReactiveRunJournal | None = None,
) -> list[tuple[str, Any]]:
    """Run a reactive DeerFlow graph as a background worker.

    This is a minimal version of upstream `runtime.runs.worker.run_agent`:
    it publishes public stream modes, supports cooperative cancellation,
    journals lifecycle/completion facts when a journal is supplied, and always
    terminates the run record. Goal continuations, rollback, extension hooks,
    durable persistence, and post-terminal side effects remain upstream-only.
    """
    events: list[tuple[str, Any]] = []
    modes = stream_modes or ["values"]
    metadata = {"run_id": record.run_id, "thread_id": record.thread_id}
    events.append(("metadata", metadata))
    bridge.publish(record.run_id, "metadata", metadata)
    if journal is not None:
        journal.record(event_type="run.start", category="lifecycle")
    latest_state: dict[str, Any] = {}
    try:
        iterator = iter(
            graph.stream(
                graph_input,
                config,
                stream_mode=modes,
            )
        )
        while True:
            if record.abort_event.is_set():
                break
            def _next(iterator=iterator) -> tuple[bool, tuple[str, Any] | None]:
                try:
                    return True, next(iterator)
                except StopIteration:
                    return False, None

            has_event, event = await asyncio.to_thread(_next)
            if not has_event or event is None:
                break
            mode, payload = event
            if mode == "values" and isinstance(payload, dict):
                latest_state = payload
            events.append((mode, payload))
            bridge.publish(record.run_id, mode, payload)
        if record.abort_event.is_set():
            run_manager.set_status(record.run_id, "interrupted")
        else:
            run_manager.set_status(record.run_id, "success")
    except Exception as exc:
        error_msg = str(exc)
        run_manager.set_status(record.run_id, "error", error=error_msg)
        bridge.publish(
            record.run_id,
            "error",
            {"message": error_msg, "name": type(exc).__name__},
        )
        raise
    finally:
        if journal is not None:
            journal.observe_state(latest_state)
            journal.record(
                event_type="run.end",
                category="lifecycle",
                content={"status": record.status, "error": record.error},
            )
            journal.record_delivery()
            await journal.flush()
        terminal = (record.status, {"status": record.status})
        events.append(("terminal", terminal[1]))
        bridge.publish(record.run_id, "terminal", terminal[1])
        bridge.publish_end(record.run_id)
    return events


def _tool_error_message(call: dict[str, Any], exc: Exception) -> dict[str, Any]:
    detail = str(exc).strip() or type(exc).__name__
    if len(detail) > 500:
        detail = detail[:497] + "..."
    tool_name = str(call.get("name") or "unknown_tool")
    content = (
        f"Error: Tool '{tool_name}' failed with {type(exc).__name__}: {detail}. "
        "Continue with available context, or choose an alternative tool."
    )
    return {
        "role": "tool",
        "tool_call_id": call.get("id", ""),
        "name": tool_name,
        "content": content,
        "status": "error",
    }


def create_deerflow_agent(
    model: Any,
    tools: list[Any] | None = None,
    *,
    system_prompt: str | None = None,
    middleware: list[Any] | None = None,
    features: Any | None = None,
    extra_middleware: list[Any] | None = None,
    plan_mode: bool = False,
    state_schema: type | None = None,
    checkpoint_channel_mode: str = "full",
    checkpoint_snapshot_frequency: int | None = None,
    checkpointer: Any | None = None,
    name: str = "default",
    subagent_runtime: Any | None = None,
    summarizer: Any | None = None,
    summarization_trigger: tuple[str, int] | None = None,
    summarization_keep: tuple[str, int] | None = None,
    **kwargs: Any,
) -> _ReactiveDeerFlowGraph:
    """Create a ReactiveGraph-backed DeerFlow-compatible baseline agent.

    Supported baseline: model + tools + system prompt. Middleware, features,
    checkpointer adapters, delta channels, plan mode, and subagent runtime are
    intentionally rejected rather than silently approximated.
    """
    unsupported = {
        "middleware": middleware,
        "features": features,
        "extra_middleware": extra_middleware,
        "plan_mode": plan_mode,
        "state_schema": state_schema,
        "checkpoint_channel_mode": checkpoint_channel_mode != "full",
        "checkpoint_snapshot_frequency": checkpoint_snapshot_frequency,
        "subagent_runtime": subagent_runtime,
        "kwargs": kwargs,
    }
    del kwargs
    active = [key for key, value in unsupported.items() if value]
    if active:
        raise ValueError(f"Reactive DeerFlow baseline does not support: {', '.join(active)}")

    effective_tools = list(tools or [])
    bind_tools = getattr(model, "bind_tools", None)
    if callable(bind_tools):
        model = bind_tools(effective_tools)

    model_call_count = 0
    usage_totals: dict[str, int] = {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }
    tool_call_counts: dict[str, int] = {
        tool.name: 0 for tool in effective_tools if getattr(tool, "name", None)
    }

    def model_task(state: dict[str, Any]) -> dict[str, Any]:
        nonlocal model_call_count
        model_call_count += 1
        messages = list(state.get("messages", []))
        prompt = state.get("system_prompt") or system_prompt or ""
        model_messages = ([{"role": "system", "content": prompt}] if prompt else []) + messages
        assistant = _invoke_model(model, model_messages)
        interrupted = any(
            call.get("name") == "ask_clarification"
            for call in assistant.get("tool_calls", [])
        )
        return {
            "assistant": assistant,
            "model_calls": model_call_count,
            "interrupted": interrupted,
        }

    def tool_task(state: dict[str, Any]) -> dict[str, Any]:
        assistant = state.get("assistant") or {}
        calls = assistant.get("tool_calls") or []
        # Do not early-return: historical dangling tool calls still need a
        # synthetic response before the next model call.
        by_name = {
            tool.name: tool
            for tool in effective_tools
            if getattr(tool, "name", None)
        }
        results: list[dict[str, Any]] = []

        # Dangling ToolCall middleware subset: answer historical assistant
        # tool calls that never received a ToolMessage (interrupt/cancel).
        answered = {
            str(message.get("tool_call_id"))
            for message in state.get("messages", [])
            if message.get("role") == "tool" and message.get("tool_call_id")
        }
        for message in state.get("messages", []):
            if message.get("role") != "assistant":
                continue
            for call in message.get("tool_calls") or []:
                call_id = str(call.get("id") or "")
                if not call_id or call_id in answered:
                    continue
                answered.add(call_id)
                results.append(
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "name": str(call.get("name") or "unknown_tool"),
                        "content": (
                            "Tool call could not be executed. Continue with available "
                            "context, or choose an alternative tool."
                        ),
                        "status": "error",
                    }
                )

        # ToolErrorHandling middleware subset: exceptions become recoverable
        # ToolMessages instead of aborting the run.
        command_updates: dict[str, Any] = {}
        for call in calls:
            tool_name = call.get("name", "")
            selected = by_name.get(tool_name)
            if selected is None:
                results.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.get("id", ""),
                        "name": tool_name,
                        "content": f"Error: unknown tool {tool_name!r}",
                        "status": "error",
                    }
                )
                continue
            try:
                result = _invoke_tool(
                    selected,
                    dict(call.get("args") or call.get("arguments") or {}),
                )
            except Exception as exc:  # noqa: BLE001 - recoverable tool error
                results.append(_tool_error_message(call, exc))
                continue
            update = getattr(result, "update", None)
            if update is None and isinstance(result, dict) and isinstance(result.get("update"), dict):
                update = result["update"]
            if isinstance(update, dict):
                command_updates.update({k: v for k, v in update.items() if k != "messages"})
                command_messages = update.get("messages")
                if isinstance(command_messages, list):
                    results.extend(command_messages)
                else:
                    results.append(
                        {
                            "role": "tool",
                            "tool_call_id": call.get("id", ""),
                            "name": tool_name,
                            "content": _as_text(result),
                        }
                    )
            else:
                tool_call_counts[tool_name] += 1
                results.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.get("id", ""),
                        "name": tool_name,
                        "content": result,
                    }
                )
        output: dict[str, Any] = {"tool_messages": results, "tool_calls": tool_call_counts}
        output.update(command_updates)
        return output

    def final_task(state: dict[str, Any]) -> dict[str, Any]:
        nonlocal model_call_count
        # If tools ran, the same model receives their results; otherwise return
        # the first model response.
        assistant = state.get("assistant") or {}
        tool_messages = state.get("tool_messages") or []
        if state.get("return_direct"):
            messages = list(state.get("messages", [])) + [assistant] + tool_messages
            return {
                "messages": messages,
                "final": messages[-1] if messages else assistant,
                "return_direct": True,
                "interrupted": False,
            }
        if not tool_messages:
            messages = list(state.get("messages", []))
            return {
                "messages": [*messages, assistant],
                "final": assistant,
                "model_calls": model_call_count,
                "interrupted": bool(state.get("interrupted")),
            }
        messages = list(state.get("messages", [])) + [assistant] + tool_messages
        next_assistant = _invoke_model(model, _model_input(messages))
        model_call_count += 1
        return {
            "messages": [*messages, next_assistant],
            "final": next_assistant,
            "model_calls": model_call_count,
            "interrupted": bool(state.get("interrupted")),
        }

    def build(builder: GraphBuilder) -> None:
        builder.task(
            "model",
            kind="pure",
            fn=model_task,
            on=("run",),
            reads=("messages", "system_prompt"),
            writes=("assistant",),
        )
        builder.task(
            "tools",
            kind="pure",
            fn=tool_task,
            on=("model:written",),
            reads=("messages", "assistant"),
            writes=("tool_messages",),
        )
        builder.task(
            "final",
            kind="pure",
            fn=final_task,
            on=("tools:written",),
            reads=("messages", "assistant", "tool_messages"),
            writes=("messages", "final"),
        )

    graph = ReactiveGraph.build(build, graph_id=f"deerflow_reactive_{name}")
    return _ReactiveDeerFlowGraph(
        graph=graph,
        model=model,
        tools=effective_tools,
        checkpointer=checkpointer,
        usage_totals=usage_totals,
        summarizer=summarizer,
        summarization_trigger=summarization_trigger,
        summarization_keep=summarization_keep,
    )
