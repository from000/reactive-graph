# Real DeerFlow Port PoC — ReactiveGraph Base Replacement

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the runtime base of a small but real `bytedance/deer-flow` path with ReactiveGraph and verify semantic parity plus selective-execution advantages.

**Architecture:** Use DeerFlow's own `create_deerflow_agent` API as the cut point. Build a same-shaped ReactiveGraph graph that accepts DeerFlow model/tool-like objects without importing LangChain at runtime. Reuse DeerFlow's actual middleware feature API only where its interfaces are plain Python; keep unsupported middleware out of the PoC rather than silently changing semantics.

**Tech Stack:** Python 3.12, ReactiveGraph, pytest, real `bytedance/deer-flow` source pinned at `5d8a9492eea97e2f06f28d46df69c29355acf4da`.

---

## Source state

Cloned outside this repository:

```text
$DEERFLOW_UPSTREAM_ROOT
commit 5d8a9492eea97e2f06f28d46df69c29355acf4da
```

Scale:

- backend harness: 589 Python files
- backend tests: 791 test files
- frontend: 1057 files
- direct LangChain/LangGraph references: 172 harness Python files
- agent assembly uses `langchain.agents.create_agent`
- state uses `AgentState`, reducers, `DeltaChannel`, and `Command`
- runtime expects `astream(values/messages/updates)` and checkpoint APIs

## PoC boundary

A full DeerFlow product port is not feasible as one task. The honest PoC is:

- keep DeerFlow's public factory signature
- support model callable, tools, system prompt, feature flags off
- map native messages/tool calls to ReactiveGraph
- preserve invoke result shape
- provide `stream` for values/messages/custom
- prove selective execution across repeated identical turns
- run against representative DeerFlow-style tests adapted from its source
- do not claim middleware, gateway, sandbox, channels, MCP, or frontend parity

## Task 1: Reactive DeerFlow agent factory

**Files:**
- Create: `benchmarks/deerflow_real/deerflow_reactive.py`
- Test: `benchmarks/deerflow_real/test_agent.py`

**Behavior:**

```python
from deerflow_reactive import create_deerflow_agent
```

The returned object must support:

```python
out = graph.invoke({"messages": [...]}, {"configurable": {"thread_id": "..."}})
for ev in graph.stream({"messages": [...]}, {"configurable": {"thread_id": "..."}}):
    ...
```

**Acceptance:**
- model callable is called
- tool call invokes tool
- tool message is appended
- final assistant response is returned
- no LangChain/LangGraph import in the implementation module
- no dependency on LangChain tool object internals

## Task 2: Selective execution proof

**Acceptance:**
- identical run skips pure model/task work
- changed user input reruns
- `why_skipped`, `explain_run`, `export_trace` work
- repeated turn performance is not worse
- task execution count is recorded

## Task 3: Real-source-derived tests

Adapt tests directly from DeerFlow source:

- `backend/tests/test_create_deerflow_agent.py`
- minimal model/tool mocks from upstream tests
- state shape and turn behavior

**Acceptance:**
- no execution-time LangChain import in reactive module
- same input/output for supported cases
- unsupported feature errors explicitly, not silent drift

## Task 4: Real-project report

**Deliverable:** `benchmarks/deerflow_real/REPORT.md`

It must say:

- exact DeerFlow commit
- what was replaced
- what was not replaced
- tests run
- output parity
- performance/selectivity evidence
- why this is a real-path proof, not a full product claim

## Verification

```bash
uv run --directory python/reactivechain python -m pytest "$PWD/benchmarks/deerflow_real/" -q
```

Full regression:

```bash
REPO_ROOT="$PWD" uv run --directory python/reactivegraph python -m pytest -q
uv run --directory python/reactivechain python -m pytest -q
pnpm --dir packages/driver test
```
