# Beyond LangGraph / LangChain Gap Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the remaining productization gaps required to be the preferred reactive, explainable, production-grade agent runtime — not merely a faster graph executor.

**Architecture:** Build on the already working RGP/1 Driver, reactive scheduling, durable receipts, checkpoints, tool protocol, and computed wire. The plan upgrades three user-facing surfaces: explainability, agent orchestration, and operational production readiness. Each phase has measurable acceptance criteria and failing-first tests.

**Tech Stack:** TypeScript Driver + Reactivity, Python 3.10+, pytest, vitest, RGP/1, SQLite/Postgres/Redis, OTel-like spans.

---

## Current Position

### Already strong / ahead

| Dimension | Current state | Evidence |
|---|---|---|
| Selective execution | Pure fingerprint skips, computed read-set invalidation, effect receipts, real Driver path | `benchmarks/differential`; 130p+1s reactivegraph, 207p reactivechain, 180p Driver |
| Durability | Checkpoints, durable log, restart-safe effect receipts | Driver durability tests + restart e2e |
| Correctness | Read/write tracking, transactions, parallel batches, write-conflict detection | Driver scheduler tests |
| Tool protocol | JSON Schema, async tools, artifacts, receipts, injected state/secrets, return_direct, streaming | Reactive Tools commits |
| Remote runtime | Python/JS clients, RGP/1 Driver, gateway, multi-tenant, TLS | gateway and protocol tests |
| Benchmarks | Real upstream differential, same-output assertion, no-gain row | `docs/benchmarks.md` |
| Computed on real Driver | RGP/1 wire, host callback, scheduler cache/invalidation, `why_invalidated()` | commit `93cd215` |

### Remaining gaps by target dimension

The goal is **“preferred runtime for complex production agents.”** That is stronger than feature parity. It means users migrating from LangGraph/LangChain should get lower cost, fewer duplicate side effects, and better operational answers without losing essential workflow ergonomics.

#### Capability scorecard (0–5)

| Dimension | Current | Target | Main gap |
|---|---:|---:|---|
| Reactive execution correctness | 4.5 | 5 | conflict explanation; provenance details |
| Explainability / audit | 3.5 | 5 | no conflict explain, no historical explain, weak cost model |
| Agent ergonomics | 3.5 | 5 | multi tool-call fanout; agent-level state model |
| Production operations | 3.5 | 5 | remote security hardening, deploy guide, load tests |
| Ecosystem breadth | 2.0 | 3.5 | integration count intentionally limited; adapters needed |
| Migration / compatibility | 2.0 | 3.0 | no read-only LangGraph importer / migration analyzer |
| Docs & examples | 3.0 | 4.5 | explainability and migration guides absent |
| Community & release | 2.0 | 4.0 | no public users/stars/docs site/secrets configured |

**Overall engineering completion:** approximately **78–82%** toward the internal technical goal.  
**Overall product competitiveness:** approximately **65–70%** toward “clearly preferred over LangGraph/LangChain for complex production agents.”  
**Current claim:** we can honestly claim a strong architectural lead on reactive execution, durability, and correctness. We cannot yet claim complete replacement for LangGraph’s ecosystem or LangChain’s integration breadth.

---

## Non-Negotiable Principles

1. **No false parity.** Do not advertise feature parity with LangGraph/LangChain.
2. **Native first, adapter second.** Core APIs follow ReactiveGraph semantics.
3. **Every decision is explainable.** Execution, skip, invalidation, conflict, side effect, and cost all need a reason.
4. **Every optimization claim is reproducible.** Performance/cost claims link to a command and raw output.
5. **Failing test first.** Each phase starts with a red test or benchmark.
6. **One independent commit per task.**
7. **No LangChain runtime dependency.** Optional adapters only.

---

## Phase P4 — Explainable Runtime Closure

**Why first:** our strongest architectural claim must be fully user-visible. We already answer “why skipped” and “why invalidated”; users still cannot ask “why conflict” or “why did historical run 42 behave that way.”

### P4.1 `explain_conflict()`

**Files:**
- Modify: `packages/driver/src/scheduler/scheduler.ts`
- Modify: `packages/driver/src/runtime.ts`
- Modify: `python/reactivegraph/reactivegraph/graph.py`
- Test: `python/reactivegraph/tests/test_observability.py`
- Test: `packages/driver/test/scheduler/scheduler.test.ts`

**Design:**
- Scheduler records every rejected write conflict as a decision object:
  - `runId`, `taskId`, `reason` (`declared_write_conflict` / `actual_write_conflict`)
  - conflict `path`
  - `winnerTaskId`
  - declared and actual write sets
- Span channel emits `span:conflict`.
- Python normalizes it into `_conflicts`.
- API returns:

```python
g.explain_conflict()
```

Example:

```python
{
  "kind": "write_conflict",
  "reason": "actual_write_conflict",
  "task": "b",
  "winner": "a",
  "path": "x",
  "declared_writes": {"a": ["x"], "b": []},
  "actual_writes": {"a": ["x"], "b": ["x"]},
}
```

**Acceptance:**
- Declared conflict test returns exact reason/path/winner.
- Under-declared actual conflict test distinguishes `actual_write_conflict`.
- Fallback and Driver APIs have the same response shape.
- No conflict → clear `GraphBuildError("no conflict decision found")`.

### P4.2 Historical `explain_run(run_id=...)`

**Files:**
- Modify: Driver durable log/query APIs.
- Modify: `python/reactivegraph/reactivegraph/host.py`
- Modify: `python/reactivegraph/reactivegraph/graph.py`
- Test: `python/reactivegraph/tests/test_observability.py`

**Design:**
- Persist normalized decisions, not just raw events.
- Add a small trace query API over durable log/checkpoints.
- Python exposes:

```python
g.explain_run(run_id="run-42")
```

**Acceptance:**
- Works after process restart.
- Returns same shape as latest-run explain.
- Unknown run → deterministic error.
- No historical mutation: query is read-only.

### P4.3 Provenance and causal trace export

**Files:**
- Modify: `packages/driver/src/trace.ts`
- Modify: `python/reactivegraph/reactivegraph/graph.py`
- Test: `python/reactivegraph/tests/test_observability.py`

**Design:**
- Export one causal DAG containing:
  - run/event
  - task start/end/error
  - read set
  - write set
  - skip reason
  - invalidation reason
  - conflict decision
  - receipt decision
  - checkpoint
- Export as JSON and DOT.

**Acceptance:**
- Every final state top-level key has a producer.
- Every task decision has a cause.
- DOT renders acyclic graph.
- Restart-stable run IDs.

### P4.4 Cost model

**Files:**
- Modify: Python graph API and tool metadata.
- Test: `python/reactivegraph/tests/test_observability.py`
- Test: `python/reactivechain/tests/test_tools.py`

**Design:**
- Task/tool metadata can declare estimated cost:
  - `llm_tokens_in/out`
  - `estimated_usd`
  - `cost_unit`
- `cost_saved()` upgrades to:

```python
{
  "executions": 3,
  "skipped": 7,
  "llm_calls_saved": 2,
  "tools_saved": 1,
  "tokens_in_saved": 1000,
  "tokens_out_saved": 500,
  "estimated_usd_saved": 0.03,
}
```

**Acceptance:**
- Skipped LLM and tool tasks contribute estimated savings.
- Unknown cost is explicit, never silently zero.
- Driver and fallback agree.
- Cost model docs distinguish measured vs estimated.

### P4 regression gate

```bash
uv run --directory python/reactivegraph pytest -q
uv run --directory python/reactivechain pytest -q
uv run --directory python/reactivechain pytest "$PWD/benchmarks/e2e/" -q
pnpm --dir packages/driver test
uvx ruff check python/reactivegraph python/reactivechain
uvx mypy python/reactivegraph/reactivegraph
uvx mypy python/reactivechain/reactivechain
python scripts/check-docs.py
git diff --check
```

**P4 exit:** any run can answer:
- why executed
- why skipped
- why invalidated
- why conflicted
- what it cost / saved
- historical provenance

---

## Phase P5 — Agent Orchestration Parity+

**Why:** LangGraph/LangChain’s strongest practical advantage is agent ergonomics. We need native equivalents that preserve our reactive contracts.

### P5.1 Multi tool-call fanout

**Goal:** one model response with N tool calls becomes N independently schedulable tasks.

**Files:**
- Modify: `python/reactivegraph/reactivegraph/prebuilt.py`
- Modify: tool/agent integration.
- Test: `python/reactivegraph/tests/test_prebuilt*.py`

**Acceptance:**
- Parallel execution with bounded concurrency.
- Independent writes commit in parallel.
- Same-path writes raise explainable conflict.
- Each tool has its own receipt and error policy.
- One failing tool does not hide other tool decisions.
- Result ordering is deterministic by tool_call_id even when execution is parallel.

### P5.2 Native agent state schema

**Goal:** provide a typed, versioned agent state model without LangGraph reducer compatibility hacks.

**Acceptance:**
- Messages, tool calls, artifacts, receipts, interrupts, and human decisions have stable schema.
- State migration validates schema versions.
- Agent state can be inspected without executing tasks.

### P5.3 Agent-level interrupts and human transactions

**Acceptance:**
- `interrupt_before` / `interrupt_after` task boundaries.
- Resume accepts structured human response.
- Multiple pending interrupts are possible and individually addressable.
- Interrupt history is durable.

### P5.4 Tool policy and capability integration

**Acceptance:**
- Field permissions are enforced at tool patch commit.
- Budget/rate-limit/circuit-breaker policies reject before side effects.
- Terminal tools remain deny-by-default.
- Secret injection never leaks into model-visible tool schema or trace.

### P5.5 MCP adapter

**Acceptance:**
- Import MCP tools as Reactive ToolSpec with JSON Schema and read/write metadata.
- Local and remote MCP servers supported.
- Receipt and artifact mapping preserved.
- No requirement to copy LangChain code.

### P5 regression gate

Same as P4 plus:
```bash
uv run --directory python/reactivechain pytest "$PWD/benchmarks/deerflow/" -q
```

**P5 exit:** complex agents can be authored natively with fewer concepts than LangGraph while retaining reactive correctness.

---

## Phase P6 — Production Runtime Hardening

**Why:** architecture alone does not win production users. Operational trust must be demonstrable.

### P6.1 Remote gateway security

**Acceptance:**
- Auth token rotation without dropping active runs.
- Per-tenant rate limits and budgets.
- TLS/mTLS by default for non-local deployments.
- Redacted logs and traces.
- Penetration-style tests for malformed frames, oversized payloads, slow consumers, and concurrent auth races.

### P6.2 Operational observability

**Acceptance:**
- OTLP export from SpanEmitter.
- Run/task/tool dashboards with p50/p95/p99.
- Trace correlation across Python host, Driver, and external calls.
- No secrets or artifacts leaked by default.

### P6.3 Scale and stability

**Acceptance:**
- 1,000 concurrent threads smoke test.
- 10,000-node graph stability test.
- Durable log compaction under write pressure.
- Restart storm recovery.
- Memory ceiling and leak tests.

### P6.4 Deployment

**Acceptance:**
- Docker image for Driver + gateway.
- Health/readiness endpoints.
- Graceful shutdown with in-flight run preservation.
- Versioned migration guide.
- Clean install/uninstall test on Linux/macOS/Windows.

### P6 regression gate

Add:
```bash
pnpm --dir packages/driver typecheck
pnpm --dir packages/driver test
pnpm --dir packages/driver build
docker compose up -d && pnpm --dir packages/driver test -- --run e2e
```

**P6 exit:** deployable as a long-lived multi-tenant service, not just an embedded runtime.

---

## Phase P7 — Ecosystem and Migration

**Why:** users need a path from existing systems. We do not need to copy LangChain, but we do need adapters.

### P7.1 LangGraph read-only importer

**Goal:** convert a LangGraph `StateGraph` into ReactiveGraph while preserving semantics.

**Scope:**
- Nodes → tasks.
- Ordinary edges → event routes.
- Conditional edges → explicit event emission.
- State channels/reducers → typed state contract.
- Checkpoint metadata → compatibility notes.

**Non-goal:** full Pregel compatibility mode.

**Acceptance:**
- Representative LangGraph samples import without executing code.
- Analyzer reports unsupported constructs before migration.
- Imported graph executes same-output differential tests.
- No LangGraph dependency in runtime; test-only dependency.

### P7.2 LangChain tool/message adapters

**Acceptance:**
- Existing LangChain tool can run through optional adapter.
- Message shapes normalize without changing native core.
- No LangChain runtime dependency.
- Adapter tests use pinned upstream package.

### P7.3 OpenAPI and MCP parity

**Acceptance:**
- OpenAPI operations import as tools with schemas.
- MCP tools import as reactive tools.
- Tool tests include auth, pagination, streaming, and error semantics.

### P7.4 Docs and examples

**Acceptance:**
- Migration guide.
- Side-by-side LangGraph comparison.
- Agent cookbook.
- Production deployment guide.
- Every claim links to reproducible benchmark or test.

**P7 exit:** users can evaluate and migrate without being told “rewrite everything first.”

---

## Phase P8 — Community, Release, and Differentiation Proof

**Why:** technical superiority needs external proof and trust.

### P8.1 Public release readiness

**Acceptance:**
- NPM/PyPI secrets configured.
- `v0.1.0` tagged.
- Release smoke test from clean environments.
- CHANGELOG and migration notes published.
- Security policy and disclosure channel active.

### P8.2 Documentation site

**Acceptance:**
- API reference generated.
- Tutorials deploy.
- Search works.
- Benchmark dashboard reproduces from CI artifacts.

### P8.3 Community

**Acceptance:**
- Contribution guide.
- Reproducible issue templates.
- Example gallery.
- At least 5 external users or teams running pilot workloads.
- Public roadmap tied to this document.

### P8.4 Competitive proof

**Acceptance:**
- Publish:
  - same-output performance matrix,
  - no-gain matrix,
  - duplicate side-effect matrix,
  - explainability comparison,
  - migration cost comparison.
- Every number has raw output and command.
- Public comparison does not claim unsupported parity.

**P8 exit:** the project is externally usable, reproducible, and credible.

---

## Master Verification Matrix

| Phase | Primary proof | Must-pass commands |
|---|---|---|
| P4 | Explainability closure | Python/Driver tests, ruff/mypy, docs |
| P5 | Agent parity+ with reactive correctness | reactivegraph/chain/DeerFlow/e2e/Driver |
| P6 | Production hardening | Driver typecheck/test/build, Docker/e2e, load/stability |
| P7 | Migration path | importer/adapters, differential outputs, docs |
| P8 | Public trust | clean-install smoke, release CI, docs site, benchmark artifacts |

### Full regression suite

```bash
uv run --directory python/reactivegraph pytest -q
uv run --directory python/reactivechain pytest -q
uv run --directory python/reactivechain pytest "$PWD/benchmarks/deerflow/" -q
uv run --directory python/reactivechain pytest "$PWD/benchmarks/e2e/" -q
uv run --directory python/reactivegraph python "$PWD/scripts/check-tutorials.py"
python scripts/check-docs.py
uvx ruff check python/reactivegraph python/reactivechain
uvx mypy python/reactivegraph/reactivegraph
uvx mypy python/reactivechain/reactivechain
pnpm --dir packages/driver typecheck
pnpm --dir packages/driver test
pnpm --dir packages/driver build
git diff --check
```

Expected current baseline:

```text
reactivegraph 130 passed, 1 skipped
reactivechain 207 passed
DeerFlow      26 passed
e2e           43 passed
tutorials     8 scripts
docs          34 links
Driver        180 passed
ruff/mypy     clean
```

---

## Recommended Execution Order

1. **P4.1 `explain_conflict()`** — highest leverage, directly extends core differentiation.
2. **P4.2 historical explain** — converts explainability into auditability.
3. **P4.3 causal trace export** — makes the architecture visible.
4. **P4.4 cost model** — turns skips into business value.
5. **P5.1 multi tool-call fanout** — closes the most obvious agent gap.
6. **P5.2 agent state schema**
7. **P5.3 interrupts**
8. **P5.4 policies**
9. **P5.5 MCP adapter**
10. **P6 production hardening**
11. **P7 migration**
12. **P8 release/community**

---

## Honest Current Claim

> ReactiveGraph is already ahead on the reactive execution core: selective execution, computed invalidation, durable effect receipts, transactions, write-conflict detection, and restart-safe execution. It is not yet a complete LangGraph ecosystem replacement or a LangChain integration catalog. The goal of P4–P8 is to turn the architectural advantage into a preferred production runtime with explainability, agent ergonomics, deployment trust, migration paths, and public reproducible proof.
