# Beyond LangGraph Gap Plan Completion Audit

Date: 2026-09-30
Branch: `beyond-langgraph-p4`
Status: **P4/P5/P6/P7 engineering-complete; P8 external release not done**

> Commit hashes quoted below identify checkpoints in the pre-squash private
> history and do not resolve in the public single-commit repository; the
> evidence they anchor is the test file named next to each hash. DeerFlow fork
> hashes refer to the separate `deer-flow-reactive` checkout.

## Evidence summary

### P4 — Explainable Runtime Closure: complete

| Requirement | Evidence |
|---|---|
| Declared and actual write-conflict explanation | `python/reactivegraph/tests/test_observability.py`; commits `7a84cae` |
| Historical run explanation | `test_driver_historical_explain_run_after_restart`; commit `95e40ee` |
| Causal trace JSON/DOT | `test_export_causal_trace_json_and_dot`; commit `d55fd31` |
| Token/USD savings estimate | `test_cost_saved_reports_declared_task_estimates`; commit `6608267` |

### P5 — Agent Orchestration Parity+: complete

| Requirement | Evidence |
|---|---|
| Bounded parallel tool calls with order/error isolation | `test_prebuilt.py::TestParallelToolCalls`; commit `932b6e6` |
| Versioned native agent state schema | `TestAgentStateSchema`; commit `91ace51` |
| Interrupts and human transactions | `TestAgentInterrupts`; commit `3c0e0bc` |
| Permissions/budgets before side effects | `test_driver_*policy*`; commit `e82253c` |
| MCP adapter with reactive metadata/artifact | `tests/test_mcp.py`; commit `798e2c1` |

### P6 — Production Runtime Hardening: complete

| Requirement | Evidence |
|---|---|
| Token rotation and tenant limits | `test/gateway.test.ts`; commit `644a198` |
| OTLP JSON export | `test/controls.test.ts`; commit `6e97f33` |
| Bounded spans + p50/p95/p99 metrics | `test/controls.test.ts`; P6 hardening branch |
| Atomic token rotation + concurrent auth-race coverage | `test/gateway.test.ts`; P6 hardening branch |
| Bounded slow-consumer backpressure + session metrics | `test/gateway.test.ts`; P6 hardening branch |
| 1,000 threads / 10,000 nodes / compaction | `test/runtime.test.ts`, `test/durability/recovery.test.ts`; commit `fc3515d` |
| Health/readiness, graceful shutdown, deploy artifacts | CLI tests and generated Docker/Compose; commit `7af7b97` |
| Restart-storm recovery | `test/durability/recovery.test.ts`; `test/runtime.test.ts`; commits `0a1e091`, `9698773` |
| Memory ceiling + leak bounds | `RuntimeLimits`/`RuntimeStats` LRU + fail-closed ceilings; commit `9698773` |

### P7 — Ecosystem and Migration: complete

| Requirement | Evidence |
|---|---|
| Read-only LangGraph importer | `tests/test_langgraph_import.py`; commit `45960be` |
| LangChain tool/message adapters | `tests/test_tools.py`; commit `7e877fd` |
| OpenAPI/MCP auth/error semantics | `tests/test_openapi.py`, `tests/test_mcp.py`; commit `6b15a79` |
| Migration/agent/deployment docs | `docs/migration-guide.md`, `docs/agent-cookbook.md`, `docs/production-deployment.md`; commit `7d9a052` |

### P8 — Local release/community readiness: partial

| Requirement | Evidence |
|---|---|
| Python package builds | 4/4 distributions build with `uv build` |
| TS builds | `pnpm -r build` |
| Documentation site/search | `mkdocs build --strict` + CI `docs-site` job |
| Benchmark artifact | `differential-benchmark-matrix` in benchmark workflow |
| Contribution/security docs | `CONTRIBUTING.md`, `SECURITY.md` |
| Reproducible issue templates | bug, feature, pilot, docs templates |
| Example gallery | `docs/example-gallery.md` |
| Competitive proof | `docs/competitive-proof.md`, all required matrices |
| Public roadmap tied to plan | `docs/plans/roadmap.md` |
| Clean-install matrix CI | GitHub Actions `clean-install` job: Python 3.10/3.11/3.12 and npm packages |
| **Not yet done** | public release; five external pilots |

## Fresh verification record

The canonical serial gate was run on 2026-09-30 against the final
branch head with `scripts/check-release-gate.sh`:

```text
reactivegraph        802 passed, 27 skipped
reactivechain        219 passed
reactivegraph-sdk     12 passed, 1 collection warning
reactivegraph-cli     10 passed
ReactiveGraph e2e     43 passed
DeerFlow offline       26 passed
JavaScript tests     255 passed (Driver 216, Protocol 17, other packages 22)
ruff                 clean
mypy                 clean (55 source files)
mkdocs build         success (strict)
docs links           59 resolved
tutorials             8 executable scripts passed
clean install        4 Python distributions + publishable npm tarballs passed
git diff --check     clean
```

Real-service provider evidence is intentionally reported separately from the
hermetic local gate:

```text
ReactiveGraph native PostgreSQL checkpoint/store  16 passed (PostgreSQL 15.4)
Driver real-service storage                        8 passed (3 RedisCache + 5 PostgreSQL)
```

DeerFlow fork (`deer-flow-reactive`, separate checkout):

```text
fork current head                                       ab0737f
backend full offline suite   19,098 passed, 168 skipped, 21 deselected
                              (equivalent to 606187b)
PostgreSQL retention             60 passed (real PostgreSQL)
PostgreSQL remaining suites     268 passed, 3 skipped (real PostgreSQL)
Redis Streams bridge              7 passed (real Redis 7.2)
frontend typecheck                passed
frontend unit tests          1,969 passed
frontend E2E                   299 passed
                              (CI-equivalent: 1 worker, 2 retries, 20.6m)
```

The `ab0737f` follow-up only normalizes the asyncpg DSN in one migration
fixture. The frontend E2E run covers the full Playwright suite, including the bookmark
plugin fixture, plugin asset gateway, scheduled-task routing, sidecar scroll
restore, thread ordering/archive, and mobile polish cases.

Provider scope is deliberately narrow and explicit:

- PostgreSQL checkpoint/store is a native ReactiveGraph Python provider.
- The Driver PostgreSQL adapter has separate real-service coverage.
- RedisCache is a native TypeScript Driver cache, not a Python checkpoint/store.
- The Redis Streams bridge lives in the DeerFlow fork and is evidenced there.

## External blockers

These requirements cannot be satisfied from this git worktree and were verified
as absent on 2026-09-30:

| Required state | Evidence |
|---|---|
| `NPM_TOKEN` secret | GitHub Actions secrets API returned `total_count: 0` |
| `PYPI_TOKEN` secret | GitHub Actions secrets API returned `total_count: 0` |
| npm `@reactivegraph` org | npm registry returned `Scope not found` |
| npm authentication | `npm whoami` returned `ENEEDAUTH` |
| public `main` branch/code | GitHub repository is empty and has no branches |
| `v0.1.0` public tag/release | GitHub tags API returned empty |
| public release workflow run | GitHub actions runs API returned `total_count: 0` |
| external pilot users/teams | GitHub issues API returned no pilot issues |

The exact actions needed are in `RELEASE_CHECKLIST.md`.

## Conclusion

All requirements that can be implemented and verified inside the repository are
complete. The objective is not globally complete because public release and
five external pilots require external credentials/organizations/accounts and
people that the repository cannot create on its own.
