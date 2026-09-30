# Beyond LangGraph Gap Plan Completion Audit

Date: 2026-09-30 (external state re-verified 2026-10-05)
Branch: `beyond-langgraph-p4`
Status: **P4/P5/P6/P7 engineering-complete; P8 published to GitHub with docs,
CI, release and benchmark workflows green; registry publishing and external
pilots still not done (both require external accounts/people)**

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

Packaging and compatibility hardening re-verified on 2026-10-05 (core
`main`, commit `3a77ae8`):

```text
npm tarballs           each of the four publishable packages now carries its own
                       README.md (previously absent, so the npm pages would
                       have rendered blank); check-clean-install.sh asserts this
docs link check        71 relative links resolved across 74 markdown files
                       (previously only docs/ was scanned, 59 links/59 files)
release gate           PASS (all phases; 877 passed locally, 16 skipped)
compat extra           langgraph 1.2.11 + langchain 1.2.11 + langchain-core +
                       langgraph-checkpoint-sqlite + aiosqlite installed in CI
CI                     14/14 check-runs green; reactivegraph job now reports
                       893 passed / 0 skipped on Python 3.10, 3.11 and 3.12
                       (previously 822 passed / 71 skipped — 49 upstream
                       compatibility assertions were silently not running)
```

Two real defects were found and fixed by turning those skipped assertions on:

```text
SummarizationMiddleware.summary_prompt   byte-identical to upstream again
                                        (four stray blank lines removed)
test_is_final / langgraph on 3.10       stdlib typing.final does not write
                                        __final__ before CPython 3.11; the
                                        assertion is now version-aware and the
                                        native fallback is always marked
```

Real-service provider evidence is intentionally reported separately from the
hermetic local gate:

```text
ReactiveGraph native PostgreSQL checkpoint/store  16 passed (PostgreSQL 15.4)
Driver real-service storage                        8 passed (3 RedisCache + 5 PostgreSQL)
```

DeerFlow fork (`deer-flow-reactive`, separate checkout):

```text
fork head at first audit                                ab0737f
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
fixture.

Superseded by later work on the same branch (re-verified 2026-10-05 at fork
head `6629cb53`):

```text
backend unit tests (4 CI shards)  19,101 passed, 82 skipped, 0 failed
                                  (CI runs real postgres:17 + redis:7-alpine,
                                   so the integration tests execute)
Redis Streams bridge               7 passed (real Redis, plus an end-to-end
                                   publish -> replay -> end-sentinel check)
native Postgres checkpoint/store  wired: config.type=postgres resolves to
                                  reactivegraph.checkpoint.PostgresCheckpointSaver
                                  and reactivegraph.store.PostgresStore
CI                                 9/9 check-runs green
```

The frontend E2E run covers the full Playwright suite, including the bookmark
plugin fixture, plugin asset gateway, scheduled-task routing, sidecar scroll
restore, thread ordering/archive, and mobile polish cases.

Provider scope is deliberately narrow and explicit:

- PostgreSQL checkpoint/store is a native ReactiveGraph Python provider.
- The Driver PostgreSQL adapter has separate real-service coverage.
- RedisCache is a native TypeScript Driver cache, not a Python checkpoint/store.
- The Redis Streams bridge lives in the DeerFlow fork and is evidenced there.

### Code-quality hardening (2026-10-05, commit `cd85809`)

The quality review findings were all addressed, with behaviour pinned by
differential tests rather than inspection alone:

```text
cyclomatic complexity   every D/E/F-grade function removed from
                        python/reactivegraph and python/reactivechain
                        (was 1 F + 4 E + 15 D; now 0 D and above).
                        ESLint now enforces the same ceiling for TypeScript
                        (complexity<=32, max-depth<=6, max-lines<=1250 down
                        from disabled). Turning it on found two real hotspots
                        and both were refactored: Driver `handleRequest`
                        (56 -> per-method handlers) and Scheduler `runTask`
                        (50 -> extracted skip/gate/invoke/conflict/retry
                        helpers)
oversized modules       store.py 1755 -> 1437 lines + store_types.py (233)
                        + store_ops.py (142); checkpoint.py 1678 -> 1377
                        lines + checkpoint_types.py (346); create_agent.py
                        2367 -> 1885 lines + streaming.py (349)
                        + agent_state.py (60)
                        + agent_middleware_tools.py (189)
                        + agent_recursion.py (201). All keep the same public
                        surface via re-exports (`reactivegraph.store`,
                        `reactivegraph.checkpoint` and
                        `reactivegraph.create_agent` still expose every name
                        in their __all__). Every source module is now under
                        1500 lines (largest: messages.py at 1496;
                        create_agent.py went 2367 -> 1328)
docstring gate          [tool.interrogate] in every package's pyproject.toml,
                        wired into scripts/check-release-gate.sh and the CI
                        Python job. Coverage is now enforced per distribution:
                          reactivegraph      81.7%
                          reactivechain      84.5%  (was 41.8%)
                          reactivegraph_sdk  92.3%  (was 21.2%)
                          reactivegraph_cli 100.0%  (was 40.0%)
                        Nested closures, overload stubs, __init__ and dunders
                        are excluded as implementation details.
                        message_utils: E38->C17, E32->C16, D29->C12, D22->split
                        graph.py: E37->split into 4 helpers, E33->split
                        create_agent.py: F43->layout/graph-view helpers
                        serde.py: D27/D27/D24 -> C-level registries
behaviour pinning       4,100 randomized differential cases comparing the
                        pre-refactor and post-refactor message_utils byte for
                        byte: 0 mismatches
public docstrings       50.8% -> 95.5% (532/557 public defs)
mypy                    clean; the 21 `type: ignore[override]` entries in
                        reactivechain are gone (base declares read-only
                        properties), total ignores 111 -> 90
add_messages            format="langchain-openai" now implemented (delegates to
                        langchain-core's converter, explicit error without it)
                        instead of raising NotImplementedError
docs nav completeness  `scripts/check-docs.py` now fails when a published
                        page exists but is missing from `mkdocs.yml` nav —
                        the whole `tutorials/` tree (11 pages) had been
                        building yet unreachable from the site. 39 published
                        pages are now all reachable.
PEP 561 typing          all four distributions now ship `py.typed` and the
                        clean-install gate asserts it. `reactivechain` had the
                        marker file but no `[tool.setuptools.package-data]`
                        entry, so it never reached the wheel; the other three
                        had no marker at all. Downstream type checkers now use
                        the inline annotations instead of reporting missing
                        stubs.
resource leaks          `DriverHost.close()` now closes the child's
                        stdin/stdout/stderr pipes explicitly. Running the
                        suite with `-W error` went from 42 failures + 17
                        errors (PytestUnraisableExceptionWarning on unclosed
                        `_io.FileIO`) to a single GC-timing warning.
reads-scoped fingerprint the pure-task fingerprint now covers only a task's
                        declared `reads`, on **both** execution paths:
                        - the wire carries task `reads` to the Driver
                          (`_to_wire_spec`), and `WireTask`/`TaskDef`/
                          `RuntimeTask` all carry the field;
                        - the Driver resolves each read against `baseInput`
                          first (user input — stable across runs) and the store
                          otherwise (so an upstream task's new write still
                          triggers its downstream). Reading only `baseInput`
                          would miss the upstream case; reading the merged task
                          input would make every run look different.
                        Measured on a 1000-task graph with one field changed:
                        Driver 1/1000 executed, Python fallback 1/1000 (was
                        1000/1000 on both before the two fixes below).
pure-task store guard   `store_get`/`store_put`/`store_delete` now raise when
                        called from inside a task declared `kind="pure"`. The
                        long-term store is not part of the state fingerprint,
                        so such a task could be skipped even after an earlier
                        run wrote new data — the DeerFlow memory port did
                        exactly that and silently returned stale memory.
                        `kind="opaque"` is the correct declaration and is
                        covered by a positive test. Regression tests:
                        `TestPureTaskStoreGuard` (reject + allow).
fallback fingerprint    the in-process fallback fingerprint now covers a
                        task's declared `reads` paths instead of the whole
                        state. With whole-state keys, changing one field
                        invalidated every task, so a 1000-task graph re-ran
                        all 1000 — while the Driver path skipped unaffected
                        tasks. A/B on the same 1000-task graph: 1000/1000
                        executed (969ms) before, 1/1000 (151ms) after.
                        Tasks that declare no `reads` keep the conservative
                        whole-state key, and non-JSON inputs still never
                        cache. Regression test:
                        `test_fallback_fingerprint_uses_declared_reads_not_whole_state`.
concurrent isolation    `ReactiveGraph.invoke` now forwards
                        `config.configurable.thread_id` to the Driver. It
                        previously always used the Driver's "default" store,
                        so two concurrent invokes under distinct thread ids
                        shared and clobbered one run's state (~1-3% of rounds).
                        A/B evidence on the same 300-round stress case:
                        3/3 failures before, 3/3 passes after. Regression test
                        added as TestConcurrentThreadIsolation.
```

## CI verification (resolved)

Three consecutive `CI` runs on `main` (heads `9f39a8b6`, `5e6f86d7`, and the
queued `02f359c2`) completed with the run-level conclusion `failure` even
though **no job reported a failure**. Per-job inspection shows the affected
jobs never started:

- job `started_at` is set, but no step logs exist and the log blob returns
  `BlobNotFound`;
- the jobs sit for exactly 15 minutes and are then cancelled by the platform.

GitHub's status page confirms the cause — an incident opened at
`2026-10-05T19:11:58Z` reporting *"delays in assigning GitHub-hosted runners,
affecting workflow start times across multiple runner configurations"* — which
matches the first affected run. The code itself is verified locally:

```text
scripts/check-release-gate.sh --python --static   EXIT=0
reactivegraph      880 passed, 16 skipped
reactivechain      221 passed
reactivegraph-sdk   12 passed
reactivegraph-cli   10 passed
ruff               clean (63 source files)
mypy               clean
docstring gates    81.7 / 84.5 / 92.3 / 100.0  all PASSED
docs nav           39/39 published pages present
flake check        three consecutive full-suite runs, 880 passed each
```

**Resolved.** After GitHub's runner-assignment incident cleared, CI on
`9218454` reported **14/14 jobs success** (JavaScript, Python 3.10/3.11/3.12,
ReactiveChain 3.10/3.11/3.12, Coverage gate, Docs checks, Docs site build, and
all four Clean-install jobs). The local gate above remains the reproducible
record of what those jobs assert.

## External blockers

These requirements cannot be satisfied from this git worktree. State
re-verified on 2026-10-05:

| Required state | Current evidence |
|---|---|
| `NPM_TOKEN` secret | Actions secrets API: `total_count: 0` — still absent (account action) |
| `PYPI_TOKEN` secret | Actions secrets API: `total_count: 0` — still absent (account action) |
| npm `@reactivegraph` scope | registry returns 404 for `@reactivegraph/protocol` — org not created |
| npm authentication | `npm whoami` returns `ENEEDAUTH` — not logged in locally |
| public `main` branch/code | **resolved** — `from000/reactive-graph` public, `main` = single squashed commit, CI 14/14 green |
| docs site | **resolved** — GitHub Pages enabled (workflow build), `https://from000.github.io/reactive-graph/` serves 200, `search_index.json` 200 |
| repository metadata | **resolved** — description, homepage, 8 topics, community profile 100%, Discussions enabled |
| public release workflow | **resolved** — release workflow dispatched on `main` and completes green; publish steps skip with a notice until both tokens exist |
| differential benchmark workflow | **resolved** — dispatched on `main`, all three jobs green, `differential-benchmark-matrix` + `realworld-report` artifacts uploaded |
| `v0.1.0` public tag/release | deliberately not created yet — checklist requires the registry publish first, so the tag stays the last release action |
| external pilot users/teams | no external pilot participation yet |
| PyPI `reactivegraph` project | PyPI JSON API returns 404 — still unpublished (account action) |

The exact actions needed are in `RELEASE_CHECKLIST.md`.

## Conclusion

All requirements that can be implemented and verified inside the repository are
complete, including CI, GitHub Pages docs deployment, the release workflow and
the differential benchmark workflow on the public single-commit `main`.

The objective is not globally complete because publishing the eight packages
and confirming five external pilots require registry accounts/credentials and
people outside the repository. Publishing is one token upload away: local
release gate PASS, `twine check` PASS on all 8 distributions, npm publish
dry-run PASS on all 4 tarballs.
