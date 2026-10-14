# Beyond LangGraph Gap Plan Completion Audit

Date: 2026-09-30 (external state re-verified 2026-10-14)
Branch: `beyond-langgraph-p4`
Status: **P4–P8 engineering-complete. v0.1.0 is published on GitHub with docs,
CI, release and benchmark workflows green, and both repositories report zero
in-repo-defect advisories (the one remaining fork advisory has no upstream fix
release). Registry publishing stays deferred by the 2026-10-10 scope decision;
the five external pilots are the only outstanding objective, and they require
people outside the repository.**

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

### P8 — Release/community readiness: engineering-complete (external actions pending)

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
| **Not yet done** | five external pilots (public release shipped 2026-10-12; registry publication deferred by scope decision) |

## Late finding: a selective-execution cost bug (2026-10-12)

The user asked what makes this engine better than LangGraph, and required the
answer to come from the real code rather than the docs. Driving the **actual
Python path** (RGP/1 + Node child process) instead of the benchmark's bare
`Scheduler` exposed a defect the published numbers could not see:

- A 1000-node graph with 10 changed inputs took **3969 ms** end-to-end, not the
  2.5 ms the component benchmark reported — a ~1500x gap between the measured
  component and the user-facing path.
- Executor invocation count proved the cost was pure overhead: the run invoked
  **0** task callbacks (selective skip worked) yet still burned seconds.
- Root cause: `DriverRuntime` rebuilt the merged task input
  (`{...store.raw, ...baseInput}`) once **per task** in the dispatch loop. With
  a 1000-node store (~2000 keys) one V8 spread measured ~4.6 ms, so 1000
  rebuilds cost ~3.1 s — O(n^2) in the graph size.
- Fix: the merge is now lazy (`Scheduler.runTaskLazy`) and only runs for tasks
  that actually execute. A first attempt that memoised a single shared object
  was rejected because it broke the per-task input-pollution isolation the
  dispatch loop guarantees.

| Path (1000 nodes, 10 affected, median of 3) | Before | After |
|---|---:|---:|
| Bare `Scheduler.runAll` | 16.4 ms | 16.4 ms |
| `DriverRuntime.run` (full TS engine) | 3104 ms | 255 ms |
| Python `invoke` over RGP/1 + Node | 3969 ms | 257 ms |

Two documentation corrections came out of the same investigation, because the
published ratios overstated what a user experiences:

1. `docs/benchmarks.md` and `competitive-proof.md` §1a now state that the
   millisecond columns measure the **bare Driver `Scheduler`** against
   upstream's end-to-end `graph.invoke()`.
2. Section 1b was added with the end-to-end numbers: against upstream's default
   configuration the real ratio is ~23x, and against upstream with a
   hand-written per-node `key_func` it is ~9x — a single-digit multiple, not
   four digits. The execution-count claim (1/1000 vs 1000/1000, and upstream
   needing an explicit `key_func`) is unaffected and remains accurate.

The DeerFlow real-factory report's `7.19x/8.07x` figures were annotated for the
same reason: `create_agent` builds its `ReactiveGraph` with no `DriverHost`, so
those numbers compare the in-process Python executor against the complete
LangGraph stack.

Regression guard: `packages/driver/test/scheduler/task-input-scaling.test.ts`
fails if a fully-skipped 1000-node run exceeds one second (it measured 4.1 s
before the fix).

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
release gate           PASS (all phases; 1166 Python tests + 258 JS tests + 69 benchmark tests)
compat extra           langgraph 1.2.11 + langchain 1.4.4 + langchain-core 1.6.9+ +
                       langgraph-checkpoint-sqlite + aiosqlite installed in CI
CI                     14/14 check-runs green; reactivegraph job reports 931
                       passed on Python 3.12 (incl. the 16 native Postgres
                       contracts) and 893 passed on 3.10/3.11 — the 3.12 delta
                       is the Postgres suite whose opt-in DSN CI sets.
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
reverse LangChain       `ReactiveGraph` and `AgentGraph` inherit
                        `langchain_core.Runnable` when it is importable (the
                        same optional-base pattern `reactivegraph.messages`
                        uses), and they now satisfy the *call* contract as well
                        as the identity check. Inheriting the base alone only
                        satisfied `coerce_to_runnable`; a follow-up audit drove
                        the real composed calls and found 7 of 7 failing —
                        `RunnableSequence` invokes steps as
                        `step.invoke(input, config)`, which the native
                        `(event, payload)` signature rejected, and
                        `AgentGraph.batch` treated LangChain's per-input config
                        list as a mapping
                        (`AttributeError: 'list' object has no attribute
                        'get'`). Now `graph_calls.py` resolves the call shape
                        (one positional argument = Runnable form, a declared
                        event name = native form, an undeclared string beside a
                        real `RunnableConfig` = Runnable form with a string
                        payload), single-event graphs infer their entry event,
                        multi-event graphs declare `b.entry(event)`, and
                        `batch`/`abatch` accept per-input config lists plus
                        `return_exceptions`. Ambiguities fail loudly:
                        `invoke("run")` without a payload, a bare-string
                        `batch`, and mistyped event names all raise with hints.
                        Measured: 10 of 14 new composition cases failed before
                        the change and all 22 pass after; the engine still
                        imports and runs with langchain blocked
                        (`_runnable_base()` returns no bases).
extension contracts     `reactivegraph/protocols.py` publishes the supported
                        extension points as `runtime_checkable` Protocols:
                        `StoreProtocol`, `CheckpointSaverProtocol`,
                        `RetrieverProtocol`. They are structural (no base class
                        needed) and `check_protocol()` reports exactly which
                        methods are missing instead of failing deep inside a
                        run. The engine's own MemoryStore/SqliteStore/
                        CheckpointStore are asserted against them, alongside a
                        from-scratch implementation written for the test.
                      `MCPServer` (reactivechain/mcp.py) exposes native tools
                        over MCP JSON-RPC with `initialize`/`tools/list`/
                        `tools/call`, and mirrors `kind`/`reads`/`writes`/
                        `receipt` under `_meta.reactivegraph`. Combined with the
                        existing `from_mcp`, a tool survives a full protocol
                        round trip without degrading into an opaque effect.
                      `docs/extending.md` documents every extension point with
                        runnable signatures, the `pure`/`effect`/`opaque` rules,
                        and the "a pure task must not read the long-term store"
                        guard.
langchain ecosystem      a new optional extra (`langchain-ecosystem`) pins
                        langchain-openai + langchain-anthropic, and
                        `tests/test_langchain_ecosystem.py` drives the REAL
                        `ChatOpenAI` against a local OpenAI-format server —
                        proving `create_agent(ChatOpenAI(...))` works end to
                        end rather than happening to work. CI installs the
                        extra and asserts the deps are importable, so the test
                        cannot silently skip. Core stays at four runtime
                        dependencies; the engine still owns the scheduler,
                        so receipts/conflicts/fingerprints are unaffected.
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
| `v0.1.0` public tag | **resolved** — annotated tag on the single `main` commit; GitHub Release **published** (not a draft) |
| external pilot users/teams | no external pilot participation yet |
| PyPI `reactivegraph` project | PyPI JSON API returns 404 — still unpublished (account action) |

The exact actions needed are in `RELEASE_CHECKLIST.md`.

Post-release hardening completed 2026-10-13:

```text
security posture        secret scanning + push protection + Dependabot
                        security updates all enabled (verified via REST API);
                        private vulnerability reporting enabled; zero open
                        Dependabot alerts at the time of this record
PR refs audit           19 refs/pull/* heads fetched and inspected: each is
                        an independent 2-commit Dependabot snapshot (squashed
                        root + bump), none is an ancestor of main, and a
                        regex + filename scan over every reachable blob found
                        no key material, .env/.pem/.key files or credentials
clean install           Python 3.12 venv outside the repo installed
                        reactivegraph==0.1.0 + reactivegraph-cli from
                        git+ssh://git@github.com/...; the CLI entry point ran;
                        a selective-execution probe executed 2/2 tasks then
                        skipped 2/2 on an identical re-run
fork E2E on main        first full browser E2E run on the public default
                        branch: 299 passed + 5 auth-recovery passed
                        (the E2E workflow gained workflow_dispatch so it can
                        be re-run on demand)
fork head               `from000/deer-flow-reactive` main advances to
                        `16f3c447` (docs: state the remaining langgraph
                        types, not execution code); Lint/Unit/Frontend
                        workflows green on `main` and
                        `reactivegraph-checkpointer`

```

Post-release hardening completed 2026-10-14:

```text
pilot funnel             both READMEs link the pilot invitation AND the
                         pre-filled issue form directly; Discussion #20 carries
                         the same CTA; the `Pilot user` template is registered
                         with the `pilot`/`community` labels;
                         `.github/ISSUE_TEMPLATE/config.yml` adds Discussions
                         and private-vulnerability-reporting contact links;
                         CODE_OF_CONDUCT.md names an enforcement contact;
                         the from000/deer-flow-reactive fork links the same
                         invitations from README.md and REACTIVEGRAPH*.md
dependency security      main repo: 0 open Dependabot alerts (6 fixed);
                         fork: 31 -> 1 open alert. The single remainder is
                         `braces <= 3.0.3` (high, stack-exhaustion DoS). It is
                         unreachable-to-fix in-repo: no patched release exists
                         for ANY major line (npm's newest braces is 3.0.3, the
                         vulnerable one; the advisory's first_patched_version
                         is empty). Tracked upstream and documented in
                         REACTIVEGRAPH*.md, not silenced.
                         langchain-anthropic is pinned `>=1.4.6,<1.5` because
                         1.5+ pulls anthropic 1.x whose `_format_messages`
                         drops the no-model call form the gateway relies on
                         (pinned by tests/test_tool_call_args.py).
pilot onboarding audit    a fresh-venv walkthrough of the documented install
                         found and fixed three real pilot blockers:
                         quickstart promised Driver/`DriverHost` examples right
                         after a core-only `pip install` (they need a source
                         checkout + built Node Driver + the previously
                         undocumented REACTIVEGRAPH_DRIVER_ENTRY), it never
                         stated Node/pnpm prerequisites, and it gave no
                         expected output so a pilot could not self-check.
                         Both READMEs gained an install-troubleshooting note
                         for the local `insteadOf`/proxy TLS failure mode.
                         Re-verified: quickstart prints `hi Ada`,
                         `reactivegraph health` prints `reactivegraph 0.1.0
                         ready`, docs link check passes.
pilot value demo         quickstart gained a "Selective execution (the point)"
                         section: a self-contained `kind="pure"` example whose
                         verbatim run prints {'a': 1, 'b': 1} / {'a': 1, 'b':
                         1} / {'a': 2, 'b': 1} and explains
                         executed=2 skipped=1 cache_hits=1. Previously no
                         quickstart path demonstrated the differentiating
                         capability at all. Verified by executing the fenced
                         block itself and diffing the output against the
                         documented block.
canonical release gate   `scripts/check-release-gate.sh` executed phase by
                         phase on 36ef42b and every phase PASSED:
                           --python        915+229+12+10 = 1166 tests, 16 skipped,
                                           working-tree diff check clean
                           --js            typecheck, lint, format, 258 vitest
                                           tests (241 driver + 17 sdk-js), build
                           --benchmarks    43 cross-language e2e + 26 DeerFlow
                                           offline port
                           --static        ruff clean, mypy clean (66 files),
                                           docstring coverage 82.3/84.3/92.3/100%,
                                           all >= the 80% gate
                           --docs          mkdocs --strict build, 78 links across
                                           75 files, 8 tutorial scripts ran
                           --clean-install Python wheels + npm tarballs installed
                                           into fresh environments; all four
                                           imports, CLI ownership (reactivegraph ->
                                           reactivegraph-cli), `reactivegraph
                                           health`, a graph invoke and an npm sdk
                                           invoke all verified
                         (This sandbox's `uv` cannot reach PyPI — `tls handshake
                         eof` — so the runs were made with UV_OFFLINE=1 and a
                         pre-populated wheelhouse; CI runs the same gate with
                         real network.)
native Postgres          16/16 contract tests pass against a real local
                         PostgreSQL (ReactiveGraph native PostgresCheckpointSaver
                         + PostgresStore). The 7 stale "PostgresStore not
                         implemented yet" guards were removed, so CI's
                         postgres:16 service now exercises all 16 contracts
                         instead of silently skipping 7. Python suite: 915
                         passed, 16 skipped.
final main head          6904c46 — single commit, tag v0.1.0, CI 14/14 + release
                         green
fork head                bff1731d — Lint/Unit/Frontend/E2E/Blocking-IO green on
                         18f03f1a (the dependency commit); only docs follow up
```

## Conclusion

All requirements that can be implemented and verified inside the repository are
complete, including CI, GitHub Pages docs deployment, the release workflow and
the differential benchmark workflow on the public single-commit `main`.

**Distribution scope (decided 2026-10-10):** v0.1.0 ships from GitHub source.
The npm and PyPI publication steps are deferred — the `@reactivegraph` npm
organization does not exist and no registry tokens are configured — and every
install instruction in the READMEs and docs uses the working GitHub form
(`pip install "git+https://github.com/from000/reactive-graph.git#subdirectory=..."`,
plus a source build for the TypeScript packages, whose pnpm `workspace:*`
dependencies npm cannot resolve). That form was verified end-to-end in clean
virtualenvs: all four Python packages install together and the README
quickstart runs; installing `reactivechain` alone fails dependency resolution,
which is why the docs require the four-package command.

The objective is not globally complete because confirming five external pilots
requires people outside the repository. Registry publication remains deferred
rather than blocked work: the release gate PASSes, `twine check` PASSes on all
8 distributions, and the npm publish dry-runs PASS on all 4 tarballs, so a
future publish is one token upload away.
