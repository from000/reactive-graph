# D-Series Objective Tracker

Every D-series objective from the implementation plan (Appendix D) is tracked here
with its acceptance-test linkage and manifest capability IDs. Because the workspace
is a local git repository without a remote issue tracker yet, this file is the
authoritative tracker; it is exported to GitHub issues before release work begins
(plan Appendix H).

Status values: `pending` (not started), `in_progress`, `passed` (failing test added,
code green), `blocked`.

## D.1 Foundation and protocol

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.1.1 | Root workspace + lockfile; `pnpm -r test`, Python discovery | (workspace) | pending |
| D.1.2 | Shared protocol-version fixture read by Python + TS | (protocol) | pending |
| D.1.3 | Valid one-frame MessagePack fixture | (protocol) | pending |
| D.1.4 | Split-frame fixture | (protocol) | pending |
| D.1.5 | Concatenated-frame fixture | (protocol) | pending |
| D.1.6 | Malformed-length fixture | (protocol) | pending |
| D.1.7 | bytes/timestamp/int64 round trip Node↔Python | (protocol) | pending |
| D.1.8 | Handshake version negotiation | (protocol) | pending |
| D.1.9 | Request multiplexer out-of-order responses | (protocol) | pending |
| D.1.10 | Cancellation race, one terminal result | (protocol) | pending |

## D.2 Driver process and host

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.2.11 | Driver executable discovery (clean wheel) | A03 | pending |
| D.2.12 | Child process start/stop with fake Driver | A03 | pending |
| D.2.13 | Real handshake integration test | A03 | pending |
| D.2.14 | Sync Python callback registration | A09 | pending |
| D.2.15 | Async Python callback registration | A09 | pending |
| D.2.16 | Callback exception + sanitized traceback | A09 | pending |
| D.2.17 | Driver exit with pending requests | A03 | pending |
| D.2.18 | Host close idempotency | A03 | pending |
| D.2.19 | Concurrent run multiplexing | A03, A20 | pending |
| D.2.20 | Remote transport parity | A20 | pending |

## D.3 Paths, state, and transactions

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.3.21 | Path codec: map key escaping, numeric indexes | A04 | pending |
| D.3.22 | Nested dict read tracking | A04 | pending |
| D.3.23 | Branch-switch dependency cleanup | A04, A05 | pending |
| D.3.24 | Array append/splice dependency | A04, A05 | pending |
| D.3.25 | Map and set mutation | A04, A05 | pending |
| D.3.26 | Direct Python proxy mutation patch | A09 | pending |
| D.3.27 | Returned-update-dict patch | A05 | pending |
| D.3.28 | Transaction rollback | A05 | pending |
| D.3.29 | Stale pre-image conflict | A05 | pending |
| D.3.30 | Reducer-owned conflict resolution | A05 | pending |
| D.3.31 | Readonly state-path rejection | A05 | pending |
| D.3.32 | Property-based patch apply/invert | A04 | pending |

## D.4 Durability

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.4.33 | Append-only event serialization | A06 | pending |
| D.4.34 | Snapshot + patch replay equivalence | A06 | pending |
| D.4.35 | Crash before effect | A06 | pending |
| D.4.36 | Crash after effect intent | A06 | pending |
| D.4.37 | Crash after receipt, before commit | A06 | pending |
| D.4.38 | Crash after commit, before stream terminal | A06 | pending |
| D.4.39 | Unknown-commit recovery | A06 | pending |
| D.4.40 | Compaction retention + historical lookup | A06 | pending |
| D.4.41 | Encrypted event payload | A06 | pending |
| D.4.42 | Redacted trace vs full durable value | A06, A22 | pending |

## D.5 Scheduler and native APIs

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.5.43 | Native graph construction validation | A10 | pending |
| D.5.44 | Event-to-task route | A08 | pending |
| D.5.45 | Conditional route | A08 | pending |
| D.5.46 | Computed caching | A08 | pending |
| D.5.47 | Computed invalidation on relevant path | A08 | pending |
| D.5.48 | Irrelevant-path non-invalidation | A08 | pending |
| D.5.49 | Pure-task input-fingerprint skip | A08, `lg-cache-policy` | pending |
| D.5.50 | Effect-task no-auto-rerun | A08 | pending |
| D.5.51 | Independent task concurrency | A08 | pending |
| D.5.52 | Same-path write serialization | A08 | pending |
| D.5.53 | Timeout and cancellation propagation | A08, `lg-timeout-policy` | pending |
| D.5.54 | Retry backoff and idempotency | A08, `lg-retry-policy` | pending |
| D.5.55 | Error-boundary fallback | A08 | pending |
| D.5.56 | Cycle limit and termination diagnostic | A08, `lg-recursion-limit` | pending |

## D.6 High-level workflow features

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.6.57 | Values stream ordering | A12, `lg-stream-values-updates` | pending |
| D.6.58 | Updates stream coalescing | A12, `lg-stream-values-updates` | pending |
| D.6.59 | Custom event stream | A12, `lg-stream-custom-events` | passed |
| D.6.60 | Message/token stream | A12, `lg-stream-messages-transformers`, `lg-stream-data-transformers` | pending |
| D.6.61 | Bounded slow consumer | A12 | pending |
| D.6.62 | Disconnect and cursor resume | A12 | pending |
| D.6.63 | Interrupt-before task | A12, `lg-interrupt-node`, `prebuilt-human-interrupt` | pending |
| D.6.64 | Interrupt-after task | A12 | pending |
| D.6.65 | Human transaction update | A12, `lg-time-travel-update-state` | pending |
| D.6.66 | Nested subgraph input/output isolation | A13 | pending |
| D.6.67 | Nested interrupt/resume | A13 | pending |
| D.6.68 | Function/decorator workflow persistence | A13, `lg-func-entrypoint-task` | pending |

## D.7 Storage and upstream conformance

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.7.69 | Memory checkpointer base fixture | A07, `ckpt-*` | pending |
| D.7.70 | Memory store/cache base fixture | A07, `ckpt-store-*`, `ckpt-cache-*` | pending |
| D.7.71 | Serializer JSON/MessagePack fixture | A07, `ckpt-serde-*` | pending |
| D.7.72 | Encrypted serializer fixture | A07, `ckpt-serde-encrypted` | pending |
| D.7.73 | SQLite sync checkpointer | A14, `sqlite-saver-sync` | pending |
| D.7.74 | SQLite async checkpointer | A14, `sqlite-saver-async` | passed |
| D.7.75 | SQLite delta history + migration | A14, `sqlite-delta-migration`, `sqlite-ttl`, `ckpt-delta-history` | passed |
| D.7.76 | PostgreSQL sync checkpointer | A15, `pg-saver-sync`, `pg-shallow-saver` | passed |
| D.7.77 | PostgreSQL async checkpointer | A15, `pg-saver-async` | passed |
| D.7.78 | PostgreSQL store/search | A15, `pg-store-search` | passed |
| D.7.79 | Redis cache + TTL | A16, `ckpt-cache-redis` | passed |
| D.7.80 | Backend fault-injection + atomicity | A07, `conformance-*` | pending |

## D.8 Compatibility and migration

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.8.81 | Compatibility import smoke test | A17 | pending |
| D.8.82 | `StateGraph` compile/invoke | A17, `lg-graph-stategraph` | pending |
| D.8.83 | `MessagesState` + message reducer | A17, `lg-graph-messagesstate-reducer`, `lg-graph-messagegraph` | pending |
| D.8.84 | START/END + ordinary edges | A17, `lg-graph-start-end-constants` | pending |
| D.8.85 | Conditional edges | A17, `lg-graph-conditional-edges` | pending |
| D.8.86 | `Command` + `Send` | A17, `lg-graph-command`, `lg-graph-send` | pending |
| D.8.87 | Channel types | A17, `lg-channels-*`, `lg-managed-values` | pending |
| D.8.88 | Pregel barrier | A17, `lg-pregel-*` | pending |
| D.8.89 | Async invoke + each stream-mode | A17, `lg-stream-values-updates`, `lg-async-invoke` | pending |
| D.8.90 | Retry/task/error | A17, `lg-retry-policy`, `lg-errors-catalog` | pending |
| D.8.91 | Functional API | A17, `lg-func-entrypoint-task`, `lg-runtime-api` | pending |
| D.8.92 | Checkpoint read-only dry-run importer | A18, `lg-checkpointer-integration` | pending |
| D.8.93 | Checkpoint import integrity | A18 | pending |
| D.8.94 | Automated source migration | A18 | pending |
| D.8.95 | Unsupported-object migration warning | A18 | pending |

## D.9 Operations and user trust

| # | Objective | Capability IDs | Status |
|---|---|---|---|
| D.9.96 | Prebuilt tool-node fixture | A19, `prebuilt-tool-*`, `prebuilt-injected-state-store` | pending |
| D.9.97 | Prebuilt agent fixture | A19, `prebuilt-react-agent` | pending |
| D.9.98 | Remote gateway auth failure | A20, `sdk-py-auth`, `sdk-py-stream-sse` | pending |
| D.9.99 | Remote run/stream/cancel parity | A20, `sdk-js-stream-primitives` | pending |
| D.9.100 | Python SDK endpoint compatibility | A20, `sdk-py-*` | pending |
| D.9.101 | JavaScript SDK endpoint compatibility | A20, `sdk-js-*` | pending |
| D.9.102 | CLI project validation | A21, `cli-validate`, `cli-new` | pending |
| D.9.103 | CLI local dev + trace export | A21, `cli-up`, `cli-dev`, `cli-build-dockerfile` | pending |
| D.9.104 | CLI checkpoint migration dry-run | A21 | pending |
| D.9.105 | Field-permission dispatch rejection | A23 | pending |
| D.9.106 | Trace causal-chain snapshot | A22, `lg-trace-policy` | pending |
| D.9.107 | OpenTelemetry redaction | A22, A23 | pending |
| D.9.108 | Clean Python install + Driver startup + migration + uninstall | A21, A24 | pending |
