# Changelog

All notable changes to ReactiveGraph. Format: `0.x.0` milestones; each entry
lists the user-visible capabilities added and the fixes that matter.

## 0.1.0 (workspace) — native engine

Milestones completed against `docs/plans/roadmap.md`:

- **M1 correctness**: write-conflict detection in parallel batches,
  `stream()`/`resume()` on the Python native API, effect idempotency
  receipts from Python tasks.
- **M2 persistence & observability**: checkpoint / long-term store on the
  Python native API; periodic durable-log snapshot + compaction (idempotency
  preserved across restarts); field-level permissions enforced at the commit
  point; `scope` sub-state namespaces consumed by the scheduler; causal trace
  export (`graph.trace()`).
- **M3 ecosystem**: native prebuilt agent (`ToolNode` + ReAct loop);
  Postgres + Redis durable backends; real TCP gateway transport with tenant
  token authentication.
- **Foundation**: the LangGraph compatibility layer was removed — the
  repository is exclusively the native reactive engine.

## Prior work (pre-changelog)

Before the changelog started, the LangGraph compatibility layer existed and
was removed; the repository is exclusively the native reactive engine.
