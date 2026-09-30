# Why Reactive

A conventional graph runtime walks all reachable nodes on every invocation.
A reactive runtime changes only what depends on the changed state.

ReactiveGraph gives users:

1. **Selective execution** — unchanged pure tasks and confirmed effect tasks do
   not rerun.
2. **Read-set invalidation** — computed selectors recompute only when declared
   reads change.
3. **Idempotent effects** — durable receipts prevent duplicate external calls
   after restart.
4. **Transactional commits** — task writes are patches with read/write sets.
5. **Explainability** — execution, skip, invalidation, conflict, cost, and
   historical decisions are queryable.
