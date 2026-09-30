# ReactiveGraph

ReactiveGraph is a reactive runtime for agent and workflow systems. It keeps
declarative state, selects only the tasks that must run, records every decision,
and persists effects safely.

## Why it is different

LangGraph and LangChain focus on graph and component orchestration.
ReactiveGraph treats execution as a reactive, transactional system:

- task `reads` / `writes`
- pure-task fingerprint skips
- computed read-set invalidation
- effect receipts for idempotent external calls
- transactional state and explainable write conflicts
- durable checkpoints and event logs
- OTLP-shaped spans and causal traces

## Learn

1. [Why Reactive](why-reactive.md)
2. [Quickstart](quickstart.md)
3. [Migration guide](migration-guide.md)
4. [Agent cookbook](agent-cookbook.md)
5. [Production deployment](production-deployment.md)
6. [Commercial readiness](commercial-readiness.md)
7. [FAQ](faq.md)

## Verify claims

Every performance claim links to a reproducible benchmark in
[benchmarks](benchmarks.md). Every documented API has tests in the repository;
CI runs the same commands used by contributors.
