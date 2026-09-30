# Migrating from LangGraph / LangChain

ReactiveGraph is not a drop-in LangGraph replacement. It is a reactive runtime:
tasks declare effects, the scheduler decides what must run, and every decision
can be explained. This guide converts the common LangGraph state-graph path
while preserving semantics.

## 1. Analyze a LangGraph StateGraph

The importer is read-only and does not import the upstream package in
ReactiveChain runtime code:

```python
from reactivechain import inspect_langgraph
from langgraph.graph import END, START, StateGraph

# Build your upstream graph, then inspect it without executing nodes.
report = inspect_langgraph(upstream_graph)
print(report["supported"])
print(report["nodes"])
print(report["edges"])
print(report["unsupported"])
```

Mapping:

| LangGraph | ReactiveGraph |
|---|---|
| Node | Task |
| `START -> n` | `run -> n` |
| `a -> b` | `a:written -> b` |
| `b -> END` | omitted; final thread state is returned |
| State schema keys | reactive state contract |
| Conditional edges | explicit event emission |

Conditional edges are intentionally reported as unsupported rather than
silently compiled into a graph with different semantics. Replace a conditional
edge with a small event-emitting node, or emit an explicit event in the parent
task.

## 2. Import a supported graph

```python
from reactivechain import import_langgraph

g = import_langgraph(upstream_graph)
out = g.invoke("run", {"x": 0, "y": 0})
```

For the common two-node graph, the imported output matches the upstream graph:

```python
assert out == {"x": 1, "y": 2}
```

The test suite verifies this against pinned upstream `langgraph==1.2.11`:

```bash
uv run --directory python/reactivechain \
  python -m pytest tests/test_langgraph_import.py -q
```

## 3. Reuse tools

ReactiveChain's tool protocol is native and follows industry standards (JSON
Schema, OpenAI tool calling). Existing LangChain-compatible tools can use the
optional adapter:

```python
from reactivechain.tools_ext.langchain import from_langchain_tool

native_tool = from_langchain_tool(langchain_tool)
```

Message objects can also be normalized:

```python
from reactivechain.tools_ext.langchain import from_langchain_message
message = from_langchain_message(langchain_message)
```

The optional adapters do not make ReactiveChain depend on LangChain at runtime.

## 4. Choose an integration source

- OpenAPI: `from_openapi(spec, base_url=..., headers=...)`
- MCP: `from_mcp(server_url, headers=...)`
- Native: `@tool(reads=..., writes=..., kind=..., receipt=True)`

OpenAPI and MCP adapters support authenticated external services and convert
errors into agent-recoverable tool output.

## 5. Reuse the execution model

Once tools and nodes are imported, use the native runtime:

```python
from reactivechain import Toolkit

graph = Toolkit(tools).to_graph(graph_id="migrated")
result = graph.invoke("run", {"tool_call": {"arguments": {...}}})
```

This gives imported workflows ReactiveGraph semantics:

- task `reads` / `writes`
- `pure` fingerprint skip
- `effect` receipt idempotency
- computed read-set invalidation
- transaction and conflict explanation
- checkpoint, resume, and durable state

## 6. Explain the migration

After execution, inspect the decisions:

```python
g.explain_run()
g.explain_run(run_id="...")
g.why_skipped("task")
g.why_invalidated("selector")
g.explain_conflict()
g.export_trace(format="json")
g.export_trace(format="dot")
```

For cost visibility:

```python
g.cost_saved()
```

This includes estimated token and USD savings when task metadata is supplied.
