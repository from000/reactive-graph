# Extending ReactiveGraph

This page is for people who want to plug their own component into the engine:
a storage backend, a checkpoint saver, a retriever, or a tool that other
agents can call.

The engine is deliberately small at the core — the scheduler, transaction and
durability layer are ours, and everything at the edges is replaceable.

## 0. The one rule: fail loudly

Every extension point below is checked, and an implementation that is missing
a required method raises with **the names of the missing methods**, rather than
silently returning empty data or failing somewhere deep inside a run:

```python
from reactivegraph.protocols import StoreProtocol, check_protocol

check_protocol(my_store, StoreProtocol, context="graph.store")
# TypeError: MyStore does not satisfy StoreProtocol for graph.store; missing: search
```

That matters because "backend misconfigured" and "no data yet" are very
different problems, and an engine that confuses them loses data.

## 1. Custom store

Used by `graph.store_get` / `store_put` / `store_search` and by
`reactivegraph.store.get_store()`.

```python
from collections.abc import Sequence
from typing import Any

from reactivegraph.protocols import StoreProtocol
from reactivegraph.store_types import Item, SearchItem


class MyStore:
    """Satisfies StoreProtocol structurally — no base class needed."""

    def get(
        self, namespace: Sequence[str], key: str, *, refresh_ttl: bool | None = None
    ) -> Item | None: ...

    def put(
        self,
        namespace: Sequence[str],
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
    ) -> None: ...

    def delete(self, namespace: Sequence[str], key: str) -> None: ...

    def search(
        self,
        namespace_prefix: Sequence[str],
        /,
        *,
        query: str | None = None,
        filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[SearchItem]: ...

    def list_namespaces(
        self,
        *,
        prefix: Sequence[str] | None = None,
        suffix: Sequence[str] | None = None,
        max_depth: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[tuple[str, ...]]: ...
```

Wire it up:

```python
from reactivegraph import ReactiveGraph
from reactivegraph.store import reset_store

agent = ReactiveGraph.build(...)
agent.store = MyStore()          # or configure get_store() for the process
```

The engine ships `MemoryStore`, `SqliteStore` and `PostgresStore`; all three
satisfy the same protocol, which is what the test suite asserts.

## 2. Custom checkpoint saver

Used by `get_state` / `update_state` / `get_state_history` and by the
`checkpointer=` argument of `create_agent`.

```python
from collections.abc import Iterator, Sequence
from typing import Any

from reactivegraph.checkpoint_types import CheckpointTuple
from reactivegraph.protocols import CheckpointSaverProtocol


class MySaver:
    def put(
        self,
        config: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata: dict[str, Any],
        new_versions: dict[str, Any],
    ) -> dict[str, Any]: ...

    def put_writes(
        self,
        config: dict[str, Any],
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None: ...

    def get_tuple(self, config: dict[str, Any] | None) -> CheckpointTuple | None: ...

    def list(
        self,
        config: dict[str, Any] | None,
        *,
        before: dict[str, Any] | None = None,
        limit: int | None = None,
        filter: dict[str, Any] | None = None,
    ) -> Iterator[CheckpointTuple]: ...

    def delete_thread(self, thread_id: str) -> None: ...
```

If you already use LangGraph's `BaseCheckpointSaver` (or a saver built on it),
it can be passed **directly** — the engine detects and uses the host methods.
Async variants (`aput`, `aget_tuple`, `alist`, …) are optional; implement them
only if you call the async API.

## 3. Custom retriever

Consumed by RAG pipelines in `reactivechain`:

```python
class MyRetriever:
    def get_relevant_documents(self, query: str, **kwargs) -> list[Document]:
        ...
```

Any object whose results expose `page_content` and `metadata` works — including
LangChain's `Document`, so an existing retriever can be passed unchanged.

## 4. Custom tool

Tools are the most common extension. A tool declares its **reactive metadata**,
which is what lets the engine skip work and detect conflicts:

```python
from reactivechain.tools import tool


@tool(
    reads={"user.id"},        # state paths this tool depends on
    writes={"search.hits"},   # state paths this tool produces
    kind="effect",            # "pure" (skippable) | "effect" (idempotent) | "opaque"
    receipt=True,             # ask for a durable idempotency receipt
    return_direct=False,
)
def search(user_id: str, q: str) -> list[str]:
    """Search for *q* on behalf of *user_id*."""
    ...
```

Choosing `kind` correctly matters:

| `kind` | Engine behaviour | Use when |
|---|---|---|
| `pure` | Skipped when its declared `reads` are unchanged | The output is a function of the declared inputs only |
| `effect` | Runs, but a confirmed receipt prevents a duplicate call for the same input (across restarts) | The tool performs a side effect (payment, email, external write) |
| `opaque` | Never skipped, never deduplicated | The tool reads state the engine cannot see (e.g. the long-term store) |

> **A `pure` task must not touch the long-term store.** The store is not part
> of the state fingerprint, so such a task could be skipped even after another
> run wrote new data. The engine raises if it detects this, and points at
> `kind="opaque"`.

## 5. Exposing tools over MCP

Any tool can be served to an MCP-speaking agent. The reactive metadata travels
with it, so a client sees the same `kind`/`reads`/`writes`/`receipt`:

```python
from reactivechain.mcp import MCPServer

server = MCPServer([search, other_tool], name="my-service")

# Answer one JSON-RPC 2.0 message:
response = server.handle({
    "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}
})
```

The counterpart, `from_mcp(url)`, discovers remote tools and wraps them as
native ReactiveChain tools — a full round trip preserves the metadata:

```python
from reactivechain.mcp import from_mcp

tools = from_mcp("https://my-service/mcp")   # native BaseTool instances
```

## 6. Using a LangChain ecosystem component

The engine drives any `langchain-core` `BaseChatModel` / `BaseTool` — this is a
tested guarantee, not an accident:

```bash
# Not yet on PyPI; the extras syntax requires pip >= 24 because the package
# is installed from a git subdirectory.
pip install "reactivegraph[langchain-ecosystem] @ \
  git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
```

```python
from langchain_openai import ChatOpenAI
from reactivegraph import create_agent

agent = create_agent(ChatOpenAI(model="gpt-4o"), tools=[...])
```

The engine still owns the scheduler: selective execution, receipts and conflict
detection all keep working. Swapping in an ecosystem model does not hand
execution over to another framework.

## 7. What you do not need to extend

- **Scheduling / transactions / conflict detection** — engine-owned; a custom
  backend never reimplements them.
- **Streaming** — modes and framing are the engine's; a custom tool only
  returns values (or yields, for streaming tools).
- **Serialisation** — if your values are msgpack- or JSON-serialisable, the
  engine handles them; `reactivegraph.serde` is extensible if you need custom
  codecs.

## Where to look next

- `reactivegraph/protocols.py` — the contracts with their exact signatures
- `reactivegraph/tests/test_protocols.py` — a from-scratch implementation used
  as the acceptance test
- `reactivechain/tests/test_mcp_server.py` — the MCP round trip end to end
- `docs/spec/rgp-1.md` — the wire protocol if you are writing a non-Python client
