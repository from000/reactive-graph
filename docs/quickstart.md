# Quickstart

**Prerequisites:** Python 3.10+ and `git`. The **Build a graph** section needs
only Python. The **Run through the Driver** section needs more: **Node.js 20+**,
**pnpm**, and a clone of this repository built from source
(`pnpm install && pnpm -r build`) — see that section.

## Install

> **Not yet on PyPI.** Install from GitHub until the package is published.

Core engine only (enough for the in-process examples on this page):

```bash
pip install "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph"
```

> **Installing `reactivechain`, `reactivegraph-sdk` or `reactivegraph-cli`?**
> Install them **together with the core package in one command** — each one
> depends on `reactivegraph>=0.1.0`, which is not on PyPI yet, so installing any
> of them alone fails dependency resolution:
>
> ```bash
> pip install \
>   "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph" \
>   "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivechain" \
>   "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_sdk" \
>   "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_cli"
> ```
>
> This also gives you the `reactivegraph` console command
> (`reactivegraph health`); the core package alone installs no console script.

## Build a graph

```python
from reactivegraph import ReactiveGraph

def build(b):
    b.task("greet", fn=lambda state: {"msg": f"hi {state['name']}"}, on=("run",))

g = ReactiveGraph.build(build)
print(g.invoke("run", {"name": "Ada"})["msg"])
```

```text
hi Ada
```

## Selective execution (the point)

Declare a task `kind="pure"` with its `reads` and ReactiveGraph skips it when
those inputs are unchanged — even across separate `invoke` calls:

```python
from reactivegraph import ReactiveGraph

calls = {"a": 0, "b": 0}

def make(name):
    def _fn(state):
        calls[name] += 1
        return {f"out_{name}": state[f"in_{name}"] * 2}
    return _fn

def build(g):
    g.task("a", fn=make("a"), on=("run",), reads=("in_a",), writes=("out_a",), kind="pure")
    g.task("b", fn=make("b"), on=("run",), reads=("in_b",), writes=("out_b",), kind="pure")

g = ReactiveGraph.build(build)
g.invoke("run", {"in_a": 1, "in_b": 2})
print(calls)                    # {'a': 1, 'b': 1}
g.invoke("run", {"in_a": 1, "in_b": 2})   # nothing changed
print(calls)                    # {'a': 1, 'b': 1} — both skipped
g.invoke("run", {"in_a": 5, "in_b": 2})   # only in_a changed
print(calls)                    # {'a': 2, 'b': 1} — only a reruns
print({k: g.explain_run()[k] for k in ("executed", "skipped", "cache_hits")})
```

```text
{'a': 1, 'b': 1}
{'a': 1, 'b': 1}
{'a': 2, 'b': 1}
{'executed': 2, 'skipped': 1, 'cache_hits': 1}
```

A conventional graph runtime re-runs every reachable node on every invoke; a
reactive runtime changes only what depends on the changed state. That is the
capability the [1000-node tutorial](tutorials/06-1000-nodes.md) and the
[differential benchmarks](benchmarks.md) quantify.

## Run through the Driver

> **Requires a source checkout, not just `pip install`.** The Driver is the
> Node.js runtime, and a wheel does not bundle it. Clone the repository, then
> `cd reactive-graph && pnpm install && pnpm -r build` (Node.js 20+, pnpm), and
> point the host at the built entry point:
>
> ```bash
> export REACTIVEGRAPH_DRIVER_ENTRY=/absolute/path/to/reactive-graph/packages/driver/dist/main.js
> ```
>
> Without `REACTIVEGRAPH_DRIVER_ENTRY` (and with no bundled entry present),
> `DriverHost(...)` raises
> `FileNotFoundError: bundled Driver entry not found`.

```python
from reactivegraph import DriverHost

host = DriverHost(env={"REACTIVEGRAPH_DB": "runtime.db"})
host.start()
host.handshake()
g = ReactiveGraph.build(build, host=host)
```

The Driver owns scheduling, transactions, checkpoints, receipts, and stream
events.

## Explain execution

```python
list(g.stream("run", {"name": "Ada"}, trace=True))
print(g.explain_run())
print(g.export_trace(format="dot"))
```

```text
hi Ada
{'events': [{'event': 'task:greet:start', 'task': 'greet'},
            {'event': 'task:greet', 'task': 'greet'}],
 'executed': 1, 'skipped': 0, 'cache_hits': 0, 'errors': 0}
digraph "latest" { ... }
```

The in-process path above needs no Driver. Swap `g` for the Driver-backed graph
from the previous section to get the same explanation from the real runtime.

## Next

- [Migration guide](migration-guide.md)
- [Agent cookbook](agent-cookbook.md)
- [API reference](api-reference.md)
