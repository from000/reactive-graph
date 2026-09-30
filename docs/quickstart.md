# Quickstart

## Install

```bash
pip install reactivegraph
```

## Build a graph

```python
from reactivegraph import ReactiveGraph

def build(b):
    b.task("greet", fn=lambda state: {"msg": f"hi {state['name']}"}, on=("run",))

g = ReactiveGraph.build(build)
print(g.invoke("run", {"name": "Ada"})["msg"])
```

## Run through the Driver

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

## Next

- [Migration guide](migration-guide.md)
- [Agent cookbook](agent-cookbook.md)
- [API reference](api-reference.md)
