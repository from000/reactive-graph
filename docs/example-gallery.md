# Example Gallery

Each example is executable from the repository and covered by tests. No
example depends on a paid service unless explicitly marked “real model mode.”

## Native DeerFlow-style workbench

Location: `examples/deerflow/`

Capabilities:

- selective task execution
- computed selectors
- scope isolation
- streaming through the real Driver
- long-term store
- checkpoints
- time travel / restore
- HITL resume
- vector search
- DOT export
- browser E2E

Run offline tests:

```bash
uv run --directory examples/deerflow --extra test pytest -q
```

Open the local workbench:

```bash
uv run --directory examples/deerflow python frontend/server.py --port 8000
```

Optional real-model mode uses an AGNES-compatible OpenAI endpoint and is
skipped when `AGNES_API_KEY` is absent.

## Tutorials

The docs tutorials are runnable scripts, not pseudocode. CI checks each script
and every performance or behavior claim points back to a reproducible command:

```bash
uv run --directory python/reactivegraph python scripts/check-tutorials.py
```

Start with:

1. `docs/tutorials/01-why-reactive.md`
2. `docs/tutorials/02-first-graph.md`
3. `docs/tutorials/03-computed-scope.md`
4. `docs/tutorials/04-streams-resume.md`
5. `docs/tutorials/05-durability.md`
6. `docs/tutorials/06-1000-nodes.md`

## Real DeerFlow base-replacement PoC

The real `bytedance/deer-flow` factory path was compared against a
ReactiveGraph replacement:

```bash
uv run --directory python/reactivechain   python -m pytest $PWD/benchmarks/deerflow_real/ -q
```

See `benchmarks/deerflow_real/REPORT.md`.

## Migration example

The LangGraph importer test contains a complete upstream graph → native graph
example:

```bash
uv run --directory python/reactivechain \
  python -m pytest tests/test_langgraph_import.py -q
```

See [migration guide](migration-guide.md) for the API.

## Production example

Generate a production-ready Dockerfile, health check, and Compose lifecycle:

```bash
pip install \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph" \
  "git+https://github.com/from000/reactive-graph.git#subdirectory=python/reactivegraph_cli"
reactivegraph new my-app
cd my-app
reactivegraph dockerfile .
reactivegraph up .
```

See [production deployment guide](production-deployment.md).
