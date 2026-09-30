# Deerflow — native API example

A DeerFlow-style SuperAgent built entirely on the **native ReactiveGraph API**
(dependency-free, no external services required). It verifies the headline
capabilities in one place:

- **Selective task execution** — `TaskDef` with `scope`, computed selectors
- **Scope isolation** — scoped counters never collide (`tenantA`/`tenantB`)
- **TrackedStateProxy** — read-set awareness without a Driver
- **Prebuilt ReAct agent** — `create_react_agent` + `ToolNode`
- **Streaming** — `graph.stream(...)` through the real bundled Driver
- **Long-term store** — `store_put` / `store_get`
- **Checkpoint / state** — `get_state`, `list_checkpoints`

## Run

```bash
uv run --directory examples/deerflow --extra test pytest -q
```

Driver-backed tests launch the bundled Node Driver automatically and skip
cleanly when `node` / `packages/driver/dist/main.js` are unavailable.

## Frontend workbench (DeerFlow-style, pure Python)

A zero-dependency single-page workbench (stdlib `http.server` + native JS/SSE)
that exercises every engine capability through the real Driver:

```bash
uv run --directory examples/deerflow python frontend/server.py --port 8000
# open http://127.0.0.1:8000
```

- **对话 tab**:ask a research question → generator task streams tokens
  (`messages`) live, `values` snapshots on the side.
- **能力实验室 tab**:13 cards, each a real endpoint over RGP/1 —
  event routing, token streaming, computed, scope isolation, checkpoint
  persistence/list, time-travel restore, human-in-the-loop resume, prebuilt
  ReAct agent, long-term store, **recursion limit** (config-driven
  `RecursionLimitError` with Hint), **dot export** (`EXPORT_DOT` →
  Graphviz DOT) and **vector search** (`VECTOR_UPSERT`/`VECTOR_SEARCH`
  cosine ranking).
- Persistence: the server starts its Driver with `REACTIVEGRAPH_DB` (sqlite
  checkpoints in a temp dir).
- Browser E2E is **automated** with pytest-playwright (headless Chromium):
  `tests/test_browser_e2e.py` opens the workbench and asserts DOM rendering
  (chat values stream, scope isolation, DOT export, vector search, HITL
  resume) against the real Driver.

Endpoint tests: `uv run --directory examples/deerflow --extra test pytest tests/test_frontend.py -q`
Browser E2E tests: `uv run --directory examples/deerflow --extra test pytest tests/test_browser_e2e.py -q`

> 安全注记（生产化审查）:该服务器是本地演示工作台,不是公网服务。POST 端点
> 仅接受 `application/json` 并校验 Origin(同机跨站伪造请求拒绝);store
> namespace/key 有白名单;calculator 有长度上限;错误回显截断。公网部署需
> 自行加鉴权/TLS。

## Real-model mode (AGNES gateway)

The workbench can drive a real LLM — no third-party deps (stdlib `urllib`):

```bash
export AGNES_API_KEY=...            # or already in your shell profile
export CUSTOM_LLM_URL=https://apihub.agnes-ai.com/v1
export CUSTOM_MODEL=agnes-2.5-flash
uv run --directory examples/deerflow python frontend/server.py --port 8000
```

- Chat tab: tick **真实模型** and ask — TRUE token streaming over SSE
  (`/api/chat-stream`, `chat.completions stream=true`): every `data:` frame
  is one model token rendered as it arrives.
- Capability lab: **真实 LLM Agent** card runs a real multi-tool ReAct loop:
  AGNES picks among **weather**, a safe **calculator**, and **knowledge
  retrieval** over the engine's vector store (VECTOR_SEARCH with a
  deterministic n-gram embedding); tool_calls are normalised to the engine's
  `{id,name,arguments}` shape both ways.
- Real-model endpoint tests run only when `AGNES_API_KEY` is set; otherwise
  they skip cleanly.
