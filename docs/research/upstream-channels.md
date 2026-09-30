# Research Notes: Upstream LangGraph Channels Semantics

Source: upstream `langchain-ai/langgraph` @ `11ee185999b86bfea2d8c0e69cef9a5e37acf686`
`libs/langgraph/langgraph/channels/`. For ReactiveGraph compatibility layer (Task 10).

## Unified contract (channels/base.py)

- Pregel calls each channel's `update(values)` **exactly once per superstep**;
  `values` = all updates collected this step (order arbitrary, may be empty).
- Checkpoint persists `channel.checkpoint()` snapshot into `channel_values`.
- Delta persistence is a separate mechanism (`DeltaChannel` + periodic snapshot +
  `checkpoint_writes` incremental replay in `pregel/_checkpoint.py`). The five
  plain channels below only emit value snapshots.

## Per-channel semantics

| Channel | update() semantics | Checkpoint | Notes |
|---|---|---|---|
| LastValue | `values[-1]`; `len != 1` → `InvalidUpdateError` | single value snapshot | default state channel; overwrite semantics; TimeTravel restores snapshot |
| Topic | `_flatten` list values; `accumulate=False` → clear old then extend new; `accumulate=True` → append | entire message list snapshot (back-compat tuple format) | PubSub/streaming messages; non-accumulate clears after consumed |
| BinaryOperatorAggregate | MISSING → init with `values[0]`, then fold `operator(value, v)`; supports Overwrite (3 forms: `Overwrite` instance / `{"__overwrite__": v}` / `{"type":"__overwrite__","value":v}`), at most one per step else error | folded snapshot | messages accumulation, counters, dict merge |
| NamedBarrierValue | merge into `seen`; value not in `names` → `InvalidUpdateError`; ready when `seen == names`; `consume()` unlocks and clears | seen set snapshot | multi-branch sync barrier |
| EphemeralValue | empty updates → auto-clear to MISSING ("clears after"); `guard=True` → `len != 1` errors; `guard=False` → multiple, take `values[-1]` | usually MISSING | single-step handshake / resume signal |

## Hard constraints ReactiveGraph must replicate

1. Exactly one merge per channel per superstep; **must call `update([])` even with no
   writes** — EphemeralValue clearing depends on it.
2. Single-value constraints: LastValue / EphemeralValue(guard=True) concurrent
   writes must error, not silently overwrite.
3. BinaryOperatorAggregate merge is ordered and atomic; MISSING init; Overwrite
   takes effect within the same batch.
4. Topic accumulate switch + non-accumulate "clear then add"; checkpoint must
   persist unconsumed message list snapshot (TimeTravel replay depends on it).
5. NamedBarrierValue set-equality unlock + consume reset + unknown-name strict check.
6. All five persist value snapshots; delta is a separate mechanism (snapshot first,
   delta later).