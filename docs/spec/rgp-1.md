# RGP/1 Wire Protocol Specification

Version: 1 · Status: frozen · Cross-language contract between the Node Driver and
all SDK bindings (Python, JS). Source of truth for byte layout: this document plus
the shared fixture corpus in `packages/protocol/test/framing.test.ts` and
`python/reactivegraph/tests/test_protocol.py`.

## 1. Transport

1. Local transport: a single long-lived child-process stdio connection.
2. Frames: **four-byte unsigned big-endian byte length** followed by one
   MessagePack envelope.
3. Maximum decoded frame: 32 MiB by default, configurable downward. Oversized
   frames close the session with a protocol error.
4. Frame order is preserved on the wire, but requests may complete out of order;
   every request and callback has a globally unique `id` within its session.
5. Remote transport uses the exact same envelope schema over WebSocket or HTTP/2;
   no second JSON API with different lifecycle meanings.
6. Secrets, raw prompts, tool arguments, and state values are absent from
   transport-level error messages unless explicit unsafe development mode is enabled.

## 2. Envelope schema

```ts
type Envelope = {
  version: 1;
  id: string;
  kind: "request" | "response" | "event" | "cancel";
  method: Method;
  trace?: { runId?: string; transactionId?: string; parentSpanId?: string };
  payload: unknown;
};

type Method =
  | "DRIVER_HELLO"
  | "COMPILE_GRAPH"
  | "RELEASE_GRAPH"
  | "RUN"
  | "RESUME"
  | "GET_STATE"
  | "TASK_INVOKE"
  | "TASK_RESULT"
  | "STREAM_EVENT"
  | "CHECKPOINT_OP"
  | "STORE_OP"
  | "EXPORT_DOT"
  | "VECTOR_UPSERT"
  | "VECTOR_SEARCH"
  | "TRACE_QUERY"
  | "CANCEL"
  | "SHUTDOWN";
```

## 3. Canonical values (codec)

Cross-language canonical state values (see `docs/research/msgpack-cross-language.md`):

| Value                 | Encoding                                                                  |
| --------------------- | ------------------------------------------------------------------------- |
| null / boolean        | msgpack nil / bool                                                        |
| integer               | int; int64 range `-2^63 ≤ n < 2^64`. TS uses `bigint + useBigInt64:true`. |
| float                 | msgpack float                                                             |
| string                | msgpack str                                                               |
| bytes                 | msgpack bin (Uint8Array / bytes)                                          |
| list / array          | msgpack array                                                             |
| map                   | msgpack map with **string keys**                                          |
| timestamp             | msgpack native ext **-1** (32/64/96-bit layouts)                          |
| decimal               | custom ext **type 0**, payload = UTF-8 decimal string                     |
| registered extensions | custom ext types ≥1                                                       |

Non-canonical objects (functions, undefined, cyclic refs, arbitrary class instances,
Map/Set as bare values) are rejected by the codec with a `CodecError`/`ProtocolError`.

## 4. Validation rules

1. Unknown `version` (≠1) → reject.
2. Frame length > `maxFrameBytes` → `ProtocolError`, close session (decoder resets).
3. Duplicate request `id` in a session → reject second.
4. Malformed payload / undecodable envelope → reject that session only.
5. A `response` whose `method` does not match the pending request's method → reject.

## 5. Method payload contracts

| Method          | Required fields                                         | Negative cases                                                                                                  |
| --------------- | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `DRIVER_HELLO`  | sdkVersion, protocolVersions[], featureFlags            | no common version; unknown required feature                                                                     |
| `COMPILE_GRAPH` | graphSpec, callbackIds[], schemaHash                    | duplicate id; unregistered callback; cycle with no policy                                                       |
| `RUN`           | graphId, input, config, threadId                        | missing thread for durable run; unserializable value                                                            |
| `RESUME`        | runId, threadId, interruptResponse, expectedVersion     | stale version; already-terminal run                                                                             |
| `TASK_INVOKE`   | invocationId, snapshotVersion, callbackId, allowedPaths | unknown callback; duplicate invocation; response may carry optional `stream_chunks` (transient messages/custom) |
| `TASK_RESULT`   | invocationId, readSet, patches, result, receipt         | invalid path; write denied; stale state                                                                         |
| `STREAM_EVENT`  | cursor, eventType, payload, terminal                    | cursor regression; event after terminal                                                                         |
| `EXPORT_DOT`    | graphId                                                 | unknown graph                                                                                                   |
| `TRACE_QUERY`   | runId                                                   | no durable log; unknown run                                                                                     |
| `VECTOR_UPSERT` | namespace, id, vector, metadata?                        | missing id/vector                                                                                               |
| `VECTOR_SEARCH` | namespace, query, limit?, minScore?                     | missing query                                                                                                   |
| `CANCEL`        | targetId, reason                                        | unknown target; terminal run                                                                                    |

## 6. Callback result contract (Python)

```python
TaskSuccess(reads, patches, return_value, external_receipts)
TaskFailure(error_type, message, traceback, retryable)
TaskInterrupted(value, checkpoint_hint)
TaskCancelled(reason)
```

The Driver validates each form against the invocation's transaction and permission
context. A callback never decides whether its own state changes commit.

**Streaming callbacks**: a host may register a _generator_ callback (sync or
async). Each yielded value becomes a transient `stream_chunks` entry in the
`TASK_INVOKE` response — a `{type, payload}` dict passes through, anything else
(e.g. a token string) is wrapped as a `messages` chunk. The Driver forwards
these as `STREAM_EVENT`s (`messages`/`custom`) before committing the generator's
return value (which must still be a `TaskSuccess` shape). Consumers see LLM
token churn in `stream()` before the task commits. The Python binding exposes
this as a plain generator task function:

```python
def llm(model_state):
    for token in model_tokens:
        yield token                 # streamed as a messages chunk
    return {"patches": [...], "writes": [...], "return_value": "done"}
```
