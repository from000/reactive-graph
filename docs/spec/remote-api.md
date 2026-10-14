# Remote API (Task 11 / D.9.98-101)

ReactiveGraph exposes RGP/1 over an authenticated duplex transport
(WebSocket / HTTP/2 in production; `InMemoryDuplex` in-process for the
integration suites). The protocol, framing, and canonical value set are the
single cross-language source of truth in `docs/spec/rgp-1.md` and
`packages/protocol/src/`.

## Transport

A `Duplex` is the byte-level contract shared by the Driver stdio link and the
remote gateway:

```
send(bytes)            — write outbound bytes (false = transport backpressure)
ingest(chunk)          — feed inbound bytes
onData(cb) / onClose(cb)
onDrain(cb)            — optional: transport is writable again
```

`RgpGateway` authenticates an accepted duplex against a token→tenant map,
wraps it in a server-side `RgpSession`, and hands behavior to a `DriverLink`
(compile/run/get_state). Cancellation is propagated as a CANCEL frame; the
gateway applies per-session backpressure above a high-water mark.

When `send()` returns `false`, the gateway holds subsequent frames in a bounded
per-session queue and flushes them in order when `onDrain` fires. If the queue
would exceed `highWaterMark`, the slow consumer is disconnected with a
`SlowConsumerError` instead of allowing unbounded server memory growth.
`RgpGateway.metrics()` exposes `sessions`, `blockedSessions`, `queuedBytes`, and
`evictedSlowConsumers` for monitoring.

## Envelope (RGP/1 §2)

Every exchange is an `Envelope { version:1, id, kind, method, payload, trace }`.

| kind | direction | purpose |
|---|---|---|
| `request` | client → server | invoke a method |
| `response` | server → client | correlated reply (errors as `{error:{type,message}}`) |
| `event` | server → client | `STREAM_EVENT` etc. |
| `cancel` | either | cancel an in-flight request/run |

## Methods (subset used by SDKs)

| method | payload | response |
|---|---|---|
| `DRIVER_HELLO` | `{}` | `{ok}` |
| `COMPILE_GRAPH` | `{id, tasks, routes}` | `{graphId}` |
| `RUN` | `{graphId, input, stream, config}` | `{runId, output, state}` |
| `GET_STATE` | `{config}` | `{values, next, config}` |
| `STORE_OP` | `{op, ...}` | op-specific (assistants/threads/crons/store) |
| `CANCEL` | `{runId}` | `{cancelled}` |

## SDK surface

* Python `reactivegraph_sdk` — sync + async clients; assistants / threads /
  runs / crons / store; SSE-shaped `stream`.
* JS `@reactivegraph/sdk-js` `RemoteClient` — compile / run / get_state /
  cancel / stream over the same RGP/1.

Both suites run the **same fixture scenarios** locally (in-process duplex) and
remotely (through the gateway) — the integration tests assert parity by driving
the identical handler map through each path.

## Backpressure & cancellation

* Writes above the transport high-water mark are stalled (not dropped) until
  the reader drains; a peer that remains stalled beyond the bounded queue is
  evicted with an explicit `SlowConsumerError`.
* `CANCEL` frames are fire-and-forget; the gateway emits a `cancelled` event so
  observability can correlate cancellation with the run.
* Client request timeouts reject the caller and clean up the pending table.

## Auth

Gateway tenants are configured as `{token: tenant}`. An unknown token receives
an error frame and the session is closed. When **no tenant map is configured**,
every token authenticates to the single `default` tenant — the explicit
in-process/local no-auth mode; production deployments must set a tenant map.
The tenant is passed to request handlers so a multi-tenant deployment can
isolate runs and checkpoints.

`rotateToken(oldToken, newToken, tenant)` is atomic for new handshakes: the old
token stops authenticating and the new token starts authenticating without
dropping already-authenticated sessions. A rotation that would overwrite a
token owned by another tenant is rejected. Prototype-shaped tokens such as
`__proto__` are stored as ordinary credentials and cannot mutate the auth map.
