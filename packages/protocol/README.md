# @reactivegraph/protocol

The RGP/1 wire contract: envelope and method definitions, length-prefixed
framing, and the canonical MessagePack value codec.

This is the lowest layer of the stack — both the Driver and every host binding
speak it. You only need to depend on it directly when implementing a new RGP/1
endpoint.

## Install

```bash
npm install @reactivegraph/protocol
```

## What is in here

| Export | Purpose |
|---|---|
| `PROTOCOL_NAME`, `PROTOCOL_VERSION` | Handshake identity; the version is bumped on breaking wire changes |
| `METHOD_LIST`, `isMethod`, `makeId` | Method surface shared by both ends |
| `isEnvelope`, type `Envelope` | Request / response / notification envelope validation |
| `encodeFrame`, `FrameDecoder`, `ProtocolError` | Length-prefixed framing over a byte stream |
| `encodeValue`, `decodeValue`, `CodecError` | Canonical MessagePack codec |

## Framing

Each frame is a 4-byte big-endian length header followed by one MessagePack
payload. `FrameDecoder.push()` returns whatever complete frames are available,
buffering partial and multi-frame chunks — so it can be driven straight from a
socket's `data` event:

```ts
import { encodeFrame, FrameDecoder } from "@reactivegraph/protocol";

const decoder = new FrameDecoder();
const frames = decoder.push(encodeFrame(new Uint8Array([1, 2, 3])));
// frames[0] -> Uint8Array [1, 2, 3]
```

A frame longer than `maxFrameBytes` (default 32 MiB) throws `ProtocolError` and
resets the decoder instead of allocating unbounded memory.

## Canonical values

`encodeValue` / `decodeValue` define the set of values that may cross the wire:

- `null`, `boolean`, `number` (safe integers encode as int, other numbers as float)
- `bigint` (int64 / uint64), `string`, `Uint8Array` (bin)
- `Date` (MessagePack timestamp), decimal via the tagged
  `{ __type__: "decimal", value: "1.23" }` marker
- arrays and plain objects with string keys

Non-canonical values (`function`, `undefined` in value position, cyclic
references, `Map` / `Set`, class instances) are rejected with `CodecError`
rather than being silently coerced.

Decoding returns data only and never executes code.

## License

MIT — see [`LICENSE`](./LICENSE). Third-party attributions are in
[`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md).
