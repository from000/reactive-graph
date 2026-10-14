# Research Notes: Cross-Language MsgPack Canonical Values (RGP/1 codec)

Python `msgpack 1.2.2` vs TS `@msgpack/msgpack 3.1.3`.

## Capability table

| Dimension | Python | TS |
|---|---|---|
| bytes/str | `use_bin_type=True` (default): bytes→bin, str→str | Uint8Array→bin, string→str; `rawStrings` optional |
| int64 | int arbitrary precision; `-2^63 ≤ n < 2^64` encodes directly; overflow → `default=` callback (fires once) else `OverflowError` | `useBigInt64:true`: bigint→int64/uint64, >2^64 → RangeError (no fallback); number only safe-int as int else float |
| int64 decode | always exact int | `useBigInt64:true`→bigint; default uses number, loses precision >2^53 |
| timestamp | native ext **-1** `Timestamp` (32/64/96 layouts); datetime needs `datetime=True` + tzinfo, naive raises; else TypeError | built-in timestampExtension: any Date auto-encodes ext -1, same layout; decodes back to Date, **ms precision only** |
| custom ext | `ExtType(code, data)` code 0..127, decode via `ext_hook` | `extensionCodec.register({type, encode, decode})`; type≥0 custom, type<0 overrides builtin; unregistered type → `ExtData` |
| map key | decode `strict_map_key=True`: str/bytes only | default `mapKeyConverter` accepts string/number |

## Normalized envelope conventions (byte-identical both sides)

- **timestamp**: native ext **-1** (layouts verified identical). Python: pass
  `Timestamp` or `packb(dt, datetime=True)`; TS: pass `Date`.
- **decimal**: custom ext **type 0**, payload = UTF-8 decimal string (`b"3.14"`).
  Python `ExtType(0, ...)` + `ext_hook`; TS `register({type:0, encode: TextEncoder, ...})`.
- **big int64**: TS always `bigint + useBigInt64:true` (number banned >2^53);
  Python plain int works for `-2^63≤n<2^64`. For arbitrary-precision ints prefer a
  string ext to avoid Python `default=` callback / TS no-fallback asymmetry.
- **bytes**: keep default bin on both sides; **map keys forced to string** to
  satisfy both default decode constraints.

## Risks

- TS Date is ms precision (ns needs custom type -1 decode override).
- TS >2^64 raises RangeError; Python routes to default callback — define the
  fallback contract explicitly.
- Python Cython extension binary not directly verified; behavior based on
  fallback.py (consistent per docs).