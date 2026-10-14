# State and Transactions Specification

Status: implemented (Task 5). Mirrors in TypeScript
(`packages/driver/src/state/*.ts`) and Python (`python/reactivegraph/reactivegraph/state.py`).

## 1. Canonical paths

A path is an ordered list of segments: **string** segments address object keys,
**numeric** segments address array indexes. Canonical string encoding `a.b[0].c`:
- string segments joined by `.`, with `.` escaped as `\.` and `[` as `\[`;
- numeric segments written as `[n]` with no leading dot.

Round-trip is exact in both languages; `pathToString`/`parsePath` in TS and
`path_to_string`/`parse_path` in Python are the byte-identical implementations.

## 2. Patches

A patch carries:

```ts
interface Patch {
  path: Path
  operation: "set" | "delete" | "append" | "insert" | "remove"
         | "map_set" | "map_delete" | "set_add" | "set_delete"
  beforeHash: string | null   // SHA-256 of canonical msgpack pre-image
  value?: unknown
  taskId: string
  transactionId: string
}
```

- `beforeHash` is the SHA-256 hex digest of the **canonical msgpack encoding**
  (RGP/1 codec) of the value at `path` before the operation.
- Missing paths hash the fixed marker `"rgp:missing"` in both languages, so a
  pre-image for a never-set path is stable and equal everywhere.
- `applyPatch` rejects a mismatched pre-image with `PatchError` unless a
  reducer owns the path (Task 10).

## 3. Store and versioning

`ReactiveStore` wraps `@vue/reactivity` (TypeScript):
- `raw` holds the plain state object; `state` is the deep reactive proxy.
- In-transaction mutations go to `raw` directly (no auto-trigger).
- `commitVersion(writes)` applies **one synchronous burst** of public `trigger`
  calls (with `ITERATE_KEY` complements for add/delete) so all computed values
  observe a single atomic version bump. Version increments once per commit.
- Dependency capture uses public `onTrack`/`onTrigger` (dev) and manual
  `track`/`trigger` (prod) — never Vue private internals (Architecture Invariant 6;
  see `docs/research/vue-reactivity-api.md`).

## 4. Transactions

`Transaction` semantics:

- `open → committed | rolled_back`, with an optional `expectedVersion`.
- `read(path)` records read paths (canonical strings) for dependency/conflict sets.
- `set(path, value)` mutates raw, records a write with pre-image; rejects
  readonly paths (`ReadonlyPathError`) and stale versions (`StaleVersionError`).
- `commit()` returns the patch list and bumps the store version once.
- `rollback()` restores all prior values **without triggering**.
- After commit/rollback any further write raises.

Conflict policy (Appendix C.5): reads record transaction version + path hashes;
disjoint writes may commit concurrently; same-path writes require a reducer,
explicit priority, or serialized execution; a stale pure task may rerun with a
new snapshot; a stale effect task may not rerun until its receipt is inspected.

## 5. Python TrackedStateProxy

`TrackedStateProxy` is the Python binding mirror:
- recursive dict/list wrappers with `base_path` chains rooted at a shared raw object;
- reads record canonical path strings (recorded on every ancestor, deduplicated);
- `__setitem__`/`__delitem__`/`append` record mutation patches with pre-image hash;
- `snapshot()` returns a plain deep copy for opaque libraries;
- `canonical_hash` produces the byte-identical digest to the TS side.

## 6. Shared patch corpus

Both test suites hard-code the identical SHA-256 digests for the same values
(`packages/driver/test/state/transaction.test.ts` and
`python/reactivegraph/tests/test_tracked_state.py`):

| Value | SHA-256 (hex, canonical msgpack) |
|---|---|
| `1` | `4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a` |
| `"hello"` | `2b57c5b79a3aee10237006d2fc64b7ecd13b761867f5992f43eda5777a0726d9` |
| `[1,2,3]` | `efd2ce5d1b243784f054828796128a9e3f85044cbfc21f7144a7a448ea3361e6` |
| `{a:1,b:[true,null]}` | `781ba872c03932379df02707033108b09a372c8e65767985971101cde7d79c0a` |
| `"rgp:missing"` | `e6849aec7e0b007792f96dd2ed18e7388696b0d6eca1d01f2a4b1f28d6889351` |

A regression in either language's canonical encoding is caught by the corpus.