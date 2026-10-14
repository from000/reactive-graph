# Research Notes: @vue/reactivity Public API for Transactional State

Target: `@vue/reactivity@3.5.42` (only depends on `@vue/shared`, standalone from vue).

## onTrack / onTrigger

```ts
type DebuggerEvent = { effect: Subscriber } & {
  target: object; type: TrackOpTypes | TriggerOpTypes; key: any;
  newValue?: any; oldValue?: any; oldTarget?: Map | Set;
}
interface DebuggerOptions { onTrack?: (e: DebuggerEvent) => void;
                            onTrigger?: (e: DebuggerEvent) => void }
```

- `onTrack`: fired inside `Dep.track()` when an active effect reads target/key
  (`GET | HAS | ITERATE`) — collect read paths.
- `onTrigger`: fired inside `Dep.notify()` per subscriber before scheduling
  (`SET | ADD | DELETE | CLEAR`, carries new/old values).
- **Production limitation**: both hooks exist only in dev builds (prod build
  strips them). For path collection in production, use exported `track`/`trigger`.

## Core functions

```ts
reactive(t)  ref(v)  computed(g, {onTrack,onTrigger}?)  effect(fn, {scheduler,onStop,allowRecurse})
stop(runner)  pauseTracking()  enableTracking()  resetTracking()
ReactiveEffect class (pause/resume/run/stop/trigger/dirty; runner.effect)
customRef(factory)
```

## Manual path control (dev+prod, public exports)

- `track(target, type, key)` / `trigger(target, type, key, newValue?, oldValue?, oldTarget?)`
  take **raw** objects — call `toRaw(proxy)` first.
- Public symbols: `ITERATE_KEY` (object for-in/spread), `MAP_KEY_ITERATE_KEY`
  (Map key iteration), `ARRAY_ITERATE_KEY` (array iteration).

## Recommended transactional store design (Task 5)

1. `raw = toRaw(reactiveState)`; begin: snapshot or write-ahead log
   `(key → {type,new,old})`, call `pauseTracking()` to suspend outer read collection.
2. In-transaction writes go through the store facade: mutate `raw` directly (no
   side effects, no auto-trigger), record dirty paths; reads via `reactive` proxy stay deep.
3. **commit**: for each dirty key call `trigger(raw, type, key, new, old)` with
   correct `ITERATE_KEY` / `MAP_KEY_ITERATE_KEY` / `ARRAY_ITERATE_KEY` / `'length'`
   complements (proptypes for adds/deletes/array-index/Map-key), then
   `resetTracking()`. All triggers in one synchronous block → externally atomic;
   rollback restores old values without triggering.

## Pitfalls

- Array index add does NOT auto-trigger `length` deps — add
  `trigger(raw,'set','length')` yourself; `Map.set` on existing key also triggers
  ITERATE_KEY; Set same as Map branch.
- `pauseTracking` suppresses track only, NOT trigger — in-transaction writes must
  go to raw, never let the proxy auto-trigger.
- `track` records only when an activeSub exists and shouldTrack; manual track is a
  no-op with no active effect.
- `computed({onTrack,onTrigger})` is dev-only too — do not rely in production.

Evidence: `node_modules/.pnpm/@vue+reactivity@3.5.42/node_modules/@vue/reactivity/dist/{reactivity.d.ts,reactivity.cjs.js,reactivity.cjs.prod.js,reactivity.esm-bundler.js}`,
`package.json`.