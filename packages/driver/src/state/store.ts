/**
 * Reactive store (Task 5).
 *
 * Wraps @vue/reactivity so that all dirty paths from one transaction are grouped
 * into a single committed version. Dependencies are captured through public
 * onTrack/onTrigger hooks and manual track/trigger — never private Vue internals
 * (Architecture Invariant 6).
 *
 * Write path: in-transaction mutations go to the raw object (no auto-trigger);
 * commit applies one synchronous batch of public `trigger` calls so all computed
 * values observe a single atomic version bump.
 */

import {
  ITERATE_KEY,
  TrackOpTypes,
  TriggerOpTypes,
  reactive,
  toRaw,
  track,
  trigger,
  type DebuggerEvent,
} from "@vue/reactivity";

/** A commit-time trigger we produce ourselves (not a Vue DebuggerEvent). */
export interface StoreTriggerEvent {
  /** The raw target object whose key changed. */
  readonly target: object;
  readonly type: TriggerOpTypes;
  readonly key: string | symbol;
  readonly newValue?: unknown;
  readonly oldValue?: unknown;
}

export interface StoreOptions {
  /** Forwarded exactly as Vue emits it (a real effect read the target). */
  onTrack?: (event: DebuggerEvent) => void;
  /** Emitted once per key during our own synchronous commit burst. */
  onTrigger?: (event: StoreTriggerEvent) => void;
}

export interface VersionInfo {
  version: number;
}

/** Commit write-set entry: top-level key -> trigger op. */
export interface CommitWrite {
  op: TriggerOpTypes;
}

export class ReactiveStore {
  readonly state: Record<string, unknown>;
  readonly raw: Record<string, unknown>;
  private _version = 0;
  private readonly onTrack?: (event: DebuggerEvent) => void;
  private readonly onTrigger?: (event: StoreTriggerEvent) => void;

  constructor(initial: Record<string, unknown> = {}, opts: StoreOptions = {}) {
    this.raw = initial;
    this.onTrack = opts.onTrack;
    this.onTrigger = opts.onTrigger;
    this.state = reactive(initial);
  }

  /** Current committed version number. */
  get version(): number {
    return this._version;
  }

  /** Read `key` through the reactive proxy (records dependency when inside effect). */
  get(key: string): unknown {
    return this.state[key];
  }

  /** Capture a manual read dependency on `key` (no-op outside an active effect). */
  captureRead(key: string): void {
    track(toRaw(this.state), TrackOpTypes.GET, key);
  }

  /**
   * Commit a batch of mutations as one atomic version bump.
   * @param writes map of (topLevelKey -> trigger op). Applies one synchronous
   *               burst of public `trigger` calls with ITERATE_KEY complements.
   */
  commitVersion(writes: Map<string, CommitWrite>): number {
    const raw = toRaw(this.state);
    for (const [key, { op }] of writes) {
      const newValue = raw[key];
      trigger(raw, op, key, newValue);
      if (op === TriggerOpTypes.ADD || op === TriggerOpTypes.DELETE) {
        trigger(raw, op, ITERATE_KEY, newValue);
      }
      this.onTrigger?.({ target: raw, type: op, key, newValue });
    }
    this._version += 1;
    return this._version;
  }

  /** Debug/observability: current version info. */
  debugVersion(): VersionInfo {
    return { version: this._version };
  }
}

export { ITERATE_KEY, TrackOpTypes, TriggerOpTypes, toRaw, track, trigger };
