/**
 * Test utilities for ReactiveGraph (Task 11 testkit).
 *
 * Deterministic clock + a tiny fault injector, used by scheduler / durability
 * tests to control time and force failures without real sleeps or mocks.
 */

export const TESTKIT_VERSION = "0.1.0";

/** Deterministic clock: manual time control for tests. */
export class FakeClock {
  private nowMs: number;
  constructor(initialMs = 0) {
    this.nowMs = initialMs;
  }
  now(): number {
    return this.nowMs;
  }
  advance(ms: number): number {
    this.nowMs += ms;
    return this.nowMs;
  }
}

/** Fault injector: force a function to fail N times before succeeding. */
export class FaultInjector {
  private failures: number;
  constructor(initialFailures = 0) {
    this.failures = initialFailures;
  }
  /** True if the next call should fail (decrements the counter). */
  shouldFail(): boolean {
    if (this.failures > 0) {
      this.failures -= 1;
      return true;
    }
    return false;
  }
  remaining(): number {
    return this.failures;
  }
}
