/**
 * Retry policy (Task 7). Mirrors upstream LangGraph RetryPolicy semantics:
 * initial_interval / backoff_factor / max_interval / max_attempts / jitter /
 * retry_on. Only tasks declared safe to retry may use it; an effect task's
 * receipt is inspected and idempotency policy must approve before re-run.
 */

export interface RetryPolicy {
  /** Seconds before the first retry. */
  initialInterval?: number;
  /** Multiplier applied to the interval after each retry. */
  backoffFactor?: number;
  /** Maximum seconds between retries. */
  maxInterval?: number;
  /** Maximum attempts including the first (like upstream). */
  maxAttempts?: number;
  /** Add random jitter to the interval. */
  jitter?: boolean;
  /** Decide whether an error is retryable. */
  retryOn?: (err: unknown) => boolean;
}

export interface RetryDecision {
  readonly retry: boolean;
  readonly attempt: number;
  /** Seconds to wait before the next attempt. */
  readonly backoffSeconds: number;
  readonly reason?: string;
}

export const DEFAULT_RETRY: Required<RetryPolicy> = {
  initialInterval: 0.5,
  backoffFactor: 2.0,
  maxInterval: 128.0,
  maxAttempts: 3,
  jitter: true,
  retryOn: () => true,
};

/** fnv-1a based deterministic jitter source so traces stay reproducible. */
function deterministicRandom(taskId: string, attempt: number): number {
  let h = 2166136261;
  const s = `${taskId}#${attempt}`;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0) / 4294967296;
}

/**
 * Decide whether to retry after a failed attempt.
 * @param attempt 1-based attempt number that just failed.
 */
export function decideRetry(
  policy: RetryPolicy,
  err: unknown,
  attempt: number,
  taskId: string,
  _opts?: { now?: number },
): RetryDecision {
  const p: Required<RetryPolicy> = { ...DEFAULT_RETRY, ...policy };
  if (attempt >= p.maxAttempts) {
    return { retry: false, attempt, backoffSeconds: 0, reason: "max_attempts" };
  }
  if (!p.retryOn(err)) {
    return { retry: false, attempt, backoffSeconds: 0, reason: "not_retryable" };
  }
  // exponential backoff with cap
  const exponent = Math.max(attempt - 1, 0);
  let interval = p.initialInterval * Math.pow(p.backoffFactor, exponent);
  interval = Math.min(interval, p.maxInterval);
  if (p.jitter) {
    interval *= 0.5 + deterministicRandom(taskId, attempt);
  }
  return { retry: true, attempt: attempt + 1, backoffSeconds: interval };
}
