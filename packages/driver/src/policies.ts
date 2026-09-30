/**
 * Reusable task policies (Task 12 / D.9.105-106): budget, rate-limit,
 * circuit-breaker, and priority. Each policy is a small pure object with an
 * `allow(context)` check; the scheduler can consult them before dispatch.
 * They compose: a task may carry a budget, a rate limiter, and a priority.
 */

export interface TaskContext {
  readonly taskId: string;
  readonly runId?: string;
  readonly cost?: number;
  readonly at?: number;
}

export interface PolicyDecision {
  readonly allowed: boolean;
  readonly reason?: string;
  readonly delayMs?: number;
}

export interface Policy {
  readonly kind: string;
  allow(ctx: TaskContext): PolicyDecision;
}

// ---------------------------------------------------------------------------
// Budget: cumulative cost cap across a run.
// ---------------------------------------------------------------------------

export class BudgetPolicy implements Policy {
  readonly kind = "budget";
  private spent = 0;
  constructor(
    private readonly limit: number,
    private readonly costPerCall = 1,
  ) {}

  allow(ctx: TaskContext): PolicyDecision {
    const cost = ctx.cost ?? this.costPerCall;
    if (this.spent + cost > this.limit) {
      return { allowed: false, reason: `budget exceeded (${this.spent}/${this.limit})` };
    }
    this.spent += cost;
    return { allowed: true };
  }

  get spentAmount(): number {
    return this.spent;
  }
}

// ---------------------------------------------------------------------------
// Rate limit: sliding-window count of calls.
// ---------------------------------------------------------------------------

export class RateLimitPolicy implements Policy {
  readonly kind = "rate_limit";
  private timestamps: number[] = [];
  constructor(
    private readonly maxCalls: number,
    private readonly windowMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  allow(ctx: TaskContext): PolicyDecision {
    const t = ctx.at ?? this.now();
    this.timestamps = this.timestamps.filter((ts) => t - ts < this.windowMs);
    if (this.timestamps.length >= this.maxCalls) {
      const oldest = this.timestamps[0] ?? t;
      return {
        allowed: false,
        reason: "rate limit exceeded",
        delayMs: Math.max(0, this.windowMs - (t - oldest)),
      };
    }
    this.timestamps.push(t);
    return { allowed: true };
  }
}

// ---------------------------------------------------------------------------
// Circuit breaker: fail-open after N consecutive errors, then a cool-down.
// ---------------------------------------------------------------------------

export class CircuitBreakerPolicy implements Policy {
  readonly kind = "circuit_breaker";
  private consecutiveErrors = 0;
  private openedAt: number | undefined;
  constructor(
    private readonly failureThreshold: number,
    private readonly cooldownMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  allow(ctx: TaskContext): PolicyDecision {
    const t = ctx.at ?? this.now();
    if (this.openedAt !== undefined) {
      if (t - this.openedAt >= this.cooldownMs) {
        // half-open: allow a probe
        this.openedAt = undefined;
        return { allowed: true, reason: "half-open probe" };
      }
      return { allowed: false, reason: "circuit open" };
    }
    return { allowed: true };
  }

  onSuccess(): void {
    this.consecutiveErrors = 0;
    this.openedAt = undefined;
  }

  onError(): void {
    this.consecutiveErrors += 1;
    if (this.consecutiveErrors >= this.failureThreshold) {
      this.openedAt = this.now();
      this.consecutiveErrors = 0;
    }
  }
}

// ---------------------------------------------------------------------------
// Priority: a static ordering hint, plus an optional concurrency allowance.
// ---------------------------------------------------------------------------

export class PriorityPolicy implements Policy {
  readonly kind = "priority";
  constructor(
    readonly priority: number, // higher = more important
    private readonly concurrencyAllowance = 0, // 0 = no limit
  ) {}

  allow(): PolicyDecision {
    return { allowed: true };
  }
}

export function checkAll(policies: readonly Policy[], ctx: TaskContext): PolicyDecision {
  for (const policy of policies) {
    const decision = policy.allow(ctx);
    if (!decision.allowed) return decision;
  }
  return { allowed: true };
}
