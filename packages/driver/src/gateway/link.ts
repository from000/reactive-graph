/**
 * Local Driver link for the gateway.
 *
 * Registers the local graph execution surface (compile/run/get_state) with a
 * gateway via `onRequest`. Cancellation lives in the gateway; execution stays
 * in the Driver. Tenant isolation: pass a per-tenant factory so each tenant
 * gets its own handler set (e.g. its own DriverRuntime) — graph registry,
 * thread state, checkpoints and store are then fully partitioned per tenant.
 * A shared handlers object keeps the pre-isolation behavior (all tenants see
 * one surface).
 */

import type { Method } from "@reactivegraph/protocol";
import type { RgpGateway, RequestHandler } from "./transport.js";

export interface CompileGraphPayload {
  id?: string;
}
export interface RunPayload {
  graphId: string;
  input: unknown;
  stream?: boolean;
  config?: Record<string, unknown>;
}
export interface RunResult {
  runId: string;
  output: unknown;
  state?: Record<string, unknown>;
}
export interface GetStatePayload {
  config: { threadId: string };
}

export interface DriverHandlers {
  compileGraph?: (def: CompileGraphPayload) => { graphId: string };
  run?: (payload: RunPayload) => RunResult;
  getState?: (payload: GetStatePayload) => { values: Record<string, unknown> };
  resume?: (payload: ResumePayload) => { runId: string; state?: Record<string, unknown> };
}

export interface ResumePayload {
  runId: string;
  threadId?: string;
  interruptResponse?: unknown;
}

function defaultCompile(payload: unknown): { graphId: string } {
  void payload;
  throw new Error("COMPILE_GRAPH: no request handler registered (DriverLink requires handlers)");
}

function defaultRun(_payload: unknown): RunResult {
  throw new Error("RUN: no request handler registered (DriverLink requires handlers)");
}

function defaultGetState(): { values: Record<string, unknown> } {
  throw new Error("GET_STATE: no request handler registered (DriverLink requires handlers)");
}

function defaultResume(_payload: unknown): { runId: string; state?: Record<string, unknown> } {
  throw new Error("RESUME: no request handler registered (DriverLink requires handlers)");
}

/**
 * Attach the local Driver surface to a gateway.
 *
 * - pass a plain `DriverHandlers` object for a shared surface (all tenants
 *   see the same handlers — the pre-isolation behavior);
 * - pass a `(tenant) => DriverHandlers` factory for per-tenant isolation:
 *   the factory is invoked lazily on each tenant's first request and its
 *   result is cached, so each tenant's graph registry / thread state /
 *   checkpoints / store live in a separate handler set (e.g. a separate
 *   DriverRuntime).
 */
export class DriverLink {
  private readonly byTenant = new Map<string, DriverHandlers>();
  private readonly handler: RequestHandler;

  constructor(
    gateway: RgpGateway,
    handlersOrFactory: DriverHandlers | ((tenant: string) => DriverHandlers) = {},
  ) {
    const factory =
      typeof handlersOrFactory === "function"
        ? (handlersOrFactory as (tenant: string) => DriverHandlers)
        : () => handlersOrFactory as DriverHandlers;
    this.handler = (method: Method, payload: unknown, tenant: string) => {
      const handlers = this.handlersFor(tenant, factory);
      switch (method) {
        case "COMPILE_GRAPH":
          return handlers.compileGraph
            ? handlers.compileGraph(payload as CompileGraphPayload)
            : defaultCompile(payload);
        case "RUN":
          return handlers.run ? handlers.run(payload as RunPayload) : defaultRun(payload);
        case "GET_STATE":
          return handlers.getState
            ? handlers.getState(payload as GetStatePayload)
            : defaultGetState();
        case "RESUME":
          return handlers.resume
            ? handlers.resume(payload as ResumePayload)
            : defaultResume(payload);
        default:
          throw new Error(`no gateway handler for ${method}`);
      }
    };
    gateway.onRequest(this.handler);
  }

  private handlersFor(tenant: string, factory: (tenant: string) => DriverHandlers): DriverHandlers {
    let handlers = this.byTenant.get(tenant);
    if (!handlers) {
      handlers = factory(tenant);
      this.byTenant.set(tenant, handlers);
    }
    return handlers;
  }
}

export type { Method, RequestHandler };
