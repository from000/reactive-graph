/**
 * Message assembler (upstream `langgraph-sdk` stream projection).
 *
 * Folds `messages` stream events into complete messages by id:
 *
 *  - `message-start` opens a partial message for an id;
 *  - deltas append content chunks / tool-call blocks;
 *  - `message-end` marks it complete and yields the assembled message.
 *
 * This is the client-side half of streaming: the server emits deltas, the
 * assembler reconstructs whole messages without buffering the entire stream.
 */

export interface MessageEventData {
  event?: string;
  id?: string;
  node?: string;
  metadata?: Record<string, unknown>;
  content?: unknown;
  tool_calls?: Array<{ id?: string; name?: string; args?: unknown }>;
  usage?: { input?: number; output?: number; total?: number };
  [key: string]: unknown;
}

export interface MessageEvent {
  namespace: readonly string[];
  node?: string;
  data: MessageEventData;
}

export interface AssembledMessage {
  id: string;
  namespace: readonly string[];
  node?: string;
  metadata?: Record<string, unknown>;
  content: unknown;
  tool_calls: Array<{ id?: string; name?: string; args?: unknown }>;
  usage?: { input?: number; output?: number; total?: number };
  complete: boolean;
}

export type AssemblyUpdate =
  | { kind: "message-start"; key: string; message: AssembledMessage }
  | { kind: "message-delta"; key: string; message: AssembledMessage }
  | { kind: "message-end"; key: string; message: AssembledMessage; final: AssembledMessage }
  | { kind: "message-error"; key: string; message: AssembledMessage; error: string };

/** Key under which a message is assembled (namespace + node + id). */
export function messageKeyFor(event: MessageEvent): string {
  return `${event.namespace.join("/")}::${event.node ?? ""}::${event.data.id ?? ""}`;
}

function appendContent(target: unknown, delta: unknown): unknown {
  if (delta === undefined || delta === null) return target;
  if (typeof target === "string") {
    return target + String(delta);
  }
  return delta;
}

export class MessageAssembler {
  private readonly active = new Map<string, AssembledMessage>();

  consume(event: MessageEvent): AssemblyUpdate {
    const key = messageKeyFor(event);
    const evt = event.data.event ?? "message-delta";

    if (evt === "message-start") {
      const message: AssembledMessage = {
        id: event.data.id ?? "",
        namespace: [...event.namespace],
        node: event.node,
        metadata: event.data.metadata,
        content: "",
        tool_calls: [],
        complete: false,
      };
      if (event.data.usage) message.usage = event.data.usage;
      this.active.set(key, message);
      return { kind: "message-start", key, message };
    }

    const existing = this.active.get(key);
    if (!existing) {
      // delta without a prior start: synthesize a shell so we don't drop content
      const shell: AssembledMessage = {
        id: event.data.id ?? "",
        namespace: [...event.namespace],
        node: event.node,
        metadata: event.data.metadata,
        content: "",
        tool_calls: [],
        complete: false,
      };
      this.active.set(key, shell);
      return this.consume(event);
    }

    if (evt === "message-end") {
      existing.complete = true;
      if (event.data.usage) existing.usage = event.data.usage;
      const final = { ...existing, tool_calls: [...existing.tool_calls] };
      return { kind: "message-end", key, message: existing, final };
    }

    if (evt === "message-error") {
      existing.complete = true;
      return {
        kind: "message-error",
        key,
        message: existing,
        error: String(event.data.error ?? "unknown"),
      };
    }

    // delta
    if (event.data.content !== undefined) {
      existing.content = appendContent(existing.content, event.data.content);
    }
    if (Array.isArray(event.data.tool_calls)) {
      for (const tc of event.data.tool_calls) {
        const idx = existing.tool_calls.findIndex((t) => t.id && t.id === tc.id);
        if (idx >= 0) {
          // fold delta blocks: merge fields, not replace whole call
          const current = existing.tool_calls[idx]!;
          existing.tool_calls[idx] = {
            ...current,
            ...tc,
            args: {
              ...((current.args ?? {}) as object),
              ...((tc.args as object | undefined) ?? {}),
            },
          };
        } else {
          existing.tool_calls.push({ ...tc });
        }
      }
    }
    if (event.data.usage) existing.usage = event.data.usage;
    return { kind: "message-delta", key, message: existing };
  }

  /** Reset assembler state (e.g. when starting a new run). */
  reset(): void {
    this.active.clear();
  }
}
