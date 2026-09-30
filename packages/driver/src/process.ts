/**
 * Driver child process management: one long-lived bundled Driver process per
 * Python/JS host. Guarantees no orphan child processes (kill on exit, watchdog).
 */
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

export interface DriverProcessOptions {
  /** Node executable to run the Driver with. */
  nodeBin?: string;
  /** Path to the Driver entry (dist/main.js). Defaults to bundled entry. */
  entry?: string;
  /** Extra args. */
  args?: string[];
  /** Env overrides. */
  env?: Record<string, string>;
  /** Graceful shutdown timeout before SIGKILL, ms. */
  shutdownTimeoutMs?: number;
}

export interface DriverProcess {
  /** The child process. */
  child: ChildProcess;
  /** stdin/stdout stream pair exposed to a session. */
  stdin: NodeJS.WritableStream;
  stdout: NodeJS.ReadableStream;
  /** Promise resolving when the process exits (code/signal). */
  exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  /** Gracefully stop (SIGTERM, then SIGKILL after timeout). */
  stop: (graceMs?: number) => Promise<void>;
  /** Force kill. */
  kill: () => Promise<void>;
}

export function defaultDriverEntry(): string {
  const here = path.dirname(fileURLToPath(import.meta.url));
  // dist/process.js -> dist/main.js
  return path.resolve(here, "main.js");
}

/**
 * Start a Driver child process with stdio pipes. Registers process.exit hooks so
 * the child is never orphaned if the parent dies unexpectedly.
 */
export function startDriverProcess(opts: DriverProcessOptions = {}): DriverProcess {
  const nodeBin = opts.nodeBin ?? process.execPath;
  const entry = opts.entry ?? defaultDriverEntry();
  const child = spawn(nodeBin, [entry, ...(opts.args ?? [])], {
    stdio: ["pipe", "pipe", "inherit"],
    env: { ...process.env, ...opts.env },
  });

  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });

  // Never leave an orphan behind.
  const cleanup = () => {
    if (!child.killed && child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM");
    }
  };
  process.once("exit", cleanup);
  child.once("exit", () => {
    process.removeListener("exit", cleanup);
  });

  async function stop(graceMs = 2000): Promise<void> {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGTERM");
    const timer = setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    }, graceMs);
    await exit;
    clearTimeout(timer);
  }

  async function kill(): Promise<void> {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGKILL");
    await exit;
  }

  return { child, stdin: child.stdin, stdout: child.stdout, exit, stop, kill };
}
