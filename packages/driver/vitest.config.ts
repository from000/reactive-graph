import { defineConfig } from "vitest/config";

// The driver suite is executed from the repository root and, crucially, from
// this package directory (the CI coverage gate uses `pnpm --filter`). Vitest
// resolves config relative to the current working directory, so the package
// needs its own config for timeouts and coverage thresholds to apply.
export default defineConfig({
  test: {
    environment: "node",
    // The 10k-node scale smoke test takes double-digit seconds on loaded
    // machines; the default 5s budget is a flaky failure, not a regression.
    testTimeout: 60_000,
    server: {
      deps: {
        // node:sqlite is a Node builtin; keep Vite's resolver away from it.
        external: ["node:sqlite", /^node:/],
      },
    },
    coverage: {
      provider: "v8",
      thresholds: {
        lines: 75,
        functions: 80,
        branches: 70,
        statements: 75,
      },
    },
  },
});
