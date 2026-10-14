import { defineConfig } from "vitest/config";

// Vitest 4 replaced the standalone workspace file with `test.projects`.
// The first project covers every package suite; benchmarks are isolated so
// `pnpm benchmark` (`vitest run benchmarks`) keeps its longer budget.
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "packages",
          include: ["packages/*/test/**/*.test.ts"],
          environment: "node",
          testTimeout: 15000,
          server: {
            deps: {
              // node:sqlite is a Node builtin; keep Vite's resolver away from it.
              external: ["node:sqlite", /^node:/],
            },
          },
          coverage: {
            // CI gate (`pnpm test -- --coverage` / `make coverage`): the driver
            // core currently sits at ~79% lines / ~86% funcs; keep a margin so
            // regressions fail the build without being brittle.
            provider: "v8",
            thresholds: {
              lines: 75,
              functions: 80,
              branches: 70,
              statements: 75,
            },
          },
        },
      },
      {
        test: {
          name: "benchmarks",
          include: ["benchmarks/**/*.test.ts"],
          environment: "node",
          testTimeout: 30000,
        },
      },
    ],
  },
});
