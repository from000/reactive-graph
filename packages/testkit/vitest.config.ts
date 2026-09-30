import { defineConfig } from "vitest/config";

// Each package owns its Vitest config so `pnpm -r test` (and
// `pnpm --filter <pkg> test`) behave identically from any working directory.
export default defineConfig({
  test: {
    environment: "node",
    testTimeout: 60_000,
  },
});
