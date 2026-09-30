import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["**/dist/**", "**/node_modules/**", "**/*.d.ts"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["packages/**/*.ts"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
    },
    rules: {
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      // Quality ceilings mirroring the Python side (radon enforces "no D or
      // worse" there). Thresholds are set just above the current maximum so
      // they stop regressions without demanding a rewrite today.
      complexity: ["error", 32],
      "max-depth": ["error", 6],
      "max-lines": ["error", { max: 1250, skipBlankLines: true, skipComments: true }],
    },
  },
);
