# Contributing to ReactiveGraph

Thanks for considering contributing. This project aims for the same quality bar
as the projects it benchmarks against (`langchain-ai/langgraph`,
`@vue/reactivity`). If a PR wouldn't be accepted there, we hold ours to it too.

## Development setup

```bash
# toolchain (Node 22 + pnpm + uv) — see docs/toolchain.md
source ~/.toolchain/env.sh

pnpm install                 # TS workspace
uv sync --all-packages       # Python workspace
uv run --directory python/reactivegraph pytest -q
```

## Before you open a PR

1. **Tests** — every new behavior ships with a failing-then-passing test
   (TDD). The driver uses vitest; Python uses pytest.
2. **Type/lint** — must be green, not just "passing locally":
   - `pnpm typecheck` (all TS packages, strict)
   - `pnpm lint` (eslint, per-package)
   - `uvx ruff check python/reactivegraph`
   - `uvx mypy python/reactivegraph` (pragmatic baseline; see pyproject.toml)
3. **No regressions** — run the full suite from `README.md` "命令" section,
   including `pnpm benchmark`.

## Commit style

Imperative subject line, e.g. `feat: add ...` / `fix: ...` / `docs: ...`.
One logical change per commit; each commit must keep the test suite green.

## Review

Both code review and security review run before merge (see `SECURITY.md`).
Be explicit in the PR description about what you changed and why.