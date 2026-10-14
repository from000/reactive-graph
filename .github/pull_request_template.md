## Summary

<!-- What changed and why. Link the milestone (roadmap.md) when relevant. -->

## Verification

- [ ] `pnpm typecheck` and `pnpm test` green
- [ ] `uv run --directory python/reactivegraph --extra test pytest -q` green
- [ ] `uvx ruff check python/reactivegraph` and `uvx mypy python/reactivegraph/reactivegraph` clean
- [ ] Driver-backed tests run against the real bundled Driver (node + dist)

## Notes

<!-- Behavior changes, new capabilities (each with its own commit), known limits. -->
