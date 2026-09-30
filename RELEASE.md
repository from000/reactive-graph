# Release Guide

## Versioning

All packages share `0.x` until the 1.0 gate passes (below). Bump the root
`version`, each `package.json` / `pyproject.toml`, and
`docs/spec/protocol-version.json` together; protocol version changes are
**breaking** once published.

## Verification before any release

```bash
scripts/check-release-gate.sh
```

The gate intentionally serializes all Python workspace commands because the
members share one uv-managed `.venv`. Run `pnpm benchmark` separately for the
advisory benchmark matrix linked from `docs/competitive-proof.md`.

1.0 requires, in addition:

1. **Clean-install Driver** — the Driver runs from a clean Python environment
   with no user-managed Node installation (`pip install reactivegraph`).
2. **Security review** — SECURITY.md policy followed; permission/redaction
   tests green (`packages/driver/test/controls.test.ts`).
3. **Benchmark reproducibility** — every performance statement links the
   `pnpm benchmark` raw output and harness source
   (`benchmarks/harness/selective.test.ts`).

## How to release

1. Create a git tag `v<version>`; the release workflow publishes Python and
   npm packages after the release gates pass.
2. Build and publish Python distributions for `reactivegraph`,
   `reactivechain`, `reactivegraph-sdk`, and `reactivegraph-cli`.
3. Publish npm packages in dependency order: `protocol`,
   `devtools-protocol`, `driver`, then `sdk-js`. `testkit` is private and is
   intentionally not published.
4. Update `CHANGELOG.md` and paste `.github/RELEASE_NOTES_v<version>.md` into
   the GitHub release.

## Current status

`0.1.0` (workspace, engineering-complete). See
[`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md) for the exact local gates,
external secrets/organization prerequisites, public release actions, and
post-release pilot verification. The release is not public until those
external checkboxes are completed.

## Publishing (`.github/workflows/release.yml`)

Tag `v*` (or manual dispatch) triggers:

- **npm**: `pnpm -r build` then publishes `@reactivegraph/*` in dependency
  order (protocol → devtools-protocol → driver → sdk-js). Requires the
  `@reactivegraph` npm org and the `NPM_TOKEN` secret.
- **PyPI**: `uv build` + `twine upload` for `reactivegraph`, `reactivechain`,
  `reactivegraph-sdk`, and `reactivegraph-cli`. Requires the `PYPI_TOKEN`
  secret.

Until the secrets exist the publish steps fail fast after building/tests —
that is expected; configure the secrets in the repo settings, then re-run.
