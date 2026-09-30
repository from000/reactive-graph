# v0.1.0 Release Checklist

This is the operational checklist for the public release. Engineering must be
green before any external action is taken.

## 1. Local engineering gate

Run the canonical gate from a clean worktree:

```bash
scripts/check-release-gate.sh
```

The script runs every phase serially. This is intentional: all Python workspace
members share one uv-managed `.venv`, so parallel `uv run --directory ...`
commands can uninstall and reinstall dependencies underneath another test
process. Individual phases can be rerun during triage with `--python`, `--js`,
`--benchmarks`, `--static`, `--docs`, or `--clean-install`; the phase order
stays fixed.

The clean-install phase builds and installs the produced artifacts rather than
the workspace, so it also verifies console-script ownership:

Expected: every phase exits 0, all four Python imports work, `reactivegraph
health` and the scaffolded project validate, and the installed npm SDK performs
a real graph invoke.

The `reactivegraph` console command is shipped by `reactivegraph-cli`; the
`reactivegraph` core distribution intentionally has no console entry point.

## 2. External prerequisites

These are outside the git worktree and cannot be satisfied locally:

- [x] Done: GitHub repository `from000/reactive-graph` is public with an MIT
      license, topics, description, and this checklist at `main`.
- [x] Done: docs site deployment is enabled (GitHub Pages, workflow build);
      `mkdocs.yml` targets `https://from000.github.io/reactive-graph/`.
- [x] Done: Actions secret inventory verified empty (`NPM_TOKEN` and
      `PYPI_TOKEN` still absent — required before any registry publish).
- [ ] Blocked on account action: create the `@reactivegraph` npm organization
      under the publishing account and confirm `npm whoami` returns it.
      `https://registry.npmjs.org/-/org/reactivegraph/user` returns 404.
- [ ] Blocked on account action: log in locally and configure repository secret
      `NPM_TOKEN` (npm access token with publish rights to
      `@reactivegraph/protocol`, `@reactivegraph/devtools-protocol`,
      `@reactivegraph/driver`, `@reactivegraph/sdk-js`).
- [ ] Blocked on account action: log in and configure repository secret
      `PYPI_TOKEN` for projects `reactivegraph`, `reactivechain`,
      `reactivegraph-sdk`, `reactivegraph-cli` (all four currently 404, i.e.
      unclaimed).
- [ ] Verify the security disclosure channel is monitored.

## 3. Release actions

- [x] Squash-merged the verified `beyond-langgraph-p4` work into the single
      public `main` commit.
- [x] Confirm `CHANGELOG.md` contains the v0.1.0 entry.
- [x] Review `.github/RELEASE_NOTES_v0.1.0.md` and paste it into the
      GitHub release draft.
- [ ] Create and push the signed annotated tag:

```bash
git tag -a v0.1.0 -m "ReactiveGraph v0.1.0"
git push origin v0.1.0
```

- [ ] Watch the tag-triggered release workflow. Until both registry tokens
      exist it builds, tests and dry-run-packs every artifact, then skips only
      the publish steps with a notice. (Workflow itself already verified green
      via `workflow_dispatch` on `d0215c1`.)
- [ ] Verify all four npm packages are public.
- [ ] Verify all four Python distributions are live.
- [ ] Run clean install tests from a machine outside this repository.
- [ ] Spot-check the rendered npm and PyPI pages: each of the eight packages
      must show its own README (the npm packages gained per-package READMEs in
      the packaging metadata; PyPI renders the sdist `readme`).

## 4. Public verification

- [x] Enable the docs site (GitHub Pages workflow deployment).
- [x] Verify docs search (search_index.json served, 200).
- [x] Run the manual differential benchmark workflow (dispatched on `d0215c1`, green).
- [x] Download the `differential-benchmark-matrix` artifact (raw langgraph.json/reactive.json verified).
- [x] Verify competitive proof links and raw results (`docs/proof/` + the artifact above; links resolved by `scripts/check-docs.py`).
- [ ] Create or confirm the v0.1.0 GitHub release.

## 5. Post-release community

- [ ] Announce the release.
- [ ] Invite at least five pilot users/teams using `.github/PILOT_INVITATION.md`.
- [ ] Collect pilot feedback via the `Pilot user` issue template.
- [ ] Publish a pilot summary once five external users/teams have verified
      production-like workloads.

The goal is not complete until these external checkboxes are completed and
verified.
