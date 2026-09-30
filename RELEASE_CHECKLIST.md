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
> **Scope decision (2026-10-10):** the maintainer chose GitHub-source
> distribution for v0.1.0. The registry items below are therefore deferred, not
> pending work: they stay unchecked as future actions, and every install
> instruction in the docs uses the working GitHub form. Nothing in this
> repository claims PyPI/npm availability.

- [ ] Deferred (optional, future): create the `@reactivegraph` npm organization
      under the publishing account and confirm `npm whoami` returns it.
      `https://registry.npmjs.org/-/org/reactivegraph/user` returns 404.
- [ ] Deferred (optional, future): configure repository secret `NPM_TOKEN`
      (npm access token with publish rights to `@reactivegraph/protocol`,
      `@reactivegraph/devtools-protocol`, `@reactivegraph/driver`,
      `@reactivegraph/sdk-js`).
- [ ] Deferred (optional, future): configure repository secret `PYPI_TOKEN` for
      projects `reactivegraph`, `reactivechain`, `reactivegraph-sdk`,
      `reactivegraph-cli` (all four currently 404, i.e. unclaimed).
- [ ] Verify the security disclosure channel is monitored.

## 3. Release actions

- [x] Squash-merged the verified `beyond-langgraph-p4` work into a single
      root commit on `main` (the earlier multi-commit public history was
      consolidated after the LangChain Runnable call-contract fix; a local
      bundle of that history is kept out of the repository). `main` currently
      has exactly one commit — verify with
      `gh api repos/from000/reactive-graph/commits`.
- [x] Confirm `CHANGELOG.md` contains the v0.1.0 entry.
- [x] Review `.github/RELEASE_NOTES_v0.1.0.md` and paste it into the
      GitHub release draft.
- [x] Done: created and pushed the annotated tag `v0.1.0`, pointing at the
      `main` commit that contains this checklist (it moved while the LangChain
      Runnable call contract was fixed and the audit corrected; no earlier tag
      produced a release, because every publish step was skipped):

```bash
git tag -a v0.1.0 -m "ReactiveGraph v0.1.0"
git push origin v0.1.0
```

- [x] Done: CI and release run green on the single tagged commit (release
      builds, tests and dry-run-packs every artifact; the two publish steps are
      skipped with a notice until the registry tokens exist). Verified runs:
      https://github.com/from000/reactive-graph/actions/runs/37817297600 and
      the CI run on the current `main` commit.
- [ ] Deferred: verify all four npm packages are public (after a registry publish).
- [ ] Deferred: verify all four Python distributions are live (after a registry publish).
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
