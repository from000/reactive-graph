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

- [ ] Create the `@reactivegraph` npm organization under the publishing account.
- [ ] Confirm `npm whoami` returns that account locally.
- [ ] Configure repository secret `NPM_TOKEN`.
- [ ] Configure repository secret `PYPI_TOKEN`.
- [ ] Ensure npm publish rights for `@reactivegraph/protocol`,
      `@reactivegraph/devtools-protocol`, `@reactivegraph/driver`, and
      `@reactivegraph/sdk-js`.
- [ ] Ensure PyPI project rights for `reactivegraph`,
      `reactivechain`, `reactivegraph-sdk`, and `reactivegraph-cli`.
- [ ] Verify the security disclosure channel is monitored.

## 3. Release actions

- [ ] Merge the verified `beyond-langgraph-p4` branch into `main`.
- [ ] Confirm `CHANGELOG.md` contains the v0.1.0 entry.
- [ ] Review `.github/RELEASE_NOTES_v0.1.0.md` and paste it into the
      GitHub release draft.
- [ ] Create and push the signed annotated tag:

```bash
git tag -a v0.1.0 -m "ReactiveGraph v0.1.0"
git push origin v0.1.0
```

- [ ] Watch the tag-triggered release workflow.
- [ ] Verify all four npm packages are public.
- [ ] Verify all four Python distributions are live.
- [ ] Run clean install tests from a machine outside this repository.

## 4. Public verification

- [ ] Publish the docs site.
- [ ] Verify docs search.
- [ ] Run the manual differential benchmark workflow.
- [ ] Download the `differential-benchmark-matrix` artifact.
- [ ] Verify competitive proof links and raw results.
- [ ] Create or confirm the v0.1.0 GitHub release.

## 5. Post-release community

- [ ] Announce the release.
- [ ] Invite at least five pilot users/teams using `.github/PILOT_INVITATION.md`.
- [ ] Collect pilot feedback via the `Pilot user` issue template.
- [ ] Publish a pilot summary once five external users/teams have verified
      production-like workloads.

The goal is not complete until these external checkboxes are completed and
verified.
