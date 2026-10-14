#!/usr/bin/env bash
# Canonical local release gate.
#
# The Python workspace shares one uv-managed .venv. Running uv commands for
# different workspace members in parallel can uninstall and reinstall shared
# dependencies underneath another test process, producing flaky import/class
# identity failures. This script deliberately runs every phase serially.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_PYTHON=1
RUN_JS=1
RUN_BENCHMARKS=1
RUN_STATIC=1
RUN_DOCS=1
RUN_CLEAN_INSTALL=1
SELECTED=0

usage() {
  cat <<'USAGE'
Usage: scripts/check-release-gate.sh [PHASE ...]

Run the complete local release gate by default. One or more phases may be
selected explicitly; phases always execute in the canonical order below.

Phases:
  --python         lock check + four Python package test suites
  --js             pnpm install, typecheck, lint, format, tests, build
  --benchmarks     ReactiveGraph e2e + DeerFlow offline suites
  --static         ruff + mypy
  --docs           strict MkDocs build + repository link/tutorial checks
  --clean-install  Python and npm artifact clean-install smoke tests
  -h, --help       show this help

All Python workspace commands are serial. Do not replace this script with a
parallel invocation of `uv run --directory ...` commands.
USAGE
}

if [[ $# -gt 0 ]]; then
  RUN_PYTHON=0
  RUN_JS=0
  RUN_BENCHMARKS=0
  RUN_STATIC=0
  RUN_DOCS=0
  RUN_CLEAN_INSTALL=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --python) RUN_PYTHON=1; SELECTED=1 ;;
      --js) RUN_JS=1; SELECTED=1 ;;
      --benchmarks) RUN_BENCHMARKS=1; SELECTED=1 ;;
      --static) RUN_STATIC=1; SELECTED=1 ;;
      --docs) RUN_DOCS=1; SELECTED=1 ;;
      --clean-install) RUN_CLEAN_INSTALL=1; SELECTED=1 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
  done
fi

if [[ "$SELECTED" -eq 0 ]]; then
  RUN_PYTHON=1
  RUN_JS=1
  RUN_BENCHMARKS=1
  RUN_STATIC=1
  RUN_DOCS=1
  RUN_CLEAN_INSTALL=1
fi

run() {
  local label="$1"
  shift
  echo
  echo "==> $label"
  "$@"
}

prepare_workspace() {
  run "Lockfile check" uv lock --check
  run "Sync Python workspace (shared .venv, serial)" \
    uv sync --frozen --all-packages --all-extras
}

run_python_tests() {
  local project
  for project in reactivegraph reactivechain reactivegraph_sdk reactivegraph_cli; do
    run "Python: $project" \
      uv run --frozen --no-sync --directory "python/$project" python -m pytest -q
  done
}

run_js() {
  run "Install JavaScript workspace" pnpm install --frozen-lockfile
  run "TypeScript typecheck" pnpm typecheck
  run "JavaScript lint" pnpm lint
  run "JavaScript format check" pnpm format:check
  run "JavaScript tests" pnpm test
  run "JavaScript workspace build" pnpm -r build
}

run_benchmarks() {
  run "ReactiveGraph cross-language e2e" \
    env REPO_ROOT="$ROOT" uv run --frozen --no-sync --directory python/reactivechain \
      python -m pytest "$ROOT/benchmarks/e2e/" -q
  run "DeerFlow offline port suite" \
    env REPO_ROOT="$ROOT" uv run --frozen --no-sync --directory python/reactivechain \
      python -m pytest "$ROOT/benchmarks/deerflow/" -q
}

run_static() {
  run "Ruff" uvx ruff check \
    python/reactivegraph \
    python/reactivechain \
    python/reactivegraph_sdk \
    python/reactivegraph_cli \
    benchmarks/e2e \
    benchmarks/deerflow \
    benchmarks/differential
  run "mypy" uvx mypy \
    python/reactivegraph/reactivegraph \
    python/reactivechain/reactivechain \
    python/reactivegraph_sdk/reactivegraph_sdk \
    python/reactivegraph_cli/reactivegraph_cli
  # Each package carries its own [tool.interrogate] config; run the gate per
  # package so a regression in any one of them fails the release.
  local pkg mod
  for pkg in reactivegraph reactivechain reactivegraph_sdk reactivegraph_cli; do
    mod="$pkg"
    run "Docstring coverage: $pkg" \
      uvx interrogate --config "python/$pkg/pyproject.toml" "python/$pkg/$mod"
  done
}

run_docs() {
  run "Strict documentation build" uvx --from mkdocs-material mkdocs build --strict
  run "Repository documentation links" python3 scripts/check-docs.py
  run "Tutorial code checks" python3 scripts/check-tutorials.py
}

run_clean_install() {
  run "Clean install: Python artifacts" bash scripts/check-clean-install.sh --python-only
  run "Clean install: npm artifacts" bash scripts/check-clean-install.sh --js-only
}

echo "ReactiveGraph release gate"
echo "root: $ROOT"
echo "head: $(git rev-parse --short HEAD)"

if [[ "$RUN_PYTHON" -eq 1 || "$RUN_BENCHMARKS" -eq 1 || "$RUN_STATIC" -eq 1 ]]; then
  prepare_workspace
fi
[[ "$RUN_PYTHON" -eq 1 ]] && run_python_tests
[[ "$RUN_JS" -eq 1 ]] && run_js
[[ "$RUN_BENCHMARKS" -eq 1 ]] && run_benchmarks
[[ "$RUN_STATIC" -eq 1 ]] && run_static
[[ "$RUN_DOCS" -eq 1 ]] && run_docs
[[ "$RUN_CLEAN_INSTALL" -eq 1 ]] && run_clean_install

run "Working tree diff check" git diff --check
echo
echo "release gate: PASS"
