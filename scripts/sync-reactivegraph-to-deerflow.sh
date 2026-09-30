#!/usr/bin/env bash
# Copy the reactivegraph core package into the DeerFlow fork.
#
# The fork vendors the engine so it is self-contained (a clone builds without
# reaching back to the reactive-graph repo). This script is the single direction
# of truth: the core repo is edited, then pushed into the fork.
set -euo pipefail

CORE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/python/reactivegraph/reactivegraph"
FORK_DIR="${DEERFLOW_FORK:-$HOME/code/git/deer-flow-reactive}/backend/packages/reactivegraph/reactivegraph"

if [[ ! -d "$FORK_DIR" ]]; then
  echo "fork package dir not found: $FORK_DIR" >&2
  echo "set DEERFLOW_FORK to the deer-flow-reactive checkout" >&2
  exit 1
fi

rm -rf "$FORK_DIR"
mkdir -p "$FORK_DIR"
# Copy the whole package tree: reactivegraph ships both top-level modules and
# subpackages (reactivegraph.channels, with the langgraph-compatible
# channels.binop shim). A flat *.py copy silently dropped the subpackages.
cp -R "$CORE_DIR"/. "$FORK_DIR"/
find "$FORK_DIR" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
echo "synced $(find "$CORE_DIR" -name '*.py' | wc -l | tr -d ' ') modules -> $FORK_DIR"
