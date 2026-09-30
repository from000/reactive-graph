#!/usr/bin/env bash
# Copy the reactivegraph core package into the DeerFlow fork.
#
# The fork vendors the engine so it is self-contained (a clone builds without
# reaching back to the reactive-graph repo). This script is the single direction
# of truth: the core repo is edited, then pushed into the fork.
#
# We copy the *package directory* (python/reactivegraph), not just its inner
# module tree: the licence and attribution files live beside the modules and
# must travel with them. An earlier version only synced the modules, which is
# how the fork ended up shipping adapted LangGraph/LangChain code with no
# THIRD_PARTY_NOTICES.md.
set -euo pipefail

CORE_PKG="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/python/reactivegraph"
FORK_PKG="${DEERFLOW_FORK:-$HOME/code/git/deer-flow-reactive}/backend/packages/reactivegraph"

if [[ ! -d "$FORK_PKG" ]]; then
  echo "fork package dir not found: $FORK_PKG" >&2
  echo "set DEERFLOW_FORK to the deer-flow-reactive checkout" >&2
  exit 1
fi

# Modules: replace wholesale so deletions in core propagate.
rm -rf "$FORK_PKG/reactivegraph"
mkdir -p "$FORK_PKG/reactivegraph"
cp -R "$CORE_PKG/reactivegraph"/. "$FORK_PKG/reactivegraph"/
find "$FORK_PKG/reactivegraph" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true

# Licence / attribution / readme travel with the code they cover.
for file in LICENSE THIRD_PARTY_NOTICES.md README.md; do
  if [[ -f "$CORE_PKG/$file" ]]; then
    cp "$CORE_PKG/$file" "$FORK_PKG/$file"
  else
    echo "warning: $CORE_PKG/$file is missing" >&2
  fi
done

# Keep packaging metadata in step (different requires-python, so this is a
# merge rather than a copy: update the fields that matter, leave the rest).
if [[ -f "$CORE_PKG/pyproject.toml" && -f "$FORK_PKG/pyproject.toml" ]]; then
  VERSION="$(grep -m1 '^version' "$CORE_PKG/pyproject.toml" | sed 's/.*"\(.*\)".*/\1/')"
  if [[ -n "$VERSION" ]]; then
    sed -i.bak "s/^version = \".*\"/version = \"$VERSION\"/" "$FORK_PKG/pyproject.toml"
    rm -f "$FORK_PKG/pyproject.toml.bak"
  fi
fi

echo "synced $(find "$CORE_PKG/reactivegraph" -name '*.py' | wc -l | tr -d ' ') modules + licence files -> $FORK_PKG"
