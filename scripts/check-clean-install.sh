#!/usr/bin/env bash
# Clean-install release smoke test.
#
# Builds the publishable Python and npm artifacts, installs them into fresh
# temporary environments, and exercises the public entry points. This catches
# packaging regressions that normal workspace tests cannot see.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_PYTHON=1
RUN_NPM=1
KEEP=0

usage() {
  cat <<'USAGE'
Usage: scripts/check-clean-install.sh [--python-only|--js-only] [--keep]

  --python-only  build + install the four Python wheels only
  --js-only      build + install the four publishable npm tarballs only
  --keep         keep the temporary work directory for inspection
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-only) RUN_NPM=0 ;;
    --js-only) RUN_PYTHON=0 ;;
    --keep) KEEP=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ "$RUN_PYTHON" -eq 0 && "$RUN_NPM" -eq 0 ]]; then
  echo "nothing selected" >&2
  exit 2
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/rg-clean-install.XXXXXX")"
cleanup() {
  if [[ "$KEEP" -eq 1 ]]; then
    echo "kept work directory: $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

check_python() {
  local dist="$WORK/python-dist" venv="$WORK/python-venv"
  mkdir -p "$dist"

  echo "==> building Python sdists + wheels"
  for pkg in reactivegraph reactivechain reactivegraph_sdk reactivegraph_cli; do
    uv build --no-build-logs --directory "$ROOT/python/$pkg" --out-dir "$dist" >/dev/null
  done

  echo "==> verifying license files in every wheel + sdist"
  local artifact count=0
  for artifact in "$dist"/*; do
    [[ -f "$artifact" ]] || continue
    case "$artifact" in
      *.whl)
        python3 -c 'import sys, zipfile; names = zipfile.ZipFile(sys.argv[1]).namelist(); raise SystemExit(0 if any(name.rsplit("/", 1)[-1] == "LICENSE" for name in names) else 1)' "$artifact" || {
          echo "Python wheel is missing LICENSE: $(basename "$artifact")" >&2
          exit 1
        }
        ;;
      *.tar.gz)
        # Materialize the listing first: `tar | grep -q` kills tar with
        # SIGPIPE once grep exits early, which turns into a spurious failure
        # under `set -o pipefail` (GNU tar reports "stdout: write error").
        local listing="$WORK/sdist-listing.txt"
        tar -tzf "$artifact" >"$listing"
        grep -Eq '(^|/)LICENSE$' "$listing" || {
          echo "Python sdist is missing LICENSE: $(basename "$artifact")" >&2
          exit 1
        }
        ;;
      *)
        echo "unexpected Python artifact: $(basename "$artifact")" >&2
        exit 1
        ;;
    esac
    count=$((count + 1))
  done
  if [[ "$count" != "8" ]]; then
    echo "expected 8 Python artifacts (4 wheels + 4 sdists), found $count" >&2
    exit 1
  fi

  echo "==> installing wheels into a fresh venv"
  uv venv "$venv" --python "${CLEAN_INSTALL_PYTHON:-python3}" >/dev/null
  uv pip install --python "$venv/bin/python" "$dist"/*.whl >/dev/null

  echo "==> verifying installed distributions and CLI ownership"
  "$venv/bin/python" - <<'PY'
import importlib
import importlib.metadata as metadata

packages = {
    "reactivegraph": "reactivegraph",
    "reactivechain": "reactivechain",
    "reactivegraph-sdk": "reactivegraph_sdk",
    "reactivegraph-cli": "reactivegraph_cli",
}
for distribution, module in packages.items():
    importlib.import_module(module)
    version = metadata.version(distribution)
    if version != "0.1.0":
        raise SystemExit(f"{distribution} has unexpected version {version}")
    print(f"imported {module} ({distribution} {version})")

core = {ep.name for ep in metadata.distribution("reactivegraph").entry_points if ep.group == "console_scripts"}
cli = {ep.name for ep in metadata.distribution("reactivegraph-cli").entry_points if ep.group == "console_scripts"}
if core:
    raise SystemExit(f"reactivegraph core wheel must not own console scripts: {sorted(core)}")
if cli != {"reactivegraph"}:
    raise SystemExit(f"reactivegraph-cli wheel must own exactly the reactivegraph command: {sorted(cli)}")
print("console script ownership: reactivegraph -> reactivegraph-cli")
PY

  echo "==> running the installed CLI"
  "$venv/bin/reactivegraph" health
  (
    cd "$WORK"
    "$venv/bin/reactivegraph" new clean_install_app >/dev/null
    test -f "$WORK/clean_install_app/reactivegraph.json"
    "$venv/bin/reactivegraph" validate "$WORK/clean_install_app"
  )
  echo "python clean install: ok"
}

check_npm() {
  local tarballs="$WORK/npm-tarballs" consumer="$WORK/npm-consumer"
  mkdir -p "$tarballs" "$consumer"

  echo "==> building npm packages"
  pnpm --dir "$ROOT" -r build >/dev/null

  echo "==> packing publishable tarballs"
  for pkg in protocol devtools-protocol driver sdk-js; do
    pnpm --dir "$ROOT/packages/$pkg" pack --pack-destination "$tarballs" >/dev/null
  done
  local packed
  packed="$(find "$tarballs" -maxdepth 1 -name '*.tgz' | wc -l | tr -d ' ')"
  if [[ "$packed" != "4" ]]; then
    echo "expected 4 npm tarballs, found $packed" >&2
    exit 1
  fi

  echo "==> verifying LICENSE in every npm tarball"
  local tarball
  local listing="$WORK/npm-listing.txt"
  for tarball in "$tarballs"/*.tgz; do
    # See the Python sdist comment: never pipe tar into `grep -q`.
    tar -tzf "$tarball" >"$listing"
    grep -qx 'package/LICENSE' "$listing" || {
      echo "npm tarball is missing package/LICENSE: $(basename "$tarball")" >&2
      exit 1
    }
  done

  echo "==> installing tarballs into a fresh npm project"
  (
    cd "$consumer"
    npm init -y >/dev/null
    npm install --no-audit --no-fund --ignore-scripts "$tarballs"/*.tgz >/dev/null
    node --input-type=module - <<'JS'
import { readFileSync } from "node:fs";
import { PROTOCOL_VERSION } from "@reactivegraph/protocol";
import { TRACE_EVENT_KINDS } from "@reactivegraph/devtools-protocol";
import { ReactiveGraph, invoke } from "@reactivegraph/sdk-js";

if (!PROTOCOL_VERSION || !TRACE_EVENT_KINDS.includes("commit")) {
  throw new Error("packed protocol packages did not load");
}

for (const pkg of ["protocol", "devtools-protocol", "driver", "sdk-js"]) {
  const manifest = JSON.parse(
    readFileSync(new URL(`./node_modules/@reactivegraph/${pkg}/package.json`, import.meta.url), "utf8"),
  );
  for (const [name, version] of Object.entries(manifest.dependencies ?? {})) {
    if (String(version).includes("workspace:")) {
      throw new Error(`packed ${pkg} dependency ${name} still uses ${version}`);
    }
  }
}

const graph = ReactiveGraph.build((b) => {
  b.task({
    id: "greet",
    kind: "effect",
    handler: (input) => ({
      reads: ["name"],
      writes: ["msg"],
      patches: [{ path: ["msg"], operation: "set", value: `hi ${input.name}` }],
    }),
  });
  b.on("visit", "greet");
});
const out = await invoke(graph, "visit", { name: "Ada" });
if (out.state.msg !== "hi Ada") {
  throw new Error(`unexpected invoke result: ${JSON.stringify(out)}`);
}
console.log(`protocol ${PROTOCOL_VERSION}; sdk invoke ok`);
JS
  )
  echo "npm clean install: ok"
}

if [[ "$RUN_PYTHON" -eq 1 ]]; then
  check_python
fi
if [[ "$RUN_NPM" -eq 1 ]]; then
  check_npm
fi

echo "clean-install smoke: PASS"
