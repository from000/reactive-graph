"""ReactiveGraph CLI.

Commands:

* `reactivegraph new <dir>`       — scaffold a native-API project
* `reactivegraph validate [path]` — validate a project (importable, graphs build)
* `reactivegraph dev`             — compile a graph through the bundled Driver in-process
* `reactivegraph trace-export <dir>` — write a sample causal-trace JSON (example schema)
* `reactivegraph dockerfile <path>` — generate a Dockerfile + docker-compose.yml
* `reactivegraph up <path>`       — generate deploy artifacts and validate compose
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="reactivegraph", description="ReactiveGraph CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_new = sub.add_parser("new", help="scaffold a new project")
    p_new.add_argument("dir", help="project directory to create")

    p_val = sub.add_parser("validate", help="validate a project")
    p_val.add_argument("path", nargs="?", default=".", help="project path")

    sub.add_parser("dev", help="compile a graph through the bundled Driver in-process")

    sub.add_parser("health", help="report container readiness")

    p_tr = sub.add_parser("trace-export", help="write a sample causal-trace JSON (example schema)")
    p_tr.add_argument("dir", help="directory to write trace.json into")

    p_dockerfile = sub.add_parser(
        "dockerfile", help="generate a Dockerfile + docker-compose.yml for a project"
    )
    p_dockerfile.add_argument("path", nargs="?", default=".", help="project path")
    p_dockerfile.add_argument("--save-path", default=None, help="directory to write artifacts into")

    p_up = sub.add_parser(
        "up", help="generate deploy artifacts and validate with docker compose config"
    )
    p_up.add_argument("path", nargs="?", default=".", help="project path")
    p_up.add_argument(
        "--build", action="store_true", help="also run docker build (requires network + daemon)"
    )

    return parser.parse_args(argv)


def cmd_new(args: argparse.Namespace) -> int:
    d = Path(args.dir)
    d.mkdir(parents=True, exist_ok=True)
    (d / "src").mkdir(exist_ok=True)
    (d / "reactivegraph.json").write_text(
        json.dumps({"name": d.name, "graphs": [f"{d.name}:graph"]}, indent=2) + "\n"
    )
    (d / "src" / f"{d.name}.py").write_text(
        "from reactivegraph import GraphBuilder\n\n"
        "def graph():\n"
        "    b = GraphBuilder()\n"
        "    b.task(\"echo\", kind=\"effect\", fn=lambda s: dict(s), on=(\"run\",))\n"
        "    return b.build()\n"
    )
    print(f"created project at {d}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    root = Path(args.path).resolve()
    manifest = root / "reactivegraph.json"
    if not manifest.exists():
        print(f"error: no reactivegraph.json at {root}")
        return 1
    config = json.loads(manifest.read_text())
    # make the project's src/ importable
    sys.path.insert(0, str(root))
    sys.path.insert(0, str(root / "src"))
    ok = True
    for graph_ref in config.get("graphs", []):
        mod, _, fn = graph_ref.partition(":")
        try:
            import importlib

            module = importlib.import_module(mod)
            fn_obj = getattr(module, fn)
            graph = fn_obj() if callable(fn_obj) else fn_obj
            compile_fn = getattr(graph, "compile", None)
            if compile_fn is not None:
                compile_fn()
            print(f"ok: {graph_ref}")
        except Exception as exc:  # noqa: BLE001
            print(f"fail: {graph_ref}: {exc}")
            ok = False
    return 0 if ok else 1


def cmd_dev(args: argparse.Namespace) -> int:
    # In-process local Driver: compile a graph through the bundled Driver host.
    from reactivegraph import ReactiveGraph
    from reactivegraph.host import DriverHost

    host = DriverHost()
    try:
        ReactiveGraph.build(
            lambda b: b.task("echo", kind="effect", fn=lambda s: dict(s), on=("run",)),
            host=host,
        )
        print("dev: Driver ready (graph 'echo' compiled, in-process)")
    finally:
        host.close()
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    """Container readiness: imports the runtime and reports ready."""
    import reactivegraph

    print(f"reactivegraph {reactivegraph.__version__} ready")
    return 0


def cmd_trace_export(args: argparse.Namespace) -> int:
    out = Path(args.dir)
    out.mkdir(parents=True, exist_ok=True)
    trace = {
        "schema": "reactivegraph.causal-trace.v1",
        "events": [
            {"seq": 0, "type": "commit", "path": ["x"], "value": 1},
            {"seq": 1, "type": "invalidate", "selector": "total"},
            {"seq": 2, "type": "schedule", "task": "sum"},
        ],
    }
    (out / "trace.json").write_text(json.dumps(trace, indent=2) + "\n")
    print(f"wrote {out / 'trace.json'}")
    return 0


def _load_project_config(path: str) -> tuple[Path, dict]:
    root = Path(path).resolve()
    manifest = root / "reactivegraph.json"
    if not manifest.exists():
        raise FileNotFoundError(f"no reactivegraph.json at {root}")
    return root, json.loads(manifest.read_text())


def _dockerfile_content(root: Path, config: dict) -> str:
    name = config.get("name", root.name)
    graphs = ", ".join(config.get("graphs", [])) or f"{name}:graph"
    return (
        f"# Generated by `reactivegraph dockerfile` for project {name}\n"
        "FROM python:3.11-slim\n"
        "WORKDIR /app\n"
        "COPY src/ ./src/\n"
        "COPY pyproject.toml reactivegraph.json ./\n"
        "RUN pip install --no-cache-dir reactivegraph\n"
        f"ENV REACTIVEGRAPH_GRAPHS={graphs}\n"
        "STOPSIGNAL SIGTERM\n"
        "HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \\\n"
        "  CMD reactivegraph health || exit 1\n"
        'CMD ["python", "-c", "import reactivegraph; print(\\"reactivegraph ready\\")"]\n'
    )


def _compose_content(root: Path, config: dict) -> str:
    name = config.get("name", root.name)
    image = f"reactivegraph-{name}-driver:dev"
    return (
        f"# Generated by `reactivegraph up` for project {name}\n"
        "services:\n"
        "  driver:\n"
        "    build: .\n"
        f"    image: {image}\n"
        '    ports:\n'
        '      - "8123:8123"\n'
        "    stop_grace_period: 30s\n"
        '    environment:\n'
        "      - REACTIVEGRAPH_GRAPHS=\n"
    )


def cmd_dockerfile(args: argparse.Namespace) -> int:
    """Generate Dockerfile + docker-compose.yml for a project."""
    root, config = _load_project_config(args.path)
    out = Path(args.save_path).resolve() if args.save_path else root
    out.mkdir(parents=True, exist_ok=True)
    (out / "Dockerfile").write_text(_dockerfile_content(root, config))
    (out / ".dockerignore").write_text("__pycache__/\n*.egg-info/\n.venv/\n")
    (out / "docker-compose.yml").write_text(_compose_content(root, config))
    print(f"created Dockerfile, .dockerignore, docker-compose.yml at {out}")
    return 0


def _docker_bin() -> str:
    """Locate the docker binary (Homebrew on macOS may not be on PATH in some shells)."""
    import shutil

    found = shutil.which("docker")
    if found:
        return found
    for candidate in ("/usr/local/bin/docker", "/opt/homebrew/bin/docker", "/usr/bin/docker"):
        if Path(candidate).exists():
            return candidate
    raise FileNotFoundError(
        "docker not found in PATH. Install Docker Desktop or add the docker binary to PATH."
    )


def cmd_up(args: argparse.Namespace) -> int:
    """Validate deploy artifacts with the real `docker compose config`."""
    root, config = _load_project_config(args.path)
    out = root / "deploy"
    out.mkdir(parents=True, exist_ok=True)
    (out / "Dockerfile").write_text(_dockerfile_content(root, config))
    (out / "docker-compose.yml").write_text(_compose_content(root, config))

    import subprocess

    docker = _docker_bin()
    if args.build:
        build = subprocess.run(
            [docker, "build", "-t", f"{config.get('name', 'app')}-dev", "."],
            cwd=out,
            capture_output=True,
        )
        if build.returncode != 0:
            print("error: docker build failed (check daemon/network)", file=sys.stderr)
            return 1
        print("docker build succeeded")
    else:
        # validate the compose file with a real docker invocation (no side effects)
        result = subprocess.run(
            [docker, "compose", "config"],
            cwd=out,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"error: docker compose config failed:\n{result.stderr}", file=sys.stderr)
            return 1
        print("docker compose config validated")
    return 0


_COMMANDS = {
    "new": cmd_new,
    "validate": cmd_validate,
    "dev": cmd_dev,
    "health": cmd_health,
    "trace-export": cmd_trace_export,
    "dockerfile": cmd_dockerfile,
    "up": cmd_up,
}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    fn = _COMMANDS[args.command]
    try:
        return fn(args)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())