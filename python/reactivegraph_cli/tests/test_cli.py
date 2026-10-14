"""CLI tests."""

from __future__ import annotations

import importlib.metadata
import tempfile
from pathlib import Path

from reactivegraph_cli.cli import main


def test_reactivegraph_console_script_has_one_owner() -> None:
    entries = [
        entry
        for entry in importlib.metadata.entry_points(group="console_scripts")
        if entry.name == "reactivegraph"
    ]
    assert {entry.value for entry in entries} == {"reactivegraph_cli.cli:main"}


class TestCli:
    def test_new_scaffolds_project(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rc = main(["new", f"{d}/proj"])
            assert rc == 0
            assert (Path(d) / "proj" / "reactivegraph.json").exists()
            assert (Path(d) / "proj" / "src" / "proj.py").exists()

    def test_validate_compiles_graph(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            main(["new", f"{d}/proj"])
            rc = main(["validate", f"{d}/proj"])
            assert rc == 0

    def test_validate_fails_without_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rc = main(["validate", d])
            assert rc == 1

    def test_dev_runs_local_driver(self) -> None:
        rc = main(["dev"])
        assert rc == 0

    def test_trace_export_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rc = main(["trace-export", f"{d}/traces"])
            assert rc == 0
            assert (Path(d) / "traces" / "trace.json").exists()

class TestDockerCommands:
    def test_dockerfile_generates_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            main(["new", f"{d}/proj"])
            rc = main(["dockerfile", f"{d}/proj", "--save-path", f"{d}/deploy"])
            assert rc == 0
            assert (Path(d) / "deploy" / "Dockerfile").exists()
            assert (Path(d) / "deploy" / "docker-compose.yml").exists()

    def test_up_validates_compose_with_real_docker(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            main(["new", f"{d}/proj"])
            rc = main(["up", f"{d}/proj"])
            assert rc == 0

    def test_dockerfile_contains_healthcheck_and_graceful_shutdown(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            main(["new", f"{d}/proj"])
            rc = main(["dockerfile", f"{d}/proj", "--save-path", f"{d}/deploy"])
            assert rc == 0
            dockerfile = (Path(d) / "deploy" / "Dockerfile").read_text()
            compose = (Path(d) / "deploy" / "docker-compose.yml").read_text()
            assert "HEALTHCHECK" in dockerfile
            assert "reactivegraph health" in dockerfile
            assert "STOPSIGNAL SIGTERM" in dockerfile
            assert "stop_grace_period:" in compose


def test_health_command_reports_driver_ready() -> None:
    rc = main(["health"])
    assert rc == 0
