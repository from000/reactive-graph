import subprocess
import sys
from pathlib import Path

import pytest

from reactivegraph.cli import main as cli_main


def test_new_scaffolds_runnable_project(tmp_path: Path) -> None:
    # cli_main uses Path.cwd(); chdir into tmp_path for the test
    import os

    old = Path.cwd()
    os.chdir(tmp_path)
    try:
        code = cli_main(["new", "myapp"])
        assert code == 0
    finally:
        os.chdir(old)

    root = tmp_path / "myapp"
    assert (root / "main.py").exists()
    assert (root / "pyproject.toml").exists()
    assert (root / "README.md").exists()
    assert 'name = "myapp"' in (root / "pyproject.toml").read_text()


def test_scaffolded_main_runs(tmp_path: Path) -> None:
    import os

    old = Path.cwd()
    os.chdir(tmp_path)
    try:
        cli_main(["new", "hello"])
    finally:
        os.chdir(old)

    r = subprocess.run(
        [sys.executable, str(tmp_path / "hello" / "main.py")],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )
    assert r.returncode == 0, r.stderr
    assert "state:" in r.stdout and "hi Ada" in r.stdout


def test_new_refuses_nonempty_dest(tmp_path: Path) -> None:
    (tmp_path / "taken").mkdir()
    (tmp_path / "taken" / "keep.txt").write_text("x")
    import os

    old = Path.cwd()
    os.chdir(tmp_path)
    try:
        with pytest.raises(SystemExit):
            cli_main(["new", "taken"])
    finally:
        os.chdir(old)