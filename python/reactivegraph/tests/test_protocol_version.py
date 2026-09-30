"""Both SDKs must expose the same RGP/1 protocol version (Task 2 / D.1.2)."""

import json
from pathlib import Path

import pytest

from reactivegraph import PROTOCOL_NAME, PROTOCOL_VERSION

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "docs" / "spec" / "protocol-version.json").read_text())


def test_protocol_version_matches_shared_fixture() -> None:
    assert PROTOCOL_NAME == FIXTURE["protocolName"]
    assert PROTOCOL_VERSION == FIXTURE["protocolVersion"]


def test_protocol_version_is_exact_integer_1() -> None:
    assert PROTOCOL_VERSION == 1
    assert isinstance(PROTOCOL_VERSION, int)


@pytest.mark.parametrize(
    "attr, expected",
    [("PROTOCOL_NAME", "RGP/1"), ("PROTOCOL_VERSION", 1)],
)
def test_public_exports(attr: str, expected: object) -> None:
    assert getattr(__import__("reactivegraph", fromlist=[attr]), attr) == expected
