"""Deprecation-warning contract used by the compatibility shims."""

from __future__ import annotations

import warnings

import pytest

from reactivegraph.warnings import (
    ReactiveGraphDeprecatedSinceV10,
    ReactiveGraphDeprecationWarning,
)


def test_deprecation_message_normalizes_and_adds_removal_versions() -> None:
    warning = ReactiveGraphDeprecationWarning(
        "old_api is deprecated.",
        since=(1, 2),
        expected_removal=(2, 0),
    )
    assert str(warning) == (
        "old_api is deprecated. Deprecated in V1.2 to be removed in V2.0."
    )


def test_default_removal_version_is_the_next_major() -> None:
    warning = ReactiveGraphDeprecatedSinceV10("legacy")
    assert warning.expected_removal == (2, 0)
    assert str(warning) == "legacy. Deprecated in V1.0 to be removed in V2.0."


def test_deprecation_category_can_be_escalated_without_silencing_others() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", ReactiveGraphDeprecationWarning)
        with pytest.raises(ReactiveGraphDeprecatedSinceV10):
            warnings.warn("gone", ReactiveGraphDeprecatedSinceV10, stacklevel=2)
