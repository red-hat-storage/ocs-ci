# -*- coding: utf-8 -*-

import pytest

from ocs_ci import framework
from ocs_ci.framework.pytest_customization.ocscilib import store_csv_changes


@pytest.fixture(autouse=True)
def reset_config():
    framework.config.reset()


def test_store_csv_changes_without_configured_values():
    """
    Test that the changes passed via cli are stored in the config even when
    the DEPLOYMENT section doesn't define them yet.
    """
    store_csv_changes(["from-image::to-image"])

    assert framework.config.DEPLOYMENT["csv_change_from"] == ["from-image"]
    assert framework.config.DEPLOYMENT["csv_change_to"] == ["to-image"]


def test_store_csv_changes_appends_to_configured_values():
    """
    Test that the changes passed via cli are appended to the ones already
    configured in the DEPLOYMENT section.
    """
    framework.config.DEPLOYMENT["csv_change_from"] = ["configured-from"]
    framework.config.DEPLOYMENT["csv_change_to"] = ["configured-to"]

    store_csv_changes(["cli-from::cli-to"])

    assert framework.config.DEPLOYMENT["csv_change_from"] == [
        "configured-from",
        "cli-from",
    ]
    assert framework.config.DEPLOYMENT["csv_change_to"] == ["configured-to", "cli-to"]


def test_store_csv_changes_normalizes_configured_string():
    """
    Test that values configured as a plain string are turned into a list.
    """
    framework.config.DEPLOYMENT["csv_change_from"] = "configured-from"
    framework.config.DEPLOYMENT["csv_change_to"] = "configured-to"

    store_csv_changes(["cli-from::cli-to"])

    assert framework.config.DEPLOYMENT["csv_change_from"] == [
        "configured-from",
        "cli-from",
    ]
    assert framework.config.DEPLOYMENT["csv_change_to"] == ["configured-to", "cli-to"]


def test_store_csv_changes_multiple_values():
    """
    Test that multiple --csv-change values are all stored, and that only the
    first '::' is used as the separator.
    """
    store_csv_changes(
        [
            "quay.io/from/a:1::quay.io/to/a:2",
            "registry.redhat.io/from/b@sha256:abc::quay.io/to/b:latest",
        ]
    )

    assert framework.config.DEPLOYMENT["csv_change_from"] == [
        "quay.io/from/a:1",
        "registry.redhat.io/from/b@sha256:abc",
    ]
    assert framework.config.DEPLOYMENT["csv_change_to"] == [
        "quay.io/to/a:2",
        "quay.io/to/b:latest",
    ]


def test_store_csv_changes_invalid_value():
    """
    Test that a value without the '::' separator is rejected.
    """
    with pytest.raises(ValueError):
        store_csv_changes(["missing-separator"])
