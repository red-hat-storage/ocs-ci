"""Target Release matching tests."""

import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "jira-repro" / "discovery")
)
from release_match import (
    cli_to_target_release_value,
    target_release_matches,
)  # noqa: E402


def test_cli_to_target_release_value():
    assert cli_to_target_release_value("4.19") == "odf-4.19.z"
    assert cli_to_target_release_value("4.22.1") == "odf-4.22.z"


def test_exact_odf_z_match():
    assert target_release_matches("odf-4.19.z", "4.19")
    assert target_release_matches({"name": "odf-4.19.z"}, "4.19")


def test_zstream_cli_patch_match():
    assert target_release_matches("odf-4.22.z", "4.22.1")


def test_mismatch():
    assert not target_release_matches("odf-4.18.z", "4.19")
    assert not target_release_matches("odf-4.20.z", "4.19")
