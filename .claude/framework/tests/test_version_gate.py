"""Build version gate tests."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from version_gate import (  # noqa: E402
    evaluate_build_version_gate,
    extract_jira_product_build_versions,
    parse_build_version,
    version_gte,
)


def test_parse_build_version():
    assert parse_build_version("4.22.0-77.stable") == (4, 22, 0, 77)
    assert parse_build_version("4.20") == (4, 20, 0, 0)


def test_version_gte():
    assert version_gte((4, 22, 0, 77), (4, 20, 0, 0))
    assert not version_gte((4, 19, 0, 0), (4, 20, 0, 0))


def test_gate_blocks_lower_cluster():
    gate = evaluate_build_version_gate("4.19.0-10.stable", ["4.20"])
    assert gate["check_applied"] is True
    assert gate["proceed"] is False
    assert gate["version_mismatch"] is True


def test_gate_allows_higher_cluster():
    gate = evaluate_build_version_gate("4.22.0-77.stable", ["4.20"])
    assert gate["proceed"] is True


def test_gate_skipped_when_jira_silent():
    gate = evaluate_build_version_gate("4.22.0", [])
    assert gate["check_applied"] is False
    assert gate["proceed"] is True


def test_extract_from_description():
    raw = {
        "fields": {
            "description": {
                "type": "doc",
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {
                                "type": "text",
                                "text": "The version of all relevant components \
                                    (OCP, ODF, RHCS, ACM whichever is applicable):",
                            }
                        ],
                    },
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "4.20"}],
                    },
                ],
            }
        }
    }
    versions = extract_jira_product_build_versions(raw)
    assert any("4.20" in v for v in versions)
