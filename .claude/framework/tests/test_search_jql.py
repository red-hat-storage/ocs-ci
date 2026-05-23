"""JIRA search helpers (offline)."""

import sys
from pathlib import Path

DISC = Path(__file__).resolve().parents[2] / "jira-repro" / "discovery"
sys.path.insert(0, str(DISC))
import search_jql  # noqa: E402
from release_match import cli_to_target_release_value  # noqa: E402


def test_parse_issues_from_search():
    data = {
        "issues": [
            {"key": "DFBUGS-1", "fields": {"customfield_1": "odf-4.20.z"}},
            {"key": "DFBUGS-2", "fields": {"customfield_1": "odf-4.19.z"}},
        ],
        "total": 2,
    }
    parsed = search_jql.parse_issues_from_search(data, "customfield_1")
    keys, excluded = search_jql.filter_by_target_release(parsed, "4.20")
    assert keys == ["DFBUGS-1"]
    assert len(excluded) == 1


def test_primary_jql_template_fallback():
    assert "Target Release" in search_jql.primary_jql_template({})
    assert (
        search_jql.primary_jql_template(
            {"jql_templates": {"primary": "custom = {target_release}"}}
        )
        == "custom = {target_release}"
    )


def test_build_jql_target_release():
    tpl = (
        'project = {project} AND "Target Release" = {target_release} '
        'AND status = "{status}" ORDER BY created DESC'
    )
    q = search_jql.build_jql(
        tpl,
        "DFBUGS",
        "ON_QA",
        "4.19",
        target_release=cli_to_target_release_value("4.19"),
    )
    assert '"Target Release" = odf-4.19.z' in q
    assert "ON_QA" in q
