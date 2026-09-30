"""ON_QA search and verification-step retrieval for the Jira agent."""

import asyncio
import json

import pytest

from ocs_ci.agents.mcp.servers import jira as jira_server
from ocs_ci.agents.runtime.claude_cli import parse_agent_message
from ocs_ci.agents.jira_verification.report_store import write_verification_report
from ocs_ci.utility.jira import (
    _normalize_jira_auth,
    adf_to_text,
    extract_headed_sections,
    github_pull_requests,
    html_to_text,
    issue_summary,
    issue_verification_payload,
    on_qa_jql,
)


def test_claude_tool_call_and_final_reply_parse():
    """Claude print mode replies become tool calls or a final message."""
    tool_message = parse_agent_message(
        '{"tool_calls":[{"name":"jira_search_issues","args":{"version":"odf-5.0"}}]}'
    )
    assert tool_message.tool_calls[0]["name"] == "jira_search_issues"
    assert tool_message.tool_calls[0]["args"]["version"] == "odf-5.0"
    final_message = parse_agent_message('{"final":"{\\"version\\":\\"odf-5.0\\"}"}')
    assert final_message.content == '{"version":"odf-5.0"}'
    assert final_message.tool_calls == []


def test_github_remote_links_are_pull_requests():
    """Only GitHub pull request URLs are kept from Jira remote links."""
    prs = github_pull_requests(
        [
            {"object": {"url": "https://github.com/RamenDR/ramen/pull/1432"}},
            {"object": {"url": "https://errata.devel.redhat.com/advisory/125619"}},
            {"object": {"url": "https://github.com/RamenDR/ramen/pull/1432"}},
        ]
    )
    assert prs == ["https://github.com/RamenDR/ramen/pull/1432"]


def test_save_tool_writes_a_report_for_the_issue_just_read(tmp_path):
    """A report is written only when an issue is saved, one file per key."""
    saved = write_verification_report(
        "odf-5.0",
        "DFBUGS-1",
        {
            "bug_description": "Sync time is cleared after hub recovery.",
            "affected_version": ["odf-4.15"],
            "fix_version": ["odf-5.0"],
            "environment_reported": "VMware regional DR",
            "environment_verify": "odf-5.0 regional DR",
            "upgrade_scenario": False,
            "verification_steps": [
                {
                    "step": 1,
                    "action": "Read lastGroupSyncTime",
                    "command": "oc get drpc -A",
                }
            ],
            "additional_info": "",
            "git_prs": ["https://github.com/RamenDR/ramen/pull/1432"],
        },
        root=tmp_path,
    )
    import yaml

    written = yaml.safe_load(saved.read_text(encoding="utf-8"))
    assert written["key"] == "DFBUGS-1"
    assert written["fix_version"] == ["odf-5.0"]
    assert written["upgrade_scenario"] is False
    index = yaml.safe_load((tmp_path / "odf-5.0" / "index.yaml").read_text())
    assert index["reports"] == ["DFBUGS-1.yaml"]


def test_email_and_token_satisfy_jira_credentials():
    """data/auth.yaml may store email and token instead of username and password."""
    auth = _normalize_jira_auth(
        {
            "url": "https://redhat.atlassian.net",
            "email": "qe@example.com",
            "token": "api-token",
        }
    )
    assert auth["url"] == "https://redhat.atlassian.net"
    assert auth["username"] == "qe@example.com"
    assert auth["password"] == "api-token"


def test_on_qa_jql_matches_target_release_and_fix_version():
    """The search covers Target Version, Target Release, and Fix Version."""
    jql = on_qa_jql("odf-5.0")
    assert jql == (
        'project = "DFBUGS" AND status = "ON_QA" AND '
        '("Target Version" = "odf-5.0" OR "Target Release" = "odf-5.0" '
        'OR fixVersion = "odf-5.0") ORDER BY key ASC'
    )


def test_on_qa_jql_quotes_version_text():
    """A quote in the version cannot break out of the JQL string."""
    jql = on_qa_jql('odf "5.0"', project="RHSTOR")
    assert 'project = "RHSTOR"' in jql
    assert r'"odf \"5.0\""' in jql


def test_on_qa_jql_rejects_a_blank_version():
    """A search without a version is a caller error."""
    with pytest.raises(ValueError, match="version is required"):
        on_qa_jql("  ")


def test_issue_summary_reads_target_and_fix_versions():
    """Search hits keep the version names the JQL matched on."""
    summary = issue_summary(
        {
            "key": "DFBUGS-1",
            "fields": {
                "summary": "OSD down",
                "status": {"name": "ON_QA"},
                "fixVersions": [{"name": "odf-5.0"}],
                "customfield_10855": [{"name": "odf-5.0"}],
                "customfield_10886": {"name": "odf-5.0"},
            },
        }
    )
    assert summary == {
        "key": "DFBUGS-1",
        "summary": "OSD down",
        "status": "ON_QA",
        "versions": ["odf-5.0"],
    }


def test_html_description_splits_into_verification_sections():
    """Rendered headings become the sections the agent reads."""
    text = html_to_text(
        "<p><strong>Steps to Reproduce:</strong></p>"
        "<ol><li><p>Create a PVC.</p></li><li><p>Delete the OSD pod.</p></li></ol>"
        "<p><strong>Expected results:</strong></p>"
        "<p>The PVC stays Bound.</p>"
        "<p><strong>Verification steps:</strong></p>"
        "<p>Confirm the PVC is Bound after the pod returns.</p>"
    )
    sections = extract_headed_sections(text)
    assert "Create a PVC." in sections["steps_to_reproduce"]
    assert "Delete the OSD pod." in sections["steps_to_reproduce"]
    assert sections["expected_results"] == "The PVC stays Bound."
    assert "PVC is Bound" in sections["verification_steps"]


def test_adf_description_is_plain_text():
    """Cloud issues stored as ADF still yield readable text."""
    text = adf_to_text(
        {
            "type": "doc",
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "How to verify:"}],
                },
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "Check ceph health."}],
                },
            ],
        }
    )
    assert "How to verify:" in text
    assert "Check ceph health." in text


def test_verification_payload_prefers_rendered_comments():
    """Comment HTML is the text the agent sees."""
    payload = issue_verification_payload(
        {
            "key": "DFBUGS-2",
            "fields": {
                "summary": "Must gather misses a probe",
                "status": {"name": "ON_QA"},
                "description": {
                    "type": "doc",
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [
                                {"type": "text", "text": "Steps to Reproduce:"}
                            ],
                        }
                    ],
                },
            },
            "renderedFields": {
                "description": (
                    "<p><strong>Steps to Reproduce:</strong></p>"
                    "<p>Run must-gather.</p>"
                )
            },
        },
        comments=[
            {
                "author": {"displayName": "QE"},
                "created": "2026-09-24T06:13:52.462+0000",
                "body": {"type": "doc", "content": []},
                "renderedBody": "<p>Kernel check is present. Moving to ON_QA.</p>",
            }
        ],
    )
    assert payload["sections"]["steps_to_reproduce"] == "Run must-gather."
    assert payload["comments"][0]["author"] == "QE"
    assert "Moving to ON_QA" in payload["comments"][0]["body"]


def test_search_tool_returns_the_helper_list(monkeypatch):
    """jira_search_issues returns the ON_QA list as JSON."""

    class Helper:
        def search_on_qa(self, version, project="DFBUGS"):
            assert version == "odf-5.0"
            assert project == "DFBUGS"
            return [{"key": "DFBUGS-1", "summary": "OSD down", "status": "ON_QA"}]

    monkeypatch.setattr(jira_server, "_jira_helper", lambda: Helper())
    payload = json.loads(jira_server.jira_search_issues("odf-5.0"))
    assert payload["count"] == 1
    assert payload["issues"][0]["key"] == "DFBUGS-1"


def test_get_issue_tool_returns_verification_content(monkeypatch):
    """jira_get_issue returns the helper payload as JSON."""

    class Helper:
        def issue_for_verification(self, issue_key):
            assert issue_key == "DFBUGS-1"
            return {
                "key": issue_key,
                "sections": {"verification_steps": "Check health."},
            }

    monkeypatch.setattr(jira_server, "_jira_helper", lambda: Helper())
    payload = json.loads(jira_server.jira_get_issue("DFBUGS-1"))
    assert payload["sections"]["verification_steps"] == "Check health."


def test_jira_server_registers_both_tools():
    """The stdio server exposes the tools named in TOOL_NAMES."""
    pytest.importorskip("mcp")
    server = jira_server.build_server()
    tools = asyncio.run(server.list_tools())
    assert {tool.name for tool in tools} == set(jira_server.TOOL_NAMES)
