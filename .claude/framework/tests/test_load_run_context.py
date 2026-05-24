"""Run context loader tests."""

import json
import sys
from pathlib import Path

LIB = Path(__file__).resolve().parents[1] / "lib"
sys.path.insert(0, str(LIB))
from load_run_context import load_context, shell_exports  # noqa: E402


def test_load_from_active_run(tmp_path: Path):
    (tmp_path / "active-run.json").write_text(
        json.dumps(
            {
                "odf_version": "4.18",
                "workflow_id": "zstream-issue-verification",
                "run_id": "test-run",
                "dry_run": True,
            }
        )
    )
    ctx = load_context(tmp_path)
    assert ctx["odf_version"] == "4.18"
    exports = shell_exports(ctx)
    assert 'export ODF_VERSION="4.18"' in exports
    assert "DFBUGS_DRY_RUN=1" in exports


def test_workflow_aware_defaults(tmp_path: Path):
    """When jira_status/jira_project are missing, they come from the workflow YAML."""
    (tmp_path / "active-run.json").write_text(
        json.dumps(
            {
                "odf_version": "4.19",
                "workflow_id": "zstream-issue-verification",
                "run_id": "test-run-2",
            }
        )
    )
    ctx = load_context(tmp_path)
    assert ctx.get("jira_status") == "ON_QA"
    assert ctx.get("jira_project") == "DFBUGS"


def test_no_workflow_no_jira_fields(tmp_path: Path):
    """Without a workflow_id, JIRA fields should not appear with stale defaults."""
    (tmp_path / "active-run.json").write_text(
        json.dumps(
            {
                "odf_version": "1.0",
                "run_id": "test-run-3",
            }
        )
    )
    ctx = load_context(tmp_path)
    assert ctx["odf_version"] == "1.0"
    assert "jira_status" not in ctx
    assert "jira_project" not in ctx


def test_shell_exports_empty_jira(tmp_path: Path):
    """Shell exports should not inject DFBUGS/ON_QA for non-JIRA workflows."""
    (tmp_path / "active-run.json").write_text(
        json.dumps(
            {
                "odf_version": "1.0",
                "run_id": "test-run-4",
            }
        )
    )
    ctx = load_context(tmp_path)
    exports = shell_exports(ctx)
    assert 'JIRA_STATUS=""' in exports
    assert 'JIRA_PROJECT=""' in exports
