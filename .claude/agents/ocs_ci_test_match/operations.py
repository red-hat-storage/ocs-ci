"""High-level API for matching ocs-ci tests to JIRA issues."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

_AGENT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _AGENT_DIR.parents[2]
_ISSUE_VERIFICATION_DIR = (
    _AGENT_DIR.parents[1] / "workflow" / "issue_verification_workflow"
)

for _path in (_AGENT_DIR, _REPO_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from matcher import find_matching_tests_for_issue, run_test_matching_stage
from models import STAGE_TEST_MATCHING

log = logging.getLogger(__name__)

__all__ = [
    "STAGE_TEST_MATCHING",
    "find_matching_tests_for_issue",
    "load_issues_from_run_record",
    "load_issue_from_jira",
    "match_issue",
    "match_issues",
]


def load_issues_from_run_record(
    run_id: str,
    *,
    issue_key: str | None = None,
) -> list[dict[str, Any]]:
    """
    Load issues from a z-stream run record.

    Args:
        run_id (str): Run id from stage 1 (e.g. 20260620_091223)
        issue_key (str | None): Optional single issue key filter

    Returns:
        list[dict]: Issue dicts from the run record

    """
    if str(_ISSUE_VERIFICATION_DIR) not in sys.path:
        sys.path.insert(0, str(_ISSUE_VERIFICATION_DIR))

    from run_record import RunRecord

    run_record = RunRecord.load(run_id)
    issues = run_record.get_issues()
    if issue_key:
        issues = [issue for issue in issues if issue.get("key") == issue_key]
        if not issues:
            raise ValueError(f"Issue {issue_key} not found in run record {run_id}")
    return issues


def load_issue_from_jira(
    issue_key: str,
    *,
    jira_config: str | None = None,
) -> dict[str, Any]:
    """Fetch a single JIRA issue for standalone test matching."""
    _JIRA_DIR = _AGENT_DIR.parent / "ocs_ci_jira"
    if str(_JIRA_DIR) not in sys.path:
        sys.path.insert(0, str(_JIRA_DIR))

    from operations import get_issue

    return get_issue(issue_key, jira_config=jira_config)


def load_issue_from_file(path: Path | str) -> dict[str, Any]:
    """Load an issue dict from a JSON file."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, list):
        if len(data) != 1:
            raise ValueError("Issue file list must contain exactly one issue")
        data = data[0]
    if not isinstance(data, dict):
        raise ValueError("Issue file must contain a JSON object or single-item list")
    return data


def match_issue(
    issue: dict[str, Any],
    *,
    top_n: int = 10,
    use_claude: bool = False,
    model: str | None = None,
) -> dict[str, Any]:
    """
    Find matching ocs-ci tests for one issue.

    Args:
        issue (dict): Issue dict (run record or JIRA-parsed)
        top_n (int): Max matches to return
        use_claude (bool): Use Claude Agent SDK semantic search
        model (str | None): Claude model override

    Returns:
        dict: Stage data with matching_tests

    """
    key = issue.get("key", "")
    results = match_issues(
        [issue],
        top_n=top_n,
        use_claude=use_claude,
        model=model,
    )
    if key and key in results:
        return results[key]
    if results:
        return next(iter(results.values()))
    return {
        "issue_id": key,
        "issue_summary": issue.get("summary", ""),
        "matcher": "vector_db",
        "matching_test_count": 0,
        "matching_tests": [],
        "analysis_notes": "No matching tests found.",
    }


def match_issues(
    issues: list[dict[str, Any]],
    *,
    top_n: int = 10,
    use_claude: bool = False,
    backend: str = "auto",
    model: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Find matching ocs-ci tests for multiple issues.

    Args:
        issues (list[dict]): Issue dicts from run record or JIRA
        top_n (int): Max matches per issue
        use_claude (bool): Legacy flag — forces claude-sdk backend when True
        backend (str): auto | vector_db | claude-cli | claude-sdk
        model (str | None): Claude model override

    Returns:
        dict: issue_key -> stage data for run record append_stage_bulk

    """
    if use_claude and backend == "auto":
        backend = "claude-sdk"
    return run_test_matching_stage(
        issues,
        top_n=top_n,
        backend=backend,
        model=model,
    )
