"""JIRA client wrapper for agent use (extended auth sources)."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ocs_ci.utility.jira import JiraHelper


def get_jira_client(jira_config: str | None = None) -> JiraHelper:
    """
    Return a JiraHelper with extended credential resolution for agents.

    Resolution: AUTH.jira → jira_config → /etc/jira.cfg → data/auth.yaml → env
    """
    return JiraHelper(config_path=jira_config, allow_extended_sources=True)
