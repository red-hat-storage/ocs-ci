"""High-level API for live cluster verification."""

from __future__ import annotations

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

from cluster_context import resolve_cluster_profile
from claude_verifier import live_verify_issue
from compatibility import assess_compatibility
from dry_run_verifier import dry_run_verify_issue
from models import STAGE_LIVE_CLUSTER_VERIFICATION

log = logging.getLogger(__name__)

__all__ = [
    "STAGE_LIVE_CLUSTER_VERIFICATION",
    "load_issues_from_run_record",
    "verify_issue",
    "verify_issues",
]


def load_issues_from_run_record(
    run_id: str,
    *,
    issue_key: str | None = None,
) -> list[dict[str, Any]]:
    """Load issues from a z-stream run record."""
    if str(_ISSUE_VERIFICATION_DIR) not in sys.path:
        sys.path.insert(0, str(_ISSUE_VERIFICATION_DIR))

    from run_record import RunRecord

    run_record = RunRecord.load(run_id)
    issues = run_record.get_issues()
    if issue_key:
        issues = [i for i in issues if i.get("key") == issue_key]
        if not issues:
            raise ValueError(f"Issue {issue_key} not found in run record {run_id}")
    return issues


def verify_issue(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
    target_zstream: str | None = None,
    dry_run: bool = True,
    skip_on_env_mismatch: bool = True,
    force: bool = False,
    oc_command_path: str = "oc",
    model: str | None = None,
    max_turns: int = 40,
    backend: str = "auto",
) -> dict[str, Any]:
    """Verify a single issue — dry-run plan (Phase A) or live cluster check (Phase B)."""
    compatibility = assess_compatibility(
        issue,
        cluster_profile,
        target_zstream=target_zstream,
    )
    if dry_run:
        return dry_run_verify_issue(
            issue,
            cluster_profile=cluster_profile,
            compatibility=compatibility,
            skip_on_env_mismatch=skip_on_env_mismatch,
            force=force,
        )
    return live_verify_issue(
        issue,
        cluster_profile=cluster_profile,
        compatibility=compatibility,
        skip_on_env_mismatch=skip_on_env_mismatch,
        force=force,
        oc_command_path=oc_command_path,
        model=model,
        max_turns=max_turns,
        backend=backend,  # type: ignore[arg-type]
    )


def verify_issues(
    issues: list[dict[str, Any]],
    *,
    deploy_job_url: str,
    target_zstream: str | None = None,
    dry_run: bool = True,
    skip_on_env_mismatch: bool = True,
    force: bool = False,
    oc_command_path: str = "oc",
    model: str | None = None,
    max_turns: int = 40,
    backend: str = "auto",
) -> dict[str, dict[str, Any]]:
    """
    Verify all issues against a Jenkins deploy cluster.

    Returns:
        dict: issue_key -> stage data (includes ``stage_status`` for run record)

    """
    cluster_profile = resolve_cluster_profile(deploy_job_url, dry_run=dry_run)
    per_issue: dict[str, dict[str, Any]] = {}

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue
        try:
            per_issue[key] = verify_issue(
                issue,
                cluster_profile=cluster_profile,
                target_zstream=target_zstream,
                dry_run=dry_run,
                skip_on_env_mismatch=skip_on_env_mismatch,
                force=force,
                oc_command_path=oc_command_path,
                model=model,
                max_turns=max_turns,
                backend=backend,
            )
            log.info(
                "Cluster verify %s: verdict=%s",
                key,
                per_issue[key].get("verdict"),
            )
        except Exception as exc:
            log.error("Cluster verification failed for %s: %s", key, exc)
            per_issue[key] = {
                "stage_status": "failed",
                "issue_id": key,
                "verdict": "failed",
                "error": str(exc),
                "cluster_profile": cluster_profile,
            }

    return per_issue
