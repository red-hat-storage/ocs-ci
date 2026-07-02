"""Phase A dry-run verification — plan steps without executing on cluster."""

from __future__ import annotations

from typing import Any

from models import (
    SKIP_ENV_MISMATCH,
    SKIP_MISSING_REPRO,
    VERDICT_DRY_RUN,
    VERDICT_SKIPPED,
    VERIFIER_DRY_RUN,
)


def build_verification_plan(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
) -> list[dict[str, str]]:
    """Build the oc/kubectl checks that Phase B would run."""
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    issue_key = issue.get("key", "")
    cluster_name = cluster_profile.get("cluster_name") or "target-cluster"

    plan: list[dict[str, str]] = [
        {
            "phase": "preflight",
            "action": "verify cluster access",
            "command": "oc whoami && oc get storagecluster -n openshift-storage",
        },
        {
            "phase": "preflight",
            "action": "confirm ODF version",
            "command": "oc get storagecluster ocs-storagecluster -n openshift-storage -o jsonpath='{.status.version}'",
        },
    ]

    for idx, step in enumerate(repro.get("reproduction_steps") or [], start=1):
        plan.append(
            {
                "phase": "reproduction",
                "action": f"repro step {idx}",
                "command": f"# manual/agent: {step}",
            }
        )

    for idx, step in enumerate(repro.get("verification_steps") or [], start=1):
        plan.append(
            {
                "phase": "verification",
                "action": f"verify step {idx}",
                "command": f"# manual/agent: {step}",
            }
        )

    expected = repro.get("expected_result")
    if expected:
        plan.append(
            {
                "phase": "verification",
                "action": "confirm expected result",
                "command": f"# expected: {expected}",
            }
        )

    plan.append(
        {
            "phase": "verdict",
            "action": f"record fix status for {issue_key}",
            "command": f"# cluster={cluster_name}: fixed | not_fixed | inconclusive",
        }
    )
    return plan


def dry_run_verify_issue(
    issue: dict[str, Any],
    *,
    cluster_profile: dict[str, Any],
    compatibility: dict[str, Any],
    skip_on_env_mismatch: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """Produce per-issue stage data for dry-run verification."""
    issue_key = issue.get("key", "")
    repro_stage = issue.get("stages", {}).get("repro_steps")

    if not repro_stage or repro_stage.get("status") != "completed":
        return {
            "stage_status": "skipped",
            "issue_id": issue_key,
            "verdict": VERDICT_SKIPPED,
            "skip_reason": SKIP_MISSING_REPRO,
            "matcher": VERIFIER_DRY_RUN,
            "compatibility": compatibility,
            "cluster_profile": cluster_profile,
            "verification_plan": [],
            "analysis_notes": "repro_steps stage must complete before cluster verification",
        }

    if not compatibility.get("compatible") and skip_on_env_mismatch and not force:
        return {
            "stage_status": "skipped",
            "issue_id": issue_key,
            "verdict": VERDICT_SKIPPED,
            "skip_reason": SKIP_ENV_MISMATCH,
            "matcher": VERIFIER_DRY_RUN,
            "compatibility": compatibility,
            "cluster_profile": cluster_profile,
            "verification_plan": [],
            "analysis_notes": (
                "Skipped: cluster environment does not match issue requirements. "
                "Use force=true to plan verification anyway."
            ),
        }

    plan = build_verification_plan(issue, cluster_profile=cluster_profile)
    return {
        "stage_status": "completed",
        "issue_id": issue_key,
        "issue_summary": issue.get("summary", ""),
        "verdict": VERDICT_DRY_RUN,
        "matcher": VERIFIER_DRY_RUN,
        "dry_run": True,
        "compatibility": compatibility,
        "cluster_profile": cluster_profile,
        "verification_plan": plan,
        "planned_command_count": len(plan),
        "analysis_notes": (
            "Phase A dry-run: verification plan recorded. "
            "No cluster commands executed. Enable live verification in Phase B."
        ),
    }
