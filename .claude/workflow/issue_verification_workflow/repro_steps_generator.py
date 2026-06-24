"""
Generate issue reproduction and verification steps from JIRA issue details.

Designed for use with JIRA MCP (Claude) or ocs-ci JiraHelper (CLI).
Refreshes each issue from JIRA before analysis when jira_config is provided.
"""

import logging
import re
import sys
from pathlib import Path
from typing import Any

_ISSUE_VERIFICATION_DIR = Path(__file__).resolve().parent
_OCS_CI_JIRA_DIR = _ISSUE_VERIFICATION_DIR.parents[2] / "agents" / "ocs_ci_jira"
if str(_OCS_CI_JIRA_DIR) not in sys.path:
    sys.path.insert(0, str(_OCS_CI_JIRA_DIR))

from topology_mapper import TOPOLOGY_ENVIRONMENT, Topology, classify_topology

log = logging.getLogger(__name__)

STAGE_REPRO_STEPS = "repro_steps"

_VERSION_PATTERNS = (
    re.compile(r"\bOCP\s+(\d+\.\d+(?:\.\d+)?)", re.IGNORECASE),
    re.compile(r"\bOpenShift\s+(\d+\.\d+(?:\.\d+)?)", re.IGNORECASE),
    re.compile(r"\bODF\s+(\d+\.\d+(?:\.\d+)?)", re.IGNORECASE),
    re.compile(r"\bodf-(\d+\.\d+(?:\.\d+)?)", re.IGNORECASE),
)

_REPRO_SECTION_HEADERS = (
    "steps to reproduce",
    "step to reproduce",
    "reproduction steps",
    "repro steps",
    "how to reproduce",
    "steps to re-create",
    "steps to recreate",
)

_VERIFICATION_SECTION_HEADERS = (
    "expected result",
    "expected behavior",
    "expected outcome",
    "verification",
    "actual result",
)


def _first_match(patterns: tuple[re.Pattern, ...], text: str) -> str | None:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return match.group(1)
    return None


def _extract_section_lines(description: str, headers: tuple[str, ...]) -> list[str]:
    """Extract bullet/numbered lines following a section header in the description."""
    if not description:
        return []

    lines = description.splitlines()
    captured: list[str] = []
    in_section = False

    for line in lines:
        stripped = line.strip()
        lower = stripped.lower().rstrip(":")

        if any(header in lower for header in headers):
            in_section = True
            continue

        if in_section:
            if not stripped:
                if captured:
                    break
                continue
            if re.match(r"^[A-Za-z].*:$", stripped) and not re.match(
                r"^\d+\.", stripped
            ):
                break
            if stripped.startswith(("-", "*", "#")) or re.match(
                r"^\d+[\).\]]", stripped
            ):
                step = re.sub(r"^[-*#\s]+", "", stripped)
                step = re.sub(r"^\d+[\).\]]\s*", "", step)
                if step:
                    captured.append(step)
            elif captured:
                captured[-1] = f"{captured[-1]} {stripped}"

    return captured


def _extract_numbered_steps(description: str) -> list[str]:
    """Fallback: pull numbered lines from the full description."""
    steps = []
    for line in description.splitlines():
        stripped = line.strip()
        match = re.match(r"^(\d+)[\).\]]\s+(.+)", stripped)
        if match:
            steps.append(match.group(2).strip())
    return steps


def _infer_prerequisites(issue: dict[str, Any], topology: str) -> list[str]:
    """Build prerequisite list from issue metadata and topology."""
    prereqs = [
        f"ODF target version: {issue.get('target_odf_version', 'see issue')}",
        "OpenShift cluster with ODF installed in openshift-storage namespace",
        "oc CLI logged in as cluster-admin",
    ]

    text = _search_text(issue)
    if "must-gather" in text or "must gather" in text:
        prereqs.append("Collect must-gather if reproduction requires log analysis")
    if topology in (
        Topology.REGIONAL_DR,
        Topology.METRO_DR,
        Topology.PROVIDER_CLIENT,
        Topology.EXTERNAL_MODE,
    ):
        prereqs.append(TOPOLOGY_ENVIRONMENT[topology]["setup_notes"])
    if "noobaa" in text or "mcg" in text:
        prereqs.append("NooBaa/MCG operational (noobaa-admin secret available)")
    if any(k in text for k in ("rbd", "pvc", "cephfs", "csi")):
        prereqs.append("StorageClass from ODF available for PVC provisioning")
    if "upgrade" in text:
        prereqs.append("Cluster at source ODF version before applying z-stream upgrade")

    return prereqs


def _search_text(issue: dict[str, Any]) -> str:
    return " ".join(
        filter(
            None,
            [
                issue.get("summary", ""),
                issue.get("description", ""),
                " ".join(issue.get("components", [])),
            ],
        )
    ).lower()


def _build_environment_requirements(
    issue: dict[str, Any],
    topology_info: dict[str, Any],
    target_odf_version: str,
) -> dict[str, Any]:
    """Build structured environment requirements."""
    description = issue.get("description", "")
    ocp_version = _first_match(_VERSION_PATTERNS[:2], description)
    odf_version = _first_match(
        _VERSION_PATTERNS[2:], description
    ) or target_odf_version.replace("odf-", "")

    topology = topology_info["topology"]
    env = TOPOLOGY_ENVIRONMENT[topology]

    return {
        "odf_version": (
            f"odf-{odf_version}"
            if not str(odf_version).startswith("odf-")
            else odf_version
        ),
        "target_zstream": target_odf_version,
        "ocp_version": ocp_version
        or "Match ODF compatibility matrix for target z-stream",
        "cloud_provider": env["cloud_provider"],
        "cluster_count": env["cluster_count"],
        "topology_type": topology,
        "topology_label": topology_info["topology_label"],
        "footprint": env["footprint"],
        "prerequisites": _infer_prerequisites(
            {**issue, "target_odf_version": target_odf_version}, topology
        ),
    }


def _build_expected_result(issue: dict[str, Any], reproduction_steps: list[str]) -> str:
    """Derive expected result from description or issue summary."""
    description = issue.get("description", "")
    for line in description.splitlines():
        lower = line.strip().lower()
        if any(header in lower for header in _VERIFICATION_SECTION_HEADERS):
            idx = description.lower().find(lower)
            if idx >= 0:
                tail = description[idx:].splitlines()
                if len(tail) > 1 and tail[1].strip():
                    return tail[1].strip()

    summary = issue.get("summary", "")
    if reproduction_steps:
        return (
            f"After completing reproduction steps, the issue described in {issue.get('key')} "
            f"should NOT occur: {summary}"
        )
    return (
        f"Validate fix for {issue.get('key')}: {summary}. "
        "Confirm the reported failure mode is resolved on the target z-stream build."
    )


def _build_verification_steps(
    issue: dict[str, Any],
    reproduction_steps: list[str],
    expected_result: str,
    target_odf_version: str,
) -> list[str]:
    """Generate post-fix verification steps."""
    key = issue.get("key", "")
    steps = [
        f"Deploy/upgrade cluster to target z-stream build ({target_odf_version}).",
        "Confirm openshift-storage operators and pods are healthy.",
    ]

    if reproduction_steps:
        steps.append("Repeat the reproduction steps below on the fixed build.")
    else:
        steps.append(
            f"Reproduce the customer scenario described in {key} "
            f"(see JIRA description and comments)."
        )

    steps.extend(
        [
            f"Verify expected result: {expected_result}",
            "Check relevant operator logs and events in openshift-storage namespace.",
            "Confirm no new alerts or degraded Ceph health related to this issue.",
        ]
    )

    desc_lower = issue.get("description", "").lower()
    if "audit" in desc_lower or "404" in desc_lower:
        steps.append(
            "Review kube-apiserver audit logs for absence of the reported 404 errors."
        )
    if "must-gather" in desc_lower:
        steps.append("Collect must-gather and compare with pre-fix behavior if needed.")

    return steps


def _build_reproduction_steps(issue: dict[str, Any]) -> list[str]:
    """Extract or synthesize reproduction steps from issue description."""
    description = issue.get("description", "")
    steps = _extract_section_lines(description, _REPRO_SECTION_HEADERS)

    if not steps:
        steps = _extract_numbered_steps(description)

    if not steps:
        summary = issue.get("summary", "")
        components = ", ".join(issue.get("components", [])) or "ODF"
        steps = [
            f"Review full JIRA description and comments for {issue.get('key')}.",
            f"Set up environment matching reported versions (component: {components}).",
            f"Reproduce the scenario: {summary}",
            "Capture logs, events, and must-gather while the failure is active.",
        ]

    return steps


def merge_mcp_issue_details(
    base_issue: dict[str, Any],
    mcp_issue: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Merge JIRA MCP-fetched issue data over the run-record issue.

    MCP payloads may use raw JIRA API shape (fields nested) or parsed shape.

    Args:
      base_issue (dict): Issue from run record
      mcp_issue (dict | None): Issue from JIRA MCP get_issue / jql_search

    Returns:
      dict: Merged issue for analysis

    """
    if not mcp_issue:
        return dict(base_issue)

    from parser import parse_jira_issue

    if "fields" in mcp_issue:
        parsed = parse_jira_issue(mcp_issue)
    else:
        parsed = dict(mcp_issue)

    merged = dict(base_issue)
    for key, value in parsed.items():
        if value:
            merged[key] = value
    return merged


def generate_repro_steps_for_issue(
    issue: dict[str, Any],
    target_odf_version: str,
    *,
    mcp_issue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Generate reproduction and verification plan for a single issue.

    Args:
      issue (dict): Issue from run record (jira_intake stage)
      target_odf_version (str): Target ODF z-stream for this run
      mcp_issue (dict | None): Optional fresh issue payload from JIRA MCP

    Returns:
      dict: Stage data to store under stages.repro_steps.data

    """
    enriched = merge_mcp_issue_details(issue, mcp_issue)
    topology_info = classify_topology(enriched)
    reproduction_steps = _build_reproduction_steps(enriched)
    expected_result = _build_expected_result(enriched, reproduction_steps)
    verification_steps = _build_verification_steps(
        enriched, reproduction_steps, expected_result, target_odf_version
    )

    return {
        "issue_id": enriched.get("key", ""),
        "issue_summary": enriched.get("summary", ""),
        "topology": topology_info["topology"],
        "topology_label": topology_info["topology_label"],
        "topology_confidence": topology_info["topology_confidence"],
        "topology_match_reason": topology_info["topology_match_reason"],
        "topology_details": topology_info["topology_details"],
        "environment_requirements": _build_environment_requirements(
            enriched, topology_info, target_odf_version
        ),
        "reproduction_steps": reproduction_steps,
        "expected_result": expected_result,
        "verification_steps": verification_steps,
        "analysis_notes": (
            "Generated from JIRA description and component/keyword topology mapping. "
            "Review and refine steps using JIRA comments and linked PRs before execution."
        ),
    }


def refresh_issue_from_jira(
    issue_key: str,
    jira_config: str | None = None,
) -> dict[str, Any]:
    """
    Refresh issue details from JIRA API (CLI equivalent of JIRA MCP get_issue).

    Args:
      issue_key (str): JIRA issue key
      jira_config (str | None): Optional jira.cfg path

    Returns:
      dict: Parsed issue details

    """
    log.info("Refreshing %s from JIRA", issue_key)
    from operations import get_issue

    return get_issue(issue_key, jira_config=jira_config)


def run_repro_steps_stage(
    issues: list[dict[str, Any]],
    target_odf_version: str,
    *,
    jira_config: str | None = None,
    refresh_jira: bool = True,
    mcp_issues: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Generate reproduction steps for all issues in the run record.

    Args:
      issues (list): Issues from run record
      target_odf_version (str): Target ODF z-stream version
      jira_config (str | None): JIRA credentials for refresh
      refresh_jira (bool): Re-fetch each issue from JIRA before analysis
      mcp_issues (dict | None): issue_key -> MCP payload (used when JIRA MCP available)

    Returns:
      dict: issue_key -> stage data for append_stage_bulk

    """
    mcp_issues = mcp_issues or {}
    per_issue: dict[str, dict[str, Any]] = {}

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue

        try:
            mcp_payload = mcp_issues.get(key)
            working = dict(issue)

            if mcp_payload:
                log.info("Using JIRA MCP data for %s", key)
                working = merge_mcp_issue_details(working, mcp_payload)
            elif refresh_jira:
                try:
                    working = refresh_issue_from_jira(key, jira_config=jira_config)
                except Exception as exc:
                    log.warning(
                        "JIRA refresh failed for %s: %s — using run record data",
                        key,
                        exc,
                    )

            per_issue[key] = generate_repro_steps_for_issue(
                working,
                target_odf_version,
                mcp_issue=mcp_payload,
            )
            log.info(
                "Generated repro steps for %s (topology: %s)",
                key,
                per_issue[key]["topology"],
            )
        except Exception as exc:
            log.error("Failed to generate repro steps for %s: %s", key, exc)
            per_issue[key] = {
                "issue_id": key,
                "issue_summary": issue.get("summary", ""),
                "status": "failed",
                "error": str(exc),
            }

    return per_issue
