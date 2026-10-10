"""Match issue environment requirements against a Jenkins cluster profile."""

from __future__ import annotations

import re
from typing import Any

_TOPOLOGY_RULES: dict[str, dict[str, Any]] = {
    "regional_dr": {
        "hint_keys": ("regional_dr", "deploy_edr"),
        "min_cluster_count": 2,
    },
    "provider_client": {
        "hint_keys": ("provider_client",),
        "min_cluster_count": 2,
    },
    "external_mode": {
        "hint_keys": ("external_mode",),
        "min_cluster_count": 1,
    },
    "lso_baremetal": {
        "hint_keys": ("lso",),
        "min_cluster_count": 1,
    },
    "metro_dr": {
        "hint_keys": (),
        "min_cluster_count": 1,
    },
    "standard_ipi": {
        "hint_keys": (),
        "min_cluster_count": 1,
    },
    "unclassified": {
        "hint_keys": (),
        "min_cluster_count": 1,
    },
}


def _normalize_odf_version(version: str) -> str:
    value = str(version or "").strip().lower()
    if value.startswith("odf-"):
        value = value[4:]
    return value


def _major_minor(version: str) -> str:
    parts = [p for p in re.split(r"[.\-]", version) if p.isdigit()]
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[1]}"
    return parts[0] if parts else version


def check_odf_version(
    issue_env: dict[str, Any],
    cluster_profile: dict[str, Any],
    *,
    target_zstream: str | None = None,
) -> tuple[bool, list[str]]:
    """Return whether issue ODF version matches the cluster build."""
    mismatches: list[str] = []
    issue_ver = _normalize_odf_version(
        issue_env.get("target_zstream")
        or issue_env.get("odf_version")
        or target_zstream
        or ""
    )
    cluster_ver = _normalize_odf_version(cluster_profile.get("ocs_version", ""))

    if not cluster_ver:
        return True, ["cluster ocs_version unknown — skipping strict version check"]

    if _major_minor(issue_ver) != _major_minor(cluster_ver):
        mismatches.append(
            f"odf_version: issue expects {_major_minor(issue_ver)}, "
            f"cluster has {_major_minor(cluster_ver)}"
        )
        return False, mismatches

    return True, mismatches


def check_topology(
    issue_env: dict[str, Any],
    cluster_profile: dict[str, Any],
) -> tuple[bool, list[str]]:
    """Return whether cluster topology hints satisfy issue requirements."""
    topology = str(issue_env.get("topology_type") or "unclassified")
    rule = _TOPOLOGY_RULES.get(topology, _TOPOLOGY_RULES["unclassified"])
    hints = cluster_profile.get("topology_hints") or {}
    mismatches: list[str] = []

    required_count = int(issue_env.get("cluster_count") or rule["min_cluster_count"])
    if required_count >= 2 and not any(hints.get(k) for k in rule["hint_keys"]):
        if topology in ("regional_dr", "provider_client"):
            mismatches.append(
                f"topology: issue requires {topology} "
                f"(~{required_count} clusters), cluster hints={hints}"
            )
            return False, mismatches

    hint_keys = rule["hint_keys"]
    if hint_keys and not any(hints.get(k) for k in hint_keys):
        mismatches.append(f"topology: issue requires {topology}, cluster hints={hints}")
        return False, mismatches

    return True, mismatches


def assess_compatibility(
    issue: dict[str, Any],
    cluster_profile: dict[str, Any],
    *,
    target_zstream: str | None = None,
) -> dict[str, Any]:
    """
    Compare repro_steps environment requirements with cluster metadata.

    Returns a compatibility report with ``compatible`` bool and ``mismatches`` list.
    """
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    issue_env = repro.get("environment_requirements") or {}

    odf_ok, odf_notes = check_odf_version(
        issue_env, cluster_profile, target_zstream=target_zstream
    )
    topo_ok, topo_notes = check_topology(issue_env, cluster_profile)

    warnings = [n for n in odf_notes if "unknown" in n.lower()]
    mismatches: list[str] = []
    if not odf_ok:
        mismatches.extend(odf_notes)
    if not topo_ok:
        mismatches.extend(topo_notes)

    return {
        "compatible": odf_ok and topo_ok,
        "issue_environment": issue_env,
        "cluster_profile_summary": {
            "cluster_name": cluster_profile.get("cluster_name"),
            "ocs_version": cluster_profile.get("ocs_version"),
            "ocp_version": cluster_profile.get("ocp_version"),
            "platform": cluster_profile.get("platform"),
            "topology_hints": cluster_profile.get("topology_hints"),
        },
        "mismatches": mismatches,
        "warnings": warnings,
    }
