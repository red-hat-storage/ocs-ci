"""
Map JIRA issues and ocs-ci tests to ODF code coverage areas.

Links upstream components/repos to test directories and ocs-ci library modules
so test matching prioritizes tests that exercise the same code paths as the fix.
"""

import logging
import re
from typing import Any

log = logging.getLogger(__name__)

# Coverage area = logical code path / upstream component family
CODE_COVERAGE_AREAS: dict[str, dict[str, Any]] = {
    "ocs-operator": {
        "label": "OCS/ODF Operator",
        "upstream_repos": ["ocs-operator", "odf-operator", "odf-deps"],
        "test_dirs": [
            "tests/functional/z_cluster",
            "tests/functional/upgrade",
            "tests/functional/monitoring",
            "tests/functional/pod_and_daemons",
        ],
        "ocs_ci_paths": ["ocs_ci/deployment", "ocs_ci/ocs/resources/storagecluster"],
        "keywords": [
            "ocs-operator",
            "odf-operator",
            "storagecluster",
            "subscription",
            "olm",
            "operator",
            "deploy",
        ],
    },
    "noobaa-mcg": {
        "label": "NooBaa / MCG (S3)",
        "upstream_repos": ["noobaa-core", "noobaa-operator"],
        "test_dirs": [
            "tests/functional/object/mcg",
            "tests/functional/object/rgw",
            "tests/cross_functional/scale/noobaa",
            "tests/cross_functional/system_test",
        ],
        "ocs_ci_paths": [
            "ocs_ci/ocs/resources/objectbucket",
            "ocs_ci/ocs/bucket_utils",
        ],
        "keywords": [
            "noobaa",
            "mcg",
            "bucket",
            "obc",
            "object bucket",
            "namespace store",
            "backingstore",
            "s3",
        ],
    },
    "rook-ceph": {
        "label": "Rook / Ceph cluster",
        "upstream_repos": ["rook", "ceph"],
        "test_dirs": [
            "tests/functional/z_cluster",
            "tests/functional/pod_and_daemons",
            "tests/cross_functional/kcs",
        ],
        "ocs_ci_paths": ["ocs_ci/ocs/resources/ceph", "ocs_ci/ocs/ceph_cluster"],
        "keywords": ["rook", "ceph", "osd", "mon", "mgr", "mds", "rgw"],
    },
    "csi-rbd": {
        "label": "CSI / RBD block storage",
        "upstream_repos": ["rook", "ceph-csi"],
        "test_dirs": [
            "tests/functional/pv",
            "tests/functional/pvc_snapshot",
            "tests/functional/pvc_clone",
            "tests/functional/pv_encryption",
            "tests/functional/storageclass",
        ],
        "ocs_ci_paths": ["ocs_ci/ocs/resources/pvc", "ocs_ci/ocs/resources/pod"],
        "keywords": ["rbd", "block", "pvc", "snapshot", "clone", "csi", "volume"],
    },
    "csi-cephfs": {
        "label": "CSI / CephFS file storage",
        "upstream_repos": ["rook", "ceph-csi"],
        "test_dirs": [
            "tests/functional/pv",
            "tests/cross_functional/stress/cephfs",
        ],
        "ocs_ci_paths": ["ocs_ci/ocs/resources/pvc"],
        "keywords": ["cephfs", "file", "fs", "subvolume", "ceph fs"],
    },
    "regional-dr": {
        "label": "Regional DR (Ramen)",
        "upstream_repos": ["ramen", "odr-operator"],
        "test_dirs": ["tests/functional/disaster-recovery/regional-dr"],
        "ocs_ci_paths": ["ocs_ci/helpers/dr", "ocs_ci/ocs/dr"],
        "keywords": ["regional dr", "rdr", "ramen", "failover", "relocate", "volsync"],
    },
    "metro-dr": {
        "label": "Metro DR / stretch cluster",
        "upstream_repos": ["ramen", "rook"],
        "test_dirs": [
            "tests/functional/disaster-recovery/metro-dr",
            "tests/functional/disaster-recovery/sc_arbiter",
        ],
        "ocs_ci_paths": ["ocs_ci/helpers/dr"],
        "keywords": ["metro dr", "mdr", "stretch", "arbiter", "stretched"],
    },
    "external-mode": {
        "label": "External mode (RHCS)",
        "upstream_repos": ["ocs-operator", "rook"],
        "test_dirs": ["tests/functional/external_mode"],
        "ocs_ci_paths": ["ocs_ci/deployment"],
        "keywords": ["external mode", "external ceph", "rhcs", "external cluster"],
    },
    "provider-client": {
        "label": "Provider / consumer (managed ODF)",
        "upstream_repos": ["ocs-operator", "odf-multicluster-orchestrator"],
        "test_dirs": [
            "tests/functional/provider_mode",
            "tests/functional/object/test_obc_deletion_client_provider",
        ],
        "ocs_ci_paths": ["ocs_ci/ocs/managedservice"],
        "keywords": [
            "provider",
            "consumer",
            "managed service",
            "rosa",
            "client cluster",
        ],
    },
    "monitoring": {
        "label": "Monitoring / alerts / metrics",
        "upstream_repos": ["ocs-operator", "rook"],
        "test_dirs": [
            "tests/functional/monitoring",
            "tests/cross_functional/ui",
        ],
        "ocs_ci_paths": ["ocs_ci/utility/prometheus", "ocs_ci/ocs/monitoring"],
        "keywords": ["prometheus", "alert", "metric", "monitoring", "pagerduty"],
    },
    "encryption-kms": {
        "label": "Encryption / KMS",
        "upstream_repos": ["ocs-operator", "rook"],
        "test_dirs": ["tests/functional/encryption", "tests/functional/pv_encryption"],
        "ocs_ci_paths": ["ocs_ci/utility/kms"],
        "keywords": ["encrypt", "kms", "vault", "kmip", "key rotation"],
    },
    "upgrade": {
        "label": "ODF upgrade",
        "upstream_repos": ["ocs-operator", "rook", "noobaa-operator"],
        "test_dirs": [
            "tests/functional/upgrade",
            "tests/cross_functional/scale/upgrade",
        ],
        "ocs_ci_paths": ["ocs_ci/ocs/upgrade"],
        "keywords": ["upgrade", "z-stream", "zstream", "lane c"],
    },
    "chaos-resiliency": {
        "label": "Chaos / Krkn resiliency",
        "upstream_repos": ["rook", "ceph"],
        "test_dirs": [
            "tests/cross_functional/krkn_chaos",
            "tests/cross_functional/kcs",
            "tests/cross_functional/system_test/mon",
        ],
        "ocs_ci_paths": ["ocs_ci/krkn_chaos", "ocs_ci/resiliency"],
        "keywords": [
            "krkn",
            "krknctl",
            "chaos engineering",
            "chaos scenario",
            "pod disruption",
            "pod kill",
            "ceph-mon",
            "rook-ceph-mon",
            "ceph crash",
            "session_map",
            "quorum",
            "disruption scenario",
        ],
    },
}

# Loose keywords that appear in JIRA templates; only match high-signal fields.
_GENERIC_TEXT_KEYWORDS = frozenset(
    {
        "ceph",
        "rook",
        "osd",
        "mon",
        "mgr",
        "mds",
        "rgw",
        "block",
        "pvc",
        "volume",
        "operator",
        "deploy",
        "alert",
        "metric",
        "provider",
        "consumer",
        "encrypt",
        "upgrade",
        "bucket",
        "s3",
    }
)

_MAX_PRIMARY_COVERAGE_AREAS = 3
_MAX_PREFERRED_TEST_DIRS = 6

# JIRA component name → coverage area id
JIRA_COMPONENT_TO_AREA: dict[str, str] = {
    "ocs-operator": "ocs-operator",
    "operator": "ocs-operator",
    "noobaa": "noobaa-mcg",
    "mcg": "noobaa-mcg",
    "rbd": "csi-rbd",
    "cephfs": "csi-cephfs",
    "csi": "csi-rbd",
    "pvc": "csi-rbd",
    "snapshot": "csi-rbd",
    "clone": "csi-rbd",
    "regional dr": "regional-dr",
    "metro dr": "metro-dr",
    "dr": "regional-dr",
    "odf-dr": "regional-dr",
    "odf-dr/ramen": "regional-dr",
    "ramen": "regional-dr",
    "external mode": "external-mode",
    "external": "external-mode",
    "monitoring": "monitoring",
    "encryption": "encryption-kms",
    "upgrade": "upgrade",
    "rook": "rook-ceph",
    "ceph": "rook-ceph",
}

# Upstream repo mention in text → primary coverage area (avoids cross-area bleed)
UPSTREAM_REPO_TO_AREA: dict[str, str] = {
    "noobaa-core": "noobaa-mcg",
    "noobaa-operator": "noobaa-mcg",
    "ocs-operator": "ocs-operator",
    "odf-operator": "ocs-operator",
    "odf-deps": "ocs-operator",
    "odf-multicluster-orchestrator": "provider-client",
    "rook": "rook-ceph",
    "ceph": "rook-ceph",
    "ceph-csi": "csi-rbd",
    "ramen": "regional-dr",
    "odr-operator": "regional-dr",
}


def _keyword_in_text(keyword: str, text: str) -> bool:
    """Match keywords with word boundaries for short/generic terms."""
    if len(keyword) <= 4:
        return bool(re.search(rf"\b{re.escape(keyword)}\b", text))
    return keyword in text


def _search_text_from_issue(issue: dict[str, Any]) -> str:
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    parts = [
        issue.get("summary", ""),
        issue.get("description", ""),
        " ".join(issue.get("components", [])),
        " ".join(issue.get("labels", [])),
        repro.get("issue_summary", ""),
        " ".join(repro.get("reproduction_steps", [])),
        " ".join(repro.get("verification_steps", [])),
        repro.get("expected_result", ""),
    ]
    return " ".join(filter(None, parts)).lower()


def _high_signal_text_from_issue(issue: dict[str, Any]) -> str:
    """Summary, components, labels, and repro/verification steps (not JIRA boilerplate)."""
    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    parts = [
        issue.get("summary", ""),
        " ".join(issue.get("components", [])),
        " ".join(issue.get("labels", [])),
        repro.get("issue_summary", ""),
        " ".join(repro.get("reproduction_steps", [])),
        " ".join(repro.get("verification_steps", [])),
        repro.get("expected_result", ""),
    ]
    return " ".join(filter(None, parts)).lower()


def _infer_scenario_test_dirs(text: str) -> list[str]:
    """Direct test-dir hints from repro/verification language."""
    dirs: list[str] = []
    if any(
        token in text
        for token in (
            "krkn",
            "krknctl",
            "chaos",
            "pod disruption",
            "pod kill",
            "disruption scenario",
        )
    ):
        dirs.append("tests/cross_functional/krkn_chaos")
    if any(
        token in text
        for token in ("ceph-mon", "rook-ceph-mon", "mon quorum", "ceph mon")
    ):
        dirs.extend(
            [
                "tests/cross_functional/system_test/mon",
                "tests/cross_functional/kcs",
            ]
        )
    if any(
        token in text for token in ("regional dr", "drpc", "ramen", "failover", "rdr")
    ):
        dirs.append("tests/functional/disaster-recovery/regional-dr")
    if any(
        token in text
        for token in ("vault", "encryption-at-rest", "encryption at rest", "kms")
    ):
        dirs.extend(
            [
                "tests/functional/encryption",
                "tests/functional/pv_encryption",
            ]
        )
    return sorted(set(dirs))


def _match_coverage_areas(issue: dict[str, Any]) -> dict[str, list[str]]:
    """Return area_id -> match reasons for an issue."""
    full_text = _search_text_from_issue(issue)
    high_signal = _high_signal_text_from_issue(issue)
    matched: dict[str, list[str]] = {area_id: [] for area_id in CODE_COVERAGE_AREAS}

    for component in issue.get("components", []):
        comp_lower = component.lower().strip()
        area_id = JIRA_COMPONENT_TO_AREA.get(comp_lower)
        if area_id:
            matched[area_id].append(f"jira_component:{component}")
        for area_id, config in CODE_COVERAGE_AREAS.items():
            if any(kw in comp_lower for kw in config["keywords"]):
                matched[area_id].append(f"component_keyword:{component}")

    for area_id, config in CODE_COVERAGE_AREAS.items():
        for kw in config["keywords"]:
            kw_lower = kw.lower()
            if kw_lower in _GENERIC_TEXT_KEYWORDS:
                if _keyword_in_text(kw_lower, high_signal):
                    matched[area_id].append(f"text_keyword:{kw}")
            elif _keyword_in_text(kw_lower, full_text):
                matched[area_id].append(f"text_keyword:{kw}")

    for repo, area_id in UPSTREAM_REPO_TO_AREA.items():
        if repo in high_signal or repo in full_text:
            matched[area_id].append(f"upstream_repo:{repo}")

    return matched


def _apply_scenario_boosts(
    matched: dict[str, list[str]],
    high_signal: str,
) -> None:
    """Promote coverage areas that clearly match repro/verification scenario language."""
    if any(
        token in high_signal
        for token in (
            "krkn",
            "krknctl",
            "chaos",
            "pod disruption",
            "pod kill",
        )
    ):
        matched["chaos-resiliency"].append("scenario:krkn_chaos")
    if any(
        token in high_signal
        for token in ("ceph-mon", "rook-ceph-mon", "mon quorum", "ceph mon")
    ):
        matched["chaos-resiliency"].append("scenario:ceph_mon_resiliency")
        matched["rook-ceph"].append("scenario:ceph_mon")
    if any(
        token in high_signal
        for token in ("vault", "encryption-at-rest", "encryption at rest")
    ):
        matched["encryption-kms"].append("scenario:encryption_kms")


def _rank_coverage_areas(
    matched: dict[str, list[str]],
) -> tuple[list[str], list[str]]:
    """Split matched areas into primary (for test dir focus) and secondary."""
    active = [aid for aid, reasons in matched.items() if reasons]
    if not active:
        return ["ocs-operator"], []

    ranked = sorted(
        active,
        key=lambda aid: (_score_area_reasons(matched[aid]), len(matched[aid])),
        reverse=True,
    )
    top_score = _score_area_reasons(matched[ranked[0]])
    primary = [ranked[0]]
    for area_id in ranked[1:_MAX_PRIMARY_COVERAGE_AREAS]:
        score = _score_area_reasons(matched[area_id])
        if score < 24:
            continue
        if top_score >= 80 and score < 40:
            continue
        if top_score > 0 and score < top_score * 0.35:
            continue
        primary.append(area_id)

    secondary = [aid for aid in ranked if aid not in primary]
    return primary, secondary


def infer_issue_coverage_areas(issue: dict[str, Any]) -> dict[str, Any]:
    """
    Infer code coverage areas affected by a JIRA issue.

    Args:
        issue (dict): Issue from run record

    Returns:
        dict: coverage_areas (primary), all_coverage_areas, preferred_test_dirs, area_details

    """
    matched = _match_coverage_areas(issue)
    high_signal = _high_signal_text_from_issue(issue)
    _apply_scenario_boosts(matched, high_signal)
    primary_areas, secondary_areas = _rank_coverage_areas(matched)
    if not primary_areas:
        primary_areas = ["ocs-operator"]
        matched.setdefault("ocs-operator", ["default_fallback"])

    scenario_dirs = _infer_scenario_test_dirs(high_signal)

    preferred_test_dirs: list[str] = list(scenario_dirs)
    upstream_repos: list[str] = []
    area_details = []
    for area_id in primary_areas + secondary_areas:
        config = CODE_COVERAGE_AREAS[area_id]
        if area_id in primary_areas:
            preferred_test_dirs.extend(config["test_dirs"])
        upstream_repos.extend(config["upstream_repos"])
        area_details.append(
            {
                "area_id": area_id,
                "label": config["label"],
                "upstream_repos": config["upstream_repos"],
                "test_dirs": config["test_dirs"],
                "match_reasons": matched[area_id],
                "is_primary": area_id in primary_areas,
            }
        )

    preferred_test_dirs = sorted(set(preferred_test_dirs))[:_MAX_PREFERRED_TEST_DIRS]

    return {
        "coverage_areas": primary_areas,
        "all_coverage_areas": primary_areas + secondary_areas,
        "secondary_coverage_areas": secondary_areas,
        "coverage_area_labels": [
            CODE_COVERAGE_AREAS[a]["label"] for a in primary_areas
        ],
        "upstream_repos": sorted(set(upstream_repos)),
        "preferred_test_dirs": preferred_test_dirs,
        "scenario_test_dirs": scenario_dirs,
        "area_details": area_details,
    }


def _score_area_reasons(reasons: list[str]) -> int:
    """Higher score = stronger signal that the issue belongs in this coverage area."""
    score = 0
    for reason in reasons:
        if reason.startswith("jira_component:"):
            score += 100
        elif reason.startswith("upstream_repo:"):
            score += 80
        elif reason.startswith("component_keyword:"):
            score += 60
        elif reason.startswith("scenario:"):
            score += 95
        elif reason.startswith("text_keyword:"):
            score += 12
        elif reason.startswith("default_fallback"):
            score += 0
    return score


def select_primary_coverage_areas(
    issue: dict[str, Any],
    *,
    max_areas: int = 2,
) -> list[str]:
    """
    Return the top coverage areas for vector search filtering.

    Uses JIRA components and upstream-repo mentions ahead of loose text keywords
    so generic template text does not dilute the primary code path.
    """
    coverage = infer_issue_coverage_areas(issue)
    primary = coverage.get("coverage_areas") or []
    if primary:
        return primary[:max_areas]
    return ["ocs-operator"]


def preferred_test_dir_prefixes(
    issue: dict[str, Any], *, max_dirs: int = 6
) -> list[str]:
    """Test directory prefixes for the highest-confidence coverage areas."""
    coverage = infer_issue_coverage_areas(issue)
    dirs = list(coverage.get("preferred_test_dirs") or [])
    return dirs[:max_dirs]


def infer_test_coverage_areas(file_path: str, content: str = "") -> list[str]:
    """
    Infer code coverage areas for an ocs-ci test from path and file content.

    Args:
        file_path (str): Relative path under tests/
        content (str): Test file source (optional, for import analysis)

    Returns:
        list[str]: Coverage area ids

    """
    path_lower = file_path.lower()
    content_lower = content.lower()
    areas: set[str] = set()

    for area_id, config in CODE_COVERAGE_AREAS.items():
        if any(path_lower.startswith(d) for d in config["test_dirs"]):
            areas.add(area_id)
        if any(kw in path_lower for kw in config["keywords"]):
            areas.add(area_id)
        if content_lower:
            if any(
                p.replace("/", ".").lower() in content_lower
                for p in config["ocs_ci_paths"]
            ):
                areas.add(area_id)
            imports = re.findall(
                r"from\s+(ocs_ci\.[\w.]+)|import\s+(ocs_ci\.[\w.]+)",
                content_lower,
            )
            flat_imports = " ".join(m[0] or m[1] for m in imports)
            if any(
                p.replace("/", ".").lower() in flat_imports
                for p in config["ocs_ci_paths"]
            ):
                areas.add(area_id)

    return sorted(areas)


def coverage_area_overlap_score(
    issue_areas: list[str],
    test_areas: list[str],
) -> tuple[int, list[str]]:
    """
    Score overlap between issue and test coverage areas.

    Returns:
        tuple[int, list[str]]: (score_boost, overlap_reasons)

    """
    overlap = set(issue_areas) & set(test_areas)
    if not overlap:
        return 0, []
    labels = [
        CODE_COVERAGE_AREAS[a]["label"] for a in overlap if a in CODE_COVERAGE_AREAS
    ]
    score = min(len(overlap) * 35, 105)
    return score, [f"code coverage area: {label}" for label in labels]
