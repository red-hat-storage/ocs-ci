"""Fix-to-topology mapping and environment requirements."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEPLOYMENT_DIR = _REPO_ROOT / "conf" / "deployment"

# JIRA template: "The ODF deployment type (Internal, External, ...)"
_ODF_DEPLOYMENT_TYPE_LINE = re.compile(
    r"odf\s+deployment\s+type[^:\n]*:\s*([^\n]+)",
    re.IGNORECASE,
)
_ODF_DEPLOYMENT_TYPE_NEXT_LINE = re.compile(
    r"odf\s+deployment\s+type[^\n]*\n+\s*([^\n_]+)",
    re.IGNORECASE,
)
_DEPLOYMENT_CONFIG_PATH = re.compile(
    r"conf/deployment/[\w./_-]+\.ya?ml",
    re.IGNORECASE,
)
_DEPLOYMENT_STEM = re.compile(
    r"\b((?:ipi|upi|managed)_[\w]+(?:_\d+[mhw])+(?:_[\w]+)*)\b",
    re.IGNORECASE,
)

_SKIP_DEPLOYMENT_VALUES = frozenset(
    {"", "n/a", "na", "none", "tbd", "unknown", "-", "not applicable"}
)
_VERSION_LIKE = re.compile(r"^\s*(?:odf|ocp)\s+[\d.]", re.IGNORECASE)


class Topology:
    STANDARD_IPI = "standard_ipi"
    REGIONAL_DR = "regional_dr"
    METRO_DR = "metro_dr"
    PROVIDER_CLIENT = "provider_client"
    EXTERNAL_MODE = "external_mode"
    LSO_BAREMETAL = "lso_baremetal"
    UNCLASSIFIED = "unclassified"


TOPOLOGY_LABELS = {
    Topology.STANDARD_IPI: "Standard IPI",
    Topology.REGIONAL_DR: "Regional DR",
    Topology.METRO_DR: "Metro DR",
    Topology.PROVIDER_CLIENT: "Provider-Client",
    Topology.EXTERNAL_MODE: "External Mode",
    Topology.LSO_BAREMETAL: "LSO/Baremetal",
    Topology.UNCLASSIFIED: "Unclassified",
}


@dataclass(frozen=True)
class TopologyRule:
    topology: str
    components: tuple[str, ...] = ()
    keywords: tuple[str, ...] = ()
    priority: int = 100


# Heuristic fallback only — avoid loose "external" substring matches.
TOPOLOGY_RULES: tuple[TopologyRule, ...] = (
    TopologyRule(
        topology=Topology.REGIONAL_DR,
        keywords=(
            "regional dr",
            "regional disaster",
            "disaster recovery",
            " rdr ",
            "ramen",
            "failover",
            "relocate",
            "volsync",
        ),
        priority=90,
    ),
    TopologyRule(
        topology=Topology.METRO_DR,
        keywords=("metro dr", " mdr ", "stretch cluster", "arbiter zone"),
        priority=85,
    ),
    TopologyRule(
        topology=Topology.PROVIDER_CLIENT,
        keywords=(
            "provider-client",
            "provider client",
            "consumer cluster",
            "managed service",
            "odf managed",
            "rosa provider",
            "rosa consumer",
        ),
        priority=80,
    ),
    TopologyRule(
        topology=Topology.EXTERNAL_MODE,
        keywords=(
            "external mode",
            "external ceph",
            "external rhcs",
            "external cluster",
            "rhcs cluster",
        ),
        components=("External Mode",),
        priority=75,
    ),
    TopologyRule(
        topology=Topology.LSO_BAREMETAL,
        keywords=("internal-attached", "local storage operator", " lso "),
        components=("LSO", "Local Storage"),
        priority=70,
    ),
    TopologyRule(
        topology=Topology.STANDARD_IPI,
        components=("MCG", "NooBaa", "Object"),
        keywords=("internal mode",),
        priority=40,
    ),
)


TOPOLOGY_ENVIRONMENT: dict[str, dict[str, Any]] = {
    Topology.STANDARD_IPI: {
        "cluster_count": 1,
        "footprint": "standard",
        "cloud_provider": "AWS, vSphere, or compatible IPI platform",
        "setup_notes": "Single ODF cluster deployed via IPI.",
    },
    Topology.REGIONAL_DR: {
        "cluster_count": 2,
        "footprint": "high",
        "cloud_provider": "Two independent clusters (primary + secondary)",
        "setup_notes": (
            "High-footprint multi-cluster Regional DR pair. Requires Ramen, "
            "VolSync, and DR policy configuration across two OpenShift clusters."
        ),
    },
    Topology.METRO_DR: {
        "cluster_count": 1,
        "footprint": "high",
        "cloud_provider": "Stretched cluster with arbiter (3-site or 2-site stretch)",
        "setup_notes": (
            "High-footprint Metro DR / stretched cluster setup with arbiter node. "
            "Requires stretch cluster topology and MDR configuration."
        ),
    },
    Topology.PROVIDER_CLIENT: {
        "cluster_count": 2,
        "footprint": "high",
        "cloud_provider": "Provider cluster + client cluster (e.g. ROSA/ODF managed service)",
        "setup_notes": (
            "High-footprint Provider-Client pair. Requires provider hub and "
            "one or more consumer clusters with managed ODF subscription."
        ),
    },
    Topology.EXTERNAL_MODE: {
        "cluster_count": 1,
        "footprint": "high",
        "cloud_provider": "OpenShift cluster + external RHCS/Ceph cluster",
        "setup_notes": (
            "High-footprint External Mode setup. Requires external Ceph cluster "
            "connected to ODF on OpenShift."
        ),
    },
    Topology.LSO_BAREMETAL: {
        "cluster_count": 1,
        "footprint": "medium",
        "cloud_provider": "Baremetal / LSO-enabled platform",
        "setup_notes": "LSO or baremetal cluster with local storage devices for OSDs.",
    },
    Topology.UNCLASSIFIED: {
        "cluster_count": 1,
        "footprint": "unknown",
        "cloud_provider": "To be confirmed by QE engineer",
        "setup_notes": "Topology could not be determined automatically. Confirm setup with issue owner.",
    },
}


def _topology_result(
    topology: str,
    *,
    confidence: str,
    reason: str,
    source: str,
    odf_deployment_type: str | None = None,
    deployment_config: str | None = None,
) -> dict[str, Any]:
    env = TOPOLOGY_ENVIRONMENT[topology]
    return {
        "topology": topology,
        "topology_label": TOPOLOGY_LABELS[topology],
        "topology_confidence": confidence,
        "topology_match_reason": reason,
        "topology_source": source,
        "topology_details": env["setup_notes"],
        "cluster_count": env["cluster_count"],
        "footprint": env["footprint"],
        "odf_deployment_type": odf_deployment_type,
        "deployment_config": deployment_config,
    }


def parse_odf_deployment_type(description: str) -> str | None:
    """
    Parse the JIRA template field:
    "The ODF deployment type (Internal, External, Internal-Attached (LSO), ...)"
    """
    if not description:
        return None

    candidates: list[str] = []
    for pattern in (_ODF_DEPLOYMENT_TYPE_LINE, _ODF_DEPLOYMENT_TYPE_NEXT_LINE):
        match = pattern.search(description)
        if match:
            candidates.append(match.group(1).strip().strip("*_ "))

    for raw in candidates:
        value = raw.strip()
        if not value or value.lower() in _SKIP_DEPLOYMENT_VALUES:
            continue
        if _VERSION_LIKE.match(value):
            continue
        if value.lower().startswith("the version"):
            continue
        if value.lower().startswith("the ocp"):
            continue
        return value
    return None


def map_odf_deployment_type(value: str) -> str:
    """Map a JIRA ODF deployment type value to internal topology id."""
    text = value.lower().strip()

    if "internal-attached" in text or text == "lso" or "internal attached" in text:
        return Topology.LSO_BAREMETAL
    if re.search(r"\bexternal\b", text) and "internal" not in text:
        return Topology.EXTERNAL_MODE
    if (
        re.search(r"\binternal\b", text)
        and "external" not in text
        and "lso" not in text
    ):
        return Topology.STANDARD_IPI
    if "provider" in text:
        return Topology.PROVIDER_CLIENT
    if "consumer" in text or "client" in text:
        return Topology.PROVIDER_CLIENT
    if "metro" in text or "mdr" in text:
        return Topology.METRO_DR
    if re.search(r"\bdr\b", text) or "disaster" in text or "regional" in text:
        return Topology.REGIONAL_DR
    if "multicluster" in text or "multi-cluster" in text or "multi cluster" in text:
        return Topology.PROVIDER_CLIENT
    return Topology.UNCLASSIFIED


@lru_cache(maxsize=1)
def _deployment_config_index() -> dict[str, Path]:
    """Map deployment config stems and relative paths to files under conf/deployment/."""
    index: dict[str, Path] = {}
    if not _DEPLOYMENT_DIR.is_dir():
        log.warning("Deployment config directory not found: %s", _DEPLOYMENT_DIR)
        return index

    for path in _DEPLOYMENT_DIR.rglob("*.yaml"):
        rel = str(path.relative_to(_REPO_ROOT)).replace("\\", "/")
        stem = path.stem.lower()
        index[stem] = path
        index[rel.lower()] = path
    return index


def _is_external_mode_config_name(stem: str) -> bool:
    """True for ODF external-mode deployment configs (not vault/noobaa-external false positives)."""
    name = stem.lower()
    if "noobaa_external" in name:
        return False
    if name.endswith("_external"):
        return True
    if re.search(r"_external_(?:vault|rhcs)", name):
        return True
    if "external_rhcs" in name or "hpcs_external" in name:
        return True
    return False


def classify_deployment_config(path: Path) -> str:
    """Infer topology from an ocs-ci conf/deployment YAML file."""
    resolved = path.resolve()
    if _REPO_ROOT in resolved.parents or resolved == _REPO_ROOT:
        rel = str(resolved.relative_to(_REPO_ROOT)).replace("\\", "/")
    else:
        rel = str(path).replace("\\", "/")
    stem = resolved.stem.lower()

    try:
        import yaml

        data = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    except ImportError:
        data = {}
    except OSError as exc:
        log.debug("Cannot read deployment config %s: %s", rel, exc)
        data = {}

    env = data.get("ENV_DATA") or {}
    deployment = data.get("DEPLOYMENT") or {}

    if deployment.get("external_mode") is True or _is_external_mode_config_name(stem):
        return Topology.EXTERNAL_MODE

    cluster_type = str(env.get("cluster_type") or "").lower()
    if cluster_type in {"consumer", "provider", "hci_client"}:
        return Topology.PROVIDER_CLIENT

    if env.get("arbiter_deployment") is True:
        return Topology.METRO_DR

    platform = str(env.get("platform") or "").lower()
    if "lso" in stem or env.get("local_storage_operator"):
        return Topology.LSO_BAREMETAL
    if platform == "baremetal" or "baremetal" in stem:
        return Topology.LSO_BAREMETAL

    return Topology.STANDARD_IPI


def _extract_deployment_config_refs(issue: dict[str, Any]) -> list[str]:
    """Collect deployment config path/stem references from labels and description."""
    refs: list[str] = []
    seen: set[str] = set()

    def add(ref: str) -> None:
        key = ref.lower().strip()
        if key and key not in seen:
            seen.add(key)
            refs.append(ref)

    for label in issue.get("labels") or []:
        label_str = str(label).strip()
        add(label_str)
        for match in _DEPLOYMENT_CONFIG_PATH.findall(label_str):
            add(match)
        for match in _DEPLOYMENT_STEM.findall(label_str):
            add(match)

    description = issue.get("description") or ""
    for match in _DEPLOYMENT_CONFIG_PATH.findall(description):
        add(match)
    for match in _DEPLOYMENT_STEM.findall(description):
        add(match)

    return refs


def match_deployment_config_topology(issue: dict[str, Any]) -> dict[str, Any] | None:
    """Match JIRA deployment config labels/paths against conf/deployment/."""
    index = _deployment_config_index()
    if not index:
        return None

    matched_paths: list[Path] = []
    for ref in _extract_deployment_config_refs(issue):
        ref_lower = ref.lower().replace("\\", "/")
        path = index.get(ref_lower)
        if path is None:
            stem = Path(ref_lower).stem
            path = index.get(stem)
        if path is not None and path not in matched_paths:
            matched_paths.append(path)

    if not matched_paths:
        return None

    # Prefer the longest (most specific) config path when multiple match.
    matched_paths.sort(key=lambda p: len(str(p)), reverse=True)
    best = matched_paths[0]
    rel = str(best.relative_to(_REPO_ROOT)).replace("\\", "/")
    topology = classify_deployment_config(best)
    return _topology_result(
        topology,
        confidence="high",
        reason=f"matched ocs-ci deployment config: {rel}",
        source="deployment_config",
        deployment_config=rel,
    )


def _classify_by_heuristics(issue: dict[str, Any]) -> dict[str, Any]:
    """Keyword/component fallback when JIRA field and deployment config are unavailable."""
    text = " ".join(
        filter(
            None,
            [
                issue.get("key", ""),
                issue.get("summary", ""),
                issue.get("description", ""),
                " ".join(issue.get("components", [])),
                " ".join(issue.get("labels", [])),
            ],
        )
    ).lower()

    # Pad with spaces so word-boundary-style keywords like " rdr " match reliably.
    padded = f" {text} "
    components = issue.get("components", [])
    best_topology = Topology.UNCLASSIFIED
    best_confidence = "none"
    best_reason = "no rule matched — requires engineer review"
    best_score = -1

    for rule in sorted(TOPOLOGY_RULES, key=lambda r: r.priority, reverse=True):
        kw_hits = [kw for kw in rule.keywords if kw in padded]
        comp_hits = []
        comp_lower = {c.lower(): c for c in components}
        for rule_comp in rule.components:
            for cl, orig in comp_lower.items():
                if rule_comp.lower() == cl:
                    comp_hits.append(orig)

        if not kw_hits and not comp_hits:
            continue

        score = rule.priority + len(kw_hits) * 5 + len(comp_hits) * 10
        if score <= best_score:
            continue

        best_score = score
        best_topology = rule.topology
        if comp_hits and kw_hits:
            best_confidence = "medium"
            best_reason = f"heuristic component={comp_hits}, keyword={kw_hits}"
        elif comp_hits:
            best_confidence = "medium"
            best_reason = f"heuristic component match: {comp_hits}"
        else:
            best_confidence = "low"
            best_reason = f"heuristic keyword match: {kw_hits}"

    return _topology_result(
        best_topology,
        confidence=best_confidence,
        reason=best_reason,
        source="heuristic",
    )


def classify_topology(issue: dict[str, Any]) -> dict[str, Any]:
    """
    Return topology, confidence, and match reason for an issue.

    Priority:
      1. JIRA description ODF deployment type field
      2. Deployment config label/path matched to conf/deployment/
      3. Conservative keyword/component heuristics
    """
    description = issue.get("description") or ""
    odf_type = parse_odf_deployment_type(description)
    if odf_type:
        topology = map_odf_deployment_type(odf_type)
        if topology != Topology.UNCLASSIFIED:
            return _topology_result(
                topology,
                confidence="high",
                reason=f"JIRA ODF deployment type: {odf_type}",
                source="jira_deployment_type",
                odf_deployment_type=odf_type,
            )

    config_match = match_deployment_config_topology(issue)
    if config_match:
        return config_match

    if odf_type:
        return _topology_result(
            Topology.UNCLASSIFIED,
            confidence="low",
            reason=f"unmapped JIRA ODF deployment type: {odf_type}",
            source="jira_deployment_type",
            odf_deployment_type=odf_type,
        )

    return _classify_by_heuristics(issue)
