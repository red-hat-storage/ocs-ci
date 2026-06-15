"""Fix-to-topology mapping and environment requirements."""

import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


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


TOPOLOGY_RULES: tuple[TopologyRule, ...] = (
    TopologyRule(
        topology=Topology.REGIONAL_DR,
        keywords=("regional dr", "rdr", "ramen", "failover", "relocate", "volsync"),
        priority=90,
    ),
    TopologyRule(
        topology=Topology.METRO_DR,
        keywords=("metro dr", "mdr", "stretch", "arbiter", "stretched"),
        priority=85,
    ),
    TopologyRule(
        topology=Topology.PROVIDER_CLIENT,
        keywords=(
            "provider-client",
            "provider client",
            "consumer cluster",
            "managed service",
            "rosa",
            "odf managed",
        ),
        priority=80,
    ),
    TopologyRule(
        topology=Topology.EXTERNAL_MODE,
        keywords=("external ceph", "external mode", "rhcs", "external cluster"),
        components=("External Mode", "External"),
        priority=75,
    ),
    TopologyRule(
        topology=Topology.LSO_BAREMETAL,
        keywords=("lso", "local storage", "baremetal", "bare metal"),
        components=("LSO", "Local Storage"),
        priority=70,
    ),
    TopologyRule(
        topology=Topology.STANDARD_IPI,
        components=(
            "RBD",
            "CephFS",
            "PVC",
            "CSI",
            "Snapshot",
            "Clone",
            "Block",
            "File",
        ),
        keywords=("snapshot", "clone", "cephfs", "rbd", "pvc", "csi"),
        priority=50,
    ),
    TopologyRule(
        topology=Topology.STANDARD_IPI,
        components=("MCG", "NooBaa", "Object", "ocs-operator"),
        keywords=("mcg", "noobaa", "bucket", "s3", "obc", "object bucket", "noobaa"),
        priority=40,
    ),
    TopologyRule(
        topology=Topology.STANDARD_IPI,
        components=("OCS Operator", "Operator", "Upgrade", "Deployment"),
        keywords=("ocs operator", "upgrade", "deployment", "install"),
        priority=30,
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


def _search_text(issue: dict[str, Any]) -> str:
    parts = [
        issue.get("key", ""),
        issue.get("summary", ""),
        issue.get("description", ""),
        " ".join(issue.get("components", [])),
        " ".join(issue.get("labels", [])),
    ]
    return " ".join(parts).lower()


def classify_topology(issue: dict[str, Any]) -> dict[str, Any]:
    """Return topology, confidence, and match reason for an issue."""
    text = _search_text(issue)
    components = issue.get("components", [])
    best_topology = Topology.UNCLASSIFIED
    best_confidence = "none"
    best_reason = "no rule matched — requires engineer review"
    best_score = -1

    for rule in sorted(TOPOLOGY_RULES, key=lambda r: r.priority, reverse=True):
        kw_hits = [kw for kw in rule.keywords if kw in text]
        comp_hits = []
        comp_lower = {c.lower(): c for c in components}
        for rule_comp in rule.components:
            for cl, orig in comp_lower.items():
                if rule_comp.lower() in cl or cl in rule_comp.lower():
                    comp_hits.append(orig)

        if not kw_hits and not comp_hits:
            continue

        score = rule.priority + len(kw_hits) * 5 + len(comp_hits) * 10
        if score <= best_score:
            continue

        best_score = score
        best_topology = rule.topology
        if comp_hits and kw_hits:
            best_confidence = "high"
            best_reason = f"component={comp_hits}, keyword={kw_hits}"
        elif comp_hits:
            best_confidence = "high"
            best_reason = f"component match: {comp_hits}"
        else:
            best_confidence = "medium"
            best_reason = f"keyword match: {kw_hits}"

    env = TOPOLOGY_ENVIRONMENT[best_topology]
    return {
        "topology": best_topology,
        "topology_label": TOPOLOGY_LABELS[best_topology],
        "topology_confidence": best_confidence,
        "topology_match_reason": best_reason,
        "topology_details": env["setup_notes"],
        "cluster_count": env["cluster_count"],
        "footprint": env["footprint"],
    }
