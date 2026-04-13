"""Analysis modules for different ODF components"""

from .summary import generate_summary
from .nodes import analyze_nodes
from .pods import analyze_pods
from .operators import analyze_csv, analyze_subscriptions
from .ceph import analyze_ceph_status, analyze_ceph_pools, analyze_osd_tree
from .noobaa import analyze_noobaa, analyze_backingstores
from .storage import (
    analyze_storagecluster,
    analyze_storageclient,
    analyze_pvcs,
    analyze_csi_drivers,
)
from .events import analyze_events

__all__ = [
    "generate_summary",
    "analyze_nodes",
    "analyze_pods",
    "analyze_csv",
    "analyze_subscriptions",
    "analyze_ceph_status",
    "analyze_ceph_pools",
    "analyze_osd_tree",
    "analyze_noobaa",
    "analyze_backingstores",
    "analyze_storagecluster",
    "analyze_storageclient",
    "analyze_pvcs",
    "analyze_csi_drivers",
    "analyze_events",
]
