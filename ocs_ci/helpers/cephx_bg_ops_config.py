"""
Shared accessors for CephX key rotation settings in background_cluster_operations.

Used by krkn and resiliency workload config loaders and BackgroundClusterOperations.
"""

from typing import Any, Dict, List

DEFAULT_CEPHX_KEYS = ["rook_daemon"]
DEFAULT_CEPHX_KEYROTATION_INTERVAL = 180


def is_cephx_keyrotation_enabled(bg_ops_config: Dict[str, Any]) -> bool:
    """Return whether CephX key rotation background ops are enabled."""
    return bg_ops_config.get("enable_cephx_keyrotation", False)


def get_cephx_keys(bg_ops_config: Dict[str, Any]) -> List[str]:
    """
    Normalize cephx_keys from background_cluster_operations config.

    Accepts a list or a dict with a ``components`` key; defaults to rook_daemon.
    """
    cephx_keys = bg_ops_config.get("cephx_keys", DEFAULT_CEPHX_KEYS)
    if isinstance(cephx_keys, dict):
        cephx_keys = cephx_keys.get("components", DEFAULT_CEPHX_KEYS)
    return list(cephx_keys or DEFAULT_CEPHX_KEYS)


def get_cephx_keyrotation_interval(bg_ops_config: Dict[str, Any]) -> int:
    """Return minimum seconds between CephX key rotation iterations."""
    return int(
        bg_ops_config.get(
            "cephx_keyrotation_interval", DEFAULT_CEPHX_KEYROTATION_INTERVAL
        )
    )
