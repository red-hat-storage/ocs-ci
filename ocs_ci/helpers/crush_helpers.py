"""
Helper functions for CRUSH map operations and weight validation in Ceph clusters.
This module provides utilities for verifying CRUSH weight balance, particularly
important for stretch cluster configurations (DFBUGS-2885).
"""

import logging
from typing import Dict, List, Tuple, Optional

from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)


def get_osd_crush_weights() -> Dict[int, float]:
    """
    Get CRUSH weights for all OSDs in the cluster.

    Returns:
        dict: {osd_id: crush_weight}
    """
    logger.info("Fetching CRUSH weights for all OSDs")
    ceph_tools_pod = get_ceph_tools_pod()

    try:
        osd_df_output = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph osd df")
        osd_weights = {}

        for node in osd_df_output.get("nodes", []):
            osd_id = node.get("id")
            crush_weight = node.get("crush_weight")
            if osd_id is not None and crush_weight is not None:
                osd_weights[osd_id] = float(crush_weight)

        logger.info(f"Retrieved CRUSH weights for {len(osd_weights)} OSDs")
        logger.debug(f"OSD CRUSH weights: {osd_weights}")
        return osd_weights

    except (CommandFailed, KeyError) as e:
        logger.error(f"Failed to get OSD CRUSH weights: {e}")
        raise


def get_zone_crush_weights(
    zones: Optional[List[str]] = None,
) -> Dict[str, float]:
    """
    Get aggregated CRUSH weights for each zone in the cluster.

    FIX: Uses JSON output from ``ceph osd tree`` instead of fragile
    shell grep+awk parsing.

    Args:
        zones (list, optional): Zone labels to check. Defaults to DATA_ZONE_LABELS.

    Returns:
        dict: {zone_name: total_crush_weight}
    """
    if zones is None:
        zones = constants.DATA_ZONE_LABELS

    logger.info(f"Fetching CRUSH weights for zones: {zones}")
    ceph_tools_pod = get_ceph_tools_pod()
    zone_weights = {}

    try:
        crush_tree = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")

        for node in crush_tree.get("nodes", []):
            if node.get("type") == "zone" and node.get("name") in zones:
                weight = float(node.get("crush_weight", 0.0))
                zone_weights[node["name"]] = weight
                logger.info(f"Zone '{node['name']}' CRUSH weight: {weight}")

        for zone in zones:
            if zone not in zone_weights:
                logger.warning(f"No CRUSH weight found for zone '{zone}'")
                zone_weights[zone] = 0.0

        return zone_weights

    except (CommandFailed, ValueError) as e:
        logger.error(f"Failed to get zone CRUSH weights: {e}")
        raise


def calculate_weight_difference_percentage(weight1: float, weight2: float) -> float:
    """Calculate the percentage difference between two CRUSH weights."""
    if weight1 == 0 and weight2 == 0:
        return 0.0

    avg_weight = (weight1 + weight2) / 2
    if avg_weight == 0:
        return 0.0

    difference = abs(weight1 - weight2)
    percentage = (difference / avg_weight) * 100

    logger.debug(f"Weight difference: {weight1} vs {weight2} = {percentage:.2f}%")
    return percentage


def verify_zone_weight_balance(
    zones: Optional[List[str]] = None,
    tolerance_percentage: float = 1.0,
) -> Tuple[bool, Dict[str, float], str]:
    """
    Verify that CRUSH weights are balanced across zones within a tolerance.

    Args:
        zones (list, optional): Zone labels to check.
        tolerance_percentage (float): Max allowed % difference (default: 1.0%)

    Returns:
        tuple: (is_balanced, zone_weights, message)
    """
    if zones is None:
        zones = constants.DATA_ZONE_LABELS

    logger.info(
        f"Verifying zone weight balance with tolerance: {tolerance_percentage}%"
    )

    try:
        zone_weights = get_zone_crush_weights(zones)

        if len(zone_weights) < 2:
            message = f"Insufficient zones for balance check: {len(zone_weights)}"
            logger.warning(message)
            return False, zone_weights, message

        weights = list(zone_weights.values())

        if any(w == 0 for w in weights):
            message = f"One or more zones have zero weight: {zone_weights}"
            logger.error(message)
            return False, zone_weights, message

        max_diff_percentage = 0.0
        zone_pairs = []

        for i, zone1 in enumerate(zones):
            for zone2 in zones[i + 1 :]:
                if zone1 in zone_weights and zone2 in zone_weights:
                    diff_pct = calculate_weight_difference_percentage(
                        zone_weights[zone1], zone_weights[zone2]
                    )
                    if diff_pct > max_diff_percentage:
                        max_diff_percentage = diff_pct
                        zone_pairs = [zone1, zone2]

        is_balanced = max_diff_percentage <= tolerance_percentage

        if is_balanced:
            message = (
                f"Zone weights are balanced within {tolerance_percentage}% tolerance. "
                f"Max difference: {max_diff_percentage:.2f}% between zones. "
                f"Weights: {zone_weights}"
            )
            logger.info(message)
        else:
            message = (
                f"Zone weights are UNBALANCED! "
                f"Difference: {max_diff_percentage:.2f}% "
                f"(tolerance: {tolerance_percentage}%) "
                f"between zones {zone_pairs[0]} and {zone_pairs[1]}. "
                f"Weights: {zone_weights}"
            )
            logger.error(message)

        return is_balanced, zone_weights, message

    except Exception as e:
        message = f"Failed to verify zone weight balance: {str(e)}"
        logger.error(message)
        return False, {}, message


def get_crush_tree() -> Dict:
    """Get the complete CRUSH tree structure from Ceph as JSON."""
    logger.info("Fetching CRUSH tree structure")
    ceph_tools_pod = get_ceph_tools_pod()

    try:
        crush_tree = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
        logger.debug(
            f"CRUSH tree retrieved with {len(crush_tree.get('nodes', []))} nodes"
        )
        return crush_tree

    except CommandFailed as e:
        logger.error(f"Failed to get CRUSH tree: {e}")
        raise


def get_osds_in_zone(zone: str) -> List[int]:
    """
    Get list of OSD IDs in a specific zone.

    Args:
        zone (str): Zone label to query

    Returns:
        list: List of OSD IDs in the specified zone
    """
    logger.info(f"Fetching OSDs in zone: {zone}")

    try:
        crush_tree = get_crush_tree()
        osds_in_zone = []

        nodes_by_id = {node["id"]: node for node in crush_tree.get("nodes", [])}

        zone_node = None
        for node in crush_tree.get("nodes", []):
            if node.get("type") == "zone" and node.get("name") == zone:
                zone_node = node
                break

        if not zone_node:
            logger.warning(f"Zone '{zone}' not found in CRUSH tree")
            return osds_in_zone

        def find_osds_recursive(node_id):
            node = nodes_by_id.get(node_id)
            if node is None:
                return
            if node.get("type") == "osd":
                osds_in_zone.append(node_id)
            for child_id in node.get("children", []):
                find_osds_recursive(child_id)

        for child_id in zone_node.get("children", []):
            find_osds_recursive(child_id)

        logger.info(f"Found {len(osds_in_zone)} OSDs in zone '{zone}': {osds_in_zone}")
        return osds_in_zone

    except Exception as e:
        logger.error(f"Failed to get OSDs in zone '{zone}': {e}")
        raise


def verify_stretch_mode_enabled() -> bool:
    """
    Verify that stretch mode is enabled in the Ceph cluster.

    FIX: Uses ``ceph osd dump`` with the dedicated ``stretch_mode_enabled``
    field instead of loose string matching on mon dump.

    Returns:
        bool: True if stretch mode is enabled, False otherwise
    """
    logger.info("Verifying stretch mode status")
    ceph_tools_pod = get_ceph_tools_pod()

    try:
        osd_dump = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph osd dump")
        is_stretch_enabled = osd_dump.get("stretch_mode_enabled", False)

        if is_stretch_enabled:
            logger.info("Stretch mode is ENABLED")
        else:
            logger.warning("Stretch mode is NOT enabled")

        return bool(is_stretch_enabled)

    except CommandFailed as e:
        logger.error(f"Failed to verify stretch mode status: {e}")
        return False


def log_crush_weight_details(zones: Optional[List[str]] = None):
    """Log detailed CRUSH weight information for debugging purposes."""
    if zones is None:
        zones = constants.DATA_ZONE_LABELS

    logger.info("=" * 80)
    logger.info("CRUSH Weight Analysis")
    logger.info("=" * 80)

    try:
        osd_weights = get_osd_crush_weights()
        logger.info(f"\nTotal OSDs: {len(osd_weights)}")
        logger.info(f"OSD CRUSH Weights: {osd_weights}")

        zone_weights = get_zone_crush_weights(zones)
        logger.info("\nZone CRUSH Weights:")
        for zone, weight in zone_weights.items():
            logger.info(f"  {zone}: {weight}")

        logger.info("\nOSDs per Zone:")
        for zone in zones:
            osds = get_osds_in_zone(zone)
            logger.info(f"  {zone}: {len(osds)} OSDs - {osds}")

        is_balanced, _, message = verify_zone_weight_balance(zones)
        logger.info(f"\nBalance Status: {'BALANCED' if is_balanced else 'UNBALANCED'}")
        logger.info(f"Details: {message}")

        stretch_enabled = verify_stretch_mode_enabled()
        logger.info(f"\nStretch Mode: {'ENABLED' if stretch_enabled else 'DISABLED'}")

    except Exception as e:
        logger.error(f"Error during CRUSH weight analysis: {e}")

    logger.info("=" * 80)
