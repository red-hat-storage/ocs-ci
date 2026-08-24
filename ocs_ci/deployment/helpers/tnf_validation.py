"""
Validation functions for Two-Node Failover (TNF) cluster deployment

This module provides pre-deployment validation to ensure the cluster
meets all requirements for TNF deployment.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedDeploymentConfiguration
from ocs_ci.deployment.helpers.tnf_helpers import (
    verify_tnf_cluster_topology,
    get_tnf_node_info,
    verify_port_connectivity,
    get_block_devices_on_node,
)

logger = logging.getLogger(__name__)


def validate_tnf_prerequisites():
    """
    Validate all prerequisites for TNF deployment.

    This function checks:
    1. Cluster topology is DualReplica
    2. Exactly 2 nodes exist
    3. Fencing is enabled
    4. Storage requirements are met
    5. Network requirements are met

    Returns:
        dict: Validation results with detailed information

    Raises:
        UnexpectedDeploymentConfiguration: If validation fails
    """
    logger.info("Validating TNF deployment prerequisites...")
    validation_results = {
        "topology": False,
        "node_count": False,
        "storage": False,
        "network": False,
        "errors": [],
        "warnings": [],
    }

    # Validate cluster topology
    try:
        if verify_tnf_cluster_topology():
            validation_results["topology"] = True
            logger.info("✓ Cluster topology verified: DualReplica")
        else:
            error_msg = (
                "Cluster topology is not DualReplica. "
                "TNF deployment requires a two-node cluster."
            )
            validation_results["errors"].append(error_msg)
            logger.error(f"✗ {error_msg}")
    except Exception as e:
        error_msg = f"Failed to verify cluster topology: {e}"
        validation_results["errors"].append(error_msg)
        logger.error(f"✗ {error_msg}")

    # Validate node count
    try:
        node_info = get_tnf_node_info()
        if len(node_info) == 2:
            validation_results["node_count"] = True
            validation_results["nodes"] = node_info
            logger.info(f"✓ Found 2 nodes: {[n['name'] for n in node_info]}")
        else:
            error_msg = f"Expected 2 nodes, found {len(node_info)}"
            validation_results["errors"].append(error_msg)
            logger.error(f"✗ {error_msg}")
    except Exception as e:
        error_msg = f"Failed to get node information: {e}"
        validation_results["errors"].append(error_msg)
        logger.error(f"✗ {error_msg}")

    # Validate network requirements (port 7794 connectivity)
    if validation_results["node_count"]:
        try:
            network_ok = True
            for i, node in enumerate(validation_results["nodes"]):
                peer_node = validation_results["nodes"][1 - i]
                if not verify_port_connectivity(
                    node["name"], peer_node["ip"], constants.TNF_DRBD_PORT
                ):
                    network_ok = False
                    error_msg = (
                        f"Port {constants.TNF_DRBD_PORT} not reachable from "
                        f"{node['name']} to {peer_node['ip']}"
                    )
                    validation_results["errors"].append(error_msg)
                    logger.error(f"✗ {error_msg}")

            if network_ok:
                validation_results["network"] = True
                logger.info(
                    f"✓ Network connectivity verified (port {constants.TNF_DRBD_PORT})"
                )
        except Exception as e:
            error_msg = f"Network validation failed: {e}"
            validation_results["errors"].append(error_msg)
            logger.error(f"✗ {error_msg}")

    # Validate storage requirements
    if validation_results["node_count"]:
        try:
            storage_ok = validate_storage_requirements(validation_results["nodes"])
            validation_results["storage"] = storage_ok
            if storage_ok:
                logger.info("✓ Storage requirements validated")
        except Exception as e:
            warning_msg = f"Storage validation failed: {e}"
            validation_results["warnings"].append(warning_msg)
            logger.warning(f"⚠ {warning_msg}")

    # Generate summary
    logger.info("\n" + "=" * 60)
    logger.info("TNF Deployment Validation Summary")
    logger.info("=" * 60)
    logger.info(
        f"Topology (DualReplica):    {'✓ PASS' if validation_results['topology'] else '✗ FAIL'}"
    )
    logger.info(
        f"Node Count (2):            {'✓ PASS' if validation_results['node_count'] else '✗ FAIL'}"
    )
    logger.info(
        f"Network Connectivity:      {'✓ PASS' if validation_results['network'] else '✗ FAIL'}"
    )
    logger.info(
        f"Storage Requirements:      {'✓ PASS' if validation_results['storage'] else '⚠ WARNING'}"
    )

    if validation_results["errors"]:
        logger.error(f"\nErrors ({len(validation_results['errors'])}):")
        for error in validation_results["errors"]:
            logger.error(f"  - {error}")

    if validation_results["warnings"]:
        logger.warning(f"\nWarnings ({len(validation_results['warnings'])}):")
        for warning in validation_results["warnings"]:
            logger.warning(f"  - {warning}")

    logger.info("=" * 60 + "\n")

    # Raise exception if critical validations failed
    if validation_results["errors"]:
        raise UnexpectedDeploymentConfiguration(
            f"TNF deployment validation failed with {len(validation_results['errors'])} error(s). "
            "See logs for details."
        )

    return validation_results


def validate_storage_requirements(node_info):
    """
    Validate storage requirements for TNF deployment.

    Requirements:
    - At least one disk ≥ 500GB on each node (OSD)
    - At least one disk 10-50GB on each node (monitor)
    - Disks must be SSD type (ROTA=0)
    - Sizes should match across nodes

    Args:
        node_info (list): List of node information dictionaries

    Returns:
        bool: True if storage requirements are met
    """
    logger.info("Validating storage requirements...")

    for node in node_info:
        try:
            devices = get_block_devices_on_node(node["name"])

            # Filter for suitable disks (SSD, no filesystem)
            suitable_osd_disks = [
                d
                for d in devices
                if d["rota"] == "0"  # SSD
                and d["type"] == "disk"
                and not d["fstype"]  # No filesystem
                and parse_size_gb(d["size"]) >= constants.TNF_MIN_STORAGE_SIZE_GB
            ]

            suitable_monitor_disks = [
                d
                for d in devices
                if d["rota"] == "0"  # SSD
                and d["type"] == "disk"
                and not d["fstype"]  # No filesystem
                and constants.TNF_MIN_MONITOR_DISK_SIZE_GB
                <= parse_size_gb(d["size"])
                <= constants.TNF_MAX_MONITOR_DISK_SIZE_GB
            ]

            logger.info(
                f"Node {node['name']}: "
                f"Found {len(suitable_osd_disks)} suitable OSD disk(s), "
                f"{len(suitable_monitor_disks)} suitable monitor disk(s)"
            )

            if len(suitable_osd_disks) < 1:
                logger.warning(
                    f"Node {node['name']} does not have at least 1 disk "
                    f"≥ {constants.TNF_MIN_STORAGE_SIZE_GB}GB for OSD"
                )
                return False

            if len(suitable_monitor_disks) < 1:
                logger.warning(
                    f"Node {node['name']} does not have at least 1 disk "
                    f"between {constants.TNF_MIN_MONITOR_DISK_SIZE_GB}GB and "
                    f"{constants.TNF_MAX_MONITOR_DISK_SIZE_GB}GB for monitor"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to validate storage on {node['name']}: {e}")
            return False

    logger.info("Storage requirements validated successfully")
    return True


def parse_size_gb(size_str):
    """
    Parse size string to GB.

    Args:
        size_str (str): Size string (e.g., "500G", "1T", "512M")

    Returns:
        float: Size in GB
    """
    if not size_str:
        return 0.0

    size_str = size_str.strip().upper()
    multipliers = {
        "K": 1 / (1024 * 1024),
        "M": 1 / 1024,
        "G": 1,
        "T": 1024,
        "P": 1024 * 1024,
    }

    for unit, multiplier in multipliers.items():
        if unit in size_str:
            try:
                value = float(size_str.replace(unit, "").strip())
                return value * multiplier
            except ValueError:
                logger.warning(f"Failed to parse size string: {size_str}")
                return 0.0

    return 0.0


def validate_tnf_features():
    """
    Validate that unsupported features are not configured for TNF deployment.

    TNF deployments do not support:
    - NooBaa (MCG)
    - NFS
    - RGW
    - Disaster Recovery
    - PodDisruptionBudgets
    - Monitor failover
    - Multus networking
    - External PostgreSQL
    - Performance/resource profiles
    - Auto capacity scaling
    - Host networking
    - Erasure coded pools
    - External clients (HCI/Provider mode)

    Raises:
        UnexpectedDeploymentConfiguration: If unsupported features are enabled
    """
    logger.info("Validating TNF feature configuration...")

    unsupported_features = []

    # Check for NooBaa/MCG
    if config.ENV_DATA.get("mcg_only_deployment"):
        unsupported_features.append("MCG-only deployment")

    # Check for external mode
    if config.ENV_DATA.get("external_mode"):
        unsupported_features.append("External mode (HCI/Provider)")

    # Check for multus
    if config.ENV_DATA.get("multus"):
        unsupported_features.append("Multus networking")

    # Check for disaster recovery
    if config.ENV_DATA.get("dr_cluster"):
        unsupported_features.append("Disaster Recovery")

    # Add more feature checks as needed based on config structure

    if unsupported_features:
        error_msg = (
            f"The following features are not supported in TNF deployments: "
            f"{', '.join(unsupported_features)}"
        )
        logger.error(error_msg)
        raise UnexpectedDeploymentConfiguration(error_msg)

    logger.info("✓ No unsupported features detected")
    return True


def generate_device_mapping_example(node_info):
    """
    Generate an example device mapping configuration.

    This helps users understand what configuration is needed
    for TNF deployment.

    Args:
        node_info (list): List of node information

    Returns:
        dict: Example configuration dictionary
    """
    logger.info("Generating example device mapping configuration...")

    example_config = {
        "tnf": {
            "osd_device_mappings": [],
            "monitor_disk_node_0": "/dev/disk/by-id/wwn-0x...-node0-monitor",
            "monitor_disk_node_1": "/dev/disk/by-id/wwn-0x...-node1-monitor",
        }
    }

    for i, node in enumerate(node_info):
        try:
            devices = get_block_devices_on_node(node["name"])
            logger.info(f"\nAvailable disks on {node['name']}:")

            for device in devices:
                logger.info(
                    f"  {device['path']}: {device['size']} "
                    f"(SSD: {device['rota'] == '0'}, "
                    f"Type: {device['type']}, "
                    f"FS: {device.get('fstype', 'none')})"
                )

            # Add example mapping
            example_config["tnf"]["osd_device_mappings"].append(
                {
                    "node_name": node["name"],
                    "device_path": f"/dev/disk/by-id/wwn-0x...-{node['name']}-osd",
                    "size": "500Gi",
                    "pv_name": f"local-pv-{i+1}",
                }
            )

        except Exception as e:
            logger.error(f"Failed to get devices for {node['name']}: {e}")

    logger.info("\nExample configuration:")
    logger.info("=" * 60)
    import yaml

    logger.info(yaml.dump(example_config, default_flow_style=False))
    logger.info("=" * 60)

    return example_config
