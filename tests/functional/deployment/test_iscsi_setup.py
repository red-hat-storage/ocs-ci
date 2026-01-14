"""
Test cases for verifying iSCSI setup after OCP deployment.
"""

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.framework.testlib import deployment, polarion_id
from ocs_ci.utility.iscsi_config import verify_iscsi_setup
from ocs_ci.utility.reporting import get_polarion_id

log = logging.getLogger(__name__)


@purple_squad
@deployment
@polarion_id(get_polarion_id())
def test_iscsi_setup_verification():
    """
    Verify that iSCSI setup was successful after OCP deployment.

    This test verifies:
    1. Network connectivity to iSCSI target from all worker nodes
    2. iSCSI sessions are established on all worker nodes
    3. iSCSI devices are visible on worker nodes

    Test Steps:
    1. Check if iSCSI is configured in ENV_DATA
    2. Verify connectivity to iSCSI target
    3. Verify iSCSI sessions are established
    4. Verify iSCSI devices are visible

    Raises:
        AssertionError: If any verification step fails
    """
    # Skip test if iSCSI is not configured
    if not config.ENV_DATA.get("iscsi_target_ip") or not config.ENV_DATA.get(
        "iscsi_target_iqn"
    ):
        pytest.skip("iSCSI not configured in ENV_DATA, skipping test")

    log.info("Starting iSCSI setup verification test")
    log.info(f"iSCSI Target IP: {config.ENV_DATA.get('iscsi_target_ip')}")
    log.info(f"iSCSI Target IQN: {config.ENV_DATA.get('iscsi_target_iqn')}")

    # Run comprehensive verification
    verification_results = verify_iscsi_setup()

    # Check if verification was skipped
    if verification_results.get("skipped"):
        pytest.skip(f"iSCSI verification skipped: {verification_results.get('reason')}")

    # Check for errors
    if "error" in verification_results:
        pytest.fail(
            f"iSCSI verification failed with error: {verification_results['error']}"
        )

    # Get summary
    summary = verification_results.get("summary", {})
    overall_status = summary.get("overall_status", False)

    # Detailed assertions
    assert summary.get("all_nodes_connected", False), (
        "Not all worker nodes can connect to iSCSI target. "
        f"Connectivity results: {verification_results.get('connectivity', {})}"
    )

    assert summary.get("all_sessions_established", False), (
        "Not all worker nodes have established iSCSI sessions. "
        f"Session results: {verification_results.get('sessions', {})}"
    )

    assert summary.get("devices_found", False), (
        "No iSCSI devices found on worker nodes. "
        f"Device results: {verification_results.get('devices', {})}"
    )

    # Overall status check
    assert overall_status, (
        f"iSCSI setup verification failed. Summary: {summary}. "
        f"Full results: {verification_results}"
    )

    log.info("iSCSI setup verification completed successfully")
    log.info(f"Worker nodes verified: {verification_results.get('worker_nodes', [])}")
    log.info(f"Devices found per node: {verification_results.get('devices', {})}")


@purple_squad
@deployment
@polarion_id(get_polarion_id())
def test_iscsi_connectivity():
    """
    Verify network connectivity to iSCSI target from all worker nodes.

    This test specifically checks network connectivity without requiring
    full iSCSI session establishment.

    Raises:
        AssertionError: If connectivity check fails
    """
    # Skip test if iSCSI is not configured
    if not config.ENV_DATA.get("iscsi_target_ip"):
        pytest.skip("iSCSI target IP not configured, skipping test")

    from ocs_ci.utility.iscsi_config import (
        verify_iscsi_target_connectivity,
        get_worker_node_names,
    )

    log.info("Starting iSCSI connectivity verification test")
    target_ip = config.ENV_DATA.get("iscsi_target_ip")

    worker_node_names = get_worker_node_names()

    if not worker_node_names:
        pytest.skip("No worker nodes found, skipping test")

    log.info(
        f"Verifying connectivity from {len(worker_node_names)} worker node(s) to {target_ip}"
    )

    connectivity_results = verify_iscsi_target_connectivity(
        worker_node_names, target_ip
    )

    # Verify all nodes can connect
    failed_nodes = [
        node_name
        for node_name, result in connectivity_results.items()
        if not result.get("connected", False)
    ]

    assert not failed_nodes, (
        f"Connectivity check failed for worker nodes: {failed_nodes}. "
        f"Full results: {connectivity_results}"
    )

    log.info(
        f"Connectivity verification passed for all {len(worker_node_names)} worker node(s)"
    )


@purple_squad
@deployment
@polarion_id(get_polarion_id())
def test_iscsi_sessions():
    """
    Verify iSCSI sessions are established on all worker nodes.

    This test checks that iSCSI initiator sessions are active
    and connected to the configured target.

    Raises:
        AssertionError: If session verification fails
    """
    # Skip test if iSCSI is not configured
    if not config.ENV_DATA.get("iscsi_target_ip") or not config.ENV_DATA.get(
        "iscsi_target_iqn"
    ):
        pytest.skip("iSCSI not configured, skipping test")

    from ocs_ci.utility.iscsi_config import verify_iscsi_sessions, get_worker_node_names

    log.info("Starting iSCSI session verification test")
    target_iqn = config.ENV_DATA.get("iscsi_target_iqn")

    worker_node_names = get_worker_node_names()

    if not worker_node_names:
        pytest.skip("No worker nodes found, skipping test")

    log.info(f"Verifying iSCSI sessions on {len(worker_node_names)} worker node(s)")

    session_results = verify_iscsi_sessions(worker_node_names, target_iqn)

    # Verify all nodes have sessions
    failed_nodes = [
        node_name
        for node_name, result in session_results.items()
        if not result.get("session", False)
    ]

    assert not failed_nodes, (
        f"iSCSI session check failed for worker nodes: {failed_nodes}. "
        f"Full results: {session_results}"
    )

    log.info(
        f"Session verification passed for all {len(worker_node_names)} worker node(s)"
    )


@purple_squad
@deployment
@polarion_id(get_polarion_id())
def test_iscsi_devices():
    """
    Verify iSCSI devices are visible on all worker nodes.

    This test checks that iSCSI LUNs are accessible as block devices
    on the worker nodes.

    Raises:
        AssertionError: If device verification fails
    """
    # Skip test if iSCSI is not configured
    if not config.ENV_DATA.get("iscsi_target_ip") or not config.ENV_DATA.get(
        "iscsi_target_iqn"
    ):
        pytest.skip("iSCSI not configured, skipping test")

    from ocs_ci.utility.iscsi_config import verify_iscsi_devices, get_worker_node_names

    log.info("Starting iSCSI device verification test")
    target_iqn = config.ENV_DATA.get("iscsi_target_iqn")

    worker_node_names = get_worker_node_names()

    if not worker_node_names:
        pytest.skip("No worker nodes found, skipping test")

    log.info(f"Verifying iSCSI devices on {len(worker_node_names)} worker node(s)")

    device_results = verify_iscsi_devices(worker_node_names, target_iqn)

    # Verify at least some devices are found
    total_devices = sum(result.get("count", 0) for result in device_results.values())

    assert total_devices > 0, (
        f"No iSCSI devices found on any worker node. " f"Full results: {device_results}"
    )

    # Log device information
    for node_name, result in device_results.items():
        device_count = result.get("count", 0)
        log.info(f"Node {node_name}: {device_count} iSCSI device(s) found")

    log.info(f"Device verification passed. Total devices found: {total_devices}")
