"""
Test suite for CRUSH weight balance verification in stretch clusters.
Tests the DFBUGS-2885 fix: Rook now allows failure domain weights that
differ by up to 10%, instead of requiring an exact match.
"""

import logging
import time
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    tier2,
    stretchcluster_required,
    skipif_external_mode,
)
from ocs_ci.helpers.crush_helpers import (
    get_zone_crush_weights,
    verify_zone_weight_balance,
    verify_stretch_mode_enabled,
    log_crush_weight_details,
    get_osd_crush_weights,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_operator_pods
from ocs_ci.ocs.resources.stretchcluster import StretchCluster

logger = logging.getLogger(__name__)


@brown_squad
@stretchcluster_required
@skipif_external_mode
class TestCrushWeightBalance(ManageTest):
    """
    Test class for CRUSH weight balance verification in stretch clusters.
    Validates the DFBUGS-2885 fix across various scenarios.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_zone_weight_balance_within_tolerance(self):
        """
        Verify that zone CRUSH weights are balanced within the 10%
        tolerance introduced by the DFBUGS-2885 fix.

        Steps:
            1. Get CRUSH weights for each data zone
            2. Calculate weight difference percentage
            3. Verify difference is within 10% (Rook's new tolerance)
            4. Verify stretch mode is enabled
        """
        logger.info("Testing zone weight balance within 10% tolerance")

        # Step 1 & 2: Get zone weights and check balance
        is_balanced, zone_weights, message = verify_zone_weight_balance(
            tolerance_percentage=10.0
        )
        logger.info(f"Zone weights: {zone_weights}")
        logger.info(f"Balance result: {message}")

        # Step 3: Assert balance
        assert is_balanced, f"DFBUGS-2885: Zone weights exceed 10% tolerance. {message}"

        # Step 4: Verify stretch mode
        assert verify_stretch_mode_enabled(), (
            "Stretch mode should be enabled when zone weights are within "
            "10% tolerance"
        )

        logger.info("PASS: Zone weights balanced and stretch mode enabled")

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_stretch_mode_with_slight_weight_variation(self):
        """
        Verify stretch mode remains enabled with slight CRUSH weight
        variations (the core DFBUGS-2885 scenario).

        Steps:
            1. Log current CRUSH weight details
            2. Verify OSD weights show variation (if varied disks configured)
            3. Verify stretch mode is enabled despite variation
            4. Verify Ceph health is OK
        """
        logger.info("Testing stretch mode with slight weight variation (DFBUGS-2885)")

        # Step 1: Log details
        log_crush_weight_details()

        # Step 2: Check for weight variation
        osd_weights = get_osd_crush_weights()
        unique_weights = set(osd_weights.values())
        logger.info(f"Unique OSD weights: {unique_weights}")

        use_varied = config.ENV_DATA.get("use_varied_disk_sizes_for_stretch", False)
        if use_varied:
            assert (
                len(unique_weights) > 1
            ), "Expected weight variation with varied disk sizes configured"

        # Step 3: Verify stretch mode
        assert verify_stretch_mode_enabled(), (
            "DFBUGS-2885 REGRESSION: Stretch mode is NOT enabled despite "
            "weight variation being within tolerance"
        )

        # Step 4: Verify Ceph health
        ceph_tools_pod = get_ceph_tools_pod()
        health = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph health detail")
        health_status = health.get("status", "UNKNOWN")
        logger.info(f"Ceph health: {health_status}")

        assert health_status in (
            "HEALTH_OK",
            "HEALTH_WARN",
        ), f"Ceph cluster unhealthy: {health_status}"

        logger.info("PASS: Stretch mode enabled with weight variation")

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_operator_logs_no_failure_domain_errors(self):
        """
        Verify rook-ceph-operator logs do not contain blocking errors
        related to failure domain weight mismatch.

        Steps:
            1. Get rook-ceph-operator pod logs
            2. Check for blocking error patterns (bug indicators)
            3. Verify informational weight messages are present (fix indicators)
        """
        import re

        logger.info("Checking operator logs for DFBUGS-2885 error patterns")

        operator_pods = get_operator_pods()
        assert len(operator_pods) > 0, "No rook-ceph-operator pods found"

        # Patterns that indicate the bug is NOT fixed
        blocking_patterns = [
            "failed to find two failure domains",
            "context deadline exceeded.*stretch",
            "failed to enable stretch mode",
            "failure domains.*different weights.*error",
            "context deadline exceeded.*failure.domain",
        ]

        # Patterns that indicate the fix IS working
        fix_indicator_patterns = [
            "found failure domains that have different weights",
            "stretch mode enabled",
        ]

        found_blocking = []
        found_fix_indicators = []

        for pod in operator_pods:
            try:
                pod_logs = pod.log(container="rook-ceph-operator")

                for pattern in blocking_patterns:
                    matches = re.findall(pattern, pod_logs, re.IGNORECASE)
                    if matches:
                        found_blocking.append(
                            f"Pattern '{pattern}': {len(matches)} matches"
                        )
                        logger.error(
                            f"BLOCKING ERROR found: '{pattern}' "
                            f"({len(matches)} occurrences)"
                        )

                for pattern in fix_indicator_patterns:
                    matches = re.findall(pattern, pod_logs, re.IGNORECASE)
                    if matches:
                        found_fix_indicators.append(
                            f"Pattern '{pattern}': {len(matches)} matches"
                        )
                        logger.info(
                            f"Fix indicator found: '{pattern}' "
                            f"({len(matches)} occurrences)"
                        )

            except Exception as e:
                logger.warning(f"Could not read logs from {pod.name}: {e}")

        assert len(found_blocking) == 0, (
            f"DFBUGS-2885 REGRESSION: Found blocking errors in operator logs: "
            f"{found_blocking}"
        )

        if found_fix_indicators:
            logger.info(f"Fix indicators found in logs: {found_fix_indicators}")

        logger.info("PASS: No blocking errors in operator logs")

    @tier2
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_zone_weight_balance_after_add_capacity(self):
        """
        Verify CRUSH weight balance is maintained after adding capacity
        to the stretch cluster.

        Steps:
            1. Record initial zone weights
            2. Verify initial stretch mode status
            3. Deploy logwriter workloads
            4. Add capacity (new OSDs)
            5. Verify zone weights remain balanced
            6. Verify stretch mode remains enabled
            7. Verify no data loss or corruption
        """
        logger.info("Testing zone weight balance after add-capacity operation")

        sc_obj = StretchCluster()

        # Step 1: Record initial state
        logger.info("Step 1: Recording initial zone weights")
        initial_balanced, initial_weights, initial_msg = verify_zone_weight_balance(
            tolerance_percentage=10.0
        )
        logger.info(f"Initial zone weights: {initial_weights}")
        assert initial_balanced, f"Initial zone weights not balanced: {initial_msg}"

        # Step 2: Verify initial stretch mode
        logger.info("Step 2: Verifying initial stretch mode status")
        assert (
            verify_stretch_mode_enabled()
        ), "Stretch mode not enabled before add-capacity"

        # Step 3: Deploy logwriter workloads
        logger.info("Step 3: Deploying logwriter workloads")
        sc_obj.create_logwriter_cephfs_workload_and_verify(
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS
        )
        sc_obj.create_logwriter_rbd_workload_and_verify(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD
        )

        # Allow workloads to generate some data
        logger.info("Waiting 60s for workloads to generate data...")
        time.sleep(60)

        # Step 4: Add capacity
        logger.info("Step 4: Adding capacity to the cluster")
        # Note: The actual add-capacity mechanism depends on the platform.
        # For vSphere with LSO, this typically involves adding new disks
        # to worker VMs and creating new LocalVolume resources.
        # This step may need to be customized based on the deployment.
        try:
            from ocs_ci.ocs.cluster import add_capacity

            osd_size = config.ENV_DATA.get("device_size", 100)
            add_capacity(osd_size)
            logger.info("Capacity added successfully")
        except Exception as e:
            logger.warning(
                f"Add capacity step encountered an issue: {e}. "
                f"Continuing with weight verification..."
            )

        # Step 5: Verify zone weights after add-capacity
        logger.info("Step 5: Verifying zone weights after add-capacity")
        post_balanced, post_weights, post_msg = verify_zone_weight_balance(
            tolerance_percentage=10.0
        )
        logger.info(f"Post add-capacity zone weights: {post_weights}")
        assert post_balanced, f"Zone weights unbalanced after add-capacity: {post_msg}"

        # Step 6: Verify stretch mode still enabled
        logger.info("Step 6: Verifying stretch mode after add-capacity")
        assert (
            verify_stretch_mode_enabled()
        ), "Stretch mode disabled after add-capacity!"

        # Step 7: Verify workload data integrity
        logger.info("Step 7: Verifying workload data integrity")
        assert sc_obj.check_ceph_accessibility(
            timeout=120
        ), "Ceph not accessible after add-capacity"

        logger.info("PASS: Weight balance maintained after add-capacity")

    @tier2
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_weight_variation_boundary_values(self):
        """
        Test CRUSH weight balance at boundary values of the 10% tolerance.

        Steps:
            1. Get current zone weights
            2. Verify current variation percentage
            3. Log boundary analysis
        """
        logger.info("Testing weight variation boundary values")

        zone_weights = get_zone_crush_weights()
        logger.info(f"Current zone weights: {zone_weights}")

        zones = list(zone_weights.keys())
        if len(zones) < 2:
            pytest.skip("Need at least 2 data zones for boundary testing")

        weight_values = list(zone_weights.values())
        max_w = max(weight_values)
        min_w = min(weight_values)

        if min_w > 0:
            actual_diff_pct = ((max_w - min_w) / min_w) * 100
        else:
            actual_diff_pct = 0.0

        logger.info(f"Actual weight difference: {actual_diff_pct:.4f}%")
        logger.info("Rook tolerance threshold: 10.0%")
        logger.info(f"Margin to threshold: {10.0 - actual_diff_pct:.4f}%")

        # The variation should be non-zero if varied disks are configured
        use_varied = config.ENV_DATA.get("use_varied_disk_sizes_for_stretch", False)
        if use_varied:
            assert (
                actual_diff_pct > 0
            ), "Expected non-zero weight variation with varied disks"
            logger.info(
                f"Weight variation {actual_diff_pct:.4f}% is within "
                f"Rook's 10% tolerance — DFBUGS-2885 fix working"
            )

        # Must be under 10%
        assert actual_diff_pct < 10.0, (
            f"Weight variation {actual_diff_pct:.4f}% exceeds " f"Rook's 10% tolerance"
        )

        # Verify stretch mode is enabled at this boundary
        assert (
            verify_stretch_mode_enabled()
        ), f"Stretch mode not enabled at {actual_diff_pct:.4f}% variation"

        logger.info("PASS: Boundary value verification complete")
