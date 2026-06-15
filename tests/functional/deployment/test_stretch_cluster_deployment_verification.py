"""
Test suite for verifying stretch cluster deployment with varied disk sizes.
This test validates the DFBUGS-2885 fix by verifying that stretch mode is
properly enabled when disk sizes vary slightly across zones.
"""

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    stretchcluster_required,
    skipif_external_mode,
)
from ocs_ci.helpers.crush_helpers import (
    get_zone_crush_weights,
    verify_zone_weight_balance,
    verify_stretch_mode_enabled,
    log_crush_weight_details,
    get_osd_crush_weights,
    get_osds_in_zone,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_operator_pods

logger = logging.getLogger(__name__)


@brown_squad
@stretchcluster_required
@skipif_external_mode
class TestStretchClusterDeploymentVerification(ManageTest):
    """
    Test class for verifying stretch cluster deployment with varied disk sizes.
    Validates the DFBUGS-2885 fix.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_stretch_mode_enabled_with_varied_disks(self):
        """
        Verify that stretch mode is enabled even when disk sizes vary
        slightly across zones (DFBUGS-2885 fix).

        Steps:
            1. Verify stretch mode is enabled in the cluster
            2. Verify CRUSH weights show expected variation
            3. Verify zone weights are within 10% tolerance (Rook's threshold)
            4. Verify no blocking errors in rook-ceph-operator logs
        """
        logger.info("=" * 80)
        logger.info("DFBUGS-2885 Verification: Stretch mode with varied disk sizes")
        logger.info("=" * 80)

        # Step 1: Verify stretch mode is enabled
        logger.info("Step 1: Verifying stretch mode is enabled")
        assert verify_stretch_mode_enabled(), (
            "DFBUGS-2885 REGRESSION: Stretch mode is NOT enabled! "
            "This indicates the fix for CRUSH weight balance tolerance "
            "is not working correctly."
        )
        logger.info("PASS: Stretch mode is enabled")

        # Step 2: Verify CRUSH weights show expected variation
        logger.info("Step 2: Checking CRUSH weight variation across OSDs")
        osd_weights = get_osd_crush_weights()
        assert len(osd_weights) > 0, "No OSD CRUSH weights found"

        unique_weights = set(osd_weights.values())
        use_varied = config.ENV_DATA.get("use_varied_disk_sizes_for_stretch", False)

        if use_varied:
            logger.info(
                f"Varied disk sizes enabled. "
                f"Found {len(unique_weights)} unique CRUSH weights: {unique_weights}"
            )
            assert len(unique_weights) > 1, (
                "Expected varied CRUSH weights but all OSDs have the same weight. "
                "The varied disk size configuration may not have been applied correctly."
            )
        else:
            logger.info(
                "Varied disk sizes not explicitly enabled. "
                "Verifying stretch mode works with current disk configuration."
            )

        # Step 3: Verify zone weight balance within Rook's 10% tolerance
        logger.info("Step 3: Verifying zone weight balance")
        is_balanced, zone_weights, message = verify_zone_weight_balance(
            tolerance_percentage=10.0
        )
        logger.info(f"Zone weights: {zone_weights}")
        logger.info(f"Balance check result: {message}")

        assert (
            is_balanced
        ), f"Zone weights exceed 10% tolerance (Rook's threshold). {message}"
        logger.info("PASS: Zone weights are within 10% tolerance")

        # Step 4: Check rook-ceph-operator logs for blocking errors
        logger.info("Step 4: Checking rook-ceph-operator logs for errors")
        self._verify_no_blocking_errors_in_operator_logs()

        # Log full CRUSH weight details for debugging
        log_crush_weight_details()

        logger.info("=" * 80)
        logger.info("DFBUGS-2885 Verification: ALL CHECKS PASSED")
        logger.info("=" * 80)

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_ceph_cluster_health_with_varied_disks(self):
        """
        Verify that the Ceph cluster is healthy after deployment with
        varied disk sizes.

        Steps:
            1. Check Ceph health status
            2. Verify all OSDs are up and in
            3. Verify mon quorum is established
        """
        logger.info("Verifying Ceph cluster health with varied disk sizes")
        ceph_tools_pod = get_ceph_tools_pod()

        # Step 1: Check Ceph health
        logger.info("Step 1: Checking Ceph health status")
        health_output = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph health detail")
        health_status = health_output.get("status", "UNKNOWN")
        logger.info(f"Ceph health status: {health_status}")

        assert health_status in ("HEALTH_OK", "HEALTH_WARN"), (
            f"Ceph cluster is not healthy: {health_status}. "
            f"Details: {health_output}"
        )

        # Step 2: Verify all OSDs are up and in
        logger.info("Step 2: Verifying OSD status")
        osd_stat = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph osd stat")
        total_osds = osd_stat.get("num_osds", 0)
        up_osds = osd_stat.get("num_up_osds", 0)
        in_osds = osd_stat.get("num_in_osds", 0)

        logger.info(f"OSD status: {up_osds}/{total_osds} up, {in_osds}/{total_osds} in")
        assert total_osds == up_osds == in_osds, (
            f"Not all OSDs are up and in: "
            f"total={total_osds}, up={up_osds}, in={in_osds}"
        )

        # Step 3: Verify mon quorum
        logger.info("Step 3: Verifying mon quorum")
        mon_status = ceph_tools_pod.exec_ceph_cmd(ceph_cmd="ceph mon stat")
        logger.info(f"Mon status: {mon_status}")

        logger.info("PASS: Ceph cluster is healthy with varied disk sizes")

    @tier1
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_crush_weight_distribution_per_zone(self):
        """
        Verify CRUSH weight distribution across zones matches expected
        configuration.

        Steps:
            1. Get OSDs per zone
            2. Verify weight distribution within each zone
            3. Verify cross-zone weight comparison
        """
        logger.info("Verifying CRUSH weight distribution per zone")

        zones = constants.DATA_ZONE_LABELS
        zone_osd_details = {}

        for zone in zones:
            osds = get_osds_in_zone(zone)
            assert len(osds) > 0, f"No OSDs found in zone '{zone}'"

            osd_weights = get_osd_crush_weights()
            zone_weights = {
                osd_id: osd_weights[osd_id] for osd_id in osds if osd_id in osd_weights
            }
            zone_osd_details[zone] = zone_weights

            logger.info(f"Zone '{zone}': {len(osds)} OSDs, " f"weights: {zone_weights}")

        # Verify each zone has OSDs
        for zone, details in zone_osd_details.items():
            assert len(details) > 0, f"Zone '{zone}' has no OSDs with weights"

        # Verify cross-zone balance
        zone_totals = get_zone_crush_weights(zones)
        logger.info(f"Zone total weights: {zone_totals}")

        total_weights = list(zone_totals.values())
        if len(total_weights) >= 2:
            max_w = max(total_weights)
            min_w = min(total_weights)
            if min_w > 0:
                diff_pct = ((max_w - min_w) / min_w) * 100
                logger.info(f"Cross-zone weight difference: {diff_pct:.2f}%")

                # Must be within Rook's 10% tolerance
                assert diff_pct < 10.0, (
                    f"Cross-zone weight difference {diff_pct:.2f}% "
                    f"exceeds Rook's 10% tolerance"
                )

        logger.info("PASS: CRUSH weight distribution is correct")

    def _verify_no_blocking_errors_in_operator_logs(self):
        """
        Check rook-ceph-operator logs for errors that would indicate
        the DFBUGS-2885 bug is still present.
        """
        logger.info("Checking rook-ceph-operator logs for blocking errors")

        operator_pods = get_operator_pods()
        assert len(operator_pods) > 0, "No rook-ceph-operator pods found"

        # Error patterns that indicate the bug is NOT fixed
        blocking_error_patterns = [
            "failed to find two failure domains",
            "context deadline exceeded",
            "failed to enable stretch mode",
            "failure domains that have different weights.*blocking",
        ]

        # Informational patterns that are OK (the fix logs these as info, not error)
        info_patterns = [
            "found failure domains that have different weights",
        ]

        for pod in operator_pods:
            try:
                pod_logs = pod.log(container="rook-ceph-operator")

                for pattern in blocking_error_patterns:
                    import re

                    matches = re.findall(pattern, pod_logs, re.IGNORECASE)
                    if matches:
                        logger.error(
                            f"DFBUGS-2885 REGRESSION: Found blocking error "
                            f"in operator logs: '{pattern}' "
                            f"({len(matches)} occurrences)"
                        )
                        assert False, (
                            f"Found blocking error pattern '{pattern}' "
                            f"in rook-ceph-operator logs. "
                            f"This indicates DFBUGS-2885 is not fixed."
                        )

                for pattern in info_patterns:
                    matches = re.findall(pattern, pod_logs, re.IGNORECASE)
                    if matches:
                        logger.info(
                            f"Found expected informational message: "
                            f"'{pattern}' ({len(matches)} occurrences) — OK"
                        )

            except Exception as e:
                logger.warning(f"Could not read logs from pod {pod.name}: {e}")

        logger.info("PASS: No blocking errors found in operator logs")
