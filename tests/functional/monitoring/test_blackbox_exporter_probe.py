import logging
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    blue_squad,
    polarion_id,
    skipif_external_mode,
    skipif_managed_service,
    skipif_ibm_cloud_managed,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest, skipif_mcg_only
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.probe import Probe
from ocs_ci.utility.networking import get_pod_ips

logger = logging.getLogger(__name__)


@blue_squad
@skipif_mcg_only
@skipif_external_mode
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.22")
class TestBlackboxExporterProbe(ManageTest):

    @tier2
    @polarion_id("OCS-7941")
    def test_blackbox_probe_osd_mon_ips(self):
        """
        Test to verify odf-blackbox-exporter probe contains correct OSD and MON pod IPs.

        Test Steps:
        1. Get the odf-blackbox-exporter probe configuration
        2. Get IPs for OSD pods
        3. Get IPs for MON pods
        4. Extract IPs from the blackbox probe configuration
        5. Verify that all OSD and MON pod IPs are present in the probe configuration
        """
        logger.info("Starting test: Verify blackbox probe contains OSD and MON IPs")

        probe = Probe()
        probe_config = probe.get_probe_config("odf-blackbox-exporter")
        assert probe_config, "Failed to get probe configuration"

        logger.info("Getting OSD pod IPs...")
        osd_ips = get_pod_ips(
            constants.OSD_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert osd_ips, "No OSD pods found or no IPs available"
        logger.info(f"Found {len(osd_ips)} OSD pods with IPs")

        logger.info("Getting MON pod IPs...")
        mon_ips = get_pod_ips(
            constants.MON_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert mon_ips, "No MON pods found or no IPs available"
        logger.info(f"Found {len(mon_ips)} MON pods with IPs")

        logger.info("Extracting IPs from probe configuration...")
        probe_ips = probe.get_static_targets(probe_config)
        assert probe_ips, "No IPs found in probe configuration"
        logger.info(f"Found {len(probe_ips)} IPs in probe configuration")

        logger.info("Verifying OSD IPs are present in probe configuration...")
        missing_osd_ips = []
        for pod_name, pod_ip in osd_ips.items():
            if pod_ip not in probe_ips:
                missing_osd_ips.append(f"{pod_name}: {pod_ip}")
                logger.error(f"OSD pod IP not found in probe: {pod_name} ({pod_ip})")

        logger.info("Verifying MON IPs are present in probe configuration...")
        missing_mon_ips = []
        for pod_name, pod_ip in mon_ips.items():
            if pod_ip not in probe_ips:
                missing_mon_ips.append(f"{pod_name}: {pod_ip}")
                logger.error(f"MON pod IP not found in probe: {pod_name} ({pod_ip})")

        assert not missing_osd_ips, (
            f"The following OSD pod IPs are missing from probe configuration: "
            f"{missing_osd_ips}"
        )
        assert not missing_mon_ips, (
            f"The following MON pod IPs are missing from probe configuration: "
            f"{missing_mon_ips}"
        )
        logger.info(
            "Test passed: All OSD and MON pod IPs are present in "
            "odf-blackbox-exporter probe configuration"
        )
