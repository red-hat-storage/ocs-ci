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
from ocs_ci.framework import config
from ocs_ci.ocs.resources.probe import Probe
from ocs_ci.utility.networking import get_pod_ips, get_pod_multus_ips

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

        For multus network deployments, the probe uses multus network IPs instead of
        primary pod IPs. This test detects multus configuration and validates accordingly.

        Test Steps:
        1. Get the odf-blackbox-exporter probe configuration
        2. Check if multus networking is enabled via config
        3. Get IPs for OSD pods (primary or multus based on configuration)
        4. Get IPs for MON pods (primary or multus based on configuration)
        5. Extract IPs from the blackbox probe configuration
        6. Verify that all OSD and MON pod IPs are present in the probe configuration
        """
        logger.info("Starting test: Verify blackbox probe contains OSD and MON IPs")
        probe = Probe()
        probe_config = probe.get_probe_config("odf-blackbox-exporter")
        assert probe_config, "Failed to get probe configuration"
        is_multus = config.ENV_DATA.get("is_multus_enabled", False)
        logger.info(f"Multus networking enabled: {is_multus}")
        expected_ips = []
        if is_multus:
            logger.info("Getting OSD pod multus network IPs...")
            osd_multus_ips = get_pod_multus_ips(
                constants.OSD_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            assert osd_multus_ips, "No OSD pods found or no multus IPs available"

            for pod_name, ip_list in osd_multus_ips.items():
                logger.info(f"OSD pod {pod_name}: {ip_list}")
                expected_ips.extend(ip_list)
            logger.info(
                f"Found {len(expected_ips)} OSD multus IPs from {len(osd_multus_ips)} pods"
            )

            logger.info("Getting MON pod multus network IPs...")
            mon_multus_ips = get_pod_multus_ips(
                constants.MON_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            assert mon_multus_ips, "No MON pods found or no multus IPs available"

            mon_ip_count = 0
            for pod_name, ip_list in mon_multus_ips.items():
                logger.info(f"MON pod {pod_name}: {ip_list}")
                expected_ips.extend(ip_list)
                mon_ip_count += len(ip_list)
            logger.info(
                f"Found {mon_ip_count} MON multus IPs from {len(mon_multus_ips)} pods"
            )
        else:
            logger.info("Getting OSD pod IPs...")
            osd_ips = get_pod_ips(
                constants.OSD_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            assert osd_ips, "No OSD pods found or no IPs available"
            logger.info(f"Found {len(osd_ips)} OSD pods with IPs")
            for pod_name, pod_ip in osd_ips.items():
                logger.info(f"OSD pod {pod_name}: {pod_ip}")
                expected_ips.append(pod_ip)

            logger.info("Getting MON pod IPs...")
            mon_ips = get_pod_ips(
                constants.MON_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            assert mon_ips, "No MON pods found or no IPs available"
            logger.info(f"Found {len(mon_ips)} MON pods with IPs")
            for pod_name, pod_ip in mon_ips.items():
                logger.info(f"MON pod {pod_name}: {pod_ip}")
                expected_ips.append(pod_ip)

        logger.info("Extracting IPs from probe configuration...")
        probe_ips = probe.get_static_targets(probe_config)
        assert probe_ips, "No IPs found in probe configuration"
        logger.info(f"Found {len(probe_ips)} IPs in probe configuration")
        logger.info("Verifying all expected IPs are present in probe configuration...")
        missing_ips = []
        for ip in expected_ips:
            if ip not in probe_ips:
                missing_ips.append(ip)
                logger.error(f"Expected IP not found in probe: {ip}")

        assert (
            not missing_ips
        ), f"The following IPs are missing from probe configuration: {missing_ips}"
        logger.info(
            f"Test passed: All {len(expected_ips)} expected OSD and MON pod IPs "
            f"are present in odf-blackbox-exporter probe configuration"
        )
