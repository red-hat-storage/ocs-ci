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
        logger.test_step("Get the odf-blackbox-exporter probe configuration")
        probe = Probe()
        probe_config = probe.get_probe_config("odf-blackbox-exporter")
        logger.assertion(f"Probe configuration retrieved: {bool(probe_config)}")
        assert probe_config, "Failed to get probe configuration"

        is_multus = config.ENV_DATA.get("is_multus_enabled", False)
        logger.info(f"Multus networking enabled: {is_multus}")

        expected_ips = []
        if is_multus:
            logger.test_step(
                "Collect OSD and MON pod IPs from the multus secondary network"
            )
            osd_multus_ips = get_pod_multus_ips(
                constants.OSD_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            logger.assertion(f"OSD pods with multus IPs: {len(osd_multus_ips)}")
            assert osd_multus_ips, "No OSD pods found or no multus IPs available"
            for ip_list in osd_multus_ips.values():
                expected_ips.extend(ip_list)

            mon_multus_ips = get_pod_multus_ips(
                constants.MON_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            logger.assertion(f"MON pods with multus IPs: {len(mon_multus_ips)}")
            assert mon_multus_ips, "No MON pods found or no multus IPs available"
            for ip_list in mon_multus_ips.values():
                expected_ips.extend(ip_list)
        else:
            logger.test_step("Collect OSD and MON pod IPs from the primary network")
            osd_ips = get_pod_ips(
                constants.OSD_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            logger.assertion(f"OSD pods with IPs: {len(osd_ips)}")
            assert osd_ips, "No OSD pods found or no IPs available"
            expected_ips.extend(osd_ips.values())

            mon_ips = get_pod_ips(
                constants.MON_APP_LABEL, constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            logger.assertion(f"MON pods with IPs: {len(mon_ips)}")
            assert mon_ips, "No MON pods found or no IPs available"
            expected_ips.extend(mon_ips.values())

        logger.info(f"Collected {len(expected_ips)} expected OSD and MON pod IPs")

        logger.test_step("Extract the static target IPs from the probe configuration")
        probe_ips = probe.get_static_targets(probe_config)
        logger.assertion(f"Static target IPs in probe: {len(probe_ips)}")
        assert probe_ips, "No IPs found in probe configuration"

        logger.test_step("Compare the probe target IPs against the OSD and MON pod IPs")
        missing_ips = [ip for ip in expected_ips if ip not in probe_ips]
        extra_ips = [ip for ip in probe_ips if ip not in expected_ips]

        if missing_ips:
            logger.error(f"OSD/MON IPs missing from probe configuration: {missing_ips}")
        if extra_ips:
            logger.warning(f"Unexpected IPs found in probe configuration: {extra_ips}")

        logger.assertion(
            f"Probe target IPs: expected={len(expected_ips)}, "
            f"actual={len(probe_ips)}, missing={len(missing_ips)}, "
            f"unexpected={len(extra_ips)}"
        )
        assert (
            not missing_ips
        ), f"The following IPs are missing from probe configuration: {missing_ips}"
        assert not extra_ips, (
            f"The following unexpected IPs are present in probe configuration: {extra_ips}. "
            f"These may be unreachable or from incorrect network interfaces."
        )