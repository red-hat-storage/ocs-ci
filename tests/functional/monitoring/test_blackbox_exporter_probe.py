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
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@blue_squad
@skipif_mcg_only
@skipif_external_mode
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.22")
class TestBlackboxExporterProbe(ManageTest):

    def get_probe_config(self, probe_name="odf-blackbox-exporter"):
        """
        Get the probe configuration for odf-blackbox-exporter.
        Args:
            probe_name (str): Name of the probe resource. Default is "odf-blackbox-exporter"
        Returns:
            dict: Probe configuration as a dictionary
        """
        logger.info(f"Fetching probe configuration for: {probe_name}")
        probe_ocp = OCP(
            kind="Probe",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=probe_name,
        )
        probe_config = probe_ocp.get()
        logger.info(f"Successfully retrieved probe config for {probe_name}")
        logger.debug(f"Probe config: {probe_config}")
        return probe_config

    def get_pod_ips_from_network_annotations(self, pod_selector):
        """
        Get pod IPs from network annotations for pods matching the selector.
        Args:
            pod_selector (str): Label selector to filter pods (e.g., "app=rook-ceph-osd")
        Returns:
            dict: Dictionary mapping pod names to their IP addresses
        Example:
            {'rook-ceph-osd-0-xxx': '10.0.0.1', 'rook-ceph-osd-1-xxx': '10.0.0.2'}
        """
        logger.info(f"Getting pod IPs for selector: {pod_selector}")
        pod_ocp = OCP(
            kind=constants.POD,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        pods = pod_ocp.get(selector=pod_selector)
        if not pods or "items" not in pods:
            logger.warning(f"No pods found with selector: {pod_selector}")
            return {}
        pod_ips = {}
        for pod in pods["items"]:
            pod_name = pod["metadata"]["name"]
            pod_ip = pod.get("status", {}).get("podIP")
            if pod_ip:
                pod_ips[pod_name] = pod_ip
                logger.info(f"Pod: {pod_name}, IP: {pod_ip}")
            else:
                logger.warning(f"No IP found for pod: {pod_name}")
        logger.info(f"Found {len(pod_ips)} pod IPs for selector: {pod_selector}")
        return pod_ips

    def get_ips_from_blackbox_probe(self, probe_config):
        """
        Extract target IPs from the blackbox exporter probe configuration.
        Args:
            probe_config (dict): Probe configuration dictionary
        Returns:
            list: List of IP addresses configured in the probe
        """
        logger.info("Extracting IPs from blackbox probe configuration")
        probe_ips = []
        try:
            spec = probe_config.get("spec", {})
            targets = spec.get("targets", {})
            static_config = targets.get("staticConfig", {})
            static_ips = static_config.get("static", [])

            if static_ips:
                probe_ips = static_ips
                logger.info(f"Found {len(probe_ips)} IPs in probe static config")
                for ip in probe_ips:
                    logger.debug(f"Probe target IP: {ip}")
            else:
                logger.warning("No static IPs found in probe configuration")
                logger.debug(f"Probe config structure: {probe_config}")

        except Exception as e:
            logger.error(f"Error extracting IPs from probe config: {e}")
            logger.info(f"Probe config structure: {probe_config}")

        logger.info(f"Extracted {len(probe_ips)} IPs from probe configuration")
        return probe_ips

    @tier2
    @polarion_id("OCS-7941")
    def test_blackbox_probe_osd_mon_ips(self):
        """
        Test to verify odf-blackbox-exporter probe contains correct OSD and MON pod IPs.

        Test Steps:
        1. Get the odf-blackbox-exporter probe configuration
        2. Get IPs from network annotations for OSD pods
        3. Get IPs from network annotations for MON pods
        4. Extract IPs from the blackbox probe configuration
        5. Verify that all OSD and MON pod IPs are present in the probe configuration
        """
        logger.info("Starting test: Verify blackbox probe contains OSD and MON IPs")

        probe_config = self.get_probe_config("odf-blackbox-exporter")
        assert probe_config, "Failed to get probe configuration"

        logger.info("Getting OSD pod IPs...")
        osd_ips = self.get_pod_ips_from_network_annotations(constants.OSD_APP_LABEL)
        assert osd_ips, "No OSD pods found or no IPs available"
        logger.info(f"Found {len(osd_ips)} OSD pods with IPs")

        logger.info("Getting MON pod IPs...")
        mon_ips = self.get_pod_ips_from_network_annotations(constants.MON_APP_LABEL)
        assert mon_ips, "No MON pods found or no IPs available"
        logger.info(f"Found {len(mon_ips)} MON pods with IPs")

        logger.info("Extracting IPs from probe configuration...")
        probe_ips = self.get_ips_from_blackbox_probe(probe_config)
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
