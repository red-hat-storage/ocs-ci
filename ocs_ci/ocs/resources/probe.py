"""
Probe resource related functionalities
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)


class Probe(OCS):
    """
    Handles Probe resource operations for monitoring
    """

    def __init__(self, **kwargs):
        """
        Initialize Probe object

        Args:
            **kwargs: Additional keyword arguments passed to parent OCS class
        """
        kwargs.setdefault("kind", "Probe")
        super(Probe, self).__init__(**kwargs)

    def get_probe_config(self, probe_name, namespace=None):
        """
        Get the probe configuration.

        Args:
            probe_name (str): Name of the probe resource
            namespace (str, optional): Namespace of the probe.
                Defaults to openshift-storage if not provided.

        Returns:
            dict: Probe configuration as a dictionary
        """
        if namespace is None:
            namespace = (
                getattr(self, "namespace", None)
                or constants.OPENSHIFT_STORAGE_NAMESPACE
            )
        logger.info(f"Fetching probe configuration for: {probe_name}")
        probe_ocp = OCP(
            kind="Probe",
            namespace=namespace,
            resource_name=probe_name,
        )
        probe_config = probe_ocp.get()
        logger.info(f"Successfully retrieved probe config for {probe_name}")
        logger.debug(f"Probe config: {probe_config}")
        return probe_config

    def get_static_targets(self, probe_config):
        """
        Extract static target IPs from the probe configuration.

        Args:
            probe_config (dict): Probe configuration dictionary

        Returns:
            list: List of IP addresses configured as static targets in the probe

        Raises:
            RuntimeError: If unable to extract IPs from probe configuration
        """
        logger.info("Extracting static target IPs from probe configuration")
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
            logger.info(f"Probe config structure: {probe_config}")
            logger.error(f"Error extracting IPs from probe config: {e}")
            raise RuntimeError(
                f"Failed to extract IPs from probe configuration: {e}"
            ) from e

        logger.info(f"Extracted {len(probe_ips)} IPs from probe configuration")
        return probe_ips
