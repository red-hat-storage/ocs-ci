import json
import logging
import os
import time

import yaml

from ocs_ci.deployment.fusion_data_foundation import (
    FusionDataFoundationDeployment,
    FusionServiceInstance,
    run_patch_cmd,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.exceptions import (
    ChannelNotFound,
    ConfigurationError,
    TimeoutExpiredError,
)
from ocs_ci.ocs.resources.packagemanifest import get_packagemanifest_by_catalog_source
from ocs_ci.ocs.upgrade import BaseUpgrade
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd, load_config_file


logger = logging.getLogger(__name__)


class FDFUpgrade(BaseUpgrade):
    """
    FDF (Fusion Data Foundation) Upgrade helper class.

    This class handles the upgrade process for IBM Fusion Data Foundation,
    including pre-release catalog updates, subscription channel management,
    and upgrade execution via the fusionserviceinstance.

    """

    def __init__(self, namespace, version_before_upgrade):
        """
        Initialize FDF upgrade parameters.

        Args:
            namespace (str): Namespace where FDF is deployed
            version_before_upgrade (str): Current FDF version before upgrade

        """
        super().__init__(namespace, version_before_upgrade)
        self.fdf_deployment = FusionDataFoundationDeployment()
        self.kubeconfig = config.RUN.get("kubeconfig")
        self._fdf_upgrade_version = None
        self._channel = None

    @property
    def fdf_upgrade_version(self):
        """
        Get the FDF upgrade version.

        Returns:
            str: FDF upgrade version

        """
        if self._fdf_upgrade_version is None:
            self._fdf_upgrade_version = self.get_upgrade_version()
        return self._fdf_upgrade_version

    @fdf_upgrade_version.setter
    def fdf_upgrade_version(self, value):
        """
        Set the FDF upgrade version.

        Args:
            value (str): FDF upgrade version to set

        """
        self._fdf_upgrade_version = value

    @property
    def channel(self):
        """
        Get the FDF upgrade channel.

        Returns:
            str: FDF upgrade channel

        """
        if self._channel is None:
            self._channel = config.DEPLOYMENT.get("ocs_csv_channel")
        return self._channel

    @channel.setter
    def channel(self, value):
        """
        Set the FDF upgrade channel.

        Args:
            value (str): FDF upgrade channel to set

        """
        self._channel = value
        config.DEPLOYMENT["ocs_csv_channel"] = value

    def get_upgrade_version(self):
        """
        Get the target FDF upgrade version.

        Returns:
            str: Target FDF version for upgrade

        """
        upgrade_version = config.DEPLOYMENT.get("fdf_upgrade_image_tag")
        if not upgrade_version:
            upgrade_version = config.DEPLOYMENT.get("fdf_image_tag")
        if upgrade_version and upgrade_version.startswith("v"):
            upgrade_version = upgrade_version[1:]
        return upgrade_version or self.version_before_upgrade

    def load_version_config_file(self, upgrade_version):
        """
        Load FDF version-specific configuration file.

        Args:
            upgrade_version (str): FDF version to load config for

        """
        version_config_file = os.path.join(
            constants.FDF_VERSION_CONF_DIR, f"fdf-{upgrade_version}.yaml"
        )
        if os.path.exists(version_config_file):
            logger.info(f"Loading config file for FDF version: {upgrade_version}")
            load_config_file(version_config_file)
        else:
            logger.info(
                f"FDF version config file not found: {version_config_file}, "
                "using current configuration"
            )

    def run_upgrade(self):
        """
        Execute the complete FDF upgrade workflow.

        This method orchestrates the FDF upgrade process including:
        1. Version validation - ensures target version is >= current version
        2. Version-specific configuration loading
        3. ITMS, IDMS, and FusionServiceDefinition creation / updates
        4. Subscription channel updates
        5. Upgrade triggering via FusionServiceInstance
        6. Install plan approval and monitoring
        7. Health monitoring during upgrade via CephHealthMonitor
        8. Post-upgrade verification

        The upgrade is performed while monitoring Ceph cluster health. If health
        degrades during the upgrade, the CephHealthMonitor context manager will
        raise an exception.

        Raises:
            AssertionError: If target version is lower than current version
            ConfigurationError: If required configuration is missing or invalid
            ChannelNotFound: If the upgrade subscription channel is not available
            TimeoutExpiredError: If upgrade does not complete within timeout
            CephHealthException: If Ceph health degrades during upgrade

        """
        logger.info("Starting FDF upgrade procedure")
        self.start_time = time.time()
        logger.info(
            f"Upgrading FDF from {self.version_before_upgrade} to {self.fdf_upgrade_version}"
        )

        parsed_versions = self.get_parsed_versions()
        assert parsed_versions[1] >= parsed_versions[0], (
            f"Target upgrade version {self.fdf_upgrade_version} is not higher than or equal to "
            f"current version {self.version_before_upgrade}"
        )

        self.load_version_config_file(self.fdf_upgrade_version)
        if not self.upgrade_in_current_source:
            self.fdf_deployment.create_image_tag_mirror_set()
            self.fdf_deployment.create_image_digest_mirror_set(upgrade=True)
            self.fdf_deployment.patch_fusion_service_definition(upgrade=True)
        ceph_cluster = CephCluster()
        self.pre_upgrade_csv_data = self.get_csv_name_pre_upgrade()
        self.pre_upgrade_image_data = self.get_pre_upgrade_image(
            self.pre_upgrade_csv_data
        )
        with CephHealthMonitor(ceph_cluster):
            self.update_subscription_channel()
            self.trigger_fdf_upgrade()
            self.fdf_deployment.ensure_install_plan_approval()
            self.monitor_fusion_service_instance()
            self.end_time = time.time()
            self.duration = self.end_time - self.start_time
            old_images = self.get_images_post_upgrade(
                self.channel, self.pre_upgrade_image_data, self.fdf_upgrade_version
            )
        self.verify_required_csvs()
        self.verify_image_versions(old_images, parsed_versions[1], parsed_versions[0])

        version = self.fdf_deployment.get_installed_version()
        logger.info(f"FDF upgraded to version {version} successfully")

    def update_subscription_channel(self):
        """
        Update the ODF operator subscription channel based on fdf_upgrade_image_tag.

        This method derives the channel from fdf_upgrade_image_tag, waits for it to
        become available, and updates the odf-operator subscription channel.

        Raises:
            ConfigurationError: If fdf_upgrade_image_tag is not configured or cannot be parsed
            ChannelNotFound: If the upgrade channel does not become available within timeout

        """
        fdf_upgrade_image_tag = config.DEPLOYMENT.get("fdf_upgrade_image_tag")
        if not fdf_upgrade_image_tag:
            raise ConfigurationError(
                "fdf_upgrade_image_tag is not configured. "
                "Cannot determine upgrade subscription channel."
            )

        version_str = fdf_upgrade_image_tag.lstrip("v")
        version_parts = version_str.split(".")
        if len(version_parts) < 2:
            raise ConfigurationError(
                f"Could not parse version from fdf_upgrade_image_tag: {fdf_upgrade_image_tag}. "
                f"Expected format with at least major.minor version (e.g., 'v4.21', '4.18.8-2')"
            )

        self.channel = f"stable-{version_parts[0]}.{version_parts[1]}"
        logger.info(
            f"Derived upgrade channel '{self.channel}' from "
            f"fdf_upgrade_image_tag '{fdf_upgrade_image_tag}'"
        )

        logger.info(f"Waiting for upgrade channel '{self.channel}' to be available")
        if not self.wait_for_subscription_channel(self.channel, timeout=300):
            raise ChannelNotFound(
                f"Channel '{self.channel}' did not become available within 300 seconds. "
                f"Cannot proceed with subscription channel update."
            )

        logger.info(f"Updating odf-operator subscription channel to: {self.channel}")
        params_dict = {"spec": {"channel": self.channel}}
        params = json.dumps(params_dict)
        cmd = (
            f"oc --kubeconfig {self.kubeconfig} -n {constants.OPENSHIFT_STORAGE_NAMESPACE} patch Subscription "
            f"odf-operator -p '{params}' --type merge"
        )
        run_patch_cmd(cmd)
        logger.info("Subscription channel updated successfully")

    def trigger_fdf_upgrade(self):
        """
        Trigger FDF upgrade by patching fusionserviceinstance triggerUpdate to true.

        This initiates the FDF upgrade process by setting the triggerUpdate field
        in the fusionserviceinstance spec to true.

        """
        logger.test_step("Triggering FDF upgrade")
        params_dict = {"spec": {"triggerUpdate": True}}
        params = json.dumps(params_dict)
        cmd = (
            f"oc --kubeconfig {self.kubeconfig} -n {constants.FDF_NAMESPACE} patch FusionServiceInstance "
            f"{constants.FDF_SERVICE_NAME} -p '{params}' --type merge"
        )
        run_patch_cmd(cmd)
        logger.info("FDF upgrade triggered successfully")

    def monitor_fusion_service_instance(self, timeout=1800):
        """
        Monitor the FDF upgrade progress and verify completion.

        This method monitors the fusionserviceinstance status and waits for the
        upgrade to complete. It logs important information during the upgrade and
        in the event of failure.

        Args:
            timeout (int): Maximum time to wait for upgrade completion in seconds.
                Default is 1800 (30 minutes).

        Raises:
            TimeoutExpiredError: If upgrade does not complete within timeout
            AssertionError: If upgrade fails or encounters unhealthy state

        """
        logger.test_step("Monitoring FDF upgrade progress")

        expected_version = config.DEPLOYMENT.get("fdf_upgrade_image_tag")

        if expected_version and expected_version.startswith("v"):
            expected_version = expected_version[1:]

        logger.info(f"Expected upgrade version: {expected_version}")

        last_state = {}

        try:

            @retry(AssertionError, tries=timeout // 30, delay=30, backoff=1)
            def _wait_for_upgrade_completion():
                instance = FusionServiceInstance(
                    resource_name=constants.FDF_SERVICE_NAME,
                    namespace=constants.FDF_NAMESPACE,
                )
                instance_status = instance.data.get("status", {})
                upgrade_in_progress = instance_status.get("upgradeInProgress", False)
                health = instance_status.get("health", "Unknown")
                current_version = instance_status.get("currentVersion", "Unknown")

                current_state = {
                    "upgrade_in_progress": upgrade_in_progress,
                    "health": health,
                    "version": current_version,
                }

                if current_state != last_state:
                    logger.info(
                        f"Upgrade status - In Progress: {upgrade_in_progress}, "
                        f"Health: {health}, Version: {current_version}"
                    )
                    last_state.update(current_state)
                else:
                    logger.debug(
                        f"Upgrade status - In Progress: {upgrade_in_progress}, "
                        f"Health: {health}, Version: {current_version}"
                    )

                if health not in ["Healthy", "Unknown"]:
                    logger.warning(f"FusionServiceInstance health status: {health}")
                    logger.info("Status details:")
                    logger.info(yaml.dump(instance_status, default_flow_style=False))

                assert not upgrade_in_progress, "Upgrade still in progress"
                assert (
                    health == "Healthy"
                ), f"Service health is {health}, expected Healthy"

                if expected_version:
                    version_to_check = current_version
                    if version_to_check.startswith("v"):
                        version_to_check = version_to_check[1:]
                    assert version_to_check.startswith(
                        expected_version
                    ), f"Current version {current_version} does not match expected version {expected_version}"

            _wait_for_upgrade_completion()
            logger.info("FDF upgrade monitoring completed successfully")

        except (TimeoutExpiredError, AssertionError) as e:
            logger.error(f"FDF upgrade monitoring failed: {e}")
            self._log_upgrade_failure_details()
            raise

    def _log_upgrade_failure_details(self):
        """
        Log detailed information about upgrade failure for debugging.

        This method collects and logs relevant information when an upgrade fails,
        including fusionserviceinstance status, operator CSVs, and install plans.

        """
        logger.error("Collecting upgrade failure details")

        try:
            logger.info("FusionServiceInstance status:")
            instance = FusionServiceInstance(
                resource_name=constants.FDF_SERVICE_NAME,
                namespace=constants.FDF_NAMESPACE,
            )
            logger.info(
                yaml.dump(instance.data.get("status", {}), default_flow_style=False)
            )
        except Exception as e:
            logger.error(f"Failed to get FusionServiceInstance status: {e}")

        try:
            logger.info("Operator CSV status:")
            csvs_cmd = f"oc --kubeconfig {self.kubeconfig} get csv -n {constants.OPENSHIFT_STORAGE_NAMESPACE} -o yaml"
            result = exec_cmd(csvs_cmd)
            csvs_output = (
                result.stdout.decode("utf-8")
                if isinstance(result.stdout, bytes)
                else result.stdout
            )
            csvs_data = yaml.safe_load(csvs_output)
            for csv in csvs_data.get("items", []):
                csv_name = csv["metadata"]["name"]
                phase = csv.get("status", {}).get("phase", "Unknown")
                logger.info(f"  {csv_name}: {phase}")
        except Exception as e:
            logger.error(f"Failed to get CSV status: {e}")

        try:
            logger.info("Pending install plans:")
            ip_cmd = (
                f"oc --kubeconfig {self.kubeconfig} "
                "get installplan -n {constants.OPENSHIFT_STORAGE_NAMESPACE} "
                "-o yaml"
            )
            result = exec_cmd(ip_cmd)
            ip_output = (
                result.stdout.decode("utf-8")
                if isinstance(result.stdout, bytes)
                else result.stdout
            )
            ip_data = yaml.safe_load(ip_output)
            for ip in ip_data.get("items", []):
                ip_name = ip["metadata"]["name"]
                approved = ip["spec"].get("approved", False)
                phase = ip.get("status", {}).get("phase", "Unknown")
                logger.info(f"  {ip_name}: approved={approved}, phase={phase}")
        except Exception as e:
            logger.error(f"Failed to get install plan status: {e}")

    def wait_for_subscription_channel(self, channel_name, timeout=300):
        """
        Wait for a specific subscription channel to become available in the packagemanifest.

        This method uses the FDF catalog source to ensure we're checking the correct
        packagemanifest when multiple catalog sources are present.

        Args:
            channel_name (str): The channel name to wait for (e.g., "stable-4.21")
            timeout (int): Maximum time to wait in seconds. Default is 300 (5 minutes).

        Returns:
            bool: True if the channel becomes available, False if timeout is reached

        """
        catalog_source = defaults.FUSION_CATALOG_NAME
        package_name = defaults.ODF_OPERATOR_NAME
        logger.info(
            f"Waiting up to {timeout}s for channel '{channel_name}' to appear "
            f"in {package_name} packagemanifest from catalog source '{catalog_source}'"
        )

        start_time = time.time()
        last_channels = None

        while time.time() - start_time < timeout:
            try:
                package_manifest = get_packagemanifest_by_catalog_source(
                    package_name=package_name, catalog_source=catalog_source
                )

                channels = package_manifest.get("status", {}).get("channels", [])
                channel_names = [ch["name"] for ch in channels]

                if channel_names != last_channels:
                    logger.info(f"Available channels: {channel_names}")
                    last_channels = channel_names
                else:
                    logger.debug(f"Available channels: {channel_names}")

                if channel_name in channel_names:
                    logger.info(f"Channel '{channel_name}' is now available")
                    return True

            except Exception as e:
                logger.debug(f"Error checking for channel: {e}")

            time.sleep(10)

        elapsed = time.time() - start_time
        logger.warning(f"Channel '{channel_name}' did not appear after {elapsed:.0f}s")
        return False
