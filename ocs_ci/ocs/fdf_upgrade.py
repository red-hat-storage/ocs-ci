import json
import logging
import os
import time
from tempfile import NamedTemporaryFile

import yaml
from packaging.version import parse as parse_version

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
    CSVNotFound,
    TimeoutExpiredError,
    TimeoutException,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import (
    check_all_csvs_are_succeeded,
    get_csvs_start_with_prefix,
)
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.packagemanifest import get_packagemanifest_by_catalog_source
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.ocs.upgrade import BaseUpgrade
from ocs_ci.utility.retry import retry
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.utility.utils import (
    exec_cmd,
    load_config_file,
    TimeoutSampler,
    wait_for_machineconfigpool_status,
)

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

    def validate_upgrade_versions(self):
        """
        Validate that the target upgrade version is valid.

        Normalizes both current and target versions to X.Y format and compares them.
        Logs whether this is a Z-stream or Y-stream upgrade.

        Raises:
            AssertionError: If target version is lower than current version

        """
        current_parts = self.version_before_upgrade.split(".")
        current_xy = f"{current_parts[0]}.{current_parts[1]}"
        target_parts = self.fdf_upgrade_version.split(".")
        target_xy = f"{target_parts[0]}.{target_parts[1]}"

        assert parse_version(target_xy) >= parse_version(current_xy), (
            f"Target upgrade version {target_xy} is lower than "
            f"current version {current_xy}"
        )

        if current_xy == target_xy:
            logger.info(
                f"Z-stream upgrade: upgrading within {target_xy} stream "
                f"from {self.version_before_upgrade}"
            )
        else:
            logger.info(f"Y-stream upgrade: upgrading from {current_xy} to {target_xy}")

    def load_version_config_file(self, upgrade_version):
        """
        Load FDF version-specific configuration file.

        Args:
            upgrade_version (str): FDF version to load config for (e.g., "4.18.8-2")

        """
        # Extract major.minor version from upgrade_version for config file lookup
        # e.g., "4.18.8-2" -> "4.18"
        version_parts = upgrade_version.split(".")
        if len(version_parts) < 2:
            logger.warning(
                f"Cannot parse major.minor from upgrade_version: {upgrade_version}, "
                "skipping version config load"
            )
            return

        major_minor = f"{version_parts[0]}.{version_parts[1]}"
        version_config_file = os.path.join(
            constants.FDF_VERSION_CONF_DIR, f"fdf-{major_minor}.yaml"
        )
        if os.path.exists(version_config_file):
            logger.info(
                f"Loading config file for FDF version {major_minor} "
                f"(from upgrade_version {upgrade_version})"
            )
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

        self.validate_upgrade_versions()
        self.load_version_config_file(self.fdf_upgrade_version)
        if not self.upgrade_in_current_source:
            self.fdf_deployment.create_image_tag_mirror_set()
            self.fdf_deployment.create_image_digest_mirror_set(upgrade=True)
            wait_for_machineconfigpool_status(node_type="all")
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
        parsed_versions = self.get_parsed_versions()
        self.verify_image_versions(old_images, parsed_versions[1], parsed_versions[0])

        version = self.fdf_deployment.get_installed_version()
        logger.info(f"FDF upgraded to version {version} successfully")

    def update_subscription_channel(self):
        """
        Update the ODF operator subscription channel based on fdf_upgrade_version.

        This method derives the channel from fdf_upgrade_version, waits for it to
        become available, and updates the odf-operator subscription channel.

        Raises:
            ConfigurationError: If fdf_upgrade_version cannot be parsed
            ChannelNotFound: If the upgrade channel does not become available within timeout

        """
        version_parts = self.fdf_upgrade_version.split(".")
        if len(version_parts) < 2:
            raise ConfigurationError(
                f"Could not parse version from fdf_upgrade_version: {self.fdf_upgrade_version}. "
                f"Expected format with at least major.minor version (e.g., '4.21', '4.18.8-2')"
            )

        self.channel = f"stable-{version_parts[0]}.{version_parts[1]}"
        logger.info(
            f"Derived upgrade channel '{self.channel}' from "
            f"fdf_upgrade_version '{self.fdf_upgrade_version}'"
        )

        logger.info(f"Waiting for upgrade channel '{self.channel}' to be available")
        timeout = 300
        if not self.wait_for_subscription_channel(self.channel, timeout=timeout):
            raise ChannelNotFound(
                f"Channel '{self.channel}' did not become available within {timeout} seconds. "
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

        expected_version = self.fdf_upgrade_version

        if expected_version and expected_version.startswith("v"):
            expected_version = expected_version[1:]

        logger.info(f"Expected upgrade version: {expected_version}")

        last_state = {}
        last_logged_status = None

        try:

            @retry(AssertionError, tries=timeout // 60, delay=60, backoff=1)
            def _wait_for_upgrade_completion():
                nonlocal last_logged_status
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

                # Only log detailed status if health is not Healthy/Unknown AND it has changed
                if health not in ["Healthy", "Unknown"]:
                    # Convert status to string for comparison
                    status_str = yaml.dump(instance_status, default_flow_style=False)
                    if status_str != last_logged_status:
                        logger.warning(f"FusionServiceInstance health status: {health}")
                        logger.info("Status details:")
                        logger.info(status_str)
                        last_logged_status = status_str
                    else:
                        logger.debug(
                            f"FusionServiceInstance health status unchanged: {health}"
                        )

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
                f"get installplan -n {constants.OPENSHIFT_STORAGE_NAMESPACE} "
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

        last_channels = None

        try:
            for sample in TimeoutSampler(
                timeout=timeout,
                sleep=10,
                func=get_packagemanifest_by_catalog_source,
                package_name=package_name,
                catalog_source=catalog_source,
            ):
                channels = sample.get("status", {}).get("channels", [])
                channel_names = [ch["name"] for ch in channels]

                if channel_names != last_channels:
                    logger.info(f"Available channels: {channel_names}")
                    last_channels = channel_names
                else:
                    logger.debug(f"Available channels: {channel_names}")

                if channel_name in channel_names:
                    logger.info(f"Channel '{channel_name}' is now available")
                    return True

        except TimeoutExpiredError:
            logger.warning(
                f"Channel '{channel_name}' did not appear within {timeout}s timeout"
            )
            return False
        except Exception as e:
            logger.warning(f"Error while waiting for channel '{channel_name}': {e}")
            return False

    def run_migration(self):
        """
        Orchestrate the full ODF to FDF migration sequence.

        This method handles migrating from Red Hat ODF to IBM Fusion Data
        Foundation by replacing the ODF catalog source with the ISF FDF
        catalog and re-pointing all subscriptions.

        The migration flow:
        1. Pre-migration pod health check
        2. Record pre-migration CSV name
        3. Create FDF CatalogSource and set Manual approval
        4. Patch ODF subscriptions to point at FDF catalog
        5. Approve the pending InstallPlan
        6. Wait for all CSVs to reach Succeeded
        7. Post-migration CSV and OCS install verification

        Raises:
            AssertionError: When fdf_registry_image is not configured, or
                post-migration verification fails.
            TimeoutException: When the migration does not complete in time.

        """
        fdf_registry_image = config.UPGRADE.get("fdf_registry_image")
        assert fdf_registry_image, (
            "config.UPGRADE['fdf_registry_image'] must be set before running "
            "FDF migration. Provide the full pull-spec of the ISF Data "
            "Foundation catalog image."
        )

        logger.test_step("Pre-migration pod health check")
        _check_pod_health(namespace=self.namespace)

        csv_name_pre_migration = self._get_odf_csv_name()
        logger.info(f"Pre-migration CSV: {csv_name_pre_migration}")
        start_time = time.time()

        ceph_cluster = CephCluster()
        with CephHealthMonitor(ceph_cluster):
            logger.test_step("Creating FDF CatalogSource")
            self._create_migration_catalog_source(fdf_registry_image)

            logger.test_step("Patching ODF subscriptions to FDF catalog")
            _patch_subscriptions_source(
                catalog_source_name=constants.FDF_CATALOG_NAME,
                namespace=self.namespace,
            )

            logger.test_step("Approving InstallPlan")
            wait_for_install_plan_and_approve(self.namespace)

            logger.test_step("Waiting for FDF migration to complete")
            self._wait_for_migration_completion(csv_name_pre_migration)

        elapsed = time.time() - start_time
        logger.info(f"FDF migration took {elapsed:.1f} seconds")

        logger.test_step("Post-migration: verifying all CSVs are Succeeded")
        csvs_ok = check_all_csvs_are_succeeded(namespace=self.namespace)
        logger.assertion(f"Post-migration CSVs check: all_succeeded={csvs_ok}")
        assert csvs_ok, (
            "Post-migration verification failed: not all CSVs are in "
            "Succeeded state."
        )

        if not config.ENV_DATA.get("mcg_only_deployment"):
            logger.test_step("Post-migration: running OCS install verification")
            ocs_install_verification(
                timeout=600,
                skip_osd_distribution_check=True,
                ocs_registry_image=fdf_registry_image,
                post_upgrade_verification=True,
                version_before_upgrade=self.version_before_upgrade,
            )

        logger.info("ODF to FDF migration completed and verified successfully.")

    def _create_migration_catalog_source(self, fdf_registry_image):
        """
        Create the ISF FDF CatalogSource in the marketplace namespace.

        After creation, the ODF operator subscription is switched to Manual
        installPlanApproval to prevent accidental auto-upgrade.

        Args:
            fdf_registry_image (str): Full pull-spec of the FDF catalog image.

        """
        marketplace_ns = constants.MARKETPLACE_NAMESPACE
        catalog_data = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "CatalogSource",
            "metadata": {
                "name": constants.FDF_CATALOG_NAME,
                "namespace": marketplace_ns,
            },
            "spec": {
                "displayName": "ISF Data Foundation Catalog",
                "publisher": "IBM",
                "sourceType": "grpc",
                "image": fdf_registry_image,
                "updateStrategy": {
                    "registryPoll": {"interval": "15m"},
                },
            },
        }

        logger.info(
            f"Creating FDF CatalogSource '{constants.FDF_CATALOG_NAME}' "
            f"in namespace '{marketplace_ns}' with image "
            f"'{fdf_registry_image}'"
        )
        with NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            dump_data_to_temp_yaml(catalog_data, tmp.name)
            exec_cmd(f"oc apply -f {tmp.name}")

        logger.info(
            f"Waiting for CatalogSource '{constants.FDF_CATALOG_NAME}' "
            "to become ready"
        )
        fdf_catalog = CatalogSource(
            resource_name=constants.FDF_CATALOG_NAME,
            namespace=marketplace_ns,
        )
        fdf_catalog.wait_for_state("READY", timeout=300)

        logger.info("Setting ODF subscription installPlanApproval to Manual")
        _set_subscription_approval_strategy(approval="Manual", namespace=self.namespace)

    def _get_odf_csv_name(self):
        """
        Return the name of the currently installed ODF CSV.

        Returns:
            str: CSV name, e.g. ``"odf-operator.v4.16.0"``.

        Raises:
            CSVNotFound: When no matching CSV is found.

        """
        csv_list = get_csvs_start_with_prefix(
            defaults.ODF_OPERATOR_NAME, namespace=self.namespace
        )
        for csv in csv_list:
            name = csv.get("metadata", {}).get("name", "")
            if defaults.ODF_OPERATOR_NAME in name:
                return name
        raise CSVNotFound(
            f"No CSV found for operator '{defaults.ODF_OPERATOR_NAME}' "
            f"in namespace '{self.namespace}'"
        )

    def _check_migration_completed(self, csv_name_pre_migration):
        """
        Return True when the active CSV has changed and all CSVs are Succeeded.

        Args:
            csv_name_pre_migration (str): CSV name before migration started.

        Returns:
            bool: True when migration is complete.

        """
        if not check_all_csvs_are_succeeded(self.namespace):
            logger.debug("One or more CSVs are not yet in Succeeded state.")
            return False

        current_csvs = get_csvs_start_with_prefix(
            defaults.ODF_OPERATOR_NAME, namespace=self.namespace
        )
        for csv in current_csvs:
            name = csv.get("metadata", {}).get("name", "")
            if name and name != csv_name_pre_migration:
                logger.info(f"CSV migrated to: {name}")
                return True

        logger.debug(f"CSV is still: {csv_name_pre_migration}")
        return False

    def _wait_for_migration_completion(self, csv_name_pre_migration, timeout=725):
        """
        Poll until the migration finishes or timeout elapses.

        Args:
            csv_name_pre_migration (str): CSV name before migration.
            timeout (int): Seconds to wait.

        Raises:
            TimeoutException: When migration does not complete in time.

        """
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=self._check_migration_completed,
            csv_name_pre_migration=csv_name_pre_migration,
        ):
            try:
                if sample:
                    logger.info("FDF migration completed successfully!")
                    return
            except TimeoutException:
                raise TimeoutException(
                    "FDF migration did not complete within the allotted "
                    "time. No new CSV found after migration."
                )


def _check_pod_health(namespace=None):
    """
    Verify that all pods in the given namespace are Running or Succeeded.

    Args:
        namespace (str): Kubernetes namespace to inspect. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    Raises:
        AssertionError: When unhealthy pods are detected.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    logger.info(f"Checking pod health in namespace '{namespace}'")

    all_pods = get_all_pods(namespace=namespace)
    unhealthy = []
    for pod in all_pods:
        phase = pod.data.get("status", {}).get("phase", "Unknown")
        if phase not in ("Running", "Succeeded"):
            unhealthy.append(f"{pod.name} ({phase})")

    logger.assertion(
        f"Pod health: unhealthy_count={len(unhealthy)}, all_healthy={not unhealthy}"
    )
    assert not unhealthy, f"Unhealthy pods detected in '{namespace}': {unhealthy}"
    logger.info("All pods are healthy.")


def _get_subscription_names(namespace=None):
    """
    Return a list of subscription names present in the namespace.

    Args:
        namespace (str): Kubernetes namespace. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    Returns:
        list[str]: Subscription names.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_sub = OCP(kind="subscription.operators.coreos.com", namespace=namespace)
    data = ocp_sub.get() or {}
    return [item["metadata"]["name"] for item in data.get("items", [])]


def _patch_subscriptions_source(catalog_source_name, namespace=None):
    """
    Patch every ODF-related subscription so its ``spec.source`` points to
    the given catalog source name.

    Args:
        catalog_source_name (str): Name of the target CatalogSource.
        namespace (str): Kubernetes namespace. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    subscription_names = _get_subscription_names(namespace)

    if not subscription_names:
        logger.warning(f"No subscriptions found in namespace '{namespace}'")
        return

    for sub_name in subscription_names:
        logger.info(
            f"Patching subscription '{sub_name}' -> " f"source='{catalog_source_name}'"
        )
        patch_json = json.dumps({"spec": {"source": catalog_source_name}})
        exec_cmd(
            f"oc patch subscription.operators.coreos.com {sub_name} "
            f"-n {namespace} --type merge -p '{patch_json}'"
        )


def _set_subscription_approval_strategy(approval="Manual", namespace=None):
    """
    Set ``spec.installPlanApproval`` on the ODF operator subscription.

    Args:
        approval (str): ``"Manual"`` or ``"Automatic"``.
        namespace (str): Kubernetes namespace. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    logger.info(
        f"Setting installPlanApproval='{approval}' on subscription "
        f"'{defaults.ODF_OPERATOR_NAME}'"
    )
    patch_json = json.dumps({"spec": {"installPlanApproval": approval}})
    exec_cmd(
        f"oc patch subscription.operators.coreos.com "
        f"{defaults.ODF_OPERATOR_NAME} "
        f"-n {namespace} --type merge -p '{patch_json}'"
    )
