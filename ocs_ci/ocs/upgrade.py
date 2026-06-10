import logging
import time

from packaging.version import parse as parse_version

from ocs_ci.deployment.helpers.odf_deployment_helpers import get_required_csvs
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME
from ocs_ci.ocs.exceptions import CSVNotFound
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP, get_images
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.packagemanifest import (
    PackageManifest,
    get_selector_for_ocs_operator,
)
from ocs_ci.ocs.resources.pod import get_noobaa_pods, verify_pods_upgraded
from ocs_ci.ocs.resources.storage_cluster import get_osd_count
from ocs_ci.ocs.utils import get_expected_nb_db_psql_version, setup_ceph_toolbox
from ocs_ci.utility import version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.rgwutils import get_rgw_count


logger = logging.getLogger(__name__)


class BaseUpgrade(object):
    """
    Base class for upgrade operations.

    This class provides a common interface and shared functionality
    for upgrade implementations (OCS/ODF and FDF).

    """

    def __init__(self, namespace, version_before_upgrade):
        """
        Initialize base upgrade parameters.

        Args:
            namespace (str): Namespace where the product is deployed
            version_before_upgrade (str): Current version before upgrade

        """
        self.namespace = namespace
        self._version_before_upgrade = version_before_upgrade
        self.subscription_plan_approval = config.DEPLOYMENT.get(
            "subscription_plan_approval"
        )
        self.upgrade_in_current_source = config.UPGRADE.get(
            "upgrade_in_current_source", False
        )
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.pre_upgrade_csv_data = None
        self.pre_upgrade_image_data = None
        self.post_upgrade_csv_data = None
        self.post_upgrade_image_data = None

    @property
    def version_before_upgrade(self):
        """
        Get the version before upgrade.

        Returns:
            str: Version before upgrade

        """
        return self._version_before_upgrade

    def get_upgrade_version(self):
        """
        Get the target upgrade version.

        This method should be implemented by subclasses to determine
        the version to upgrade to based on their specific logic.

        Returns:
            str: Target version for upgrade

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError("Subclasses must implement get_upgrade_version()")

    def get_parsed_versions(self):
        """
        Get parsed version objects for current and upgrade versions.

        Returns:
            tuple: (parsed_current_version, parsed_upgrade_version)

        """
        parsed_version_before_upgrade = parse_version(self.version_before_upgrade)
        parsed_upgrade_version = parse_version(self.get_upgrade_version())

        return parsed_version_before_upgrade, parsed_upgrade_version

    def get_csv_name_pre_upgrade(self, resource_name=OCS_OPERATOR_NAME):
        """
        Get pre-upgrade CSV name

        Earlier we used to depend on packagemanifest to find the pre-upgrade
        csv name. Due to issues in catalogsource where csv names were not shown properly once
        catalogsource for upgrade version has been created, we are taking new approach of
        finding csv name from csv list and also look for pre-upgrade ocs version for finding out
        the actual csv

        Args:
            resource_name (str, optional): String the resource CSVs start with . Defaults to OCS_OPERATOR_NAME.

        Raises:
            CSVNotFound: If the CSV with the provided resource isn't found.

        Returns:
            str: Name of the resource CSV

        """
        csv_name = None
        csv_list = get_csvs_start_with_prefix(resource_name, namespace=self.namespace)
        for csv in csv_list:
            if resource_name in csv.get("metadata").get("name"):
                logger.info(
                    "searching for pre-upgrade csv with version: "
                    f"{config.PREUPGRADE_CONFIG.get('ENV_DATA').get('ocs_version')}"
                )
                if config.PREUPGRADE_CONFIG.get("ENV_DATA").get(
                    "ocs_version"
                ) in csv.get("metadata").get("name"):
                    csv_name = csv.get("metadata").get("name")
                    return csv_name
        raise CSVNotFound(f"No preupgrade CSV found for {resource_name}")

    def get_pre_upgrade_image(self, csv_name_pre_upgrade):
        """
        Getting all OCS cluster images, before upgrade

        Args:
            csv_name_pre_upgrade (str): CSV name for the pre-upgrade operator

        Returns:
            dict: Contains all OCS cluster images
                dict keys: Image name
                dict values: Image full path

        """
        logger.info(f"CSV name before upgrade is: {csv_name_pre_upgrade}")
        csv_pre_upgrade = CSV(
            resource_name=csv_name_pre_upgrade, namespace=self.namespace
        )
        return get_images(csv_pre_upgrade.get())

    def load_version_config_file(self, upgrade_version):
        """
        Load version-specific configuration file.

        This method should be implemented by subclasses to load
        configuration files appropriate for their product.

        Args:
            upgrade_version (str): Version to load config for

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError(
            "Subclasses must implement load_version_config_file()"
        )

    def run_upgrade(self):
        """
        Execute the upgrade procedure.

        This method should be implemented by subclasses to perform
        the specific upgrade steps for their product.

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError("Subclasses must implement run_upgrade()")

    def verify_required_csvs(self):
        """
        Verify all required CSVs post upgrade
        """
        ocs_operator_names = get_required_csvs()
        channel = config.DEPLOYMENT.get("ocs_csv_channel")
        operator_selector = get_selector_for_ocs_operator()
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")

        for ocs_operator_name in ocs_operator_names:
            package_manifest = PackageManifest(
                resource_name=ocs_operator_name,
                selector=operator_selector,
                subscription_plan_approval=subscription_plan_approval,
            )
            package_manifest.wait_for_resource(timeout=300)
            csv_name = package_manifest.get_current_csv(channel=channel)
            csv = CSV(resource_name=csv_name, namespace=self.namespace)
            csv.wait_for_phase("Succeeded", timeout=720)

    def verify_image_versions(
        self, old_images, upgrade_version, version_before_upgrade
    ):
        """
        Verify if all the images of OCS objects got upgraded

        Args:
            old_images (set): set with old images
            upgrade_version (packaging.version.Version): version of OCS
            version_before_upgrade (packaging.version.Version): version of OCS before upgrade

        """
        # Get all worker nodes for CSI nodeplugin count (they run on all workers, not just storage-labeled)
        all_worker_nodes = get_worker_nodes(skip_master_nodes=False)
        number_of_all_worker_nodes = len(all_worker_nodes)
        verify_pods_upgraded(old_images, selector=constants.OCS_OPERATOR_LABEL)
        if not (
            config.ENV_DATA.get("mcg_only_deployment")
            and (
                upgrade_version >= parse_version("4.20")
                or (
                    version_before_upgrade == parse_version("4.19")
                    and upgrade_version == parse_version("4.19")
                )
            )
        ):
            verify_pods_upgraded(old_images, selector=constants.OPERATOR_LABEL)
        self.verify_noobaa_pods_upgraded(old_images, upgrade_version)
        if not config.ENV_DATA.get("mcg_only_deployment"):
            odf_running_version = version.get_ocs_version_from_csv(
                only_major_minor=True
            )
            # cephfs and rbdplugin label and count
            csi_cephfsplugin_label = constants.CSI_CEPHFSPLUGIN_LABEL
            csi_rbdplugin_label = constants.CSI_RBDPLUGIN_LABEL
            csi_cephfsplugin_provisioner_label = (
                constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL
            )
            csi_rbdplugin_provisioner_label = constants.CSI_RBDPLUGIN_PROVISIONER_LABEL
            # CSI nodeplugin pods run on ALL worker nodes (including app nodes), not just storage-labeled nodes
            count_csi_cephfsplugin_label = count_csi_rbdplugin_label = (
                number_of_all_worker_nodes
            )
            if odf_running_version >= version.VERSION_4_19:
                csi_cephfsplugin_label = constants.CSI_CEPHFSPLUGIN_LABEL_419
                csi_rbdplugin_label = constants.CSI_RBDPLUGIN_LABEL_419
                csi_cephfsplugin_provisioner_label = (
                    constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419
                )
                csi_rbdplugin_provisioner_label = (
                    constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419
                )
                # CSI nodeplugin pods run on ALL worker nodes (including app nodes), not just storage-labeled nodes
                count_csi_cephfsplugin_label = count_csi_rbdplugin_label = (
                    number_of_all_worker_nodes
                )
            else:
                logger.info(
                    f"Label for cephfsplugin and rbdplugin are {csi_cephfsplugin_label} and {csi_rbdplugin_label}"
                )
            verify_pods_upgraded(
                old_images,
                selector=csi_cephfsplugin_label,
                count=count_csi_cephfsplugin_label,
            )
            verify_pods_upgraded(
                old_images, selector=csi_cephfsplugin_provisioner_label, count=2
            )
            verify_pods_upgraded(
                old_images,
                selector=csi_rbdplugin_label,
                count=count_csi_rbdplugin_label,
            )
            verify_pods_upgraded(
                old_images, selector=csi_rbdplugin_provisioner_label, count=2
            )
        if not (
            config.DEPLOYMENT.get("external_mode")
            or config.ENV_DATA.get("mcg_only_deployment")
        ):
            mon_count = 3
            if config.DEPLOYMENT.get("arbiter_deployment"):
                mon_count = 5
            verify_pods_upgraded(
                old_images,
                selector=constants.MON_APP_LABEL,
                count=mon_count,
                timeout=820,
            )
            mgr_count = constants.MGR_COUNT_415
            if upgrade_version < parse_version("4.15"):
                mgr_count = constants.MGR_COUNT
            verify_pods_upgraded(
                old_images, selector=constants.MGR_APP_LABEL, count=mgr_count
            )
            osd_timeout = 600 if upgrade_version >= parse_version("4.5") else 750
            osd_count = get_osd_count()
            # In the debugging issue:
            # https://github.com/red-hat-storage/ocs-ci/issues/5031
            # Noticed that it's taking about 1 more minute from previous check till actual
            # OSD pods getting restarted.
            # Hence adding sleep here for 120 seconds to be sure, OSD pods upgrade started.
            logger.info("Waiting for 2 minutes before start checking OSD pods")
            time.sleep(120)
            verify_pods_upgraded(
                old_images,
                selector=constants.OSD_APP_LABEL,
                count=osd_count,
                timeout=osd_timeout * osd_count,
            )
            verify_pods_upgraded(old_images, selector=constants.MDS_APP_LABEL, count=2)
            if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
                rgw_count = get_rgw_count(
                    upgrade_version.base_version, True, version_before_upgrade
                )
                verify_pods_upgraded(
                    old_images,
                    selector=constants.RGW_APP_LABEL,
                    count=rgw_count,
                )
        if upgrade_version >= parse_version("4.6"):
            skip_metrics_exporter = upgrade_version >= parse_version("4.21") and (
                config.DEPLOYMENT.get("external_mode")
                or config.ENV_DATA.get("mcg_only_deployment")
            )
            if not skip_metrics_exporter:
                verify_pods_upgraded(
                    old_images, selector=constants.OCS_METRICS_EXPORTER
                )
            else:
                logger.info(
                    "Skipping ocs-metrics-exporter upgrade verification for ODF 4.21 "
                    "external mode deployment due to bug DFBUGS-5811"
                )

    @retry(Exception, tries=3, delay=60, backoff=1)
    def verify_noobaa_pods_upgraded(self, old_images, upgrade_version):
        """
        Verify noobaa pods are upgraded

        Args:
            old_images (set): set with old images
            upgrade_version (packaging.version.Version): version of OCS

        """
        noobaa_pods = self.get_expected_noobaa_pod_count(upgrade_version)
        noobaa_db_psql_version = get_expected_nb_db_psql_version()
        ignore_psql_12_verification = (
            int(noobaa_db_psql_version) != constants.NOOBAA_POSTGRES_12_VERSION
        )

        num_nodes = (
            config.ENV_DATA["worker_replicas"]
            + config.ENV_DATA["master_replicas"]
            + config.ENV_DATA.get("infra_replicas", 0)
        )
        timeout = 600 if num_nodes < 6 else 900

        verify_pods_upgraded(
            old_images,
            selector=constants.NOOBAA_APP_LABEL,
            count=noobaa_pods,
            timeout=timeout,
            ignore_psql_12_verification=ignore_psql_12_verification,
        )

    @retry(ValueError, tries=20, delay=30, backoff=1)
    def get_expected_noobaa_pod_count(self, upgrade_version):
        """
        Get the expected count of NooBaa pods for upgrade verification.

        This method validates that the current NooBaa pod count matches expectations
        based on deployment configuration and upgrade version.

        Args:
            upgrade_version (packaging.version.Version): version of OCS

        Returns:
            int: number of expected noobaa pods

        Raises:
            ValueError: If endpoint pod count is outside allowed range or pod count mismatch

        """
        expected_noobaa_pods = [
            "noobaa-core-0",
            "noobaa-operator",
            "noobaa-db-pg-cluster-1",
            "noobaa-db-pg-cluster-2",
        ]

        endpoint_count = 0
        noobaa_pod_obj = get_noobaa_pods()
        noobaa_pod_names = [pod.name for pod in noobaa_pod_obj]
        logger.info(f"Current noobaa pods under validation: {noobaa_pod_names}")
        for pod in noobaa_pod_obj:
            if "pv-backingstore" in pod.name:
                expected_noobaa_pods.append(pod.name)
            if upgrade_version >= parse_version("4.19"):
                if "noobaa-default-backing-store" in pod.name:
                    logger.info(
                        "In some cases like MCG only or HCI we are counting noobaa-default-backing-store"
                    )
                    expected_noobaa_pods.append(pod.name)
                if "cnpg-controller-manager" in pod.name:
                    expected_noobaa_pods.append(pod.name)
            if "noobaa-endpoint" in pod.name:
                endpoint_count += 1
                expected_noobaa_pods.append(pod.name)

        noobaa = OCP(kind="noobaa", namespace=config.ENV_DATA["cluster_namespace"])
        resource = noobaa.get()["items"][0]
        endpoints = resource.get("spec", {}).get("endpoints", {})
        max_endpoints = endpoints.get("maxCount", constants.MAX_NB_ENDPOINT_COUNT)
        min_endpoints = endpoints.get(
            "minCount", constants.MIN_NB_ENDPOINT_COUNT_POST_DEPLOYMENT
        )

        if not (min_endpoints <= endpoint_count <= max_endpoints):
            raise ValueError(
                f"Endpoint pod count {endpoint_count} not in allowed range [{min_endpoints}, {max_endpoints}]"
            )

        if len(noobaa_pod_obj) != len(expected_noobaa_pods):
            raise ValueError(
                f"Expected noobaa pods: {expected_noobaa_pods} do not match actual noobaa pods: {noobaa_pod_names}"
            )
        return len(noobaa_pod_obj)

    def get_images_post_upgrade(
        self,
        channel,
        pre_upgrade_images,
        upgrade_version,
        resource_name=OCS_OPERATOR_NAME,
    ):
        """
        Checks if all images of OCS cluster upgraded,
            and return list of all images if upgrade success

        Args:
            channel (str): OCS subscription channel
            pre_upgrade_images (dict): Contains all OCS cluster images
            upgrade_version (str): version to be upgraded
            resource_name (str, optional): Operator resource name. Defaults to OCS_OPERATOR_NAME.

        Returns:
            set: Contains full path of OCS cluster old images

        """
        operator_selector = get_selector_for_ocs_operator()
        package_manifest = PackageManifest(
            resource_name=resource_name,
            selector=operator_selector,
            subscription_plan_approval=self.subscription_plan_approval,
        )
        csv_name_post_upgrade = package_manifest.get_current_csv(channel)
        csv_post_upgrade = CSV(
            resource_name=csv_name_post_upgrade, namespace=self.namespace
        )
        logger.info(f"Waiting for CSV {csv_name_post_upgrade} to be in succeeded state")

        # Workaround for patching missing ceph-rook-tools pod after upgrade
        if self.version_before_upgrade == "4.2" and upgrade_version == "4.3":
            logger.info("Force creating Ceph toolbox after upgrade 4.2 -> 4.3")
            setup_ceph_toolbox(force_setup=True)
        # End of workaround

        if config.DEPLOYMENT.get("external_mode") or config.ENV_DATA.get(
            "mcg_only_deployment"
        ):
            timeout = 200
        else:
            timeout = 200 * get_osd_count()
        csv_post_upgrade.wait_for_phase("Succeeded", timeout=timeout)

        post_upgrade_images = get_images(csv_post_upgrade.get())
        old_images, _, _ = self.get_upgrade_image_info(
            pre_upgrade_images, post_upgrade_images
        )

        return old_images

    @staticmethod
    def get_upgrade_image_info(old_csv_images, new_csv_images):
        """
        Log the info about the images which are going to be upgraded.

        Args:
            old_csv_images (dict): dictionary with the old images from pre upgrade CSV
            new_csv_images (dict): dictionary with the post upgrade images

        Returns:
            tuple: with three sets which contain those types of images
                old_images_for_upgrade - images which should be upgraded
                new_images_to_upgrade - new images to which upgrade
                unchanged_images - unchanged images without upgrade

        """
        old_csv_images = set(old_csv_images.values())
        new_csv_images = set(new_csv_images.values())
        # Ignore the same SHA images for BZ:
        # https://bugzilla.redhat.com/show_bug.cgi?id=1994007
        for old_image in old_csv_images.copy():
            old_image_sha = None
            if constants.SHA_SEPARATOR in old_image:
                _, old_image_sha = old_image.split(constants.SHA_SEPARATOR)
            if old_image_sha:
                for new_image in new_csv_images:
                    if old_image_sha in new_image:
                        logger.info(
                            f"There is a new image: {new_image} with the same SHA "
                            f"which is the same as the old image: {old_image}. "
                            "This image will be ignored because of this BZ: 1994007"
                        )
                        old_csv_images.remove(old_image)
        old_images_for_upgrade = old_csv_images - new_csv_images
        logger.info(
            f"Old images which are going to be upgraded: "
            f"{sorted(old_images_for_upgrade)}"
        )
        new_images_to_upgrade = new_csv_images - old_csv_images
        logger.info(f"New images for upgrade: " f"{sorted(new_images_to_upgrade)}")
        unchanged_images = old_csv_images.intersection(new_csv_images)
        logger.info(f"Unchanged images after upgrade: " f"{sorted(unchanged_images)}")
        return (
            old_images_for_upgrade,
            new_images_to_upgrade,
            unchanged_images,
        )
