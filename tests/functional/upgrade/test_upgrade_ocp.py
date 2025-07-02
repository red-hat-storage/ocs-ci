import logging
import os

import pytest

from semantic_version import Version

from ocs_ci.ocs import ocp
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CephHealthException
from ocs_ci.ocs.ocp import check_cluster_operator_versions
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.deployment.disconnected import mirror_ocp_release_images
from ocs_ci.framework import config
from ocs_ci.utility.rosa import upgrade_rosa_cluster
from ocs_ci.utility.utils import (
    archive_ceph_crashes,
    ceph_crash_info_display,
    TimeoutSampler,
    get_latest_ocp_version,
    expose_ocp_version,
    ceph_health_check,
    load_config_file,
)
from ocs_ci.utility.version import get_semantic_version
from ocs_ci.framework.testlib import ManageTest, ocp_upgrade, ignore_leftovers
from ocs_ci.ocs.cluster import (
    CephCluster,
    CephClusterMultiCluster,
    CephHealthMonitor,
    MulticlusterCephHealthMonitor,
)
from ocs_ci.ocs.utils import is_acm_cluster, get_non_acm_cluster_config
from ocs_ci.utility.ocp_upgrade import (
    pause_machinehealthcheck,
    resume_machinehealthcheck,
)
from ocs_ci.utility.multicluster import MDRClusterUpgradeParametrize
from ocs_ci.utility.version import (
    get_semantic_ocp_running_version,
    VERSION_4_8,
    get_latest_rosa_ocp_version,
    ocp_version_available_on_rosa,
    drop_z_version,
)
from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    multicluster_roles,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def config_index(request):
    return request.param if hasattr(request, "param") else None


@ignore_leftovers
@ocp_upgrade
@purple_squad
@multicluster_roles(["mdr-all-ocp"])
class TestUpgradeOCP(ManageTest):
    """
    1. check cluster health
    2. check OCP version
    3. perform OCP upgrade
    4. check all OCP ClusterOperators
    5. check OCP version
    5. monitor cluster health
    """

    def load_ocp_version_config_file(self, ocp_upgrade_version):
        """
        Loads config file to the ocs-ci config with upgrade version

        Args:
            ocp_upgrade_version (str): version to be upgraded

        """

        version = Version.coerce(ocp_upgrade_version)
        short_ocp_upgrade_version = ".".join([str(version.major), str(version.minor)])
        version_before_upgrade = get_semantic_version(
            config.DEPLOYMENT.get("installer_version")
        )
        version_post_upgrade = get_semantic_version(ocp_upgrade_version)
        version_change = version_post_upgrade > version_before_upgrade
        if version_change:
            version_config_file = os.path.join(
                constants.OCP_VERSION_CONF_DIR,
                f"ocp-{short_ocp_upgrade_version}-config.yaml",
            )
            logger.debug(f"config file to be loaded: {version_config_file}")
            load_config_file(version_config_file)
        else:
            logger.info(
                f"Upgrade version {version_post_upgrade} is not higher than old version:"
                f" {version_before_upgrade}, new config file will not be loaded"
            )

    def test_upgrade_ocp(
        self, zone_rank, role_rank, config_index, reduce_and_resume_cluster_load
    ):
        """
        Tests OCS stability when upgrading OCP

        """

        cluster_ver = ocp.run_cmd("oc get clusterversions/version -o yaml")
        logger.debug(f"Cluster versions before upgrade:\n{cluster_ver}")
        if (
            config.multicluster
            and config.MULTICLUSTER["multicluster_mode"] == "metro-dr"
            and is_acm_cluster(config)
        ):
            # Find the ODF cluster in current zone
            mdr_upgrade = MDRClusterUpgradeParametrize()
            mdr_upgrade.config_init()
            local_zone_odf = None
            for cluster in get_non_acm_cluster_config():
                if config.ENV_DATA["zone"] == cluster.ENV_DATA["zone"]:
                    local_zone_odf = cluster
            ceph_cluster = CephClusterMultiCluster(local_zone_odf)
            health_monitor = MulticlusterCephHealthMonitor
        else:
            ceph_cluster = CephCluster()
            health_monitor = CephHealthMonitor

        with health_monitor(ceph_cluster):
            ocp_channel = config.UPGRADE.get(
                "ocp_channel", ocp.get_ocp_upgrade_channel()
            )
            logger.info(f"OCP Channel: {ocp_channel}")

            ocp_upgrade_version = config.UPGRADE.get("ocp_upgrade_version")
            logger.info(f"OCP upgrade version: {ocp_upgrade_version}")

            provider_cluster = (
                config.ENV_DATA.get("cluster_type").lower() == constants.HCI_PROVIDER
            )

            rosa_platform = (
                config.ENV_DATA["platform"].lower() in constants.ROSA_PLATFORMS
            )

            if rosa_platform:
                # Handle ROSA-specific upgrade logic
                # On ROSA environment, Nightly builds are not supported.
                # rosa cli uses only "X.Y.Z" format for the version (builds and images are not supported)
                # If not provided ocp_upgrade_version - get the latest released version of the channel.
                # If provided - check availability and use the provided version in format "X.Y.Z"
                if ocp_upgrade_version and ocp_version_available_on_rosa(
                    ocp_upgrade_version
                ):
                    target_image = ocp_upgrade_version
                else:
                    latest_ocp_ver = get_latest_ocp_version(channel=ocp_channel)
                    # check, if ver is not available on rosa then get the latest version available on ROSA
                    if not ocp_version_available_on_rosa(latest_ocp_ver):
                        version_major_minor = drop_z_version(latest_ocp_ver)
                        latest_ocp_ver = get_latest_rosa_ocp_version(
                            version_major_minor
                        )
                    target_image = latest_ocp_ver
            else:
                # Handle non-ROSA upgrade logic
                if ocp_upgrade_version:
                    target_image = (
                        expose_ocp_version(ocp_upgrade_version)
                        if ocp_upgrade_version.endswith(".nightly")
                        else ocp_upgrade_version
                    )
                else:
                    ocp_upgrade_version = get_latest_ocp_version(channel=ocp_channel)
                    ocp_arch = config.UPGRADE["ocp_arch"]
                    target_image = f"{ocp_upgrade_version}-{ocp_arch}"
            logger.info(f"Target image: {target_image}")

            image_path = config.UPGRADE["ocp_upgrade_path"]
            cluster_operators = ocp.get_all_cluster_operators()
            logger.info(f" oc version: {ocp.get_current_oc_version()}")
            # disconnected environment prerequisites
            if config.DEPLOYMENT.get("disconnected"):
                # mirror OCP release images to mirror registry
                image_path, target_image, _, _ = mirror_ocp_release_images(
                    image_path, target_image
                )

            # Verify Upgrade subscription channel:
            if not rosa_platform:
                ocp.patch_ocp_upgrade_channel(ocp_channel)
                for sampler in TimeoutSampler(
                    timeout=250,
                    sleep=15,
                    func=ocp.verify_ocp_upgrade_channel,
                    channel_variable=ocp_channel,
                ):
                    if sampler:
                        logger.info(f"OCP Channel:{ocp_channel}")
                        break

                # pause a MachineHealthCheck resource
                # no machinehealthcheck on ROSA
                if get_semantic_ocp_running_version() > VERSION_4_8:
                    pause_machinehealthcheck()

                logger.info(f"full upgrade path: {image_path}:{target_image}")
                ocp.upgrade_ocp(image=target_image, image_path=image_path)
            else:
                logger.info(f"upgrade rosa cluster to target version: '{target_image}'")
                upgrade_rosa_cluster(config.ENV_DATA["cluster_name"], target_image)

            # Wait for upgrade
            # ROSA Upgrades Are Controlled by the Hive Operator
            # HCP Clusters use a Control Plane Queue to manage the upgrade process
            # upgrades on ROSA clusters does not start immediately after the upgrade command but scheduled
            num_nodes = (
                config.ENV_DATA["worker_replicas"]
                + config.ENV_DATA["master_replicas"]
                + config.ENV_DATA.get("infra_replicas", 0)
            )
            operator_upgrade_timeout = 4000
            if rosa_platform or num_nodes >= 6:
                operator_upgrade_timeout = 8000
            for ocp_operator in cluster_operators:
                logger.info(f"Checking upgrade status of {ocp_operator}:")
                # ############ Workaround for issue 2624 #######
                name_changed_between_versions = (
                    "service-catalog-apiserver",
                    "service-catalog-controller-manager",
                )
                if ocp_operator in name_changed_between_versions:
                    logger.info(f"{ocp_operator} upgrade will not be verified")
                    continue
                # ############ End of Workaround ###############
                if ocp_operator == "aro":
                    logger.debug(
                        f"{ocp_operator} do not match with OCP upgrade, check will be ignored!"
                    )
                    continue
                ver = ocp.get_cluster_operator_version(ocp_operator)
                logger.info(f"current {ocp_operator} version: {ver}")
                check_cluster_operator_versions(target_image, operator_upgrade_timeout)

            # resume a MachineHealthCheck resource
            if get_semantic_ocp_running_version() > VERSION_4_8 and not rosa_platform:
                if provider_cluster:
                    resume_machinehealthcheck(
                        wait_for_mcp_complete=True, force_delete_pods=True
                    )
                else:
                    resume_machinehealthcheck()

            # post upgrade validation: check cluster operator status
            operator_ready_timeout = 5400
            cluster_operators = ocp.get_all_cluster_operators()
            for ocp_operator in cluster_operators:
                logger.info(f"Checking cluster status of {ocp_operator}")
                for sampler in TimeoutSampler(
                    timeout=operator_ready_timeout,
                    sleep=60,
                    func=ocp.verify_cluster_operator_status,
                    cluster_operator=ocp_operator,
                ):
                    if sampler:
                        break
                    else:
                        logger.info(f"{ocp_operator} status is not valid")
            # Post upgrade validation: check cluster version status
            logger.info("Checking clusterversion status")
            cluster_version_timeout = 1800
            for sampler in TimeoutSampler(
                timeout=cluster_version_timeout,
                sleep=15,
                func=ocp.validate_cluster_version_status,
            ):
                if sampler:
                    logger.info("Upgrade Completed Successfully!")
                    break

        cluster_ver = ocp.run_cmd("oc get clusterversions/version -o yaml")
        logger.debug(f"Cluster versions post upgrade:\n{cluster_ver}")

        # load new config file
        self.load_ocp_version_config_file(ocp_upgrade_version)

        if not config.ENV_DATA["mcg_only_deployment"] and not config.multicluster:
            new_ceph_cluster = CephCluster()
            # Increased timeout because of this bug:
            # https://bugzilla.redhat.com/show_bug.cgi?id=2038690
            new_ceph_cluster.wait_for_rebalance(timeout=3000)
            ct_pod = get_ceph_tools_pod()
            try:
                ceph_health_check(tries=240, delay=30)
            except CephHealthException as err:
                if "daemons have recently crashed" in str(err):
                    logger.error(err)
                    ceph_crash_info_display(ct_pod)
                    archive_ceph_crashes(ct_pod)
                raise err
