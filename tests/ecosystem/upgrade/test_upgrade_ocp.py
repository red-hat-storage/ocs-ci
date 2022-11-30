import logging
import os

from pkg_resources import parse_version
from semantic_version import Version

from ocs_ci.ocs import ocp
from ocs_ci.ocs import constants
from ocs_ci.deployment.disconnected import mirror_ocp_release_images
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_latest_ocp_version,
    expose_ocp_version,
    ceph_health_check,
    load_config_file,
)
from ocs_ci.utility import version
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.framework.testlib import ManageTest, ocp_upgrade, ignore_leftovers
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.utility.ocp_upgrade import (
    pause_machinehealthcheck,
    resume_machinehealthcheck,
)
from ocs_ci.utility.version import (
    get_semantic_ocp_running_version,
    VERSION_4_8,
)
from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
)

logger = logging.getLogger(__name__)


@ignore_leftovers
@ocp_upgrade
@purple_squad
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
        version_before_upgrade = parse_version(
            config.DEPLOYMENT.get("installer_version")
        )
        version_post_upgrade = parse_version(ocp_upgrade_version)
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

    def test_upgrade_ocp(self, reduce_and_resume_cluster_load, setup_ui_class):
        """
        Tests OCS stability when upgrading OCP

        """

        cluster_ver = ocp.run_cmd("oc get clusterversions/version -o yaml")
        logger.debug(f"Cluster versions before upgrade:\n{cluster_ver}")
        ceph_cluster = CephCluster()
        with CephHealthMonitor(ceph_cluster):

            ocp_channel = config.UPGRADE.get(
                "ocp_channel", ocp.get_ocp_upgrade_channel()
            )
            ocp_upgrade_version = config.UPGRADE.get("ocp_upgrade_version")
            if ocp_upgrade_version:
                target_image = ocp_upgrade_version
            if not ocp_upgrade_version:
                ocp_upgrade_version = get_latest_ocp_version(channel=ocp_channel)
                ocp_arch = config.UPGRADE["ocp_arch"]
                target_image = f"{ocp_upgrade_version}-{ocp_arch}"
            elif ocp_upgrade_version.endswith(".nightly"):
                target_image = expose_ocp_version(ocp_upgrade_version)

            logger.info(f"Target image: {target_image}")

            image_path = config.UPGRADE["ocp_upgrade_path"]
            cluster_operators = ocp.get_all_cluster_operators()
            logger.info(f" oc version: {ocp.get_current_oc_version()}")
            # disconnected environment prerequisites
            if config.DEPLOYMENT.get("disconnected"):
                # mirror OCP release images to mirror registry
                image_path, target_image = mirror_ocp_release_images(
                    image_path, target_image
                )

            # Verify Upgrade subscription channel:
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
            if get_semantic_ocp_running_version() > VERSION_4_8:
                pause_machinehealthcheck()

            # Before upgrading OCP, login to the OCP console and look for any pop-up so as to refresh the console
            version_post_upgrade = version.get_semantic_version(
                ocp_upgrade_version, True
            )
            if version_post_upgrade >= version.VERSION_4_9:
                validation_ui_obj = ValidationUI(setup_ui_class)
                validation_ui_obj.refresh_web_console()

            # Upgrade OCP
            logger.info(f"full upgrade path: {image_path}:{target_image}")
            ocp.upgrade_ocp(image=target_image, image_path=image_path)

            # Wait for upgrade
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
                ver = ocp.get_cluster_operator_version(ocp_operator)
                logger.info(f"current {ocp_operator} version: {ver}")
                for sampler in TimeoutSampler(
                    timeout=4000,
                    sleep=60,
                    func=ocp.confirm_cluster_operator_version,
                    target_version=target_image,
                    cluster_operator=ocp_operator,
                ):
                    if sampler:
                        logger.info(f"{ocp_operator} upgrade completed!")
                        break
                    else:
                        logger.info(f"{ocp_operator} upgrade did not completed yet!")

            # resume a MachineHealthCheck resource
            if get_semantic_ocp_running_version() > VERSION_4_8:
                resume_machinehealthcheck()

            # post upgrade validation: check cluster operator status
            cluster_operators = ocp.get_all_cluster_operators()
            for ocp_operator in cluster_operators:
                logger.info(f"Checking cluster status of {ocp_operator}")
                for sampler in TimeoutSampler(
                    timeout=2700,
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
            for sampler in TimeoutSampler(
                timeout=900, sleep=15, func=ocp.validate_cluster_version_status
            ):
                if sampler:
                    logger.info("Upgrade Completed Successfully!")
                    break

        cluster_ver = ocp.run_cmd("oc get clusterversions/version -o yaml")
        logger.debug(f"Cluster versions post upgrade:\n{cluster_ver}")

        # Login to OCP console and run ODF dashboard validation
        version_post_upgrade = version.get_semantic_version(ocp_upgrade_version, True)
        if version_post_upgrade >= version.VERSION_4_9:
            validation_ui_obj = ValidationUI(setup_ui_class)
            validation_ui_obj.refresh_web_console()
            validation_ui_obj.odf_console_plugin_check()
            validation_ui_obj.odf_overview_ui()
            validation_ui_obj.odf_storagesystems_ui()

        # load new config file
        self.load_ocp_version_config_file(ocp_upgrade_version)

        if not config.ENV_DATA["mcg_only_deployment"]:
            new_ceph_cluster = CephCluster()
            # Increased timeout because of this bug:
            # https://bugzilla.redhat.com/show_bug.cgi?id=2038690
            new_ceph_cluster.wait_for_rebalance(timeout=3000)
            ceph_health_check(tries=160, delay=30)
