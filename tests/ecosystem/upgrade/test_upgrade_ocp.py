import logging

from ocs_ci.ocs import ocp
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_latest_ocp_version,
    expose_ocp_version,
    ceph_health_check
)
from ocs_ci.framework.testlib import ManageTest, ocp_upgrade, ignore_leftovers
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor

logger = logging.getLogger(__name__)


@ignore_leftovers
@ocp_upgrade
class TestUpgradeOCP(ManageTest):
    """
    1. check cluster health
    2. check OCP version
    3. perform OCP upgrade
    4. check all OCP ClusterOperators
    5. check OCP version
    5. monitor cluster health
    """

    def test_upgrade_ocp(self, reduce_cluster_load):
        """
        Tests OCS stability when upgrading OCP

        """

        ceph_cluster = CephCluster()
        with CephHealthMonitor(ceph_cluster):

            ocp_channel = config.UPGRADE.get(
                'ocp_channel', ocp.get_ocp_upgrade_channel()
            )
            ocp_upgrade_version = config.UPGRADE.get('ocp_upgrade_version')
            if not ocp_upgrade_version:
                ocp_upgrade_version = get_latest_ocp_version(channel=ocp_channel)
                ocp_arch = config.UPGRADE['ocp_arch']
                target_image = f"{ocp_upgrade_version}-{ocp_arch}"
            elif ocp_upgrade_version.endswith(".nightly"):
                target_image = expose_ocp_version(ocp_upgrade_version)

            logger.info(f"Target image; {target_image}")

            image_path = config.UPGRADE['ocp_upgrade_path']
            cluster_operators = ocp.get_all_cluster_operators()
            logger.info(f" oc version: {ocp.get_current_oc_version()}")
            # Verify Upgrade subscription channel:
            ocp.patch_ocp_upgrade_channel(ocp_channel)
            for sampler in TimeoutSampler(
                timeout=250,
                sleep=15,
                func=ocp.verify_ocp_upgrade_channel,
                channel_variable=ocp_channel
            ):
                if sampler:
                    logger.info(f"OCP Channel:{ocp_channel}")
                    break

            # Upgrade OCP
            logger.info(f"full upgrade path: {image_path}:{target_image}")
            ocp.upgrade_ocp(image=target_image, image_path=image_path)

            # Wait for upgrade
            for ocp_operator in cluster_operators:
                logger.info(f"Checking upgrade status of {ocp_operator}:")
                # ############ Workaround for issue 2624 #######
                name_changed_between_versions = (
                    'service-catalog-apiserver', 'service-catalog-controller-manager'
                )
                if ocp_operator in name_changed_between_versions:
                    logger.info(f"{ocp_operator} upgrade will not be verified")
                    continue
                # ############ End of Workaround ###############
                ver = ocp.get_cluster_operator_version(ocp_operator)
                logger.info(f"current {ocp_operator} version: {ver}")
                for sampler in TimeoutSampler(
                    timeout=2700,
                    sleep=60,
                    func=ocp.confirm_cluster_operator_version,
                    target_version=target_image,
                    cluster_operator=ocp_operator
                ):
                    if sampler:
                        logger.info(f"{ocp_operator} upgrade completed!")
                        break
                    else:
                        logger.info(f"{ocp_operator} upgrade did not completed yet!")

            # post upgrade validation: check cluster operator status
            cluster_operators = ocp.get_all_cluster_operators()
            for ocp_operator in cluster_operators:
                logger.info(f"Checking cluster status of {ocp_operator}")
                for sampler in TimeoutSampler(
                    timeout=2700,
                    sleep=60,
                    func=ocp.verify_cluster_operator_status,
                    cluster_operator=ocp_operator
                ):
                    if sampler:
                        break
                    else:
                        logger.info(f"{ocp_operator} status is not valid")
            # Post upgrade validation: check cluster version status
            logger.info("Checking clusterversion status")
            for sampler in TimeoutSampler(
                timeout=900,
                sleep=15,
                func=ocp.validate_cluster_version_status
            ):
                if sampler:
                    logger.info("Upgrade Completed Successfully!")
                    break

        new_ceph_cluster = CephCluster()
        new_ceph_cluster.wait_for_rebalance(timeout=1800)
        ceph_health_check(tries=90, delay=30)
