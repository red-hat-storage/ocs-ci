import logging

from ocs_ci.ocs import ocp
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_latest_ocp_version,
    expose_ocp_version,
)
from ocs_ci.framework.testlib import ManageTest, ocp_upgrade
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor

logger = logging.getLogger(__name__)


# TODO: add image type validation (ga to ga , nightly to nightly, newer than current etc.)


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

    def test_upgrade_ocp(self):
        """
        Tests OCS stability when upgrading OCP

        """

        ceph_cluster = CephCluster()
        with CephHealthMonitor(ceph_cluster):

            ocp_upgrade_version = config.UPGRADE.get('ocp_upgrade_version')
            if not ocp_upgrade_version:
                ocp_channel = config.UPGRADE['ocp_channel']
                ocp_upgrade_version = get_latest_ocp_version(channel=ocp_channel)
                ocp_arch = config.UPGRADE['ocp_arch']
                target_image = f"{ocp_upgrade_version}-{ocp_arch}"
            elif ocp_upgrade_version.endswith(".nightly"):
                target_image = expose_ocp_version(ocp_upgrade_version)

            logger.info(f"Target image; {target_image}")

            image_path = config.UPGRADE['ocp_upgrade_path']
            self.cluster_operators = ocp.get_all_cluster_operators()
            logger.info(f" oc version: {ocp.get_current_oc_version()}")

            # Upgrade OCP
            logger.info(f"full upgrade path: {image_path}:{target_image}")
            ocp.upgrade_ocp(image=target_image, image_path=image_path)

            # Wait for upgrade
            for ocp_operator in self.cluster_operators:
                logger.info(f"Checking upgrade status of {ocp_operator}:")
                ver = ocp.get_cluster_operator_version(ocp_operator)
                logger.info(f"current {ocp_operator} version: {ver}")
                for sampler in TimeoutSampler(
                    timeout=2700,
                    sleep=60,
                    func=ocp.confirm_cluster_operator_version,
                    target_version=target_image,
                    cluster_operator=ocp_operator
                ):
                    logger.info(
                        f"ClusterOperator upgrade "
                        f"{'completed!' if sampler else 'did not completed yet!'}"
                    )
                    if sampler:
                        break
