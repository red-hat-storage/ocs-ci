import logging

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.framework import config
from ocs_ci.ocs.node import add_new_node_and_label_it, add_new_node_and_label_upi
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated

logger = logging.getLogger(__name__)


@skipif_openshift_dedicated
@ignore_leftovers
@tier1
class TestAddNode(ManageTest):
    """
    Automates adding worker nodes to the cluster while IOs
    """

    def test_add_node(self):
        """
        Test for adding worker nodes to the cluster while IOs
        """
        new_nodes = 3
        if config.ENV_DATA["platform"].lower() in constants.CLOUD_PLATFORMS:
            dt = config.ENV_DATA["deployment_type"]
            if dt == "ipi":
                machines = machine_utils.get_machinesets()
                logger.info(
                    f"The worker nodes number before expansion {len(node.get_worker_nodes())}"
                )
                for machine in machines:
                    add_new_node_and_label_it(machine)
                logger.info(
                    f"The worker nodes number after expansion {len(node.get_worker_nodes())}"
                )

            else:
                logger.info(
                    f"The worker nodes number before expansion {len(node.get_worker_nodes())}"
                )
                if config.ENV_DATA.get("rhel_workers"):
                    node_type = constants.RHEL_OS
                else:
                    node_type = constants.RHCOS
                assert add_new_node_and_label_upi(
                    node_type, new_nodes
                ), "Add node failed"
                logger.info(
                    f"The worker nodes number after expansion {len(node.get_worker_nodes())}"
                )

        elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            logger.info(
                f"The worker nodes number before expansion {len(node.get_worker_nodes())}"
            )
            if config.ENV_DATA.get("rhel_user"):
                node_type = constants.RHEL_OS
            else:
                node_type = constants.RHCOS

            assert add_new_node_and_label_upi(node_type, new_nodes), "Add node failed"
            logger.info(
                f"The worker nodes number after expansion {len(node.get_worker_nodes())}"
            )

        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"
