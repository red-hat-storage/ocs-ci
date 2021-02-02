import logging

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
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

    def test_add_ocs_node(self, add_nodes):
        """
        Test to add ocs nodes and wait till rebalance is completed

        """

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

        add_nodes(ocs_nodes=True)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"
