import logging

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework.pytest_customization.marks import (
    skipif_flexy_deployment,
    skipif_ibm_flash,
    skipif_managed_service,
)
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources.pod import get_crashcollector_pods
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


# https://github.com/red-hat-storage/ocs-ci/issues/4802
@skipif_managed_service
@skipif_flexy_deployment
@skipif_ibm_flash
@ignore_leftovers
@tier1
class TestAddNode(ManageTest):
    """
    Automates adding worker nodes to the cluster while IOs
    """

    def test_add_ocs_node(self, add_nodes):
        """
        Test to add ocs nodes, wait till rebalance is completed, verify rook-ceph-crashcollector created

        """
        nodes_added_num = 3

        worker_nodes_before_add = get_worker_nodes()

        add_nodes(node_count=nodes_added_num, ocs_nodes=True)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"

        worker_nodes_after_add = get_worker_nodes()
        new_nodes = set(worker_nodes_before_add) - set(worker_nodes_after_add)

        # to avoid newly created nodes produce crashcollectors in middle of next test, failing leftovers check,
        # wait until all the new nodes produce crashcollectors in the current test
        logger.info("verify all new nodes have crashcollectors")
        for sample in TimeoutSampler(
            timeout=60 * 5,
            sleep=5,
            func=get_crashcollector_pods,
            func_kwargs={"nodes": new_nodes},
        ):
            if len(sample) == nodes_added_num:
                break
