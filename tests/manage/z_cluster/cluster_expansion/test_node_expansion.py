import logging

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework.pytest_customization.marks import (
    skipif_openshift_dedicated,
    skipif_rosa,
    skipif_flexy_deployment,
)

logger = logging.getLogger(__name__)


# https://github.com/red-hat-storage/ocs-ci/issues/4802
@skipif_flexy_deployment
@skipif_openshift_dedicated
@skipif_rosa
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
        add_nodes(ocs_nodes=True)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"
