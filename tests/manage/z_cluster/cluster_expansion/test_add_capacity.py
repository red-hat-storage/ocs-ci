from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id, skipif_lso_deployment
)
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier1
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster import CephCluster


@skipif_lso_deployment
@ignore_leftovers
@tier1
@polarion_id('OCS-1191')
class TestAddCapacity(ManageTest):
    """
    Automates adding variable capacity to the cluster while IOs running
    """
    def test_add_capacity(self):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        self.ceph_cluster = CephCluster()
        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )
        self.ceph_cluster.cluster_health_check(timeout=1200)
