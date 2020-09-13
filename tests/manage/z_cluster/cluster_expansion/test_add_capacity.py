import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id, pre_upgrade, skipif_aws_i3, skipif_bm, skipif_external_mode
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import CephCluster


def add_capacity_test():
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

    # Verify status of rook-ceph-osd-prepare pods. Verifies bug 1769061
    # pod.wait_for_resource(
    #     timeout=300,
    #     condition=constants.STATUS_COMPLETED,
    #     selector=constants.OSD_PREPARE_APP_LABEL,
    #     resource_count=result * 3
    # )
    # Commented this lines as a workaround due to bug 1842500

    ceph_health_check(
        namespace=config.ENV_DATA['cluster_namespace'], tries=80
    )
    ceph_cluster_obj = CephCluster()
    assert ceph_cluster_obj.wait_for_rebalance(timeout=5400), (
        "Data re-balance failed to complete"
    )


@ignore_leftovers
@tier1
@polarion_id('OCS-1191')
@pytest.mark.last
@skipif_aws_i3
@skipif_bm
@skipif_external_mode
class TestAddCapacity(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """
    def test_add_capacity(self, reduce_cluster_load):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        add_capacity_test()


@skipif_ocs_version('<4.4')
@pre_upgrade
@ignore_leftovers
@polarion_id('OCS-1191')
@skipif_aws_i3
@skipif_bm
@skipif_external_mode
class TestAddCapacityPreUpgrade(ManageTest):
    """
    Automates adding variable capacity to the cluster pre upgrade
    """
    def test_add_capacity_pre_upgrade(self, reduce_cluster_load):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        add_capacity_test()
