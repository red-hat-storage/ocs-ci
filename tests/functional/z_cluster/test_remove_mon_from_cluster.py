"""
A Testcase to remove mon from
when I/O's are happening.

Polarion-ID- OCS-355

"""

import logging
import pytest
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import ManageTest, ignore_leftovers
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    run_io_with_rados_bench,
    delete_cephblockpools,
    create_ceph_block_pool,
)
from ocs_ci.ocs.cluster import CephCluster


log = logging.getLogger(__name__)


def verify_mon_pod_up(pods):
    """
    Verify mon pods are in Running state.

    Returns:
        bool: True for wait for the resource, False otherwise

    """
    log.info("Verifying all mons pods are up and Running")
    ret = pods.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector="app=rook-ceph-mon",
        resource_count=3,
        timeout=700,
    )
    log.info(f"waited for all mon pod to come up and running {ret}")
    return ret


def run_io_on_pool(pool_obj):
    """
    Runs the I/O on the pool and delete the pool

    Returns: A thread of I/O
    """
    tools_pod = pod.get_ceph_tools_pod()
    tools_pod.add_role(role="client")

    return run_io_with_rados_bench(
        ceph_pods=[tools_pod],
        config={"time": 45, "cleanup": False, "pool": pool_obj.name},
    )


# Test case is disabled, as per requirement not to support this scenario (PR 2025)
# tier4
# tier4c
@ignore_leftovers
@brown_squad
# @pytest.mark.polarion_id("OCS-355")
class TestRemoveMonFromCluster(ManageTest):
    pool_obj = ""

    def test_remove_mon_pod_from_cluster(self):
        """
        To remove mon pod from the cluster
        after the I/O is performed on the pool
        and waiting for the operator to create a
        new mon pod on its own

        """
        ceph_cluster = CephCluster()
        pods = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        list_mons = ceph_cluster.get_mons_from_cluster()
        assert len(list_mons) > 1, pytest.skip(
            "INVALID: Mon count should be more than one to delete."
        )
        self.pool_obj = create_ceph_block_pool()
        assert run_io_on_pool(self.pool_obj), "Failed to run I/O on the pool"
        assert delete_cephblockpools([self.pool_obj]), "Failed to delete pool"
        ceph_cluster.cluster_health_check(timeout=0)
        ceph_cluster.remove_mon_from_cluster()
        assert verify_mon_pod_up(pods), "Mon pods are not up and running state"
        ceph_cluster.cluster_health_check(timeout=60)
