"""
A Testcase to remove mon from
when I/O's are happening.

Polarion-ID- OCS-355

"""

import logging
import re
import random
import time
import pytest
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import tier4, ManageTest
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod
from tests.helpers import run_io_with_rados_bench, delete_cephblockpool
from ocs_ci.ocs.cluster import CephCluster


log = logging.getLogger(__name__)

DEP = ocp.OCP(
    kind=constants.DEPLOYMENT, namespace=config.ENV_DATA['cluster_namespace']
)
POD = ocp.OCP(
    kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
)


def get_mons_from_cluster():
    """
    Getting the list of mons from the cluster

    Returns:
        available_mon (list): Returns the mons from the cluster
    """

    ret = DEP.get(
        resource_name='', out_yaml_format=False, selector='app=rook-ceph-mon'
    )
    available_mon = re.findall(r'[\w-]+mon-+[\w-]', ret)
    return available_mon


def remove_mon_from_cluster(mon):
    """
    Removing the mon pod from deployment
    """
    assert DEP.delete(resource_name=mon), f"Failed to delete mon {mon}"


def verify_mon_pod_up():
    """
    Verify mon pods are in Running state.

    Returns:
        bool: True for wait for the resource, False otherwise

    """
    log.info(f"Verifying all mons pods are up and Running")
    time.sleep(15)
    ret = POD.wait_for_resource(
        condition=constants.STATUS_RUNNING, selector='app=rook-ceph-mon',
        resource_count=3, timeout=700)
    log.info(f"waited for all mon pod to come up and running {ret}")
    return ret


def run_io_on_pool():
    """
    Runs the I/O on the pool and delete the pool

    Returns: A thread of I/O
    """
    tools_pod = pod.get_ceph_tools_pod()
    tools_pod.add_role(role='client')

    return run_io_with_rados_bench(
        ceph_pods=[tools_pod],
        config={'time': 45, 'cleanup': False,
                'pool': 'test-pool'
                }
    )


@tier4
class TestOcs355(ManageTest):

    def test_remove_mon_pod_from_cluster(self):
        """
        To remove mon pod from the cluster
        after the I/O is performed on the pool
        and waiting for the operator to create a
        new mon pod on its own

        """
        health = CephCluster()
        list_mons = get_mons_from_cluster()
        assert len(list_mons) > 1, pytest.skip("INVALID: Mon count should "
                                               "be more than one to delete.")
        assert run_io_on_pool(), 'Failed to run I/O on the pool'
        assert delete_cephblockpool('test-pool'), 'Failed to delete pool'
        health.cluster_health_check(timeout=0)
        remove_mon_from_cluster(random.choice(list_mons))
        assert verify_mon_pod_up(), f"Mon pods are not up and running state"
        health.cluster_health_check(timeout=60)
