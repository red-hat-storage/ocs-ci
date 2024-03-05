import logging
import pytest

from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.helpers.helpers import verify_pdb_mon, check_number_of_mon_pods
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_operator_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4b,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
    runs_on_provider,
)

log = logging.getLogger(__name__)


@brown_squad
@tier4b
@skipif_external_mode
@skipif_ocs_version("<4.6")
@bugzilla("1959983")
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2572")
@runs_on_provider
class TestDrainNodeMon(ManageTest):
    """
    1.Get worker node name where monitoring pod run
    2.Verify pdb status, disruptions_allowed=1, max_unavailable_mon=1
    3.Drain node where monitoring pod run
    4.Verify pdb status, disruptions_allowed=0, max_unavailable_mon=1
    5.Verify the number of mon pods is 3 for (1400 seconds)
    6.Respin  rook-ceph operator pod
    7.Uncordon the node
    8.Wait for all the pods in openshift-storage to be running
    9.Verify pdb status, disruptions_allowed=1, max_unavailable_mon=1

    """

    def test_rook_operator_restart_during_mon_failover(self, node_drain_teardown):
        """
        Verify the number of monitoring pod is three when drain node

        """
        sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=verify_pdb_mon,
            disruptions_allowed=1,
            max_unavailable_mon=1,
        )
        if not sample.wait_for_func_status(result=True):
            assert "the expected pdb state is not equal to actual pdb state"

        log.info("Get worker node name where monitoring pod run")
        mon_pod_objs = get_mon_pods()
        node_name = mon_pod_objs[0].data["spec"]["nodeName"]

        drain_nodes([node_name])

        sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=verify_pdb_mon,
            disruptions_allowed=0,
            max_unavailable_mon=1,
        )
        if not sample.wait_for_func_status(result=True):
            assert "the expected pdb state is not equal to actual pdb state"

        timeout = 1400
        log.info(f"Verify the number of mon pods is 3 for {timeout} seconds")
        sample = TimeoutSampler(
            timeout=timeout, sleep=10, func=check_number_of_mon_pods
        )
        if sample.wait_for_func_status(result=False):
            assert "There are more than 3 mon pods."

        log.info("Respin pod rook-ceph operator pod")
        rook_ceph_operator_pod_obj = get_operator_pods()
        rook_ceph_operator_pod_obj[0].delete()

        schedule_nodes([node_name])

        log.info("Wait for all the pods in openshift-storage to be running.")
        assert wait_for_pods_to_be_running(timeout=480, sleep=20)

        sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=verify_pdb_mon,
            disruptions_allowed=1,
            max_unavailable_mon=1,
        )
        if not sample.wait_for_func_status(result=True):
            assert "the expected pdb state is not equal to actual pdb state"

        ceph_health_check()

        assert check_number_of_mon_pods(), "The number of mon pods not equal to 3"
