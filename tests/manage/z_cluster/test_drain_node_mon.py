import logging

from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.resources.pod import get_mon_pods, get_pod_obj
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.helpers.helpers import verify_pdb_mon
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4a,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
)

log = logging.getLogger(__name__)


@tier4a
@skipif_external_mode
@skipif_ocs_version("<4.6")
@bugzilla("1959983")
@ignore_leftovers
class TestDrainNodeMon(ManageTest):
    """
    1.Get worker node name where monitoring pod run
    2.Verify pdb status, disruptions_allowed=1, max_unavailable_mon=1
    3.Drain node where monitoring pod run
    4.Verify pdb status, disruptions_allowed=0, max_unavailable_mon=1
    5.Verify the number of mon pods is 3 for (1400 seconds)
    6.Respin  rook-ceph operator pod
    7.Change node to be scheduled
    8.Wait for mon and osd pods to be on running state
    9.Verify pdb status, disruptions_allowed=1, max_unavailable_mon=1

    """

    def test_drain_node_mon(self, node_drain_teardown):
        """
        Verify the number of monitoring pod is three when drain node

        """
        verify_pdb_mon(disruptions_allowed=1, max_unavailable_mon=1)

        log.info("Get worker node name where monitoring pod run")
        mon_pod_objs = get_mon_pods()
        node_name = mon_pod_objs[0].data["spec"]["nodeName"]

        drain_nodes([node_name])

        verify_pdb_mon(disruptions_allowed=0, max_unavailable_mon=1)

        log.info("Verify the number of mon pods is 3")
        sample = TimeoutSampler(timeout=1400, sleep=10, func=self.check_mon_pods_eq_3)
        if sample.wait_for_func_status(result=True):
            assert "There are more than 3 mon pods."

        log.info("Respin pod rook-ceph operator pod")
        rook_ceph_operator_name = get_pod_name_by_pattern("rook-ceph-operator")
        rook_ceph_operator_obj = get_pod_obj(name=rook_ceph_operator_name[0])
        rook_ceph_operator_obj.delete()

        schedule_nodes([node_name])

        logging.info("Wait for mon and osd pods to be on running state")
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.MON_APP_LABEL,
            resource_count=3,
            timeout=100,
        )
        assert pod.wait_for_resource(
            condition="Running",
            selector=constants.OSD_APP_LABEL,
            resource_count=3,
            timeout=100,
        )
        verify_pdb_mon(disruptions_allowed=1, max_unavailable_mon=1)

    def check_mon_pods_eq_3(self):
        """
        Get number of monitoring pods

        """
        mon_pod_list = get_mon_pods()
        if len(mon_pod_list) == 3:
            return False
        else:
            log.info(f"There are {len(mon_pod_list)} mon pods")
            for mon_pod in mon_pod_list:
                log.info(f"{mon_pod.name}")
            return True
