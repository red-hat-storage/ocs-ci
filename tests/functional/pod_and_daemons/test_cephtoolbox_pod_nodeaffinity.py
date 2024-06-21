import logging
import pytest
import time

from ocs_ci.ocs import node
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import bugzilla, magenta_squad
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    unschedule_nodes,
    drain_nodes,
    schedule_nodes,
)

log = logging.getLogger(__name__)


@tier1
@magenta_squad
@bugzilla("2249640")
class TestCephtoolboxPod:
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_node_affinity_to_ceph_toolbox_pod(self):
        # This test verifies whether ceph toolbox failovered or not after applying node affinity
        other_nodes = node.get_worker_node_where_ceph_toolbox_not_running()
        # Apply node affinity with a node name other than currently running node.
        assert node.apply_node_affinity_for_ceph_toolbox(other_nodes[0])

    def test_reboot_node_affinity_node(self):
        # This test verifies ceph toolbox runs only on the node given in node-affility.
        # Reboot the node after applying node-affinity.
        # Expectation is the pod should come up only on that node mentioned in affinity.

        other_nodes = node.get_worker_node_where_ceph_toolbox_not_running()
        node.apply_node_affinity_for_ceph_toolbox(other_nodes[0])

        node_name = other_nodes[0]

        # Unschedule ceph tool box running node.
        unschedule_nodes([node_name])
        log.info(f"node {node_name} unscheduled successfully")

        # Drain node operation
        drain_nodes([node_name])
        log.info(f"node {node_name} drained successfully")

        # Make the node schedule-able
        schedule_nodes([node_name])
        log.info(f"Scheduled the node {node_name}")
        log.info(
            "Script will sleep for 3 minutes before validating the ceph toolbox running node"
        )
        time.sleep(180)

        ct_pod = pod.get_ceph_tools_pod()
        # Identify on which node the ceph toolbox is running after node drain
        ct_pod_running_node_name = ct_pod.data["spec"].get("nodeName")
        if node_name == ct_pod_running_node_name:
            log.info(
                f"ceph toolbox pod is running only on a node {ct_pod_running_node_name} which is in node-affinity"
            )
            assert True
