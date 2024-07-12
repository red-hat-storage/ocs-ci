import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, node, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import bugzilla, magenta_squad
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    unschedule_nodes,
    drain_nodes,
    schedule_nodes,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
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

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            resource_name = constants.DEFAULT_CLUSTERNAME
            if config.DEPLOYMENT["external_mode"]:
                resource_name = constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
            storagecluster_obj = ocp.OCP(
                resource_name=resource_name,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.STORAGECLUSTER,
            )
            params = '[{"op": "remove", "path": "/spec/placement/toolbox"},]'
            storagecluster_obj.patch(params=params, format_type="json")
            log.info("Patched storage cluster  back to the default")
            time.sleep(100)
            assert (
                wait_for_pods_to_be_running()
            ), "some of the pods didn't came up running"

        request.addfinalizer(finalizer)

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
    def test_nodeaffinity_to_ceph_toolbox_with_default_taints(self):
        # This test verifies whether ceph toolbox failovered or not after applying node affinity on tainted noded
        worker_nodes = node.get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_nodes}")
        node.taint_nodes(worker_nodes)
        other_nodes = node.get_worker_node_where_ceph_toolbox_not_running()
        # Apply node affinity with a node name other than currently running node.
        assert node.apply_node_affinity_for_ceph_toolbox(other_nodes[0])

    def test_nodeaffinity_to_ceph_toolbox_with_custom_taints(self):
        #This test verifies whether ceph toolbox failovered or not after applying node affinity on custom tainted node.
        worker_nodes = node.get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_nodes}")
        # <<PR9808 yet to be merged. Once it is merged, the custom taints function need to be called here.>>
        #<<The above task can be done in another PR>>
        other_nodes = node.get_worker_node_where_ceph_toolbox_not_running()
        # Apply node affinity with a node name other than currently running node.
        assert node.apply_node_affinity_for_ceph_toolbox(other_nodes[0])

