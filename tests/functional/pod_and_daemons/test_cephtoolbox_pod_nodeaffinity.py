import logging
import pytest
import random

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import bugzilla, brown_squad
from ocs_ci.framework.testlib import tier1, tier4b, polarion_id
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.node import (
    apply_node_affinity_for_ceph_toolbox,
    check_taint_on_nodes,
    drain_nodes,
    get_ceph_tools_running_node,
    get_worker_nodes,
    get_worker_node_where_ceph_toolbox_not_running,
    schedule_nodes,
    taint_nodes,
    unschedule_nodes,
    untaint_nodes,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running

log = logging.getLogger(__name__)


@brown_squad
@bugzilla("2249640")
class TestCephtoolboxPod:
    @pytest.fixture(scope="session", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            Finalizer will take care of below activities:
            1. Untaint the nodes: remove taints from nodes
            2. Removes nodeaffinity to bring storage cluster with default values.

            """
            if check_taint_on_nodes():
                untaint_nodes()
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
            assert (
                wait_for_pods_to_be_running()
            ), "some of the pods didn't came up running"

        request.addfinalizer(finalizer)

    @tier1
    @polarion_id("OCS-6086")
    def test_node_affinity_to_ceph_toolbox_pod(self):
        """
        This test verifies whether ceph toolbox failovered or not after applying node affinity

        """
        other_nodes = get_worker_node_where_ceph_toolbox_not_running()
        other_node_name = random.choice(other_nodes)
        log.info(
            "Apply node affinity with a node name other than currently running node."
        )
        assert apply_node_affinity_for_ceph_toolbox(
            other_node_name
        ), "Failed to apply node affinity for the Ceph toolbox on the specified node."

    @tier4b
    @polarion_id("OCS-6087")
    def test_reboot_node_affinity_node(self):
        """
        This test verifies ceph toolbox runs only on the node given in node-affinity.
        Reboot the node after applying node-affinity.
        Expectation is the pod should come up only on that node mentioned in affinity.

        """
        other_nodes = get_worker_node_where_ceph_toolbox_not_running()
        node_name = random.choice(other_nodes)
        apply_node_affinity_for_ceph_toolbox(node_name)
        log.info("Unschedule ceph tools pod running node.")
        unschedule_nodes([node_name])
        log.info(f"node {node_name} unscheduled successfully")
        drain_nodes([node_name])
        log.info(f"node {node_name} drained successfully")
        schedule_nodes([node_name])
        log.info(f"Scheduled the node {node_name}")
        log.info("Identify on which node the ceph toolbox is running after node drain")
        ct_pod_running_node_name = get_ceph_tools_running_node()
        if node_name == ct_pod_running_node_name:
            log.info(
                f"ceph toolbox pod is running on a node {node_name} which is in node-affinity"
            )
        else:
            log.error(
                f"Ceph toolbox pod is not running on the nodeAffinity given node {node_name}."
            )
            assert False, "Ceph toolbox pod is not on the expected node."

    @tier4b
    @polarion_id("OCS-6090")
    def test_nodeaffinity_to_ceph_toolbox_with_default_taints(self):
        """
        This test verifies whether ceph toolbox failovered or not after applying node affinity on tainted node

        """
        worker_nodes = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_nodes}")
        taint_nodes(worker_nodes)
        log.info("Applied default taints on all the worker nodes")
        other_nodes = get_worker_node_where_ceph_toolbox_not_running()
        other_node_name = random.choice(other_nodes)
        log.info(
            "Apply node affinity with a node name other than currently running node."
        )
        assert apply_node_affinity_for_ceph_toolbox(
            other_node_name
        ), "Failed to apply nodeaffinity with default taints"
