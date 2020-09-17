import logging
import pytest
import time

from ocs_ci.ocs import constants, defaults
from ocs_ci.framework.testlib import (
    ManageTest, tier4c, ignore_leftovers
)
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    wait_for_nodes_status, get_node_objs, get_typed_nodes
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.resources.pod import get_all_pods
from tests.helpers import get_node_running_pod

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4c
class TestOCSWorkerNodeShutdown(ManageTest):
    """
    Test case validate both the MDS pods not running on same node
    post shutdown and recovery

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """

        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Restart nodes that are in status NotReady
        for situations in which the test failed in between

        """

        def finalizer():

            # Validate all nodes are in READY state
            not_ready_nodes = [
                n for n in get_node_objs() if n
                .ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            log.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes_by_stop_and_start(not_ready_nodes)
                wait_for_nodes_status()

            log.info("All nodes are in Ready status")

        request.addfinalizer(finalizer)

    def list_of_nodes_running_pods(self, selector):
        """
        The function returns the list of nodes for the given selector

        Args:
            selector (str): The resource selector to search with

        Returns:
            list: Pods_running_nodes

        """
        pod_obj_list = get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE, selector=[selector]
        )
        pods_running_nodes = []
        for pod in pod_obj_list:
            pods_running_nodes.append(
                get_node_running_pod(
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE, pod_name=pod.name
                )
            )

        log.info(f"{selector} running on nodes {pods_running_nodes}")
        return pods_running_nodes

    @pytest.mark.polarion_id("OCS-1278")
    @pytest.mark.polarion_id("OCS-2315")
    def test_node_after_shutdown_and_recovery_worker_node(self, nodes):
        """
        Test case to check MDS pods rbd and cephfs plugin Provisioner
        pods not running on same node post shutdown and recovery node

        """

        # Get MDS, rbd, cephfs plugin provisioner pods running nodes
        # before shutdown

        self.list_of_nodes_running_pods(
            selector='rook-ceph-mds'
        )

        self.list_of_nodes_running_pods(
            selector='csi-rbdplugin-provisioner'
        )

        self.list_of_nodes_running_pods(
            selector='csi-cephfsplugin-provisioner'
        )

        # Get the node list
        node = get_typed_nodes(node_type='worker', num_of_nodes=2)

        # Shutdown 2 worker nodes for 10 mins
        nodes.stop_nodes(nodes=node)

        waiting_time = 600
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)

        nodes.start_nodes(nodes=node)

        # Validate all nodes are in READY state and up
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=30,
            delay=15)(
            wait_for_nodes_status(timeout=1800)
        )

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Get MDS, rbd & cephfs plugin provisioner pods running
        # nodes post-recovery
        mds_running_nodes_after_recovery = self.list_of_nodes_running_pods(
            selector='rook-ceph-mds'
        )

        rbd_provisioner_running_nodes_after_recovery = self.list_of_nodes_running_pods(
            selector='csi-rbdplugin-provisioner'
        )

        cephfs_provisioner_running_nodes_after_recovery = self.list_of_nodes_running_pods(
            selector='csi-cephfsplugin-provisioner'
        )

        assert len(set(mds_running_nodes_after_recovery)) == len(
            mds_running_nodes_after_recovery
        ), "MDS running on same node, Not expected!!!"

        assert len(set(rbd_provisioner_running_nodes_after_recovery)) == len(
            rbd_provisioner_running_nodes_after_recovery
        ), "rbd plugin provisioner pods running on Same node, Not expected"

        assert len(set(cephfs_provisioner_running_nodes_after_recovery)) == len(
            cephfs_provisioner_running_nodes_after_recovery
        ), "cephfs plugin provisioner pods running on Same node, Not expected"
