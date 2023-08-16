import logging
import pytest
import time

from ocs_ci.framework.testlib import (
    ManageTest,
    tier4b,
    ignore_leftovers,
    skipif_ibm_cloud,
    skipif_managed_service,
    skipif_external_mode,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import wait_for_nodes_status, get_nodes
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.resources.pod import wait_for_storage_pods, list_of_nodes_running_pods

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4b
class TestOCSWorkerNodeShutdown(ManageTest):
    """
    Test case validate both the MDS pods rbd and cephfs plugin Provisioner
    pods and not running on same node post shutdown and recovery

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """

        self.sanity_helpers = Sanity()

    @pytest.mark.polarion_id("OCS-2315")
    @skipif_ibm_cloud
    @skipif_managed_service
    @skipif_external_mode
    @pytest.mark.bugzilla("2232106")
    def test_check_pod_status_after_two_nodes_shutdown_recovery(
        self, nodes, node_restart_teardown
    ):
        """
        Test case to check MDS pods rbd and cephfs plugin Provisioner
        pods not running on same node post shutdown and recovery node

        """

        # Get MDS, rbd, cephfs plugin provisioner pods running nodes
        # before shutdown

        log.info("Check pod nodes before nodes shutdown")
        list_of_nodes_running_pods(selector="rook-ceph-mds")

        list_of_nodes_running_pods(selector="csi-rbdplugin-provisioner")

        list_of_nodes_running_pods(selector="csi-cephfsplugin-provisioner")

        # Get the node list
        node = get_nodes(node_type="worker", num_of_nodes=2)

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
            delay=15,
        )(wait_for_nodes_status(timeout=1800))

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()
        wait_for_storage_pods()

        # Get MDS, rbd & cephfs plugin provisioner pods running
        # nodes post-recovery
        mds_running_nodes_after_recovery = list_of_nodes_running_pods(
            selector="rook-ceph-mds"
        )

        rbd_provisioner_running_nodes_after_recovery = list_of_nodes_running_pods(
            selector="csi-rbdplugin-provisioner"
        )

        cephfs_provisioner_running_nodes_after_recovery = list_of_nodes_running_pods(
            selector="csi-cephfsplugin-provisioner"
        )

        assert len(set(mds_running_nodes_after_recovery)) == len(
            mds_running_nodes_after_recovery
        ), "MDS running on same node, Not expected!!!"
        log.info("MDS pods not running on same node")

        assert len(set(rbd_provisioner_running_nodes_after_recovery)) == len(
            rbd_provisioner_running_nodes_after_recovery
        ), "rbd plugin provisioner pods running on Same node, Not expected"
        log.info("RBD plugin provisioner pods not running on same node")

        assert len(set(cephfs_provisioner_running_nodes_after_recovery)) == len(
            cephfs_provisioner_running_nodes_after_recovery
        ), "cephfs plugin provisioner pods running on Same node, Not expected"
        log.info("CEPHFS plugin provisioner pods not running on same node")
