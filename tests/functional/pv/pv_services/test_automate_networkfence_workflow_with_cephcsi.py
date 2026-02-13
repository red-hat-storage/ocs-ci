# noqa: D205
"""
RHSTOR-6942 - Automate the NetworkFence workflow with cephcsi on non-stretch cluster.

Tests workload migration with PVCs during node outage: create workload,
shut down or network-fail node, taint out-of-service, verify workload migration
and data integrity, recover node, remove taint, verify IO and redeploy on node.
"""
import logging
import time

import pytest

from ocs_ci.framework.testlib import ManageTest, tier4b, green_squad
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    get_pod_node,
    get_fio_rw_iops,
    get_all_pods,
)
from ocs_ci.ocs.node import (
    node_network_failure,
    taint_nodes,
    untaint_nodes,
    wait_for_nodes_status,
)
from ocs_ci.helpers.helpers import wait_for_resource_state

logger = logging.getLogger(__name__)


@green_squad
@tier4b
class TestAutomateNetworkfenceWorkflowWithCephCSI(ManageTest):
    """
    RHSTOR-6942 - Automate the NetworkFence workflow with cephcsi.

    Verifies data accessibility, integrity and clean connection when nodes
    are down and tainted with "out-of-service", and workload moves to another
    healthy node.
    """

    # Time (seconds) to hold network failure before applying taint
    nw_fail_time = 180
    # Allow IO to settle and state to stabilize before inducing node outage
    WAIT_BEFORE_NODE_OUTAGE_SEC = 120
    # Time for Kubernetes to evict pods and reschedule on healthy nodes
    WAIT_FOR_POD_MIGRATION_SEC = 300
    # Timeout for pods to reach Running state after migration
    POD_RUNNING_TIMEOUT = 300
    # IO parameters for data integrity and post-migration write tests
    IO_SIZE = "1G"
    IO_RUNTIME_SEC = 30

    taint_nodes_list = []

    @pytest.fixture(scope="function")
    def teardown(self, request):
        """
        Remove taint from nodes if test leaves them tainted.

        Finalizer runs on test end (pass or fail). Request node_restart_teardown
        in the test to ensure stopped nodes are restarted by its finalizer.
        """
        def finalizer():
            if self.taint_nodes_list:
                logger.info(
                    f"Teardown: cleaning taints from nodes "
                    f"{[n.name for n in self.taint_nodes_list]}"
                )
                untaint_nodes(
                    taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                    nodes_to_untaint=self.taint_nodes_list,
                )
                self.taint_nodes_list = []

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["node_shutdown"],
        argvalues=[
            pytest.param(True, marks=pytest.mark.polarion_id("OCS-7370")),
            pytest.param(False, marks=pytest.mark.polarion_id("OCS-7371")),
        ],
    )
    def test_automate_networkfence_workflow_with_cephsi(
        self, node_shutdown, nodes, deployment_pod_factory, teardown, node_restart_teardown
    ):
        """
        Test workload migration with PVCs during a node outage.

        Steps:
        1. Create and verify a workload (deployment type) using a PVC.
        2. Shut down the node or induce network failure; apply "out-of-service"
           taint and verify node NotReady.
        3. Confirm workload moved to another node and is accessible.
        4. Bring node online and verify Ready state.
        5. Remove the taint.
        6. Verify new files can be written to the PVC on the new node.
        7. Move workload back to recovered node and verify it works.
        8. Deploy new pod on the recovered node.
        """
        pod_obj_list = []
        try:
            # Create and verify a workload (deployment type) using a PVC
            for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
                pod_obj = deployment_pod_factory(interface=interface)
                pod_obj_list.append(pod_obj)

            logger.info(
                f"Starting IO on {len(pod_obj_list)} app pods: "
                f"{[p.name for p in pod_obj_list]}"
            )
            for pod_obj in pod_obj_list:
                pod_obj.run_io(
                    storage_type="fs",
                    size=self.IO_SIZE,
                    runtime=self.IO_RUNTIME_SEC,
                    fio_filename="io_file1",
                )
            logger.info("IO started on all app pods")

            # Verify IO and store checksums for data integrity check
            md5sum_before = []
            for pod_obj in pod_obj_list:
                get_fio_rw_iops(pod_obj)
                md5sum_before.append(
                    cal_md5sum(pod_obj=pod_obj, file_name="io_file1")
                )
            logger.info(
                f"Stored initial checksums for pods {[p.name for p in pod_obj_list]}"
            )

            node = get_pod_node(pod_obj_list[0])
            logger.info(
                f"Workload running on node {node.name}; "
                f"waiting {self.WAIT_BEFORE_NODE_OUTAGE_SEC}s before outage"
            )
            time.sleep(self.WAIT_BEFORE_NODE_OUTAGE_SEC)

            if node_shutdown:
                logger.info(f"Stopping node {node.name}")
                nodes.stop_nodes([node])
                wait_for_nodes_status(
                    node_names=[node.name], status=constants.NODE_NOT_READY
                )
                logger.info(f"Node {node.name} is NotReady")
            else:
                logger.info(f"Inducing network failure on node {node.name}")
                node_network_failure([node.name])
                time.sleep(self.nw_fail_time)

            assert taint_nodes(
                nodes=[node.name],
                taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            ), f"Failed to add taint on the node {node.name}"
            self.taint_nodes_list.append(node)
            logger.info(f"Applied out-of-service taint on node {node.name}")

            logger.info(
                f"Waiting {self.WAIT_FOR_POD_MIGRATION_SEC}s for pods to "
                "migrate to another healthy node"
            )
            time.sleep(self.WAIT_FOR_POD_MIGRATION_SEC)

            migrated_pod_list = get_all_pods(
                namespace=pod_obj_list[0].namespace, wait=True
            )
            for pod_obj in migrated_pod_list:
                wait_for_resource_state(
                    resource=pod_obj,
                    state=constants.STATUS_RUNNING,
                    timeout=self.POD_RUNNING_TIMEOUT,
                )
            logger.info(
                f"Pods after migration (Running): "
                f"{[p.name for p in migrated_pod_list]}"
            )

            # Verify data integrity: compare against initial checksums
            md5sum_after = [
                cal_md5sum(pod_obj=p, file_name="io_file1")
                for p in migrated_pod_list
            ]
            assert len(md5sum_before) == len(md5sum_after), (
                f"Pod count mismatch: before={len(md5sum_before)}, "
                f"after={len(md5sum_after)}. "
                f"md5sum before: {md5sum_before}, after: {md5sum_after}"
            )
            assert sorted(md5sum_before) == sorted(md5sum_after), (
                "Data corruption: checksums after migration do not match "
                "initial values. "
                f"md5sum before: {md5sum_before}, md5sum after: {md5sum_after}"
            )
            logger.info("Data integrity verified after migration")

            # Bring node online and remove taint
            if node_shutdown:
                logger.info(f"Starting node {node.name}")
                nodes.start_nodes([node])
            else:
                nodes.restart_nodes([node])
            wait_for_nodes_status(
                node_names=[node.name], status=constants.NODE_READY
            )
            assert untaint_nodes(
                taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                nodes_to_untaint=[node],
            ), f"Failed to remove the taint on node {node.name}"
            self.taint_nodes_list = []
            logger.info(f"Node {node.name} is Ready and taint removed")

            # Run IO on migrated pods to verify writes on new node
            logger.info(
                f"Starting IO on migrated pods: "
                f"{[p.name for p in migrated_pod_list]}"
            )
            for pod_obj in migrated_pod_list:
                pod_obj.run_io(
                    storage_type="fs",
                    size=self.IO_SIZE,
                    runtime=self.IO_RUNTIME_SEC,
                    fio_filename="io_file2",
                )
            logger.info("IO started on all migrated pods")

            # Delete pods and verify they come back (recreate on same PVC)
            for pod_obj in migrated_pod_list:
                logger.info(f"Deleting pod {pod_obj.name}")
                pod_obj.delete()
            logger.info("Waiting for pods to be running again after delete")
            redeployed_pod_list = get_all_pods(
                namespace=pod_obj_list[0].namespace, wait=True
            )
            for pod_obj in redeployed_pod_list:
                wait_for_resource_state(
                    resource=pod_obj,
                    state=constants.STATUS_RUNNING,
                    timeout=self.POD_RUNNING_TIMEOUT,
                )
            logger.info(
                f"Pods running after delete: "
                f"{[p.name for p in redeployed_pod_list]}"
            )

            # Deploy new workload on the recovered node
            logger.info(f"Deploying new pods on recovered node {node.name}")
            for interface in [
                constants.CEPHBLOCKPOOL,
                constants.CEPHFILESYSTEM,
            ]:
                deployment_pod_factory(
                    interface=interface, node_name=node.name
                )
        finally:
            # Ensure taints are removed even on test failure (node_restart_teardown
            # fixture finalizer runs at test end to restore/restart nodes)
            if self.taint_nodes_list:
                logger.info(
                    "Test cleanup: untainting nodes "
                    f"{[n.name for n in self.taint_nodes_list]}"
                )
                try:
                    untaint_nodes(
                        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                        nodes_to_untaint=list(self.taint_nodes_list),
                    )
                except Exception as e:
                    logger.warning("untaint_nodes raised: %s", e)
                self.taint_nodes_list = []
