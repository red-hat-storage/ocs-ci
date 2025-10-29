import logging
import time

import pytest
from time import sleep

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
    RHSTOR-6942- Automate the NetworkFence workflow with cephcsi

    To verify data accessibility, integrity and clean connection scores
    when nodes are down and tainted with "out-of-service", the workload moved to another healthy node
    """

    nw_fail_time = 180
    taint_nodes_list = []

    @pytest.fixture(autouse=True)
    def teardown(self, request, node_restart_teardown):
        """
        Verify if taint exist on node, if yes remove the taint and restart the nodes

        """

        def finalizer():
            if self.taint_nodes_list:
                node_restart_teardown()
                untaint_nodes(
                    taint_label=constants.OUT_OF_SERVICE_TAINT,
                    nodes_to_untaint=self.taint_nodes_list,
                )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["node_shutdown"],
        argvalues=[
            pytest.param(True, marks=pytest.mark.polarion_id("OCS-7370")),
            pytest.param(False, marks=pytest.mark.polarion_id("OCS-7371")),
        ],
    )
    def test_automate_networkfence_workflow_with_cephsi(
        self, node_shutdown, nodes, deployment_pod_factory
    ):
        """

        This scenario tests workload migration with PVCs during a node outage.

        1. Create and verify a workload (deployment typyestedaye) using a PVC.
        2. Shut down the node, apply an "out-of-service" taint, and verify its "NotReady" status.
        3. Confirm workload moved, is accessible
        4. Bring node online, and verify its "Ready" state.
        5. Remove the taint
        6. Verify can write new files to the PVC on the new node.
        7. Deploy new pod on the same node which was recovered

        """

        # Create and verify a workload (deployment type) using a PVC
        pod_obj_list = []
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pod_obj = deployment_pod_factory(interface=interface)
            pod_obj_list.append(pod_obj)

        # Start IO on the pods
        logger.info(f"Starting IO on {len(pod_obj_list)} app pods")
        for pod_obj in pod_obj_list:
            logger.info(f"Starting IO on pod {pod_obj.name}")
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                runtime=30,
                fio_filename="io_file1",
            )
        logger.info(f"IO started on all {len(pod_obj_list)} app pods")

        # Verify IO results
        md5sum_before = []
        for pod_obj in pod_obj_list:
            get_fio_rw_iops(pod_obj)
            md5sum_before.append(cal_md5sum(pod_obj=pod_obj, file_name="io_file1"))

        # Get the node where the workload running
        node = get_pod_node(pod_obj_list[0])

        # Wait for 2 minutes
        logger.info("Wait for 2 minutes to breathe")
        time.sleep(120)

        if node_shutdown:
            # Shut down the node, apply an "out-of-service" taint, and verify its "NotReady" status.
            nodes.stop_nodes([node])
            wait_for_nodes_status(
                node_names=[node.name], status=constants.NODE_NOT_READY
            )
        else:
            # Networkfailure the node and verify its "NotReady" status.
            node_network_failure([node.name])
            logger.info(f"Waiting for {self.nw_fail_time} seconds")
            sleep(self.nw_fail_time)

        # Taint the node
        taint_nodes(
            nodes=[node.name], taint_label=constants.OUT_OF_SERVICE_TAINT
        ), f"Failed to add taint on the node {node.name}"
        self.taint_nodes_list.append(node)

        # Wait for 300 seconds for pod to move to another node
        logger.info("Wait for 300 seconds for pod to move to another healthy node")
        time.sleep(300)

        # Verify workload moved to another healthy node
        new_pod_obj_list = get_all_pods(namespace=pod_obj_list[0].namespace, wait=True)
        for pod_obj in new_pod_obj_list:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=300
            )

        # Verify data integrity from new pods
        md5sum_after = []
        for pod_obj in new_pod_obj_list:
            md5sum_after.append(cal_md5sum(pod_obj=pod_obj, file_name="io_file1"))
        assert set(md5sum_before) == set(md5sum_after), (
            "Data corruption found, the md5sum value doesn't match. "
            f"md5sum before: {md5sum_before}, md5sum after: {md5sum_after}"
        )

        # Start the nodes and remove taint
        nodes.start_nodes([node])
        wait_for_nodes_status(node_names=[node.name], status=constants.NODE_READY)
        assert untaint_nodes(
            taint_label=constants.OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=[node],
        ), f"Failed to remove the taint on node {node.name}"
        self.taint_nodes_list = []

        # Run IO on new pods
        logger.info(f"Starting IO on {len(new_pod_obj_list)} app pods")
        for pod_obj in new_pod_obj_list:
            logger.info(f"Starting IO on pod {pod_obj.name}")
            pod_obj.run_io(
                storage_type="fs",
                size="1G",
                runtime=30,
                fio_filename="io_file2",
            )
        logger.info(f"IO started on all {len(new_pod_obj_list)} app pods")

        # Deploy new workload on the node
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pod_obj = deployment_pod_factory(interface=interface, node_name=node.name)
            pod_obj_list.append(pod_obj)
