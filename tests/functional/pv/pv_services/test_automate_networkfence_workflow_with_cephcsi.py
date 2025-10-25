import logging
import pytest
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ocs_ci.framework.testlib import ManageTest, tier4b, green_squad
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    get_pod_node,
    get_fio_rw_iops,
    get_all_pods,
    verify_data_integrity,
)
from ocs_ci.ocs.node import (
    node_network_failure,
    taint_nodes,
    untaint_nodes,
    get_worker_nodes,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@green_squad
@tier4b
class TestAutomateNetworkfenceWorkflowWithCephCSI(ManageTest):
    """
    RHSTOR-6942- Automate the NetworkFence workflow with cephcsi

    To verify data accessibility, integrity and clean connection scores
    when nodes are down and tainted with "out-of-service", the workload moved to another healthy node
    """

    nw_fail_time = 900
    taint_nodes_list = []

    @retry(UnexpectedBehaviour, tries=10, delay=10, backoff=1)
    def verify_multi_attach_error_not_found(self, pod_list):
        """
        Checks for the expected failure event message in oc describe command

        Args:
            pod_list (list): list of pod objects

        Returns:
            bool: True if Multi-Attach Error is found in oc describe

        Raises:
            UnexpectedBehaviour: If Multi-Attach Error not found in describe command

        """
        failure_str = "Multi-Attach error for volume"
        for pod_obj in pod_list:
            if failure_str not in pod_obj.describe():
                logger.info(
                    f"Multi-Attach error is not found in oc describe of {pod_obj.name}"
                )
            else:
                logger.warning(
                    f"Multi-Attach error is present in oc describe of {pod_obj.name}"
                )
                raise UnexpectedBehaviour(pod_obj.name, pod_obj.describe())

        return True

    def run_and_verify_io(
        self, pod_list, fio_filename="io_file", return_md5sum=True, run_io_in_bg=False
    ):
        """
        Start IO on the pods and verify IO results
        Calculates md5sum of the io files which can be used to verify data
            integrity later

        Args:
            pod_list (list): list of pod objects to run ios
            fio_filename (str): name of the file for fio
            return_md5sum (bool): True if md5sum of fio file to be calculated,
                else False
            run_io_in_bg (bool): True if more background ios to be run, else False

        Returns:
            list: list of md5sum values for the fio file if return_md5sum is
                True

        """
        # Start IO on the pods
        logger.info(f"Starting IO on {len(pod_list)} app pods")
        with ThreadPoolExecutor(max_workers=4) as executor:
            for pod_obj in pod_list:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                executor.submit(
                    pod_obj.run_io,
                    storage_type="fs",
                    size="1G",
                    runtime=30,
                    fio_filename=fio_filename,
                )
        logger.info(f"IO started on all {len(pod_list)} app pods")

        # Verify IO results
        for pod_obj in pod_list:
            get_fio_rw_iops(pod_obj)

        if run_io_in_bg:
            logger.info(f"Starting IO in background on {len(pod_list)} app pods")
            for pod_obj in pod_list:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                pod_obj.run_io(
                    storage_type="fs",
                    size="256M",
                    runtime=500,
                    fio_filename="bg_io_file",
                )
            logger.info(f"IO started in background on all {len(pod_list)} app pods")

        # Calculate md5sum of io files
        md5sum_data = []
        if return_md5sum:
            with ThreadPoolExecutor() as executor:
                for pod_obj in pod_list:
                    md5sum_data.append(
                        executor.submit(cal_md5sum, pod_obj, fio_filename)
                    )
            md5sum_data = [future_obj.result() for future_obj in md5sum_data]

        return md5sum_data

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Verify if taint exist on node, if yes remove the taint and restart the nodes

        """

        def finalizer():
            for node in get_worker_nodes():
                if self.taint_nodes_list:
                    untaint_nodes(
                        taint_label=constants.OUT_OF_SERVICE_TAINT,
                        nodes_to_untaint=self.taint_nodes_list,
                    )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["node_shutdown"],
        argvalues=[
            pytest.param(True, marks=pytest.mark.polarion_id("")),
            pytest.param(False, marks=pytest.mark.polarion_id("")),
        ],
    )
    def test_automate_networkfence_workflow_with_cephsi(
        self, node_shutdown, nodes, deployment_pod_factory, node_restart_teardown
    ):
        """

        This scenario tests workload migration with PVCs during a node outage.

        1. Create and verify a workload (deployment type) using a PVC.
        2. Shut down the node, apply an "out-of-service" taint, and verify its "NotReady" status.
        3. Confirm workload moved, is accessible, and can write new files to the PVC on the new node.
        4. Remove taint, bring node online, and verify its "Ready" state.
        5. Verify overall cluster health.

        """

        # Create and verify a workload (deployment type) using a PVC
        pod_obj_list = []
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pod_obj = deployment_pod_factory(interface=interface)
            pod_obj_list.append(pod_obj)

        # Run IO on pods
        md5sum_data = self.run_and_verify_io(
            pod_list=pod_obj_list, fio_filename="io_file1", run_io_in_bg=True
        )

        # Get the node where the workload running
        app_nodes_obj = []
        for pod in pod_obj_list:
            node = get_pod_node(pod)
            app_nodes_obj.append(node)

        list1 = [n.name for n in app_nodes_obj]
        if not len(set(list1)) == 1:
            logger.info("Both the pods are running on different nodes")
            for node in app_nodes_obj:
                if node_shutdown:
                    # Shut down the node, apply an "out-of-service" taint, and verify its "NotReady" status.
                    nodes.stop_nodes([node])
                else:
                    # Networkfailure the node, apply an "out-of-service" taint, and verify its "NotReady" status.
                    node_network_failure([node])
                    logger.info(f"Waiting for {self.nw_fail_time} seconds")
                    sleep(self.nw_fail_time)

                # Taint the node
                taint_nodes(
                    nodes=[node.name], taint_label=constants.OUT_OF_SERVICE_TAINT
                ), f"Failed to add taint on the node {node.name}"
                self.taint_nodes_list.append(node)

                # Verify workload moved to another healthy node
                new_pod_obj_list = get_all_pods(
                    namespace=pod_obj_list[0].namespace, wait=True
                )
                assert self.verify_multi_attach_error_not_found(pod_obj_list)

                # Verify data integrity from new pods
                for num, pod_obj in enumerate(new_pod_obj_list):
                    verify_data_integrity(
                        pod_obj=pod_obj,
                        file_name="io_file1",
                        original_md5sum=md5sum_data[num],
                    )

                # Remove the taint and start the nodes
                assert untaint_nodes(
                    taint_label=constants.OUT_OF_SERVICE_TAINT,
                    nodes_to_untaint=[node],
                ), f"Failed to remove the taint on node {node.name}"
                nodes.start_nodes([node])
                self.taint_nodes_list = []

        else:
            logger.info("Both the pods are running on same nodes")
            if node_shutdown:
                # Shut down the node
                nodes.stop_nodes([app_nodes_obj[0]])

            else:
                # Networkfailure the node, apply an "out-of-service" taint, and verify its "NotReady" status.
                node_network_failure([app_nodes_obj[0]])
                logger.info(f"Waiting for {self.nw_fail_time} seconds")
                sleep(self.nw_fail_time)

            taint_nodes(
                nodes=[app_nodes_obj[0].name],
                taint_label=constants.OUT_OF_SERVICE_TAINT,
            )
            self.taint_nodes_list.append(app_nodes_obj[0])

            # Verify workload moved to another healthy node
            new_pod_obj_list = get_all_pods(
                namespace=pod_obj_list[0].namespace, wait=True
            )
            assert self.verify_multi_attach_error_not_found(pod_obj_list)

            # Verify data integrity from new pods
            for num, pod_obj in enumerate(new_pod_obj_list):
                verify_data_integrity(
                    pod_obj=pod_obj,
                    file_name="io_file1",
                    original_md5sum=md5sum_data[num],
                )

            # Remove the taint and start the nodes
            assert untaint_nodes(
                taint_label=constants.OUT_OF_SERVICE_TAINT,
                nodes_to_untaint=[app_nodes_obj[0]],
            ), f"Failed to remove taint on the node {app_nodes_obj.name}"
            nodes.start_nodes(app_nodes_obj[0])
            self.taint_nodes_list = []

        # Verify overall cluster health
        # Check the node are Ready state and check cluster is health ok]
        self.sanity_helpers.health_check(tries=40)
        assert ceph_health_check(), "Ceph cluster health is not OK"
        logger.info("Ceph cluster health is OK")
