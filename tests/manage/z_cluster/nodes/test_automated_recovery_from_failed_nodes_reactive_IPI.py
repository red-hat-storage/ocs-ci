import logging
import pytest
from ocs_ci.framework.testlib import (
    tier4,
    tier4a,
    tier4b,
    ManageTest,
    ipi_deployment_required,
    ignore_leftovers,
)
from ocs_ci.framework import config
from ocs_ci.ocs import machine, constants, defaults
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import get_all_pods, get_osd_pods, get_pod_node
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import (
    label_worker_node,
    remove_label_from_worker_node,
    wait_for_resource_state,
    wait_for_rook_ceph_pod_status,
)
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_app_pod_running_nodes,
    get_both_osd_and_app_pod_running_node,
    get_node_objs,
    add_new_node_and_label_it,
    get_worker_nodes,
    get_node_status,
)
from ocs_ci.ocs.exceptions import ResourceWrongStatusException, TimeoutExpiredError

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4b
@ipi_deployment_required
class TestAutomatedRecoveryFromFailedNodes(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes - Reactive
    """

    threads = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            worker_nodes = get_worker_nodes()
            # Removing created label on all worker nodes
            remove_label_from_worker_node(worker_nodes, label_key="dc")
            for thread in self.threads:
                thread.join()

            log.info("Get the machine set name from one of the worker node names")
            machine_name = machine.get_machine_from_node_name(worker_nodes[0])
            machineset_name = machine.get_machineset_from_machine_name(machine_name)
            log.info(
                "Verify that the current replica count is equal to the ready replica count"
            )
            machine.change_current_replica_count_to_ready_replica_count(machineset_name)

            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["interface", "failure"],
        argvalues=[
            pytest.param(
                *["rbd", "shutdown"],
                marks=[
                    pytest.mark.polarion_id("OCS-2102"),
                    pytest.mark.bugzilla("1845666"),
                ],
            ),
            pytest.param(
                *["rbd", "terminate"], marks=pytest.mark.polarion_id("OCS-2103")
            ),
            pytest.param(
                *["cephfs", "shutdown"],
                marks=[
                    pytest.mark.polarion_id("OCS-2104"),
                    pytest.mark.bugzilla("1845666"),
                ],
            ),
            pytest.param(
                *["cephfs", "terminate"], marks=pytest.mark.polarion_id("OCS-2105")
            ),
        ],
    )
    def test_automated_recovery_from_failed_nodes_IPI_reactive(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        failure,
        dc_pod_factory,
        interface,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI
        """
        # Get OSD running nodes
        osd_running_nodes = get_osd_running_nodes()
        log.info(f"OSDs are running on nodes {osd_running_nodes}")
        # Label osd nodes with fedora app
        label_worker_node(osd_running_nodes, label_key="dc", label_value="fedora")

        # Create DC app pods
        log.info("Creating DC based app pods")
        if interface == "rbd":
            interface = constants.CEPHBLOCKPOOL
        elif interface == "cephfs":
            interface = constants.CEPHFILESYSTEM
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(interface=interface, node_selector={"dc": "fedora"})
            self.threads.append(pod.run_io_in_bg(dc_pod, fedora_dc=True))
            dc_pod_obj.append(dc_pod)

        # Get app pods running nodes
        dc_pod_node_name = get_app_pod_running_nodes(dc_pod_obj)
        log.info(f"DC app pod running nodes are {dc_pod_node_name}")

        # Get both osd and app pod running node
        common_nodes = get_both_osd_and_app_pod_running_node(
            osd_running_nodes, dc_pod_node_name
        )
        log.info(f"Both OSD and app pod is running on nodes {common_nodes}")

        # Get the machine name using the node name
        machine_name = machine.get_machine_from_node_name(common_nodes[0])
        log.info(f"{common_nodes[0]} associated machine is {machine_name}")

        # Get the machineset name using machine name
        machineset_name = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"{common_nodes[0]} associated machineset is {machineset_name}")

        # Add a new node and label it
        add_new_node_and_label_it(machineset_name)
        # Get the failure node obj
        failure_node_obj = get_node_objs(node_names=[common_nodes[0]])

        # Induce failure on the selected failure node
        log.info(f"Inducing failure on node {failure_node_obj[0].name}")
        if failure == "shutdown":
            nodes.stop_nodes(failure_node_obj, wait=True)
            log.info(f"Successfully powered off node: " f"{failure_node_obj[0].name}")
        elif failure == "terminate":
            nodes.terminate_nodes(failure_node_obj, wait=True)
            log.info(
                f"Successfully terminated node : "
                f"{failure_node_obj[0].name} instance"
            )

        try:
            # DC app pods on the failed node will get automatically created on other
            # running node. Waiting for all dc app pod to reach running state
            pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj, timeout=720)
            log.info("All the dc pods reached running state")
            pod.wait_for_storage_pods(timeout=300)

        except ResourceWrongStatusException:
            if failure == "shutdown":
                nodes.terminate_nodes(failure_node_obj, wait=True)
                log.info(
                    f"Successfully terminated node : "
                    f"{failure_node_obj[0].name} instance"
                )
            raise

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            tries = 200
        else:
            tries = 40

        self.sanity_helpers.health_check(tries=tries)


@ignore_leftovers
@tier4
@tier4a
@ipi_deployment_required
class TestAutomatedRecoveryFromStoppedNodes(ManageTest):

    osd_worker_node = None
    extra_node = False
    machineset_name = None
    start_ready_replica_count = None

    def verify_osd_worker_node_in_ready_state(self, nodes):
        """
        Verify that the osd worker node is in ready state.
        """
        expected_statuses = [constants.NODE_NOT_READY, constants.NODE_READY]
        node_status = get_node_status(self.osd_worker_node[0])
        node_name = self.osd_worker_node[0].name

        log.info(f"The status of the node {node_name} is {node_status} ")

        if node_status not in expected_statuses:
            log.warning(
                f"The node {node_name} is not in the expected statuses: {expected_statuses}. "
                f"Trying to restart the node..."
            )
            nodes.restart_nodes_by_stop_and_start(
                nodes=self.osd_worker_node, force=True
            )
            return

        if node_status == constants.NODE_NOT_READY:
            log.info(f"Starting the node {node_name}...")
            nodes.start_nodes(nodes=self.osd_worker_node, wait=True)
            log.info(f"Successfully started node {node_name} instance")
        else:
            log.info(
                f"The node {node_name} is already in the expected status {constants.NODE_READY}"
            )

    def wait_for_current_replica_count_equal_to_start_ready_replica_count(self):
        """
        Wait for the current ready replica count to be equal to the ready replica count
        at the beginning of the test.
        """
        log.info(f"start ready replica count = {self.start_ready_replica_count}")
        timeout = 180
        log.info(
            f"Wait {timeout} seconds for the current ready replica count to be equal "
            f"to the start ready replica count"
        )
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=10,
            func=machine.get_ready_replica_count,
            machine_set=self.machineset_name,
        )
        try:
            sample.wait_for_func_value(value=self.start_ready_replica_count)
        except TimeoutExpiredError:
            log.warning(
                "The current ready replica count is not equal "
                "to the start ready replica count"
            )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        def finalizer():
            if self.extra_node:
                nodes.terminate_nodes(self.osd_worker_node, wait=True)
                log.info(
                    f"Successfully terminated node : "
                    f"{self.osd_worker_node[0].name} instance"
                )
            else:
                self.verify_osd_worker_node_in_ready_state(nodes)

            ceph_health_check()

            self.wait_for_current_replica_count_equal_to_start_ready_replica_count()
            log.info(
                "Verify that the current replica count is equal to the ready replica count"
            )
            machine.change_current_replica_count_to_ready_replica_count(
                self.machineset_name
            )
            log.info("Check again that the Ceph Health is Health OK")
            ceph_health_check()

        request.addfinalizer(finalizer)

    def add_new_storage_node(self, node_name):
        machine_name = machine.get_machine_from_node_name(node_name)
        log.info(f"{node_name} associated machine is {machine_name}")

        # Get the machineset name using machine name
        machineset_name = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"{node_name} associated machineset is {machineset_name}")

        # Add a new node and label it
        add_new_node_and_label_it(machineset_name)

    @pytest.mark.parametrize(
        argnames=["additional_node"],
        argvalues=[
            pytest.param(
                True,
                marks=pytest.mark.polarion_id("OCS-2191"),
            ),
            pytest.param(False, marks=pytest.mark.polarion_id("OCS-2190")),
        ],
    )
    def test_automated_recovery_from_stopped_node_and_start(
        self, nodes, additional_node
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI

        0) A - add new node, B - don't add new node
        1) Stop node
        2) Validate result:
             A - pods should respin on the new node
             B - pods should remain in Pending state on the stopped node
        3) Start node
        4) Validate result:
             A - pods should start on the new node
             B - pods should start on the stopped node after starting it
        """
        wnode_name = get_worker_nodes()[0]
        machine_name = machine.get_machine_from_node_name(wnode_name)
        self.machineset_name = machine.get_machineset_from_machine_name(machine_name)
        self.start_ready_replica_count = machine.get_ready_replica_count(
            self.machineset_name
        )

        temp_osd = get_osd_pods()[0]
        osd_real_name = "-".join(temp_osd.name.split("-")[:-1])
        self.osd_worker_node = [get_pod_node(temp_osd)]
        if additional_node:
            self.add_new_storage_node(self.osd_worker_node[0].name)
            self.extra_node = True
        nodes.stop_nodes(self.osd_worker_node, wait=True)
        log.info(f"Successfully powered off node: {self.osd_worker_node[0].name}")

        timeout = 420
        assert wait_for_rook_ceph_pod_status(
            temp_osd, constants.STATUS_TERMINATING, timeout
        ), (
            f"The pod {osd_real_name} didn't reach the status {constants.STATUS_TERMINATING} "
            f"after {timeout} seconds"
        )

        # Validate that the OSD in terminate state has a new OSD in Pending
        all_pod_obj = get_all_pods(namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        new_osd = None
        for pod_obj in all_pod_obj:
            if osd_real_name == "-".join(pod_obj.name.split("-")[:-1]) and (
                temp_osd.name != pod_obj.name
            ):
                new_osd = pod_obj
                break

        nodes.start_nodes(nodes=self.osd_worker_node, wait=True)
        log.info(f"Successfully powered on node: {self.osd_worker_node[0].name}")
        wait_for_resource_state(new_osd, constants.STATUS_RUNNING, timeout=180)
        if additional_node:
            new_osd_node = get_pod_node(new_osd)
            assert (
                new_osd_node.name != self.osd_worker_node[0].name
            ), "New OSD is expected to run on the new additional node"
