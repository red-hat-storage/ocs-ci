import logging
import pytest
from ocs_ci.framework.testlib import (
    tier4, tier4a, tier4b, ManageTest, aws_platform_required,
    ipi_deployment_required, ignore_leftovers
)
from ocs_ci.ocs import machine, constants, defaults
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import get_all_pods, get_osd_pods, get_pod_node
from ocs_ci.utility.utils import ceph_health_check
from tests.sanity_helpers import Sanity
from tests.helpers import (
    get_worker_nodes, label_worker_node, remove_label_from_worker_node,
    wait_for_resource_state
)
from ocs_ci.ocs.node import (
    get_osd_running_nodes, get_app_pod_running_nodes,
    get_both_osd_and_app_pod_running_node, get_node_objs,
    add_new_node_and_label_it
)
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4b
@aws_platform_required
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
                *['rbd', 'shutdown'],
                marks=[
                    pytest.mark.polarion_id("OCS-2102"),
                    pytest.mark.bugzilla("1845666")
                ]
            ),
            pytest.param(
                *['rbd', 'terminate'],
                marks=pytest.mark.polarion_id("OCS-2103")
            ),
            pytest.param(
                *['cephfs', 'shutdown'],
                marks=[
                    pytest.mark.polarion_id("OCS-2104"),
                    pytest.mark.bugzilla("1845666")
                ]
            ),
            pytest.param(
                *['cephfs', 'terminate'],
                marks=pytest.mark.polarion_id("OCS-2105")
            ),
        ]
    )
    def test_automated_recovery_from_failed_nodes_IPI_reactive(
        self, nodes, pvc_factory, pod_factory, failure, dc_pod_factory,
        interface
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI
        """
        # Get OSD running nodes
        osd_running_nodes = get_osd_running_nodes()
        log.info(f"OSDs are running on nodes {osd_running_nodes}")
        # Label osd nodes with fedora app
        label_worker_node(
            osd_running_nodes, label_key='dc', label_value='fedora'
        )

        # Create DC app pods
        log.info("Creating DC based app pods")
        if interface == 'rbd':
            interface = constants.CEPHBLOCKPOOL
        elif interface == 'cephfs':
            interface = constants.CEPHFILESYSTEM
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(
                interface=interface, node_selector={'dc': 'fedora'}
            )
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
        machineset_name = machine.get_machineset_from_machine_name(
            machine_name
        )
        log.info(
            f"{common_nodes[0]} associated machineset is {machineset_name}"
        )

        # Add a new node and label it
        add_new_node_and_label_it(machineset_name)
        # Get the failure node obj
        failure_node_obj = get_node_objs(node_names=[common_nodes[0]])

        # Induce failure on the selected failure node
        log.info(f"Inducing failure on node {failure_node_obj[0].name}")
        if failure == "shutdown":
            nodes.stop_nodes(failure_node_obj, wait=True)
            log.info(
                f"Successfully powered off node: "
                f"{failure_node_obj[0].name}"
            )
        elif failure == "terminate":
            nodes.terminate_nodes(failure_node_obj, wait=True)
            log.info(
                f"Successfully terminated node : "
                f"{failure_node_obj[0].name} instance"
            )

        try:
            # DC app pods on the failed node will get automatically created on other
            # running node. Waiting for all dc app pod to reach running state
            pod.wait_for_dc_app_pods_to_reach_running_state(
                dc_pod_obj, timeout=720
            )
            log.info("All the dc pods reached running state")
            pod.wait_for_storage_pods()

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
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()


@ignore_leftovers
@tier4
@tier4a
@aws_platform_required
@ipi_deployment_required
class TestAutomatedRecoveryFromStoppedNodes(ManageTest):

    osd_worker_node = None
    extra_node = False

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
                nodes.start_nodes(nodes=self.osd_worker_node, wait=True)
            log.info(
                f"Successfully started node : "
                f"{self.osd_worker_node[0].name} instance"
            )
            ceph_health_check()

        request.addfinalizer(finalizer)

    def add_new_storage_node(self, node_name):
        machine_name = machine.get_machine_from_node_name(node_name)
        log.info(f"{node_name} associated machine is {machine_name}")

        # Get the machineset name using machine name
        machineset_name = machine.get_machineset_from_machine_name(
            machine_name
        )
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
            pytest.param(
                False,
                marks=pytest.mark.polarion_id("OCS-2190")
            ),
        ]
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
        temp_osd = get_osd_pods()[0]
        osd_real_name = "-".join(temp_osd.name.split("-")[:-1])
        self.osd_worker_node = [get_pod_node(temp_osd)]
        if additional_node:
            self.add_new_storage_node(self.osd_worker_node[0].name)
            self.extra_node = True
        nodes.stop_nodes(self.osd_worker_node, wait=True)
        log.info(
            f"Successfully powered off node: {self.osd_worker_node[0].name}"
        )

        wait_for_resource_state(
            temp_osd, constants.STATUS_TERMINATING, timeout=240
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
        log.info(
            f"Successfully powered on node: {self.osd_worker_node[0].name}"
        )
        wait_for_resource_state(
            new_osd, constants.STATUS_RUNNING, timeout=180
        )
        if additional_node:
            new_osd_node = get_pod_node(new_osd)
            assert new_osd_node.name != self.osd_worker_node[0].name, (
                "New OSD is expected to run on the new additional node"
            )
