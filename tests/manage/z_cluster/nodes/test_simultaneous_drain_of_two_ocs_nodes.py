import logging
import pytest
from ocs_ci.framework.testlib import (
    tier4, tier4c, ManageTest,
    aws_platform_required,
    ipi_deployment_required, ignore_leftovers
)
from ocs_ci.ocs import machine, constants
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    drain_nodes, get_osd_running_nodes,
    get_node_objs, remove_nodes, add_new_node_and_label_it
)
from tests.helpers import wait_for_resource_state, label_worker_node
from ocs_ci.ocs.resources import pod


log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def Delete_unscheduled_nodes_teardown(request):
    """
    Remove unscheduled nodes from the OCS cluster
    """
    def finalizer():
        unscheduled_nodes = [
            n for n in get_node_objs() if n.ocp.get_resource_status(
                n.name
            ) == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        remove_nodes(unscheduled_nodes)
    request.addfinalizer(finalizer)


@tier4
@tier4c
@ignore_leftovers
@aws_platform_required
@ipi_deployment_required
class TestSimultaneousDrainOfTwoOCSNodes(ManageTest):
    """
    This test automates BZ 1769667
    Simultaneous drain of two OCS nodes (from different AZs)
    makes one drain operation stuck for infinite time.
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                *['rbd'],
                marks=pytest.mark.polarion_id("OCS-2128")
            ),
            pytest.param(
                *['cephfs'],
                marks=pytest.mark.polarion_id("OCS-2129")
            ),
        ]
    )
    def test_simultaneous_drain_of_two_ocs_nodes(
        self, pvc_factory, pod_factory, dc_pod_factory,
        interface
    ):
        """
        Simultaneous drain of two OCS nodes
        """
        # Get OSD running nodes
        osd_running_worker_nodes = get_osd_running_nodes()
        log.info(f"OSDs are running on nodes {osd_running_worker_nodes}")

        # Label osd nodes with fedora app
        label_worker_node(get_osd_running_nodes, label_key='dc', label_value='fedora')
        log.info("Successfully labeled worker nodes with {dc:fedora}")

        # Create DC app pods
        log.info("Creating DC based app pods and starting IO in background")
        if interface == 'rbd':
            interface = constants.CEPHBLOCKPOOL
        elif interface == 'cephfs':
            interface = constants.CEPHFILESYSTEM
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(
                interface=interface, node_selector={'dc': 'fedora'})
            pod.run_io_in_bg(dc_pod, fedora_dc=True)
            dc_pod_obj.append(dc_pod)

        # Get the machine name using the node name
        machine_names = [
            machine.get_machine_from_node_name(osd_running_worker_node)
            for osd_running_worker_node in osd_running_worker_nodes[:2]
        ]
        log.info(f"{osd_running_worker_nodes} associated "
                 f"machine are {machine_names}")

        # Get the machineset name using machine name
        machineset_names = [
            machine.get_machineset_from_machine_name(
                machine_name
            )
            for machine_name in machine_names
        ]
        log.info(
            f"{osd_running_worker_nodes} associated machineset "
            f"is {machineset_names}"
        )

        # Add a new node and label it
        add_new_node_and_label_it(machineset_names[0])
        add_new_node_and_label_it(machineset_names[1])

        # Drain 2 nodes
        drain_nodes(osd_running_worker_nodes[:2])

        # TODO - Check whether DC app pods respins on other nodes
        #  will implement it once PR-1591 is merged

        # Check the pods should be in running state
        all_pod_obj = pod.get_all_pods(wait=True)
        for pod_obj in all_pod_obj:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=200
            )

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
