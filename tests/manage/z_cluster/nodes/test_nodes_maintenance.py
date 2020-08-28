import logging
import pytest

from subprocess import TimeoutExpired

from ocs_ci.ocs.exceptions import (
    CephHealthException, ResourceWrongStatusException
)
from ocs_ci.utility.utils import ceph_health_check_base

from ocs_ci.ocs import constants, machine, ocp, defaults
from ocs_ci.ocs.node import (
    drain_nodes, schedule_nodes, get_typed_nodes, wait_for_nodes_status,
    remove_nodes, get_osd_running_nodes, get_node_objs,
    add_new_node_and_label_it
)
from ocs_ci.framework.testlib import (
    tier1, tier2, tier3, tier4, tier4b,
    ManageTest, aws_platform_required, ignore_leftovers,
    ipi_deployment_required, skipif_bm
)
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.resources import pod
from tests.helpers import (
    label_worker_node, remove_label_from_worker_node
)
from tests import helpers


log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    """
    Tear down function

    """
    def finalizer():
        """
        Make sure that all cluster's nodes are in 'Ready' state and if not,
        change them back to 'Ready' state by marking them as schedulable
        """
        scheduling_disabled_nodes = [
            n.name for n in get_node_objs() if n.ocp.get_resource_status(
                n.name
            ) == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        if scheduling_disabled_nodes:
            schedule_nodes(scheduling_disabled_nodes)

        # Remove label created for DC app pods on all worker nodes
        node_objs = get_node_objs()
        for node_obj in node_objs:
            if 'dc' in node_obj.get().get('metadata').get('labels').keys():
                remove_label_from_worker_node(
                    [node_obj.name], label_key="dc"
                )
    request.addfinalizer(finalizer)


@ignore_leftovers
class TestNodesMaintenance(ManageTest):
    """
    Test basic flows of maintenance (unschedule and drain) and
    activate operations, followed by cluster functionality and health checks

    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def health_checker(self):
        """
        Check Ceph health

        """
        try:
            status = ceph_health_check_base()
            if status:
                log.info("Health check passed")
        except CephHealthException as e:
            # skip because ceph is not in good health
            pytest.skip(str(e))

    @tier1
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1269")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1272"))
        ]
    )
    def test_node_maintenance(self, node_type, pvc_factory, pod_factory):
        """
        OCS-1269/OCS-1272:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the node as scheduable
        - Check cluster and Ceph health

        """
        # Get 1 node of the type needed for the test iteration
        typed_nodes = get_typed_nodes(node_type=node_type, num_of_nodes=1)
        assert typed_nodes, f"Failed to find a {node_type} node for the test"
        typed_node_name = typed_nodes[0].name

        # Maintenance the node (unschedule and drain)
        drain_nodes([typed_node_name])

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Mark the node back to schedulable
        schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=60)

    @tier4
    @tier4b
    @skipif_bm
    @pytest.mark.parametrize(
        argnames=["node_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1292")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1293"))
        ]
    )
    def test_node_maintenance_restart_activate(
        self, nodes, pvc_factory, pod_factory, node_type
    ):
        """
        OCS-1292/OCS-1293:
        - Maintenance (mark as unscheduable and drain) 1 worker/master node
        - Restart the node
        - Mark the node as scheduable
        - Check cluster and Ceph health
        - Check cluster functionality by creating and deleting resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)

        """
        # Get 1 node of the type needed for the test iteration
        typed_nodes = get_typed_nodes(node_type=node_type, num_of_nodes=1)
        assert typed_nodes, f"Failed to find a {node_type} node for the test"
        typed_node_name = typed_nodes[0].name

        reboot_events_cmd = (
            f"get events -A --field-selector involvedObject.name="
            f"{typed_node_name},reason=Rebooted -o yaml"
        )

        # Find the number of reboot events in 'typed_node_name'
        num_events = len(
            typed_nodes[0].ocp.exec_oc_cmd(reboot_events_cmd)['items']
        )

        # Maintenance the node (unschedule and drain). The function contains logging
        drain_nodes([typed_node_name])

        # Restarting the node
        nodes.restart_nodes(nodes=typed_nodes, wait=False)

        try:
            wait_for_nodes_status(
                node_names=[typed_node_name],
                status=constants.NODE_NOT_READY_SCHEDULING_DISABLED
            )
        except ResourceWrongStatusException:
            # Sometimes, the node will be back to running state quickly so
            # that the status change won't be detected. Verify the node was
            # actually restarted by checking the reboot events count
            new_num_events = len(
                typed_nodes[0].ocp.exec_oc_cmd(reboot_events_cmd)['items']
            )
            assert new_num_events > num_events, (
                f"Reboot event not found."
                f"Node {typed_node_name} did not restart."
            )

        wait_for_nodes_status(
            node_names=[typed_node_name],
            status=constants.NODE_READY_SCHEDULING_DISABLED
        )

        # Mark the node back to schedulable
        schedule_nodes([typed_node_name])

        # Check cluster and Ceph health and checking basic cluster
        # functionality by creating resources (pools, storageclasses,
        # PVCs, pods - both CephFS and RBD), run IO and delete the resources
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

    @tier3
    @pytest.mark.parametrize(
        argnames=["nodes_type"],
        argvalues=[
            pytest.param(*['worker'], marks=pytest.mark.polarion_id("OCS-1273")),
            pytest.param(*['master'], marks=pytest.mark.polarion_id("OCS-1271"))
        ]
    )
    def test_2_nodes_maintenance_same_type(self, nodes_type):
        """
        OCS-1273/OCs-1271:
        - Try draining 2 nodes from the same type - should fail
        - Check cluster and Ceph health

        """
        # Get 2 nodes
        typed_nodes = get_typed_nodes(node_type=nodes_type, num_of_nodes=2)
        assert typed_nodes, f"Failed to find a {nodes_type} node for the test"

        typed_node_names = [typed_node.name for typed_node in typed_nodes]

        # Try draining 2 nodes - should fail
        try:
            drain_nodes(typed_node_names)
        except TimeoutExpired:
            log.info(f"Draining of nodes {typed_node_names} failed as expected")

        schedule_nodes(typed_node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @tier2
    @pytest.mark.polarion_id("OCS-1274")
    def test_2_nodes_different_types(self, pvc_factory, pod_factory):
        """
        OCS-1274:
        - Maintenance (mark as unscheduable and drain) 1 worker node and 1
          master node
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Mark the nodes as scheduable
        - Check cluster and Ceph health

        """
        # Get 1 node from each type
        nodes = [
            get_typed_nodes(
                node_type=node_type, num_of_nodes=1
            )[0] for node_type in ['worker', 'master']
        ]
        assert nodes, "Failed to find a nodes for the test"

        node_names = [typed_node.name for typed_node in nodes]

        # Maintenance the nodes (unschedule and drain)
        drain_nodes(node_names)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Mark the nodes back to schedulable
        schedule_nodes(node_names)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()

    @tier4
    @tier4b
    @aws_platform_required
    @ipi_deployment_required
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
        OCS-2128/OCS-2129:
        - Create PVCs and start IO on DC based app pods
        - Add one extra node in two of the AZs and label the nodes
          with OCS storage label
        - Maintenance (mark as unscheduable and drain) 2 worker nodes
          simultaneously
        - Confirm that OCS and DC pods are in running state
        - Remove unscheduled nodes
        - Check cluster functionality by creating resources
          (pools, storageclasses, PVCs, pods - both CephFS and RBD)
        - Check cluster and Ceph health

        """
        # Get OSD running nodes
        osd_running_worker_nodes = get_osd_running_nodes()
        log.info(f"OSDs are running on nodes {osd_running_worker_nodes}")

        # Label osd nodes with fedora app
        label_worker_node(
            osd_running_worker_nodes, label_key='dc', label_value='fedora'
        )
        log.info("Successfully labeled worker nodes with {dc:fedora}")

        # Create DC app pods
        log.info("Creating DC based app pods and starting IO in background")
        interface = (
            constants.CEPHBLOCKPOOL if interface == 'rbd'
            else constants.CEPHFILESYSTEM
        )
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(
                interface=interface, node_selector={'dc': 'fedora'}
            )
            pod.run_io_in_bg(dc_pod, fedora_dc=True)
            dc_pod_obj.append(dc_pod)

        # Get the machine name using the node name
        machine_names = [
            machine.get_machine_from_node_name(osd_running_worker_node)
            for osd_running_worker_node in osd_running_worker_nodes[:2]
        ]
        log.info(
            f"{osd_running_worker_nodes} associated "
            f"machine are {machine_names}"
        )

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

        # Check the pods should be in running state
        all_pod_obj = pod.get_all_pods(wait=True)
        for pod_obj in all_pod_obj:
            if ('-1-deploy' or 'ocs-deviceset') not in pod_obj.name:
                try:
                    helpers.wait_for_resource_state(
                        resource=pod_obj, state=constants.STATUS_RUNNING,
                        timeout=200
                    )
                except ResourceWrongStatusException:
                    # 'rook-ceph-crashcollector' on the failed node stucks at
                    # pending state. BZ 1810014 tracks it.
                    # Ignoring 'rook-ceph-crashcollector' pod health check as
                    # WA and deleting its deployment so that the pod
                    # disappears. Will revert this WA once the BZ is fixed
                    if 'rook-ceph-crashcollector' in pod_obj.name:
                        ocp_obj = ocp.OCP(
                            namespace=defaults.ROOK_CLUSTER_NAMESPACE
                        )
                        pod_name = pod_obj.name
                        deployment_name = '-'.join(pod_name.split("-")[:-2])
                        command = f"delete deployment {deployment_name}"
                        ocp_obj.exec_oc_cmd(command=command)
                        log.info(f"Deleted deployment for pod {pod_obj.name}")

        # DC app pods on the drained node will get automatically created on other
        # running node in same AZ. Waiting for all dc app pod to reach running state
        pod.wait_for_dc_app_pods_to_reach_running_state(
            dc_pod_obj, timeout=1200
        )
        log.info("All the dc pods reached running state")

        # Remove unscheduled nodes
        # In scenarios where the drain is attempted on >3 worker setup,
        # post completion of drain we are removing the unscheduled nodes so
        # that we maintain 3 worker nodes.
        log.info(f"Removing scheduled nodes {osd_running_worker_nodes[:2]}")
        remove_node_objs = get_node_objs(osd_running_worker_nodes[:2])
        remove_nodes(remove_node_objs)

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
