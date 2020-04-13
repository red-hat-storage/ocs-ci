import logging
import pytest

from ocs_ci.framework.testlib import (
    tier4, tier4b, ManageTest,
    aws_platform_required,
    ipi_deployment_required, ignore_leftovers
)
from ocs_ci.ocs import machine, constants, defaults, ocp
from ocs_ci.ocs.resources import pod
from tests.helpers import (
    get_worker_nodes, label_worker_node, remove_label_from_worker_node
)
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    get_osd_running_nodes, get_app_pod_running_nodes,
    get_both_osd_and_app_pod_running_node,
    add_new_node_and_label_it
)
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from tests import helpers


log = logging.getLogger(__name__)


@tier4
@tier4b
@ignore_leftovers
@aws_platform_required
@ipi_deployment_required
class TestAutomatedRecoveryFromFailedNodes(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes
    """
    @pytest.fixture(autouse=True)
    def teardown(self, request):

        def finalizer():
            worker_nodes = get_worker_nodes()
            # Removing created label on all worker nodes
            remove_label_from_worker_node(worker_nodes, label_key="dc")

        request.addfinalizer(finalizer)

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
                marks=pytest.mark.polarion_id("OCS-2100")
            ),
            pytest.param(
                *['cephfs'],
                marks=pytest.mark.polarion_id("OCS-2101")
            ),
        ]
    )
    def test_automated_recovery_from_failed_nodes_IPI_proactive(
        self, interface, pvc_factory, pod_factory, dc_pod_factory
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Proactive case - IPI
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
        interface = (
            constants.CEPHBLOCKPOOL if interface == 'rbd'
            else constants.CEPHFILESYSTEM
        )
        dc_pod_obj = []
        for i in range(2):
            dc_pod = dc_pod_factory(
                interface=interface, node_selector={'dc': 'fedora'})
            pod.run_io_in_bg(dc_pod, fedora_dc=True)
            dc_pod_obj.append(dc_pod)

        # Get app pods running nodes
        dc_pod_node_name = get_app_pod_running_nodes(dc_pod_obj)
        log.info(f"DC app pod running nodes are {dc_pod_node_name}")

        # Get both osd and app pod running node
        common_nodes = get_both_osd_and_app_pod_running_node(
            osd_running_nodes, dc_pod_node_name
        )
        msg = "Common OSD and app running node(s) NOT found"
        assert (len(common_nodes) > 0), msg
        log.info(f"Common OSD and app pod running nodes are {common_nodes}")

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

        # Delete the machine
        machine.delete_machine(machine_name)
        log.info(f"Successfully deleted machine {machine_name}")

        # DC app pods on the failed node will get automatically created on
        # other running node. Waiting for all dc app pod to reach running
        # state
        pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj)
        log.info("All the dc pods reached running state")

        # Check all OCS pods status, they should be in running state
        all_pod_obj = pod.get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
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

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
