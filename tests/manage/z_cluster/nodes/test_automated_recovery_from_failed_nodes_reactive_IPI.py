import logging
import pytest
from ocs_ci.framework.testlib import (
    tier4, tier4b, ManageTest, aws_platform_required,
    ipi_deployment_required, ignore_leftovers)
from ocs_ci.ocs import machine, constants, defaults, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.node import get_node_objs, add_new_node_and_label_it
from tests.sanity_helpers import Sanity
from tests.helpers import (
    get_worker_nodes, label_worker_node, remove_label_from_worker_node)
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.node import (
    get_osd_running_nodes, get_app_pod_running_nodes,
    get_both_osd_and_app_pod_running_node)
from tests import helpers
from distutils.version import StrictVersion

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4b
@aws_platform_required
@ipi_deployment_required
@pytest.mark.skipif(
    StrictVersion(
        get_ocp_version()
    ) > StrictVersion(
        '4.3'
    ), reason="Terminate of machine behaviour is changed from 4.3"
)
class TestAutomatedRecoveryFromFailedNodes(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes - Reactive
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
        argnames=["interface", "failure"],
        argvalues=[
            pytest.param(
                *['rbd', 'shutdown'],
                marks=pytest.mark.polarion_id("OCS-2102")
            ),
            pytest.param(
                *['rbd', 'terminate'],
                marks=pytest.mark.polarion_id("OCS-2103")
            ),
            pytest.param(
                *['cephfs', 'shutdown'],
                marks=pytest.mark.polarion_id("OCS-2104")
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
        label_worker_node(osd_running_nodes, label_key='dc', label_value='fedora')

        # Create DC app pods
        log.info("Creating DC based app pods")
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
                f"Successfully powered off node: {failure_node_obj[0].name}"
            )
            nodes.terminate_nodes(failure_node_obj, wait=True)
            log.info(
                f"Successfully terminated node : {failure_node_obj[0].name} instance"
            )
        elif failure == "terminate":
            nodes.terminate_nodes(failure_node_obj, wait=True)
            log.info(
                f"Successfully terminated node : {failure_node_obj[0].name} instance"
            )

        # DC app pods on the failed node will get automatically created on other
        # running node. Waiting for all dc app pod to reach running state
        pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj)
        log.info("All the dc pods reached running state")

        # Check all OCS pods status, they should be in running state
        all_pod_obj = pod.get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
        for pod_obj in all_pod_obj:
            if '-1-deploy' and 'ocs-deviceset' not in pod_obj.name:
                try:
                    helpers.wait_for_resource_state(
                        resource=pod_obj, state=constants.STATUS_RUNNING,
                        timeout=60
                    )
                except TimeoutError as err:
                    if "rook-ceph-drain-canary" in err:
                        ocp_obj = ocp.OCP()
                        command = "label deploy -n openshift-storage -l app" \
                                  "=rook-ceph-drain-canary --overwrite=true" \
                                  " wa=\"$RANDOM\""
                        ocp_obj.exec_oc_cmd(command=command)
                        log.info(
                            "BZ 1789419 - WA executed since canary pod stuck"
                            " at pending state"
                        )

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
