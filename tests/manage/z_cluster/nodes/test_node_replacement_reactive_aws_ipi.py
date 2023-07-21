import logging
import pytest

from ocs_ci.framework.testlib import (
    tier4a,
    ManageTest,
    aws_based_platform_required,
    ipi_deployment_required,
    ignore_leftovers,
)
from ocs_ci.ocs import machine, constants, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import label_worker_node, remove_label_from_worker_node
from ocs_ci.ocs.resources.storage_cluster import (
    osd_encryption_verification,
    verify_multus_network,
)
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_app_pod_running_nodes,
    get_both_osd_and_app_pod_running_node,
    get_node_objs,
    node_network_failure,
    get_worker_nodes,
)


log = logging.getLogger(__name__)


@ignore_leftovers
@tier4a
@aws_based_platform_required
@ipi_deployment_required
class TestNodeReplacement(ManageTest):
    """
    Knip-894 Node replacement - AWS-IPI-Reactive
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            worker_nodes = get_worker_nodes()
            # Removing created label on all worker nodes
            remove_label_from_worker_node(worker_nodes, label_key="dc")
            # Verify OSD encrypted
            if config.ENV_DATA.get("encryption_at_rest"):
                osd_encryption_verification()

            # Verify Multus networks
            if config.ENV_DATA.get("is_multus_enabled"):
                verify_multus_network()

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
                *["rbd", "power off"], marks=pytest.mark.polarion_id("OCS-2118")
            ),
            pytest.param(
                *["rbd", "network failure"], marks=pytest.mark.polarion_id("OCS-2120")
            ),
            pytest.param(
                *["cephfs", "power off"], marks=pytest.mark.polarion_id("OCS-2119")
            ),
            pytest.param(
                *["cephfs", "network failure"],
                marks=pytest.mark.polarion_id("OCS-2121"),
            ),
        ],
    )
    def test_node_replacement_reactive_aws_ipi(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        dc_pod_factory,
        failure,
        interface,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Knip-894 Node replacement - AWS-IPI-Reactive

        """
        # Get worker nodes
        initial_nodes = get_worker_nodes()

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
        machineset_name = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"{common_nodes[0]} associated machineset is {machineset_name}")

        # Get the failure node obj
        failure_node_obj = get_node_objs(node_names=[common_nodes[0]])

        # Induce failure on the selected failure node
        log.info(f"Inducing failure on node {failure_node_obj[0].name}")
        if failure == "power off":
            # Power off AWS worker node instance
            nodes.stop_nodes(failure_node_obj, wait=True)
            log.info(f"Successfully powered off node: {failure_node_obj[0].name}")
        elif failure == "network failure":
            # Induce Network failure
            node_network_failure([failure_node_obj[0].name])

        # Add annotation to the failed node
        annotation = "machine.openshift.io/exclude-node-draining=''"
        machine.add_annotation_to_machine(
            annotation=annotation, machine_name=machine_name
        )

        # Delete the machine
        machine.delete_machine(machine_name)
        log.info(f"Successfully deleted machine {machine_name}")

        # Wait for the new machine to spin
        log.info("Waiting for the new node to be in ready state")
        machine.wait_for_new_node_to_be_ready(machineset_name)

        # Get the node name of new spun node
        nodes_after_new_spun_node = get_worker_nodes()
        new_spun_node = list(set(nodes_after_new_spun_node) - set(initial_nodes))
        log.info(f"New spun node is {new_spun_node}")

        # Label it
        node_obj = ocp.OCP(kind="node")
        node_obj.add_label(
            resource_name=new_spun_node[0], label=constants.OPERATOR_NODE_LABEL
        )
        log.info(f"Successfully labeled {new_spun_node} with OCS storage label")

        # DC app pods on the failed node will get automatically created on other
        # running node. Waiting for all dc app pod to reach running state
        pod.wait_for_dc_app_pods_to_reach_running_state(dc_pod_obj, timeout=1200)
        log.info("All the dc pods reached running state")

        pod.wait_for_storage_pods()

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
