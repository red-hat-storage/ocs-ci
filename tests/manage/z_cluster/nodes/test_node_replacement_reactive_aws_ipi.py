import logging
import pytest
import random
from ocs_ci.framework.testlib import (
    tier4, tier4b, ManageTest, aws_platform_required,
    ipi_deployment_required, ignore_leftovers)
from ocs_ci.ocs import machine
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from tests.helpers import wait_for_resource_state
from tests.sanity_helpers import Sanity
from tests.helpers import get_worker_nodes
from ocs_ci.ocs import ocp

log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4b
@aws_platform_required
@ipi_deployment_required
class TestNodeReplacement(ManageTest):
    """
    Knip-894 Node replacement - AWS-IPI-Reactive
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_node_replacement_reactive_aws_ipi(
            self, nodes, pvc_factory, pod_factory,
            dc_pod_factory
    ):
        """
        Knip-894 Node replacement - AWS-IPI-Reactive

        """
        # Get worker nodes
        initial_nodes = get_worker_nodes()
        log.info(f"Current available worker nodes are {initial_nodes}")

        # Get the osd associated node
        osd_pods_obj = pod.get_osd_pods()
        osd_node_obj = pod.get_pod_node(random.choice(osd_pods_obj))
        log.info(f"Selected OSD is {osd_node_obj.name}")

        # Create fedora dc app on all the worker nodes and start IO in
        # background
        dc_rbd_pod_obj = []
        for node in initial_nodes:
            # Create app pods on all the nodes
            dc_rbd = dc_pod_factory(
                interface=constants.CEPHBLOCKPOOL, node_name=node)
            if node == osd_node_obj.name:
                pod.run_io_in_bg(dc_rbd, expect_to_fail=True, fedora_dc=True)
            else:
                pod.run_io_in_bg(dc_rbd, expect_to_fail=False, fedora_dc=True)
            dc_rbd_pod_obj.append(dc_rbd)

        dc_cephfs_pod_obj = []
        for node in initial_nodes:
            # Create app pods on all the nodes
            dc_cephfs = dc_pod_factory(
                interface=constants.CEPHFILESYSTEM, node_name=node)
            if node == osd_node_obj.name:
                pod.run_io_in_bg(
                    dc_cephfs, expect_to_fail=True, fedora_dc=True
                )
            else:
                pod.run_io_in_bg(
                    dc_cephfs, expect_to_fail=False, fedora_dc=True
                )
            dc_cephfs_pod_obj.append(dc_cephfs)

        # Get the machine name using the node name
        machine_name = machine.get_machine_from_node_name(osd_node_obj.name)
        log.info(f"{osd_node_obj.name} associated machine is {machine_name}")

        # Get the machineset name using machine name
        machineset_name = machine.get_machineset_from_machine_name(
            machine_name
        )
        log.info(
            f"{osd_node_obj.name} associated machineset is {machineset_name}"
        )

        # Induce failure
        nodes.stop_nodes([osd_node_obj], wait=True)
        log.info(f"Successfully powered off node: {osd_node_obj.name}")

        # TODO - Network failure as one more failure type

        # Add annotation to the failed node
        annotation = "machine.openshift.io/exclude-node-draining=''"
        ocp_obj = ocp.OCP(
            kind='machine',
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
        )
        command = f"annotate machine {machine_name} {annotation}"
        log.info(f"Adding annotation: {command}")
        ocp_obj.exec_oc_cmd(command)

        # Delete the machine
        machine.delete_machine(machine_name)
        log.info(f"Successfully deleted machine {machine_name}")

        # Wait for the new machine to spin
        log.info("Waiting for the new node to be in ready state")
        machine.wait_for_new_node_to_be_ready(machineset_name)

        # Get the node name of new spun node
        nodes_after_new_spun_node = get_worker_nodes()
        new_spun_node = list(
            set(nodes_after_new_spun_node) - set(initial_nodes)
        )
        log.info(f"New spun node is {new_spun_node}")

        # Label it
        node_obj = ocp.OCP(kind='node')
        node_obj.add_label(
            resource_name=new_spun_node[0],
            label=constants.OPERATOR_NODE_LABEL
        )
        log.info(
            f"Successfully labeled {new_spun_node} with OCS storage label"
        )

        # Check the pods should be in running state
        all_pod_obj = pod.get_all_pods(wait=True)
        for pod_obj in all_pod_obj:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=300
            )

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
