import logging
import pytest
import random
from ocs_ci.framework.testlib import (
    tier4, ManageTest, aws_platform_required,
    ipi_deployment_required, ignore_leftovers)
from ocs_ci.ocs import machine
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from tests.helpers import wait_for_resource_state
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.node import add_new_node_and_label_it


log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@aws_platform_required
@ipi_deployment_required
class TestAutomatedRecoveryFromFailedNodes(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames="failure",
        argvalues=[
            pytest.param(
                *["shutdown"]
            ),
            pytest.param(
                *["terminate"]
            )
        ]
    )
    def test_automated_recovery_from_failed_nodes_IPI_reactive(
            self, nodes, pvc_factory, pod_factory, failure
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI
        """
        # Get the osd associated node
        osd_pods_obj = pod.get_osd_pods()
        osd_node_obj = pod.get_pod_node(random.choice(osd_pods_obj))
        log.info(f"Selected OSD is {osd_node_obj.name}")

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

        # Add a new node and label it
        add_new_node_and_label_it(machineset_name)

        # Induce failure
        if failure == "shutdown":
            nodes.stop_nodes([osd_node_obj], wait=True)
            log.info(f"Successfully powered off node: {osd_node_obj.name}")

            nodes.terminate_nodes([osd_node_obj], wait=True)
            log.info(f"Successfully terminated node : {osd_node_obj.name} instance")
        elif failure == "terminate":
            nodes.terminate_nodes([osd_node_obj], wait=True)
            log.info(f"Successfully terminated node : {osd_node_obj.name} instance")

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
