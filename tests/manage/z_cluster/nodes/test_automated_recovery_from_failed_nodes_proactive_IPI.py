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


@tier4
@ignore_leftovers
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

    def test_automated_recovery_from_failed_nodes_IPI_proactive(
            self, pvc_factory, pod_factory
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Proactive case - IPI
        """
        # Creating resources (pools, storageclasses, PVCs,
        # pods - both CephFS and RBD), run IO
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        # Get the osd associated node name
        osd_pods_obj = pod.get_osd_pods()
        osd_node_name = pod.get_pod_node(random.choice(osd_pods_obj)).name
        log.info(f"Selected OSD is {osd_node_name}")

        # Get the machine name using the node name
        machine_name = machine.get_machine_from_node_name(osd_node_name)
        log.info(f"{osd_node_name} associated machine is {machine_name}")

        # Get the machineset name using machine name
        machineset_name = machine.get_machineset_from_machine_name(
            machine_name
        )
        log.info(
            f"{osd_node_name} associated machineset is {machineset_name}"
        )

        # Add a new node and label it
        assert add_new_node_and_label_it(
            machineset_name
        ), "Failed adding new node.. Check logs"

        # Delete the machine
        machine.delete_machine(machine_name)
        log.info(f"Successfully deleted machine {machine_name}")

        # Check the pods should be in running state
        all_pod_obj = pod.get_all_pods()
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
