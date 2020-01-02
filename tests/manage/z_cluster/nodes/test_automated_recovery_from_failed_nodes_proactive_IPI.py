import logging
import pytest
import random
from ocs_ci.framework.testlib import (
    tier4, ManageTest, aws_platform_required, ignore_leftovers)
from ocs_ci.ocs import machine
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources import pod
from tests.helpers import get_worker_nodes
from tests.helpers import wait_for_resource_state
from tests.sanity_helpers import Sanity


log = logging.getLogger(__name__)


@tier4
@ignore_leftovers
@aws_platform_required
class Testknip678(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_knip678_proactive(self, pvc_factory, pod_factory):
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
        machineset_name = machine.get_machineset_from_machine_name(machine_name)
        log.info(f"{osd_node_name} associated machineset is {machineset_name}")

        # Get the initial nodes list
        initial_nodes = get_worker_nodes()
        log.info(f"Initial current available nodes are {initial_nodes}")

        # get machineset replica count
        machineset_replica_count = machine.get_replica_count(machineset_name)
        log.info(
            f"{machineset_name} has replica count: {machineset_replica_count}"
        )

        # Increase its replica count
        machine.add_node(machineset_name, count=machineset_replica_count + 1)
        log.info(
            f"Increased {machineset_name} count "
            f"by {machineset_replica_count + 1}"
        )

        # wait for the new node to come to ready state
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

        # Delete the machine
        machine.delete_machine(machine_name)
        log.info(f"Successfully deleted machine {machine_name}")

        # Check the pods should be in running state
        all_pod_obj = pod.get_all_pods()
        for pod_obj in all_pod_obj:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )

        # Check basic cluster functionality by creating resources
        # (pools, storageclasses, PVCs, pods - both CephFS and RBD),
        # run IO and delete the resources
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        self.sanity_helpers.delete_resources()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check()
