import logging
import re

import pytest
import random
from tests.helpers import get_worker_nodes
from ocs_ci.framework.pytest_customization.marks import tier4a
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    tier4, ManageTest, aws_platform_required, ignore_leftovers, ipi_deployment_required
)
from ocs_ci.ocs import (
    machine, constants, ocp, node
)
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)


@tier4
@tier4a
@ignore_leftovers
@aws_platform_required
@ipi_deployment_required
class TestNodeReplacement(ManageTest):
    """
    Knip-894 Node replacement - AWS-IPI-Proactive

    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_nodereplacement_proactive(self, pvc_factory, pod_factory, dc_pod_factory):
        """
        Knip-894 Node Replacement proactive

        """

        # Get worker nodes
        worker_node_list = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_node_list}")

        osd_pods_obj = pod.get_osd_pods()
        osd_node_name = pod.get_pod_node(random.choice(osd_pods_obj)).name
        log.info(f"Selected OSD is {osd_node_name}")

        log.info("Creating dc pod backed with rbd pvc and running io in bg")
        for worker_node in worker_node_list:
            if worker_node != osd_node_name:
                rbd_dc_pod = dc_pod_factory(interface=constants.CEPHBLOCKPOOL, node_name=worker_node, size=20)
                pod.run_io_in_bg(rbd_dc_pod, expect_to_fail=False, fedora_dc=True)

        log.info("Creating dc pod backed with cephfs pvc and running io in bg")
        for worker_node in worker_node_list:
            if worker_node != osd_node_name:
                cephfs_dc_pod = dc_pod_factory(interface=constants.CEPHFILESYSTEM, node_name=worker_node, size=20)
                pod.run_io_in_bg(cephfs_dc_pod, expect_to_fail=False, fedora_dc=True)

        # Unscheduling node
        node.unschedule_nodes([osd_node_name])
        # Draining Node
        node.drain_nodes([osd_node_name])
        log.info("Getting machine name from specified node name")
        machine_name = machine.get_machine_from_node_name(osd_node_name)
        log.info(f"Node {osd_node_name} associated machine is {machine_name}")
        log.info(f"Deleting machine {machine_name} and waiting for new machine to come up")
        machine.delete_machine_and_check_state_of_new_spinned_machine(machine_name)
        new_machine_list = machine.get_machines()
        for machines in new_machine_list:
            # Trimming is done to get just machine name
            # eg:- machine_name:- prsurve-40-ocs-43-kbrvf-worker-us-east-2b-nlgkr
            # After trimming:- prsurve-40-ocs-43-kbrvf-worker-us-east-2b
            if re.match(machines.name[:-6], machine_name):
                new_machine_name = machines.name
        machineset_name = machine.get_machineset_from_machine_name(new_machine_name)
        log.info("Waiting for new worker node to be in ready state")
        machine.wait_for_new_node_to_be_ready(machineset_name)
        new_node_obj = node.get_node_from_machine_name(new_machine_name)
        log.info("Adding ocs label to newly created worker node")
        node_obj = ocp.OCP(kind='node')
        node_obj.add_label(
            resource_name=new_node_obj.name,
            label=constants.OPERATOR_NODE_LABEL
        )
        log.info(
            f"Successfully labeled {new_node_obj.name} with OCS storage label"
        )
        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        # Deleting Resources
        self.sanity_helpers.delete_resources()
        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check()
