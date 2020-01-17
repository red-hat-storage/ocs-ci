import logging
import re

import pytest
import random
from ocs_ci.framework.testlib import (
    tier4, ManageTest, aws_platform_required, ignore_leftovers, ipi_deployment_required)
from ocs_ci.ocs import machine
from ocs_ci.ocs import constants
from ocs_ci.ocs import ocp
from ocs_ci.ocs import node
from tests.sanity_helpers import Sanity


log = logging.getLogger(__name__)

@tier4
@ignore_leftovers
@aws_platform_required
@ipi_deployment_required
class Testknip894(ManageTest):
    """
    Knip-678 Node Replacement
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()

    def test_nodereplacement_proactive(self, pvc_factory, pod_factory):
        """
        Knip-678 Node Replacement
        """
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        ocs_worker_node = node.get_ocs_worker_nodes()
        machine_name = machine.get_machine_from_node_name(random.choice(ocs_worker_node))
        log.info(f"Deleting machine {machine_name}")
        machine.delete_machine_and_check_state_of_new_spinned_machine(machine_name)
        new_machine_list = machine.get_machines()
        for machines in new_machine_list:
            if re.match(machines.name[:-6], machine_name):
                new_machine_name = machines.name
        machineset_name = machine.get_machineset_from_machine_name(new_machine_name)
        log.info("Wating for new worker node to be in ready state")
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
        self.sanity_helpers.health_check()
        self.sanity_helpers.delete_resources()
