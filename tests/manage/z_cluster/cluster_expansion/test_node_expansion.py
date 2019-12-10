import logging
import pytest

from ocs_ci.utility.utils import TimeoutSampler
from tests import helpers
from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@ignore_leftovers
@tier1
class TestAddNode(ManageTest):
    """
    Automates adding worker nodes to the cluster while IOs
    """
    def test_add_node(self):
        """
        Test for adding worker nodes to the cluster while IOs
        """
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            before_replica_counts = dict()
            count = 2
            machines = machine_utils.get_machinesets()
            for machine in machines:
                before_replica_counts.update({machine: machine_utils.get_replica_count(machine)})
            worker_nodes_before = helpers.get_worker_nodes()
            logger.info(f'The worker nodes number before adding a new node is {len(worker_nodes_before)}')
            after_replica_counts = dict()
            for machine in machines:
                machine_utils.add_node(machine, count=count)
                after_replica_counts.update(({machine: machine_utils.get_replica_count(machine)}))
            logger.info(after_replica_counts)
            for sample in TimeoutSampler(
                timeout=300, sleep=3, func=helpers.get_worker_nodes
            ):
                if len(sample) == count * len(machines):
                    break

            worker_nodes_after = helpers.get_worker_nodes()
            logger.info(f'The worker nodes number after adding a new node is {len(worker_nodes_after)}')
            wait_for_nodes_status(
                node_names=worker_nodes_after, status=constants.NODE_READY
            )
        else:
            pytest.skip("UPI not yet supported")
        # ToDo run IOs
