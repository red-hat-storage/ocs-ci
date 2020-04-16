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
            machines = machine_utils.get_machinesets()
            for machine in machines:
                before_replica_counts.update(
                    {machine: machine_utils.get_replica_count(machine)}
                )
                logger.info(machine_utils.get_replica_count(machine))
            logger.info(f'The worker nodes number before {len(helpers.get_worker_nodes())}')
            after_replica_counts = dict()
            total_count = 0
            for machine in machines:
                machine_utils.add_node(
                    machine, count=machine_utils.get_replica_count(machine) + 1
                )
                after_replica_counts.update(
                    ({machine: machine_utils.get_replica_count(machine)})
                )
                total_count += machine_utils.get_replica_count(machine)
                logger.info(total_count)
            logger.info(after_replica_counts)
            for sample in TimeoutSampler(
                timeout=600, sleep=6, func=helpers.get_worker_nodes
            ):
                if len(sample) == total_count:
                    break

            logger.info(f'The worker nodes number after {len(helpers.get_worker_nodes())}')
            wait_for_nodes_status(
                node_names=helpers.get_worker_nodes(),
                status=constants.NODE_READY
            )
        else:
            pytest.skip("UPI not yet supported")
        # ToDo run IOs
