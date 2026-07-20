import logging

import pytest

from ocs_ci.ocs import node
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    workloads,
    ignore_leftovers,
    skipif_ocp_version,
)
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top
from ocs_ci.ocs import flowtest

logger = logging.getLogger(__name__)


@magenta_squad
@skipif_ocp_version(">=4.13")
@workloads
@ignore_leftovers
class TestCouchBaseNodeDrain(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """

    @pytest.fixture()
    def cb_setup(self, couchbase_factory_fixture, node_drain_teardown):
        """
        Creates couchbase workload
        """
        logger.info("Creating CouchBase workload with 3 replicas (background mode)")
        self.cb = couchbase_factory_fixture(
            replicas=3, run_in_bg=True, skip_analyze=True
        )

        self.sanity_helpers = Sanity()

    def test_run_couchbase_node_drain(self, cb_setup, node_type="master"):
        """
        Test couchbase workload with node drain
        """
        logger.test_step("Check worker node resource utilization")
        get_node_resource_utilization_from_adm_top(node_type="worker", print_table=True)

        logger.test_step(f"Select {node_type} node for drain operation")
        typed_nodes = node.get_nodes(node_type=node_type, num_of_nodes=1)
        typed_node_name = typed_nodes[0].name
        logger.info(f"Selected node: {typed_node_name}")

        logger.test_step(f"Drain node {typed_node_name}")
        node.drain_nodes([typed_node_name])

        logger.test_step(f"Schedule node {typed_node_name} back")
        node.schedule_nodes([typed_node_name])

        logger.test_step("Wait for background workload and verify cluster health")
        bg_handler = flowtest.BackgroundOps()
        bg_ops = [self.cb.result]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
        self.sanity_helpers.health_check(tries=40)
