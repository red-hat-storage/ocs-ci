import logging

import pytest

from ocs_ci.ocs import node
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.node import get_node_resource_utilization_from_adm_top
from ocs_ci.ocs import flowtest

log = logging.getLogger(__name__)


@workloads
@ignore_leftovers
class TestCouchBaseNodeDrain(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """
    @pytest.fixture()
    def cb_setup(self, couchbase_factory_fixture):
        """
        Creates couchbase workload
        """
        self.cb = couchbase_factory_fixture(
            replicas=3, run_in_bg=True, skip_analyze=True
        )

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    def test_run_couchbase_node_drain(self, cb_setup, node_type='master'):
        """
        Test couchbase workload with node drain
        """
        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(
            node_type='worker', print_table=True
        )

        # Node drain with specific node type
        typed_nodes = node.get_typed_nodes(
            node_type=node_type, num_of_nodes=1
        )
        typed_node_name = typed_nodes[0].name

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([typed_node_name])

        # Make the node schedulable again
        node.schedule_nodes([typed_node_name])

        # Perform cluster and Ceph health checks
        bg_handler = flowtest.BackgroundOps()
        bg_ops = [self.cb.result]
        bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
        self.sanity_helpers.health_check()
