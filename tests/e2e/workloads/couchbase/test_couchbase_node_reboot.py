import logging
import random
import time
import pytest

from ocs_ci.ocs import ocp
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_typed_nodes,
    get_osd_running_nodes,
    get_node_objs,
    get_node_resource_utilization_from_adm_top)
from ocs_ci.ocs import flowtest
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException

log = logging.getLogger(__name__)


@workloads
@ignore_leftovers
class TestCouchBaseNodeReboot(E2ETest):
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

    @pytest.mark.parametrize(
        argnames=["pod_name_of_node"],
        argvalues=[
            pytest.param(
                *['osd'], marks=pytest.mark.polarion_id("OCS-776")
            ),
            pytest.param(
                *['master'], marks=pytest.mark.polarion_id("OCS-783")
            ),
            pytest.param(
                *['couchbase'], marks=pytest.mark.polarion_id("OCS-776")
            )
        ]
    )
    def test_run_couchbase_node_reboot(
            self, cb_setup, nodes, pod_name_of_node):
        """
        Test couchbase workload with node reboot
        """
        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(
            node_type='worker', print_table=True
        )
        get_node_resource_utilization_from_adm_top(
            node_type='master', print_table=True
        )

        if pod_name_of_node == 'couchbase':
            node_list = self.cb.get_couchbase_nodes()
        elif pod_name_of_node == 'osd':
            node_list = get_osd_running_nodes()
        elif pod_name_of_node == 'master':
            master_node = get_typed_nodes(pod_name_of_node, num_of_nodes=1)

        # Restart relevant node
        if pod_name_of_node == 'master':
            nodes.restart_nodes(master_node, wait=False)
            waiting_time = 40
            log.info(f"Waiting {waiting_time} seconds...")
            time.sleep(waiting_time)
        else:
            restart_node = get_node_objs(node_list[random.randint(0, len(node_list) - 1)])
            nodes.restart_nodes(restart_node)

        # Validate all nodes and services are in READY state and up

        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=60,
            delay=15)(
            ocp.wait_for_cluster_connectivity(tries=400)
        )
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=60,
            delay=15)(
            wait_for_nodes_status(timeout=1800)
        )
        bg_handler = flowtest.BackgroundOps()
        bg_ops = [self.cb.result]
        retry(
            (CommandFailed),
            tries=60,
            delay=15)(
            bg_handler.wait_for_bg_operations(bg_ops, timeout=3600)
        )
        self.sanity_helpers.health_check()
