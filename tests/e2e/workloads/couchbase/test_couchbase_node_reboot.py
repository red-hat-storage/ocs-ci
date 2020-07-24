import logging
import random

import pytest

from ocs_ci.utility.utils import TimeoutSampler
from tests.sanity_helpers import Sanity
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from ocs_ci.ocs.node import (
    get_osd_running_nodes,
    get_node_objs,
    get_node_resource_utilization_from_adm_top)
from tests.helpers import get_master_nodes

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
    def test_run_couchbase_node_reboot(self, cb_setup, nodes, pod_name_of_node):
        """
        Test couchbase workload with node reboot
        """
        if pod_name_of_node == 'couchbase':
            node_list = self.cb.get_couchbase_nodes()
        elif pod_name_of_node == 'osd':
            node_list = get_osd_running_nodes()
        elif pod_name_of_node == 'master':
            node_list = get_master_nodes()

        node_1 = get_node_objs(
            node_list[random.randint(0, len(node_list) - 1)])

        # Check worker node utilization (adm_top)
        get_node_resource_utilization_from_adm_top(
            node_type='worker', print_table=True
        )
        get_node_resource_utilization_from_adm_top(
            node_type='master', print_table=True
        )
        # Restart relevant node
        nodes.restart_nodes(node_1)
        for sample in TimeoutSampler(300, 5, self.cb.result.done):
            if sample:
                break
            else:
                logging.info(
                    "#### ....Waiting for couchbase threads to complete..."
                )
        self.sanity_helpers.health_check()
