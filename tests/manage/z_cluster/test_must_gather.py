import logging
import pytest
from random import randint

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest, tier1, tier4a, tier4, bugzilla, skipif_external_mode
)
from ocs_ci.ocs.must_gather.must_gather import MustGather
from ocs_ci.ocs.must_gather.const_must_gather import GATHER_COMMANDS_VERSION
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def mustgather(request):

    mustgather = MustGather()
    mustgather.collect_must_gather()

    def teardown(nodes):
        mustgather.cleanup()
        nodes.restart_nodes_by_stop_and_start_teardown()

    request.addfinalizer(teardown)
    return mustgather


class TestMustGather(ManageTest):
    @tier1
    @pytest.mark.parametrize(
        argnames=["log_type"],
        argvalues=[
            pytest.param(
                *["CEPH"],
                marks=[pytest.mark.polarion_id("OCS-1583"), skipif_external_mode]
            ),
            pytest.param(
                *["JSON"],
                marks=[pytest.mark.polarion_id("OCS-1583"), skipif_external_mode]
            ),
            pytest.param(*["OTHERS"], marks=pytest.mark.polarion_id("OCS-1583")),
        ],
    )
    @pytest.mark.skipif(
        float(config.ENV_DATA["ocs_version"]) not in GATHER_COMMANDS_VERSION,
        reason=(
            "Skipping must_gather test, because there is not data for this version"
        ),
    )
    def test_must_gather(self, mustgather, log_type):
        """
        Tests functionality of: oc adm must-gather

        """
        mustgather.log_type = log_type
        mustgather.validate_must_gather()

    @tier4
    @tier4a
    @bugzilla("1770199")
    @pytest.mark.polarion_id("OCS-2328")
    def test_must_gather_worker_node_down(self, nodes):
        """
        Collect must-gather OCS logs when a worker node is down

        """
        logger.info("Get all worker nodes and choose random from worker nodes list")
        worker_nodes = get_worker_nodes()
        worker_node = worker_nodes[randint(0, len(worker_nodes) - 1)]

        logger.info(f"Stop {worker_node.name} worker node")
        nodes.stop_nodes(get_node_objs([worker_node]))

        # Collect must gather and check content
        mustgather.log_type = "CEPH"
        mustgather.validate_must_gather()

        # Start worker node
        nodes.start_nodes(get_node_objs([worker_node]))
