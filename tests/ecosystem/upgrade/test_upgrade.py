import logging

import pytest

from ocs_ci.framework.testlib import ocs_upgrade, polarion_id
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.utility.reporting import get_polarion_id

log = logging.getLogger(__name__)


@pytest.fixture()
def teardown(request, nodes):

    def finalizer():
        """
        Make sure all nodes are up again

        """
        nodes.restart_nodes_by_stop_and_start_teardown()

    request.addfinalizer(finalizer)


@pytest.mark.polarion_id("OCS-1579")
def test_worker_node_abrupt_shutdown(teardown):
    """
    Test OCS upgrade with disruption of shutting down worker node,
    for 5.5 minutes

    """
    log.info(
        "Starting disruptive function: test_worker_node_abrupt_shutdown"
    )
    run_ocs_upgrade(operation=worker_node_shutdown, abrupt=True)


@pytest.mark.polarion_id("OCS-1575")
def test_worker_node_permanent_shutdown(teardown):
    """
    Test OCS upgrade with disruption of shutting down worker node

    """
    log.info(
        "Starting disruptive function: test_worker_node_permanent_shutdown"
    )
    run_ocs_upgrade(operation=worker_node_shutdown, abrupt=False)


@ocs_upgrade
@polarion_id(get_polarion_id(upgrade=True))
def test_upgrade():
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade()
