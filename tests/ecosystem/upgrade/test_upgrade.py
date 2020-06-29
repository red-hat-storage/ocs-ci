import logging

import pytest

from ocs_ci.framework.testlib import ocs_upgrade
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade


log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["operation"], argvalues=[
        pytest.param(
            worker_node_shutdown, marks=pytest.mark.polarion_id("OCS-1579")
        ),
    ]
)
def test_worker_node_abrupt_shutdown(operation):
    log.info("Starting disruptive test function: "
             "test_worker_node_abrupt_shutdown")
    abrupt = True
    run_ocs_upgrade(operation, abrupt)


@pytest.mark.parametrize(
    argnames=["operation"], argvalues=[
        pytest.param(
            worker_node_shutdown, marks=pytest.mark.polarion_id("OCS-1575")
        ),
    ]
)
def test_worker_node_permanent_shutdown(operation):
    log.info("Starting disruptive test function:"
             " test_worker_node_permanent_shutdown")
    abrupt = False
    run_ocs_upgrade(operation, abrupt)


@ocs_upgrade
def test_upgrade():
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade()
