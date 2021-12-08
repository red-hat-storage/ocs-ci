import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.framework.testlib import (
    ocs_upgrade,
    polarion_id,
)
from ocs_ci.framework import config
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown, osd_node_reboot
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


@purple_squad
@pytest.mark.polarion_id("OCS-1579")
def test_worker_node_abrupt_shutdown(teardown):
    """
    Test OCS upgrade with disruption of shutting down worker node,
    for 5.5 minutes

    """
    log.info("Starting disruptive function: test_worker_node_abrupt_shutdown")
    run_ocs_upgrade(operation=worker_node_shutdown, abrupt=True)


@purple_squad
@pytest.mark.polarion_id("OCS-1575")
def test_worker_node_permanent_shutdown(teardown):
    """
    Test OCS upgrade with disruption of shutting down worker node

    """
    log.info("Starting disruptive function: test_worker_node_permanent_shutdown")
    run_ocs_upgrade(operation=worker_node_shutdown, abrupt=False)


@purple_squad
@pytest.mark.polarion_id("OCS-1558")
def test_osd_reboot(teardown):
    """
    OCS Upgrade with node reboot: with 1 OSD going down and back up while upgrade is running

    """

    log.info("Starting disruptive function: test_osd_reboot")
    run_ocs_upgrade(operation=osd_node_reboot)


import json
import time


@pytest.fixture(scope="session")
def pause_file(tmpdir_factory):
    pause_file = tmpdir_factory.mktemp("pause").join("pause.json")
    pause_dict = {"pause": "true"}
    pause_file.write(json.dumps(pause_dict))
    log.warning(str(pause_file))
    return str(pause_file)


@ocs_upgrade
@purple_squad
@polarion_id(get_polarion_id(upgrade=True))
def test_upgrade(pause_file):
    """
    Tests upgrade procedure of OCS cluster

    """

    result = {"pause": "true"}
    log.info("Upgrade pause started")
    config.RUN["thread_pagerduty_secret_update"] = "required"
    while result["pause"] == "true":
        with open(pause_file) as open_file:
            result = json.load(open_file)
        time.sleep(3)
    log.info("Upgrade pause ended")
