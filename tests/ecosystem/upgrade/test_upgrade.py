import logging

import pytest

from ocs_ci.framework.testlib import (
    ocs_upgrade,
    polarion_id,
    post_ocs_upgrade,
    skipif_external_mode,
)
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown, osd_node_reboot
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.ocs.ocp import get_ocs_parsed_version
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.helpers.helpers import get_mon_pdb

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
    log.info("Starting disruptive function: test_worker_node_abrupt_shutdown")
    run_ocs_upgrade(operation=worker_node_shutdown, abrupt=True)


@pytest.mark.polarion_id("OCS-1575")
def test_worker_node_permanent_shutdown(teardown):
    """
    Test OCS upgrade with disruption of shutting down worker node

    """
    log.info("Starting disruptive function: test_worker_node_permanent_shutdown")
    run_ocs_upgrade(operation=worker_node_shutdown, abrupt=False)


@pytest.mark.polarion_id("OCS-1558")
def test_osd_reboot(teardown):
    """
    OCS Upgrade with node reboot: with 1 OSD going down and back up while upgrade is running

    """

    log.info("Starting disruptive function: test_osd_reboot")
    run_ocs_upgrade(operation=osd_node_reboot)


@ocs_upgrade
@polarion_id(get_polarion_id(upgrade=True))
def test_upgrade():
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade()


@skipif_external_mode
@post_ocs_upgrade
@pytest.mark.polarion_id("OCS-2449")
def test_check_mon_pdb_post_upgrade():
    """
    Testcase to check disruptions_allowed and minimum
    available mon count

    """
    ceph_obj = CephCluster()

    # Check for mon count
    mons_after_upgrade = ceph_obj.get_mons_from_cluster()
    log.info(f"Mons after upgrade {mons_after_upgrade}")

    disruptions_allowed, min_available_mon, max_unavailable_mon = get_mon_pdb()
    log.info(f"Number of Mons Disruptions_allowed {disruptions_allowed}")
    log.info(f"Minimum_available mon count {min_available_mon}")
    log.info(f"Minimum_available mon count {max_unavailable_mon}")

    # The PDB values are considered from OCS 4.5 onwards.
    assert disruptions_allowed == 1, "Mon Disruptions_allowed count not matching"
    if get_ocs_parsed_version() <= 4.6:
        assert min_available_mon == 2, "Minimum available mon count is not matching"
    else:
        # This mon pdb change is from 4.7 on wards, please refer bz1935065
        assert max_unavailable_mon == 1, "Maximum unavailable mon count is not matching"
