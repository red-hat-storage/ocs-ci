import logging

import pytest
from pkg_resources import parse_version

from ocs_ci.framework.testlib import ocs_upgrade, polarion_id, post_ocs_upgrade
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown, osd_node_reboot
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import get_mon_pdb
from ocs_ci.ocs.utils import save_live_logs

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


@pytest.fixture()
def save_and_track_nb_db_migration(request):
    """
    This fixture tracks the DB migration that is done during OCS upgrade from 4.6 to 4.7.
    It also looks for DB migration failures which are not translated to an OCS upgrade failure

    """
    version_before_upgrade = config.ENV_DATA.get("ocs_version")
    upgrade_version = config.UPGRADE.get("upgrade_ocs_version")
    if parse_version(version_before_upgrade) == parse_version("4.6") and parse_version(
        upgrade_version
    ) == parse_version("4.7"):
        pods_containers_dict = {"noobaa-db": "db", "noobaa-upgrade-job": "migrate-job"}
        pattern_to_log = ["ERROR in merging", "failed with error"]
        save_live_logs(request, pods_containers_dict, pattern_to_log)


@ocs_upgrade
@polarion_id(get_polarion_id(upgrade=True))
def test_upgrade(save_and_track_nb_db_migration):
    """
    Tests upgrade procedure of OCS cluster

    """
    run_ocs_upgrade()


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

    disruptions_allowed, min_available_mon = get_mon_pdb()
    log.info(f"Number of Mons Disruptions_allowed {disruptions_allowed}")
    log.info(f"Minimum_available mon count {min_available_mon}")

    # The PDB values are considered from OCS 4.5 onwards.
    assert disruptions_allowed == 1, "Mon Disruptions_allowed count not matching"
    assert min_available_mon == 2, "Minimum available mon count is not matching"
