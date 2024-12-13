import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import purple_squad, multicluster_roles
from ocs_ci.framework.testlib import (
    ocs_upgrade,
    polarion_id,
    mco_upgrade,
    dr_hub_upgrade,
    acm_upgrade,
)
from ocs_ci.ocs.acm_upgrade import ACMUpgrade
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown, osd_node_reboot
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.ocs.dr_upgrade import MultiClusterOrchestratorUpgrade, DRHubUpgrade
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
def test_worker_node_abrupt_shutdown(teardown, upgrade_stats):
    """
    Test OCS upgrade with disruption of shutting down worker node,
    for 5.5 minutes

    """
    log.info("Starting disruptive function: test_worker_node_abrupt_shutdown")
    run_ocs_upgrade(
        operation=worker_node_shutdown, abrupt=True, upgrade_stats=upgrade_stats
    )


@purple_squad
@pytest.mark.polarion_id("OCS-1575")
def test_worker_node_permanent_shutdown(teardown, upgrade_stats):
    """
    Test OCS upgrade with disruption of shutting down worker node

    """
    log.info("Starting disruptive function: test_worker_node_permanent_shutdown")
    run_ocs_upgrade(
        operation=worker_node_shutdown, abrupt=False, upgrade_stats=upgrade_stats
    )


@purple_squad
@pytest.mark.polarion_id("OCS-1558")
def test_osd_reboot(teardown, upgrade_stats):
    """
    OCS Upgrade with node reboot: with 1 OSD going down and back up while upgrade is running

    """

    log.info("Starting disruptive function: test_osd_reboot")
    run_ocs_upgrade(operation=osd_node_reboot, upgrade_stats=upgrade_stats)


@purple_squad
@ocs_upgrade
@polarion_id(get_polarion_id(upgrade=True))
@multicluster_roles(["mdr_all_odf"])
def test_upgrade(upgrade_stats):
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade(upgrade_stats=upgrade_stats)


@purple_squad
@mco_upgrade
@multicluster_roles(["mdr_all_acm"])
def test_mco_upgrade():
    """
    Test upgrade procedure for multicluster orchestrator operator

    """
    mco_upgrade_obj = MultiClusterOrchestratorUpgrade()
    mco_upgrade_obj.run_upgrade()


@purple_squad
@dr_hub_upgrade
@multicluster_roles(["mdr_all_acm"])
def test_dr_hub_upgrade():
    """
    Test upgrade procedure for DR hub operator

    """
    dr_hub_upgrade_obj = DRHubUpgrade()
    dr_hub_upgrade_obj.run_upgrade()


@purple_squad
@acm_upgrade
@multicluster_roles(["mdr_all_acm"])
def test_acm_upgrade():
    """
    Test upgrade procedure for ACM operator

    """
    acm_hub_upgrade_obj = ACMUpgrade()
    acm_hub_upgrade_obj.run_upgrade()
