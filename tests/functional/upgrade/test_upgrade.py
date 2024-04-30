import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    multicluster_roles,
)
from ocs_ci.framework.testlib import (
    ocs_upgrade,
    polarion_id,
    mco_upgrade,
    dr_hub_upgrade,
    dr_cluster_operator_upgrade,
    acm_upgrade,
)
from ocs_ci.ocs.acm_upgrade import ACMUpgrade
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown, osd_node_reboot
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.ocs.dr_upgrade import (
    DRClusterOperatorUpgrade,
    MultiClusterOrchestratorUpgrade,
    DRHubUpgrade,
)
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.utils import is_z_stream_upgrade

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


@purple_squad
@ocs_upgrade
@polarion_id(get_polarion_id(upgrade=True))
@multicluster_roles(["mdr-all-odf"])
def test_upgrade(zone_rank, role_rank, config_index):
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade()


@purple_squad
@mco_upgrade
@multicluster_roles(["mdr-all-acm"])
def test_mco_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade procedure for multicluster orchestrator operator

    """
    mco_upgrade_obj = MultiClusterOrchestratorUpgrade()
    mco_upgrade_obj.run_upgrade()


@purple_squad
@dr_hub_upgrade
@multicluster_roles(["mdr-all-acm"])
def test_dr_hub_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade procedure for DR hub operator

    """
    if is_z_stream_upgrade():
        pytest.skip(
            "This is z-stream upgrade and this component upgrade should have been taken care by ODF upgrade"
        )
    dr_hub_upgrade_obj = DRHubUpgrade()
    dr_hub_upgrade_obj.run_upgrade()


@purple_squad
@dr_cluster_operator_upgrade
@multicluster_roles(["mdr-all-odf"])
def test_dr_cluster_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade procedure for DR cluster operator

    """
    if is_z_stream_upgrade():
        pytest.skip(
            "This is z-stream upgrade and this component upgrade should have been taken care by ODF upgrade"
        )
    dr_cluster_upgrade_obj = DRClusterOperatorUpgrade()
    dr_cluster_upgrade_obj.run_upgrade()


@purple_squad
@acm_upgrade
@multicluster_roles(["mdr-all-acm"])
def test_acm_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade procedure for ACM operator

    """
    acm_hub_upgrade_obj = ACMUpgrade()
    acm_hub_upgrade_obj.run_upgrade()
