import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    multicluster_roles,
    runs_on_provider,
    yellow_squad,
)
from ocs_ci.framework.testlib import (
    ocs_upgrade,
    polarion_id,
    mco_upgrade,
    dr_hub_upgrade,
    dr_cluster_operator_upgrade,
    acm_upgrade,
    provider_operator_upgrade,
)
from ocs_ci.framework import config
from ocs_ci.ocs.acm_upgrade import ACMUpgrade
from ocs_ci.ocs.disruptive_operations import worker_node_shutdown, osd_node_reboot
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.ocs.dr_upgrade import (
    DRClusterOperatorUpgrade,
    MultiClusterOrchestratorUpgrade,
    DRHubUpgrade,
)
from ocs_ci.ocs.provider_client_upgrade import ProviderClusterOperatorUpgrade
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.utils import is_z_stream_upgrade

log = logging.getLogger(__name__)


operator_map = {
    "mco": MultiClusterOrchestratorUpgrade,
    "drhub": DRHubUpgrade,
    "drcluster": DRClusterOperatorUpgrade,
}


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


@pytest.fixture
def config_index(request):
    return request.param if hasattr(request, "param") else None


@purple_squad
@ocs_upgrade
@polarion_id(get_polarion_id(upgrade=True))
@multicluster_roles(["mdr-all-odf", "rdr-all-odf"])
def test_upgrade(zone_rank, role_rank, config_index, upgrade_stats=None):
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade(upgrade_stats=upgrade_stats)
    if config.multicluster and config.MULTICLUSTER["multicluster_mode"] == "metro-dr":
        # Perform validation for MCO, dr hub operator and dr cluster operator here
        # in case of z stream because we wouldn't call those tests in the case of
        # z stream
        if is_z_stream_upgrade():
            for operator, op_upgrade_cls in operator_map.items():
                temp = op_upgrade_cls()
                log.info(f"Validating upgrade for {operator}")
                temp.validate_upgrade()


@purple_squad
@mco_upgrade
@multicluster_roles(["mdr-all-acm", "rdr-all-acm"])
def test_mco_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade procedure for multicluster orchestrator operator

    """
    mco_upgrade_obj = MultiClusterOrchestratorUpgrade()
    mco_upgrade_obj.run_upgrade()


@purple_squad
@dr_hub_upgrade
@multicluster_roles(["mdr-all-acm", "rdr-all-acm"])
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
@multicluster_roles(["mdr-all-odf", "rdr-all-odf"])
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


@yellow_squad
@provider_operator_upgrade
@runs_on_provider
def test_provider_cluster_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade for provider cluster

    """
    provider_cluster_upgrade_obj = ProviderClusterOperatorUpgrade()
    provider_cluster_upgrade_obj.run_provider_upgrade()


@purple_squad
@acm_upgrade
@multicluster_roles(["mdr-all-acm", "rdr-all-acm"])
def test_acm_upgrade(zone_rank, role_rank, config_index):
    """
    Test upgrade procedure for ACM operator

    """
    acm_hub_upgrade_obj = ACMUpgrade()
    acm_hub_upgrade_obj.run_upgrade()
