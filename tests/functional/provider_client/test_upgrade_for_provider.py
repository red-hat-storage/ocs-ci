import logging
import pytest

from ocs_ci.framework.testlib import (
    ocs_upgrade,
)
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
    yellow_squad,
)
from ocs_ci.framework.testlib import ManageTest, ocp_upgrade
from ocs_ci.ocs.resources.storage_client import StorageClient
from tests.functional.upgrade.test_upgrade_ocp import TestUpgradeOCP
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.ocs.acm_upgrade import ACMUpgrade

log = logging.getLogger(__name__)


@yellow_squad
@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
@skipif_external_mode
@skipif_managed_service
class TestUpgradeForProviderClient(ManageTest):
    def setup(self):
        self.storage_clients = StorageClient()
        self.test_upgrade_ocp = TestUpgradeOCP()
        self.metallb_installer_obj = MetalLBInstaller()
        self.cnv_installer_obj = CNVInstaller()
        self.acm_hub_upgrade_obj = ACMUpgrade()

    @pytest.fixture()
    def teardown(request, nodes):
        def finalizer():
            """
            Make sure all nodes are up again

            """
            nodes.restart_nodes_by_stop_and_start_teardown()
            request.addfinalizer(finalizer)

    @runs_on_provider
    @ocp_upgrade
    @ocs_upgrade
    def test_ocp_ocs_upgrade_for_provider(self, reduce_and_resume_cluster_load):
        """
        This test is to validate ocp and ocs upgrade for provider
        upgrades for provider cluster
        eg: odf upgrade from 4.16 to 4.17 sequence---
            Upgrade ocp
            upgrade acm
            upgrade cnv
            upgrade metalLB
            upgrade odf --- odf client should automatically upgraded
            for GA to GA upgrade
        """
        self.test_upgrade_ocp.test_upgrade_ocp()
        self.acm_hub_upgrade_obj.run_upgrade()
        self.cnv_installer_obj.upgrade_cnv()
        self.metallb_installer_obj.upgrade_metallb()
        run_ocs_upgrade()
        self.storage_clients.verify_version_of_odf_client_operator()
