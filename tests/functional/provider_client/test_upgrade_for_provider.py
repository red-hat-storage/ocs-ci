import logging
import pytest

from ocs_ci.framework.testlib import (
    ocs_upgrade,
)
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade, run_ocs_upgrade
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
from ocs_ci.framework import config
from ocs_ci.utility import version

# from semantic_version import Version

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

    @pytest.fixture()
    def teardown(request, nodes):
        def finalizer():
            """
            Make sure all nodes are up again

            """
            # nodes.restart_nodes_by_stop_and_start_teardown()
            # request.addfinalizer(finalizer)
            pass

    @runs_on_provider
    @ocs_upgrade
    def test_ocs_minor_version_upgrade_for_provider(self):
        """
        Tests upgrade procedure of OCS cluster

        """
        ocs_version = str(version.get_ocs_version_from_csv()).replace(".stable", "")
        log.info(f"ocs version: {ocs_version}")
        log.info(
            "Validate major version of ocs operator for provider is same as major version of odf client operator"
        )
        self.storage_clients.verify_version_of_odf_client_operator()
        upgrade_in_current_source = config.UPGRADE.get(
            "upgrade_in_current_source", False
        )
        upgrade_ocs = OCSUpgrade(
            namespace=config.ENV_DATA["cluster_namespace"],
            version_before_upgrade=ocs_version,
            ocs_registry_image=config.UPGRADE.get("upgrade_ocs_registry_image"),
            upgrade_in_current_source=upgrade_in_current_source,
        )
        upgrade_version = upgrade_ocs.ocs_registry_image
        log.info(f"ocs upgrade version: {upgrade_version}")
        # if Version.coerce(upgrade_version.major) == Version.coerce(
        #     ocs_version_major
        # ) and Version.coerce(upgrade_version.minor) > Version.coerce(ocs_version_minor):
        run_ocs_upgrade()
        log.info("Validate post provider ocs upgrade odf client operator also upgraded")
        self.storage_clients.verify_version_of_odf_client_operator()
        # else:
        #     log.info("The upgrade request is not for minor upgrade")

    @runs_on_provider
    @ocs_upgrade
    def test_ocs_major_version_upgrade_for_provider(self):
        """
        Tests upgrade procedure of OCS cluster
        odf upgrade from 4.16 to 4.17 sequence---
            Upgrade ocp
            upgrade acm
            upgrade odf --- odf client should automatically upgrade
        """
        log.info(
            "Validate major version of ocs operator for provider is same as major version of odf client operator"
        )
        ocp_version = version.get_semantic_ocp_version_from_config()
        ocs_version = version.get_semantic_ocs_version_from_config()
        log.debug(f"Cluster versions before upgrade:\n{ocp_version}")
        log.debug(f"ocs versions before upgrade:\n{ocs_version}")
        upgrade_in_current_source = config.UPGRADE.get(
            "upgrade_in_current_source", False
        )
        upgrade_ocs = OCSUpgrade(
            namespace=config.ENV_DATA["cluster_namespace"],
            version_before_upgrade=ocs_version,
            ocs_registry_image=config.UPGRADE.get("upgrade_ocs_registry_image"),
            upgrade_in_current_source=upgrade_in_current_source,
        )
        upgrade_version = upgrade_ocs.get_upgrade_version()
        log.info(f"upgrade to ocs version: {upgrade_version}")
        # if Version.coerce(
        #     version.get_semantic_version(ocp_version, only_major_minor=True)
        # ) >= Version.coerce(
        #     version.get_semantic_version(upgrade_version, only_major_minor=True)
        # ):
        run_ocs_upgrade()
        log.info("Validate post provider ocs upgrade odf client operator also upgraded")
        self.storage_clients.verify_version_of_odf_client_operator()
        # else:
        #     self.test_upgrade_ocp.test_upgrade_ocp()
        #     run_ocs_upgrade()
        #     log.info(
        #         "Validate post provider ocs upgrade odf client operator also upgraded"
        #     )
        #     self.storage_clients.verify_version_of_odf_client_operator()

    @runs_on_provider
    @ocp_upgrade
    def test_ocp_upgrade_for_provider_without_hcp_cluster(self):
        """
        This test is to validate ocp minor version upgrade for provider

        """
        self.test_upgrade_ocp.test_upgrade_ocp()

    @ocs_upgrade
    def test_testmethod(self):
        """
        Tests methods for temporary

        """
        ocs_version = str(version.get_ocs_version_from_csv()).replace(".stable", "")
        log.info(f"ocs version: {ocs_version}")
        upgrade_in_current_source = config.UPGRADE.get(
            "upgrade_in_current_source", False
        )
        upgrade_ocs = OCSUpgrade(
            namespace=config.ENV_DATA["cluster_namespace"],
            version_before_upgrade=ocs_version,
            ocs_registry_image=config.UPGRADE.get("upgrade_ocs_registry_image"),
            upgrade_in_current_source=upgrade_in_current_source,
        )
        log.info(f"upgrade ocs: {upgrade_ocs}")
        upgrade_version = upgrade_ocs.get_upgrade_version()
        log.info(f"ocs upgrade version: {upgrade_version}")
