"""
Libtest: Deploy LSO + ODF operators and create storage cluster via UI.

Skips disk addition — assumes disks are already attached to VMs.
Use this to test the UI deployment flow in isolation on a cluster
where disks were pre-provisioned (e.g. after a failed deployment).
"""

import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import libtest, purple_squad
from ocs_ci.framework import config
from ocs_ci.deployment.deployment import create_catalog_source
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.ui.deployment_ui import DeploymentUI
from ocs_ci.utility.operators import LocalStorageOperator


logger = logging.getLogger(__name__)


@purple_squad
@libtest
@pytest.mark.polarion_id("OCS-LIBTEST")
class TestUILSODeployment:
    """
    Deploy ODF with LSO via UI, skipping disk provisioning.
    """

    def test_ui_lso_deploy_no_disks(self):
        """
        Run the full UI deployment flow (LSO operator install,
        ODF operator install, storage cluster creation) without
        adding disks to VMs.
        """
        live_deployment = config.DEPLOYMENT.get("live_deployment")
        if not live_deployment:
            create_catalog_source()

        if config.DEPLOYMENT.get("local_storage"):
            LocalStorageOperator(create_catalog=True)

        login_ui()
        try:
            deployment_obj = DeploymentUI()
            deployment_obj.install_local_storage_operator()
            deployment_obj.install_ocs_operator()
            deployment_obj.install_storage_cluster()
        finally:
            close_browser()
