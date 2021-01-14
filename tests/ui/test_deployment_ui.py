import logging

from ocs_ci.ocs.ui.deployment_ui import DeploymentUI


logger = logging.getLogger(__name__)


class TestDeploymentUI(object):
    """
    Test Deployment via UI

    """

    def test_deployment_dynamic(
        self,
        setup_ui,
        mode="internal",
        storage_class="thin",
        osd_size="0.5TiB",
        is_encryption=True,
    ):
        """"""
        deployment_ui = DeploymentUI(setup_ui)
        deployment_ui.select_mode = mode
        deployment_ui.select_storage_class = storage_class
        deployment_ui.select_osd_size = osd_size
        deployment_ui.select_encryption = is_encryption
        deployment_ui.install_ocs_opeartor()
        deployment_ui.install_storage_cluster()
