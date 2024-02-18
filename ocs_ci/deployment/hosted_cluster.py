import logging

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.helpers.hypershift_base import HyperShiftBase
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework import config
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS
from ocs_ci.ocs.exceptions import ProviderModeNotFoundException


logger = logging.getLogger(__name__)


class HypershiftHostedOCP(HyperShiftBase, MetalLBInstaller):
    def __init__(self):
        super(HyperShiftBase, self).__init__()
        super(MetalLBInstaller, self).__init__()

    def deploy_ocp(self):
        if (
            not config.default_cluster_ctx.ENV_DATA["platform"].lower()
            in HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            raise ProviderModeNotFoundException()

        if config.DEPLOYMENT.get("cnv_deployment"):
            CNVInstaller().deploy_cnv()
            logger.info("CNV deployment is completed")

        self.deploy_lb()
        self.download_hcp_binary()
        self.create_kubevirt_OCP_cluster()
