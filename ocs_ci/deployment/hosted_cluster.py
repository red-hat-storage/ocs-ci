import logging

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.helpers.hypershift_base import HyperShiftBase
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework import config
from ocs_ci.ocs.constants import HCI_PROVIDER_CLIENT_PLATFORMS
from ocs_ci.ocs.exceptions import ProviderModeNotFoundException


logger = logging.getLogger(__name__)


class HypershiftHostedOCP(HyperShiftBase, MetalLBInstaller, CNVInstaller):
    def __init__(self):
        HyperShiftBase.__init__(self)
        MetalLBInstaller.__init__(self)
        CNVInstaller.__init__(self)

    def deploy_ocp(
        self,
        deploy_cnv=True,
        deploy_acm_hub=True,
        deploy_metallb=True,
        download_hcp_binary=True,
    ):
        """
        Deploy hosted OCP cluster on provisioned Provider platform
        :param deploy_cnv: (bool) Deploy CNV
        :param deploy_acm_hub: (bool) Deploy ACM Hub
        :param deploy_metallb: (bool) Deploy MetalLB
        :param download_hcp_binary: (bool) Download HCP binary
        """
        if (
            not config.default_cluster_ctx.ENV_DATA["platform"].lower()
            in HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            raise ProviderModeNotFoundException()

        if deploy_cnv:
            self.deploy_cnv(check_cnv_ready=True)
        if deploy_acm_hub:
            self.deploy_acm_hub()
        if deploy_metallb:
            self.deploy_lb()
        if download_hcp_binary:
            self.download_hcp_binary()
        self.create_kubevirt_OCP_cluster()
