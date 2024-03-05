import logging

from ocs_ci.deployment.helpers.hypershift_base import HyperShiftBase
from ocs_ci.deployment.hosted_cluster import HypershiftHostedOCP
from ocs_ci.framework.pytest_customization.marks import (
    hci_provider_required,
    libtest,
    purple_squad,
)

logger = logging.getLogger(__name__)


@libtest
@purple_squad
class TestProviderHosted(object):
    """
    Test provider hosted
    """

    @hci_provider_required
    def test_provider_deploy_OCP_hosted(self):
        """
        Test deploy hosted OCP
        """

        logger.info("Test deploy hosted OCP on provider platform")
        HypershiftHostedOCP().deploy_ocp()

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_skip_cnv_and_lb(self):
        """
        Test deploy hosted OCP on provider platform with cnv and metallb ready beforehand
        """
        logger.info(
            "Test deploy hosted OCP on provider platform with metallb and cnv ready"
        )
        HypershiftHostedOCP().deploy_ocp(
            deploy_cnv=False, deploy_metallb=False, download_hcp_binary=True
        )

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_skip_cnv(self):
        """
        Test deploy hosted OCP on provider platform with cnv ready beforehand
        """
        logger.info("Test deploy hosted OCP on provider platform with cnv ready")
        HypershiftHostedOCP().deploy_ocp(deploy_cnv=False)

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_multiple(self):
        """
        Test deploy hosted OCP on provider platform multiple times
        """
        logger.info("Test deploy hosted OCP on provider platform multiple times")
        HypershiftHostedOCP().deploy_multiple_ocp_clusters()

    @hci_provider_required
    def test_create_kubeconfig_for_hosted_clusters(self):
        """
        Test create kubeconfig for hosted cluster
        """
        logger.info("Test create kubeconfig for hosted clusters")
        hps_base = HyperShiftBase()
        hosted_cluster_names = hps_base.get_hosted_cluster_names()
        for hosted_cluster_name in hosted_cluster_names:
            hps_base.download_hosted_cluster_kubeconfig(
                hosted_cluster_name,
            )
