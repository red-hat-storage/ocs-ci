import logging

from ocs_ci.deployment.hosted_cluster import HypershiftHostedOCP
from ocs_ci.framework.pytest_customization.marks import hci_provider_required, libtest

logger = logging.getLogger(__name__)


@libtest
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
