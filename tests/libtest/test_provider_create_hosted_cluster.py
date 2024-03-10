import logging
import random

from ocs_ci.deployment.helpers.hypershift_base import (
    HyperShiftBase,
    get_hosted_cluster_names,
)
from ocs_ci.deployment.hosted_cluster import HypershiftHostedOCP, HostedODF
from ocs_ci.framework.pytest_customization.marks import (
    hci_provider_required,
    libtest,
    purple_squad,
    runs_on_provider,
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
        hosted_cluster_names = get_hosted_cluster_names()
        for hosted_cluster_name in hosted_cluster_names:
            assert hps_base.download_hosted_cluster_kubeconfig(
                hosted_cluster_name,
            ), "Failed to download kubeconfig for hosted cluster"

    @runs_on_provider
    @hci_provider_required
    def test_install_odf_on_hosted_cluster(self):
        """
        Test install ODF on hosted cluster
        """
        logger.info("Test install ODF on hosted cluster")

        HyperShiftBase().download_hosted_cluster_kubeconfig_multiple()

        hosted_cluster_names = get_hosted_cluster_names()
        cluster_name = random.choice(hosted_cluster_names)

        hosted_odf = HostedODF(cluster_name)
        hosted_odf.do_deploy()
