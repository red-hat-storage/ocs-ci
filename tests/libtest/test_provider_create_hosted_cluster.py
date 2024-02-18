import logging

from ocs_ci.deployment.hosted_cluster import HypershiftHostedOCP
from ocs_ci.framework.pytest_customization.marks import hci_provider_required

logger = logging.getLogger(__name__)


class TestProviderHosted(object):
    """
    Test provider hosted
    """

    @hci_provider_required
    def test_provider_hosted(self):
        """
        Test provider hosted
        """

        logger.info("Test provider hosted")
        HypershiftHostedOCP().deploy_ocp()
