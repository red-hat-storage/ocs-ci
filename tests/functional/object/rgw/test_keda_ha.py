import logging

from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    tier1,
)

logger = logging.getLogger(__name__)


@skipif_disconnected_cluster
class TestKedaHA:
    """
    Test RGW's integration with Keda autoscaler for high availability
    """

    @tier1
    def test_rgw_keda_ha(self, keda_class):
        """
        Test RGW's integration with Keda autoscaler for high availability
        """
        keda = keda_class  # just an alias
        logger.info(f"KEDA: {keda}")
        assert keda.is_installed(), "KEDA is not installed"
