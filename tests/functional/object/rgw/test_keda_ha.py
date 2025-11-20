import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    tier1,
)
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@skipif_disconnected_cluster
class TestKedaHA:
    """
    Test RGW's integration with the KEDA autoscaler
    """

    @tier1
    def test_rgw_keda_ha(self, keda_class):
        """
        Test RGW's integration with Keda autoscaler
        """
        keda = keda_class
        logger.info(f"KEDA: {keda}")
        assert keda.is_installed(), "KEDA is not installed"

        deployment_name = create_unique_resource_name("hello-world", "deployment")
        OCP(namespace=config.ENV_DATA["cluster_namespace"]).exec_oc_cmd(
            f"create deployment {deployment_name} --image=busybox -- sleep 3600"
        )

        scaled_object = keda.create_thanos_metric_scaled_object(
            deployment=deployment_name,
            query="sum(sin(vector(time())))",
            threshold="0.5",
        )
        logger.info(f"ScaledObject: {scaled_object}")

        print(5)
