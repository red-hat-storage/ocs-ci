import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.warp import WarpWorkloadRunner

logger = logging.getLogger(__name__)

RGW_TARGET_REF_DICT = {
    "apiVersion": "ceph.rook.io/v1",
    "kind": constants.CEPHOBJECTSTORE,
    "name": constants.CEPHOBJECTSTORE_NAME,
    "namespace": config.ENV_DATA["cluster_namespace"],
}


@skipif_disconnected_cluster
class TestKedaHA:
    """
    Test RGW's integration with the KEDA autoscaler
    """

    @pytest.fixture(scope="class")
    def warp_workload_runner(self, request):
        host = f"{constants.RGW_SERVICE_INTERNAL_MODE}.{config.ENV_DATA['cluster_namespace']}.svc:443"
        return WarpWorkloadRunner(request, host)

    @tier1
    def test_rgw_keda_ha(
        self, request, keda_class, rgw_bucket_factory, warp_workload_runner
    ):
        """
        Test RGW's integration with Keda autoscaler
        """
        keda = keda_class
        logger.info(f"KEDA: {keda}")
        assert keda.is_installed(), "KEDA is not installed"

        scaled_object = keda.create_thanos_metric_scaled_object(
            target_ref_dict=RGW_TARGET_REF_DICT,
            query="sum(rate(ceph_rgw_req[2m]))",
            threshold="0.02",
        )
        logger.info(f"ScaledObject: {scaled_object}")

        bucket = rgw_bucket_factory(1, "RGW-OC")[0]
        bucketname = bucket.name
        obc_obj = OBC(bucketname)

        warp_workload_runner.start(
            access_key=obc_obj.access_key_id,
            secret_key=obc_obj.access_key,
            bucket_name=bucketname,
            request=request,
            workload_type="mixed",
            duration="1m",
            timeout=300,
        )

        sleep(100)
        warp_workload_runner.stop()
