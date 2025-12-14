import logging

import pandas as pd
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    red_squad,
    rgw,
    skipif_disconnected_cluster,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.warp import WarpWorkloadRunner
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

RGW_SCALE_TARGET_REF = {
    "apiVersion": "ceph.rook.io/v1",
    "kind": constants.CEPHOBJECTSTORE,
    "name": constants.CEPHOBJECTSTORE_NAME,
    "namespace": config.ENV_DATA["cluster_namespace"],
}


@rgw
@red_squad
@skipif_disconnected_cluster
class TestKedaHA:
    """
    Test RGW's integration with the KEDA autoscaler
    """

    @pytest.fixture(autouse=True, scope="class")
    def enable_rgw_hpa(self, request):
        """
        Annotate the OCS StorageCluster to enable RGW HPA and remove the annotation after the test
        """
        storagecluster_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        )

        def finalizer():
            # The trailing dash tells OCP to remove the annotation
            storagecluster_obj.annotate(
                annotation=f"{constants.ENABLE_RGW_HPA_ANNOTATION_KEY}-",
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
            )

        request.addfinalizer(finalizer)

        storagecluster_obj.annotate(
            annotation=f'{constants.ENABLE_RGW_HPA_ANNOTATION_KEY}="true"',
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        )

    @pytest.fixture(scope="class")
    def warp_workload_runner(self, request):
        host = f"{constants.RGW_SERVICE_INTERNAL_MODE}.{config.ENV_DATA['cluster_namespace']}.svc:443"
        return WarpWorkloadRunner(request, host)

    @tier1
    @polarion_id("OCS-7408")
    def test_rgw_keda_ha(self, keda_class, rgw_bucket_factory, warp_workload_runner):
        """
        Test RGW's integration with Keda autoscaler

        1. Create a ScaledObject to autoscale the RGW deployment with a low
        threshold.
        2. Start a warp workload on an RGW bucket to run in the background
        3. Wait for the RGW pods to upscale to the target max replicas
        4. Stop the warp workload
        5. Wait for the RGW pods to downscale to the min replica count
        """
        TARGET_MAX_REPLICAS = 5
        TARGET_MIN_REPLICAS = 1
        THRESHOLD = "7.00"

        # 1. Create a ScaledObject to autoscale the RGW deployment
        scaled_object = keda_class.create_thanos_metric_scaled_object(
            {
                "scaleTargetRef": RGW_SCALE_TARGET_REF,
                "query": "sum(rate(ceph_rgw_req[1m]))",
                "threshold": THRESHOLD,
                "minReplicaCount": TARGET_MIN_REPLICAS,
                "maxReplicaCount": TARGET_MAX_REPLICAS,
            }
        )
        logger.info(f"ScaledObject: {scaled_object}")

        # 2. Start a warp workload on an RGW bucket to run in the background
        bucket = rgw_bucket_factory(1, "RGW-OC")[0]
        bucketname = bucket.name
        obc_obj = OBC(bucketname)

        warp_workload_runner.start(
            access_key=obc_obj.access_key_id,
            secret_key=obc_obj.access_key,
            bucket_name=bucketname,
            workload_type="mixed",
            duration="30s",
            concurrent=10,
            obj_size="1MiB",
        )

        # 3. Wait for the RGW pods to upscale
        try:
            for rgw_pods_sampled in TimeoutSampler(
                timeout=300,
                sleep=30,
                func=get_pods_having_label,
                label=constants.RGW_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            ):
                pod_names = "\n".join(
                    [pod["metadata"]["name"] for pod in rgw_pods_sampled]
                )
                logger.info(f"RGW pods running:\n{pod_names}")
                if len(rgw_pods_sampled) == TARGET_MAX_REPLICAS:
                    logger.info("RGW pods upscaled to target max replicas as expected.")
                    break
        except TimeoutExpiredError:
            logger.error("RGW did not upscale as expected.")
            raise
        finally:
            # 4. Stop the warp workload
            warp_workload_runner.stop()

            # Optional: Log findings from the last warp run
            try:
                last_report = warp_workload_runner.warp.get_last_report()
                if last_report is not None:
                    num_of_errors = (
                        pd.to_numeric(last_report["errors"], errors="coerce")
                        .fillna(0)
                        .astype(int)
                        .sum()
                    )
                    average_throughput = (
                        pd.to_numeric(last_report["mb_per_sec"], errors="coerce")
                        .fillna(0)
                        .astype(float)
                        .mean()
                    )
                    logger.info(f"Number of errors: {num_of_errors}")
                    logger.info(f"Average throughput: {average_throughput:.2f} MB/s")
            except Exception as e:
                logger.warning(f"Failed to get last report: {e}")

        # 5. Wait for the RGW pods to downscale to the min replica count
        try:
            for rgw_pods_sampled in TimeoutSampler(
                timeout=300,
                sleep=30,
                func=get_pods_having_label,
                label=constants.RGW_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            ):
                pod_names = "\n".join(
                    [pod["metadata"]["name"] for pod in rgw_pods_sampled]
                )
                logger.info(f"RGW pods running:\n{pod_names}")
                if len(rgw_pods_sampled) == TARGET_MIN_REPLICAS:
                    logger.info(
                        "RGW pods downscaled to target min replicas as expected."
                    )
                    break
        except TimeoutExpiredError:
            logger.error("RGW did not downscale as expected.")
            raise
