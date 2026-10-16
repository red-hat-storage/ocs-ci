import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    tier2,
    red_squad,
    skipif_disconnected_cluster,
    skipif_external_mode,
    skipif_managed_service,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.warp import WarpWorkloadRunner
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_disconnected_cluster
@skipif_external_mode
@skipif_managed_service
@tier2
class TestNooBaaEndpointKeda(MCGTest):
    """
    Test NooBaa endpoint autoscaling with KEDA.

    Setting spec.multiCloudGateway.autoscaler.autoscalerType=keda on
    the StorageCluster makes the ocs-operator propagate it to the
    NooBaa CR, which in turn makes the noobaa-operator create a
    ScaledObject with a CPU utilization trigger, replacing the
    default HPA.
    """

    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2

    @pytest.fixture(scope="class")
    def setup_keda_autoscaling(self, request, keda_class):
        """
        Configure KEDA-based autoscaling for NooBaa endpoints.

        1. Set endpoint min/max counts and autoscalerType=keda via StorageCluster
        2. Wait for the noobaa-operator to create a ScaledObject and delete the HPA
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        sc_ocp = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
        sc_resource = sc_ocp.get()["items"][0]
        sc_name = sc_resource["metadata"]["name"]

        # Save original values for restoration
        mcg_spec = sc_resource.get("spec", {}).get("multiCloudGateway", {})
        endpoints_spec = mcg_spec.get("endpoints", {})
        original_min = endpoints_spec.get("minCount", 1)
        original_max = endpoints_spec.get("maxCount", 2)
        original_autoscaler_type = mcg_spec.get("autoscaler", {}).get(
            "autoscalerType", "hpav2"
        )

        def finalizer():
            logger.test_step(
                f"Restore StorageCluster config: min={original_min}, "
                f"max={original_max}, autoscaler={original_autoscaler_type}"
            )
            restore_patch = json.dumps(
                {
                    "spec": {
                        "multiCloudGateway": {
                            "endpoints": {
                                "minCount": original_min,
                                "maxCount": original_max,
                            },
                            "autoscaler": {"autoscalerType": original_autoscaler_type},
                        }
                    }
                }
            )
            try:
                sc_ocp.patch(
                    resource_name=sc_name,
                    params=restore_patch,
                    format_type="merge",
                )
            except CommandFailed:
                logger.warning("Failed to restore StorageCluster config")

            # Wait for the ScaledObject to be deleted before keda_class
            # teardown uninstalls KEDA. Otherwise the KEDA finalizer on
            # the ScaledObject gets stuck with no operator to process it.
            so_ocp = OCP(kind=constants.SCALED_OBJECT, namespace=namespace)
            try:
                so_ocp.wait_for_delete(resource_name="noobaa", timeout=120)
                logger.info("ScaledObject deleted during teardown")
            except (CommandFailed, TimeoutError):
                logger.warning(
                    "ScaledObject not deleted in time, "
                    "may leave a stuck finalizer after KEDA uninstall"
                )

        request.addfinalizer(finalizer)

        # 1. Patch StorageCluster with endpoint counts and KEDA autoscaler
        logger.test_step(
            f"Configure StorageCluster: endpoints min={self.MIN_ENDPOINT_COUNT}, "
            f"max={self.MAX_ENDPOINT_COUNT}, autoscalerType=keda"
        )
        setup_patch = json.dumps(
            {
                "spec": {
                    "multiCloudGateway": {
                        "endpoints": {
                            "minCount": self.MIN_ENDPOINT_COUNT,
                            "maxCount": self.MAX_ENDPOINT_COUNT,
                        },
                        "autoscaler": {"autoscalerType": "keda"},
                    }
                }
            }
        )
        patched = sc_ocp.patch(
            resource_name=sc_name,
            params=setup_patch,
            format_type="merge",
        )
        logger.assertion(f"StorageCluster patch applied: {patched}")
        assert patched, "Failed to patch StorageCluster with KEDA autoscaler config"

        # 2. Wait for ScaledObject creation and HPA deletion
        logger.test_step("Wait for ScaledObject creation and HPA deletion")
        so_ocp = OCP(kind=constants.SCALED_OBJECT, namespace=namespace)
        try:
            for exists in TimeoutSampler(
                120, 10, so_ocp.is_exist, resource_name="noobaa"
            ):
                if exists:
                    logger.info("ScaledObject created by noobaa-operator")
                    break
        except TimeoutExpiredError:
            raise TimeoutExpiredError("ScaledObject was not created")

        so_spec = so_ocp.get(resource_name="noobaa")["spec"]
        assert so_spec.get("maxReplicaCount") == self.MAX_ENDPOINT_COUNT, (
            f"ScaledObject maxReplicaCount is {so_spec.get('maxReplicaCount')}, "
            f"expected {self.MAX_ENDPOINT_COUNT}"
        )
        assert so_spec.get("scaleTargetRef", {}).get("name") == "noobaa-endpoint", (
            f"ScaledObject scaleTargetRef is "
            f"{so_spec.get('scaleTargetRef', {}).get('name')}, "
            f"expected noobaa-endpoint"
        )

        hpa_ocp = OCP(kind="HorizontalPodAutoscaler", namespace=namespace)
        hpa_ocp.wait_for_delete(resource_name="noobaa-hpav2", timeout=60)
        logger.info("HPA deleted by noobaa-operator")

    @pytest.fixture(scope="class")
    def warp_workload_runner(self, request):
        """Create a WarpWorkloadRunner targeting the NooBaa S3 endpoint."""
        namespace = config.ENV_DATA["cluster_namespace"]
        host = f"s3.{namespace}.svc:443"
        return WarpWorkloadRunner(request, host)

    @pytest.mark.polarion_id("OCS-8062")
    def test_noobaa_endpoint_keda_autoscaling(
        self,
        setup_keda_autoscaling,
        bucket_factory,
        warp_workload_runner,
    ):
        """
        Test KEDA-based autoscaling of NooBaa endpoint pods.

        1. Create an OBC and start S3 load via Warp
        2. Wait for endpoint pods to scale up
        3. Stop the workload
        4. Wait for endpoint pods to scale back down to minCount
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        endpoint_pods_kwargs = dict(
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=namespace,
            statuses=[constants.STATUS_RUNNING],
        )

        # 1. Create an OBC and start S3 load
        logger.test_step("Create an OBC and start S3 load via Warp")
        bucket = bucket_factory(amount=1, interface="OC")[0]
        obc_obj = OBC(bucket.name)
        warp_workload_runner.start(
            access_key=obc_obj.access_key_id,
            secret_key=obc_obj.access_key,
            bucket_name=bucket.name,
            workload_type="mixed",
            concurrent=32,
            obj_size="1MiB",
            duration="30s",
        )

        # 2. Wait for scale-up
        logger.test_step(
            f"Wait for endpoint pods to scale up to {self.MAX_ENDPOINT_COUNT}"
        )
        try:
            for endpoint_pods in TimeoutSampler(
                timeout=300,
                sleep=30,
                func=get_pods_having_label,
                **endpoint_pods_kwargs,
            ):
                count = len(endpoint_pods)
                pod_names = ", ".join(p["metadata"]["name"] for p in endpoint_pods)
                logger.debug(f"Endpoint pods ({count}): {pod_names}")
                assert count <= self.MAX_ENDPOINT_COUNT, (
                    f"Endpoint count {count} exceeds configured max "
                    f"{self.MAX_ENDPOINT_COUNT}"
                )
                if count == self.MAX_ENDPOINT_COUNT:
                    logger.info("Endpoints scaled up to max")
                    break
        except TimeoutExpiredError:
            logger.error("Endpoints did not scale up")
            raise
        finally:
            # 3. Stop the workload
            logger.test_step("Stop the Warp workload")
            warp_workload_runner.stop()

        # 4. Wait for scale-down
        logger.test_step(
            f"Wait for endpoint pods to scale down to {self.MIN_ENDPOINT_COUNT}"
        )
        try:
            for endpoint_pods in TimeoutSampler(
                timeout=1200,
                sleep=30,
                func=get_pods_having_label,
                **endpoint_pods_kwargs,
            ):
                count = len(endpoint_pods)
                logger.debug(f"Endpoint pod count: {count}")
                if count == self.MIN_ENDPOINT_COUNT:
                    logger.info("Endpoints scaled down to min")
                    break
        except TimeoutExpiredError:
            logger.error(
                f"Endpoints did not scale down to {self.MIN_ENDPOINT_COUNT} "
                f"within 1200s"
            )
            raise
