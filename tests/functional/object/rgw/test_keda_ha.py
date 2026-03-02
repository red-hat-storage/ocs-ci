import json
import logging
import time

from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    red_squad,
    rgw,
    skipif_disconnected_cluster,
    tier1,
    tier2,
    tier3,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.pod import get_pods_having_label, wait_for_pods_by_label_count
from ocs_ci.ocs.warp import Warp, WarpWorkloadRunner

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

    # Class-level constants
    DEFAULT_MIN_REPLICAS = 1
    DEFAULT_MAX_REPLICAS = 5
    DEFAULT_THRESHOLD = "7.00"

    @pytest.fixture(scope="function")
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

    @pytest.fixture(scope="function")
    def warp_workload_runner(self, request):
        host = f"{constants.RGW_SERVICE_INTERNAL_MODE}.{config.ENV_DATA['cluster_namespace']}.svc:443"
        return WarpWorkloadRunner(request, host)

    @tier1
    @polarion_id("OCS-7408")
    def test_rgw_keda_ha(
        self, keda_class, rgw_bucket_factory, warp_workload_runner, enable_rgw_hpa
    ):
        """
        Test RGW's integration with Keda autoscaler

        1. Create a ScaledObject to autoscale the RGW deployment with a low threshold
        2. Start a warp workload on an RGW bucket to run in the background
        3. Wait for the RGW pods to upscale to the target max replicas
        4. Stop the warp workload
        5. Wait for the RGW pods to downscale to the min replica count
        """
        # 1. Create a ScaledObject to autoscale the RGW deployment
        keda_class.create_thanos_metric_scaled_object(
            {
                "scaleTargetRef": RGW_SCALE_TARGET_REF,
                "query": "sum(rate(ceph_rgw_req[1m]))",
                "threshold": self.DEFAULT_THRESHOLD,
                "minReplicaCount": self.DEFAULT_MIN_REPLICAS,
                "maxReplicaCount": self.DEFAULT_MAX_REPLICAS,
            }
        )

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
            assert wait_for_pods_by_label_count(
                label=constants.RGW_APP_LABEL,
                expected_count=self.DEFAULT_MAX_REPLICAS,
                timeout=300,
            ), f"RGW did not scale up to {self.DEFAULT_MAX_REPLICAS} replica(s) within 300s"
        finally:
            # 4. Stop the warp workload
            warp_workload_runner.stop()

        # 5. Wait for the RGW pods to downscale to the min replica count
        assert wait_for_pods_by_label_count(
            label=constants.RGW_APP_LABEL,
            expected_count=self.DEFAULT_MIN_REPLICAS,
            timeout=300,
        ), f"RGW did not scale down to {self.DEFAULT_MIN_REPLICAS} replica(s) within 300s"

    @tier2
    @polarion_id("OCS-7517")
    def test_threshold_flapping(self, keda_class, pushgateway, enable_rgw_hpa):
        """
        Test RGW autoscaling stability when metrics rapidly fluctuate (threshold flapping)

        1. Setup a ScaledObject to scale RGW based off a custom metric
        2. Set the metric repeatedly above and below the ScaledObject's threshold every 5 seconds over 2 minutes
        3. Set the metric above the threshold, and wait for RGW to scale up
        4. Set the metric below the threshold, and wait for RGW to scale back down
        """
        TARGET_MAX_REPLICAS = 3
        THRESHOLD = "5.0"
        METRIC_NAME = "custom_rgw_metric"
        EXPORTED_JOB_NAME = "rgw_flapping_test"
        FLAPPING_DURATION = 120
        FLAPPING_INTERVAL = 5
        VALUE_ABOVE_THRESHOLD = "20.0"
        VALUE_BELOW_THRESHOLD = "1.0"
        SCALE_TIMEOUT = 600

        # 1. Create a ScaledObject to autoscale the RGW deployment based on custom metric
        logger.info(f"Creating ScaledObject to monitor custom metric '{METRIC_NAME}'")
        keda_class.create_thanos_metric_scaled_object(
            {
                "scaleTargetRef": RGW_SCALE_TARGET_REF,
                "query": f"{METRIC_NAME}{{exported_job='{EXPORTED_JOB_NAME}'}}",
                "threshold": THRESHOLD,
                "minReplicaCount": self.DEFAULT_MIN_REPLICAS,
                "maxReplicaCount": TARGET_MAX_REPLICAS,
            }
        )

        # 2. Simulate metric flapping: alternate above and below threshold
        iterations = FLAPPING_DURATION // FLAPPING_INTERVAL
        logger.info(
            f"Starting metric flapping test: {iterations} iterations over "
            f"{FLAPPING_DURATION} seconds (every {FLAPPING_INTERVAL} seconds)"
        )

        for i in range(iterations):
            metric_value = (
                VALUE_ABOVE_THRESHOLD if i % 2 == 0 else VALUE_BELOW_THRESHOLD
            )
            threshold_relation = "ABOVE" if i % 2 == 0 else "BELOW"
            logger.info(
                f"Iteration {i + 1}/{iterations}: Setting metric {threshold_relation} "
                f"threshold ({metric_value} vs {THRESHOLD})"
            )
            pushgateway.send_custom_metric(METRIC_NAME, metric_value, EXPORTED_JOB_NAME)
            time.sleep(FLAPPING_INTERVAL)

        logger.info("Metric flapping phase completed")

        # 3. Set the metric above the threshold and wait for RGW to scale up
        logger.info(f"Setting metric above threshold ({VALUE_ABOVE_THRESHOLD})")
        pushgateway.send_custom_metric(
            METRIC_NAME, VALUE_ABOVE_THRESHOLD, EXPORTED_JOB_NAME
        )
        assert wait_for_pods_by_label_count(
            label=constants.RGW_APP_LABEL,
            expected_count=TARGET_MAX_REPLICAS,
            timeout=SCALE_TIMEOUT,
        ), f"RGW did not scale up to {TARGET_MAX_REPLICAS} replica(s) after threshold flapping within {SCALE_TIMEOUT}s"

        # 4. Set the metric below the threshold and wait for RGW to scale down
        logger.info(f"Setting metric below threshold ({VALUE_BELOW_THRESHOLD})")
        pushgateway.send_custom_metric(
            METRIC_NAME, VALUE_BELOW_THRESHOLD, EXPORTED_JOB_NAME
        )
        assert wait_for_pods_by_label_count(
            label=constants.RGW_APP_LABEL,
            expected_count=self.DEFAULT_MIN_REPLICAS,
            timeout=SCALE_TIMEOUT,
        ), (
            f"RGW did not scale down to {self.DEFAULT_MIN_REPLICAS} replica(s) "
            f"after threshold flapping within {SCALE_TIMEOUT}s"
        )

    @tier2
    @polarion_id("OCS-7518")
    def test_min_max_boundaries(self, keda_class, pushgateway, enable_rgw_hpa):
        """
        Test that KEDA respects minReplicaCount and maxReplicaCount boundaries

        1. Setup a ScaledObject to scale RGW based off a custom metric
        2. Assert RGW replica count matches the default minimum of 1
        3. Set minReplicaCount=2 and wait for RGW to scale up to 2 replicas
        4. Set maxReplicaCount=2 and push an above-threshold custom metric
        5. Assert RGW stays at replica count 2 for over 2 minutes
        """
        MIN_REPLICAS = 2
        MAX_REPLICAS = 2
        THRESHOLD = "5.0"
        METRIC_NAME = "custom_rgw_boundary_metric"
        EXPORTED_JOB_NAME = "rgw_boundary_test"
        VALUE_ABOVE_THRESHOLD = "20.0"
        VALUE_BELOW_THRESHOLD = "1.0"
        MONITORING_DURATION = 120

        # 1. Create a ScaledObject with initial min=1, max=5
        scaled_object = keda_class.create_thanos_metric_scaled_object(
            {
                "scaleTargetRef": RGW_SCALE_TARGET_REF,
                "query": f"{METRIC_NAME}{{exported_job='{EXPORTED_JOB_NAME}'}}",
                "threshold": THRESHOLD,
                "minReplicaCount": self.DEFAULT_MIN_REPLICAS,
                "maxReplicaCount": self.DEFAULT_MAX_REPLICAS,
            }
        )

        # Push a low metric value to keep it at minimum
        pushgateway.send_custom_metric(
            METRIC_NAME, VALUE_BELOW_THRESHOLD, EXPORTED_JOB_NAME
        )
        logger.info(
            f"Pushed low metric value ({VALUE_BELOW_THRESHOLD}) to trigger scale-up attempt. "
            f"Waiting a bit to see if RGW scales up when it shouldn't"
        )
        time.sleep(60)

        # 2. Assert RGW replica count matches the initial minimum of 1
        assert wait_for_pods_by_label_count(
            label=constants.RGW_APP_LABEL,
            expected_count=self.DEFAULT_MIN_REPLICAS,
            timeout=300,
        ), f"RGW did not reach initial minimum of {self.DEFAULT_MIN_REPLICAS} replica(s) within 300s"

        # 3. Update minReplicaCount to 2 and wait for RGW to scale up
        logger.info(f"Updating ScaledObject minReplicaCount to {MIN_REPLICAS}")
        scaled_object.update_from_dict({"minReplicaCount": MIN_REPLICAS})
        assert wait_for_pods_by_label_count(
            label=constants.RGW_APP_LABEL,
            expected_count=MIN_REPLICAS,
            timeout=300,
        ), f"RGW did not scale up to new minimum of {MIN_REPLICAS} replica(s) within 300s"

        # 4. Update maxReplicaCount to 2 and push high metric value
        logger.info(
            f"Updating ScaledObject maxReplicaCount to {MAX_REPLICAS} "
            "(same as minReplicaCount)"
        )
        scaled_object.update_from_dict({"maxReplicaCount": MAX_REPLICAS})

        logger.info(
            f"Pushing high metric value ({VALUE_ABOVE_THRESHOLD}) to trigger scale-up attempt"
        )
        pushgateway.send_custom_metric(
            METRIC_NAME, VALUE_ABOVE_THRESHOLD, EXPORTED_JOB_NAME
        )

        # 5. Monitor RGW for 2+ minutes to ensure it stays at 2 replicas
        logger.info(
            f"Monitoring RGW for {MONITORING_DURATION} seconds to verify it respects "
            f"maxReplicaCount boundary of {MAX_REPLICAS}"
        )

        try:
            for current_rgw_pods in TimeoutSampler(
                timeout=MONITORING_DURATION,
                sleep=5,
                func=get_pods_having_label,
                label=constants.RGW_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            ):
                if len(current_rgw_pods) > MAX_REPLICAS:
                    logger.error(
                        f"RGW exceeded maxReplicaCount boundary! "
                        f"Current: {len(current_rgw_pods)}, Max: {MAX_REPLICAS}\n"
                        f"RGW pods:\n{current_rgw_pods}"
                    )
                    raise AssertionError(
                        f"RGW scaled beyond maxReplicaCount boundary: "
                        f"{len(current_rgw_pods)} > {MAX_REPLICAS}"
                    )
        except TimeoutExpiredError:
            logger.info(
                f"Successfully verified: RGW respected maxReplicaCount boundary of "
                f"{MAX_REPLICAS} for {MONITORING_DURATION} seconds despite high metric value"
            )

    @pytest.fixture(scope="function")
    def warp_multi_client(self, request):
        """Fixture to create Warp instance with 3 clients for multi-client benchmarking"""
        warp = Warp()
        request.addfinalizer(lambda: warp.cleanup(multi_client=True))
        warp.host = f"{constants.RGW_SERVICE_INTERNAL_MODE}.{config.ENV_DATA['cluster_namespace']}.svc:443"
        warp.create_resource_warp(multi_client=True, replicas=3)
        return warp

    @pytest.fixture(scope="function")
    def cleanup_manual_upscale(self, request):
        """
        Cleanup the manual upscale of RGW if it was performed
        """

        def finalizer():
            storagecluster_obj = OCP(
                kind=constants.STORAGECLUSTER,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
            )
            patch_params = {"spec": {"managedResources": {"cephObjectStores": None}}}
            storagecluster_obj.patch(
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                params=json.dumps(patch_params),
                format_type="merge",
            )
            assert wait_for_pods_by_label_count(
                label=constants.RGW_APP_LABEL,
                expected_count=1,
                timeout=300,
            ), f"RGW did not scale down to {self.DEFAULT_MIN_REPLICAS} replica(s)"

        request.addfinalizer(finalizer)

    @tier3
    @polarion_id("OCS-7520")
    def test_manual_upscale_performance(
        self, rgw_bucket_factory, warp_multi_client, cleanup_manual_upscale
    ):
        """
        Test the performance of RGW after manually upscaling its pods

        1. Run Warp benchmark on RGW and record the baseline throughput
        2. Manually upscale RGW's deployment by patching the ocs-storagecluster
        3. Re-run the Warp benchmark and verify throughput improvement after upscale
        """

        # Warp benchmark constants
        WARP_DURATION = "1m"
        WARP_OBJ_SIZE = "512KiB"
        WARP_CONCURRENCY = 32
        WARP_TIMEOUT = 600

        # Manual upscale constants
        MANUAL_UPSCALE_REPLICAS = 3
        MANUAL_UPSCALE_TIMEOUT = 300
        UPSCALE_WAIT_TIME = 60

        initial_throughput = None
        final_throughput = None

        # 1. Run Warp benchmark on RGW and record the baseline throughput
        bucket = rgw_bucket_factory(1, "RGW-OC")[0]
        bucketname = bucket.name
        obc_obj = OBC(bucketname)

        warp_multi_client.run_benchmark(
            access_key=obc_obj.access_key_id,
            secret_key=obc_obj.access_key,
            bucket_name=bucketname,
            workload_type="mixed",
            duration=WARP_DURATION,
            concurrent=WARP_CONCURRENCY,
            obj_size=WARP_OBJ_SIZE,
            timeout=WARP_TIMEOUT,
            tls=True,
            insecure=True,
            multi_client=True,
            clear_objects=True,
        )
        initial_throughput = warp_multi_client.get_last_avg_throughput()

        # 2. Manually upscale RGW's deployment by patching the ocs-storagecluster
        storagecluster_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        patch_params = {
            "spec": {
                "managedResources": {
                    "cephObjectStores": {"gatewayInstances": MANUAL_UPSCALE_REPLICAS}
                }
            }
        }
        storagecluster_obj.patch(
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
            params=json.dumps(patch_params),
            format_type="merge",
        )
        assert wait_for_pods_by_label_count(
            label=constants.RGW_APP_LABEL,
            expected_count=MANUAL_UPSCALE_REPLICAS,
            timeout=MANUAL_UPSCALE_TIMEOUT,
        ), f"RGW did not scale up to {MANUAL_UPSCALE_REPLICAS} replica(s) within {MANUAL_UPSCALE_TIMEOUT} seconds"
        logger.info(
            "Waiting a bit more to ensure RGW's pods are properly initialized after the upscale"
        )
        time.sleep(UPSCALE_WAIT_TIME)

        # 3. Re-run the Warp benchmark and verify throughput improvement after upscale
        warp_multi_client.run_benchmark(
            access_key=obc_obj.access_key_id,
            secret_key=obc_obj.access_key,
            bucket_name=bucketname,
            workload_type="mixed",
            duration=WARP_DURATION,
            concurrent=WARP_CONCURRENCY,
            obj_size=WARP_OBJ_SIZE,
            timeout=WARP_TIMEOUT,
            tls=True,
            insecure=True,
            multi_client=True,
            clear_objects=True,
        )
        final_throughput = warp_multi_client.get_last_avg_throughput()

        logger.info(f"Initial throughput: {initial_throughput} MiB/s")
        logger.info(f"Final throughput: {final_throughput} MiB/s")
        throughput_increase = (
            (final_throughput - initial_throughput) / initial_throughput
        ) * 100
        logger.info(f"Throughput increase: {throughput_increase:.2f}%")
        assert final_throughput > initial_throughput, (
            f"Final throughput {final_throughput} MiB/s is not greater "
            f"than initial throughput {initial_throughput} MiB/s"
        )
