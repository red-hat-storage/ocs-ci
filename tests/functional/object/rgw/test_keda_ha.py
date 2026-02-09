import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    polarion_id,
    post_upgrade,
    pre_upgrade,
    red_squad,
    rgw,
    skipif_disconnected_cluster,
    tier1,
    tier2,
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

    # Class-level constants
    DEFAULT_MIN_REPLICAS = 1
    DEFAULT_MAX_REPLICAS = 5
    DEFAULT_THRESHOLD = "7.00"

    def _wait_for_rgw_replica_count(
        self, target_count, timeout=300, operation="scale", raise_on_timeout=True
    ):
        """
        Wait for RGW pods to reach target replica count

        Args:
            target_count (int): Expected number of replicas
            timeout (int): Timeout in seconds (default: 300)
            operation (str): Description for logging (e.g., "scale up", "scale down")
            raise_on_timeout (bool): Whether to raise exception on timeout (default: True)

        Returns:
            bool: True if target count reached, False if timeout and raise_on_timeout=False

        Raises:
            TimeoutExpiredError: If target count not reached within timeout and raise_on_timeout=True
        """
        logger.info(
            f"Waiting for RGW to {operation} to {target_count} replica(s) "
            f"(timeout: {timeout}s)"
        )
        try:
            for rgw_pods_sampled in TimeoutSampler(
                timeout=timeout,
                sleep=30,
                func=get_pods_having_label,
                label=constants.RGW_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            ):
                current_count = len(rgw_pods_sampled)
                pod_names = "\n".join(
                    [pod["metadata"]["name"] for pod in rgw_pods_sampled]
                )
                logger.info(
                    f"Current RGW pod count: {current_count}/{target_count}\n"
                    f"RGW pods running:\n{pod_names}"
                )
                if current_count == target_count:
                    logger.info(
                        f"RGW successfully {operation}d to {target_count} replica(s)"
                    )
                    return True
        except TimeoutExpiredError:
            logger.error(
                f"RGW did not {operation} to {target_count} replica(s) within {timeout}s"
            )
            if raise_on_timeout:
                raise
            return False

    def _create_rgw_scaled_object(
        self,
        keda_class,
        query,
        threshold=None,
        min_replicas=None,
        max_replicas=None,
    ):
        """
        Create a ScaledObject for RGW with standard configuration

        Args:
            keda_class: KEDA fixture instance
            query (str): Prometheus query for the metric
            threshold (str): Threshold value (default: DEFAULT_THRESHOLD)
            min_replicas (int): Minimum replica count (default: DEFAULT_MIN_REPLICAS)
            max_replicas (int): Maximum replica count (default: DEFAULT_MAX_REPLICAS)

        Returns:
            ScaledObject: Created ScaledObject instance
        """
        threshold = threshold or self.DEFAULT_THRESHOLD
        min_replicas = min_replicas or self.DEFAULT_MIN_REPLICAS
        max_replicas = max_replicas or self.DEFAULT_MAX_REPLICAS

        logger.info(
            f"Creating ScaledObject with min={min_replicas}, max={max_replicas}, "
            f"threshold={threshold}"
        )
        scaled_object = keda_class.create_thanos_metric_scaled_object(
            {
                "scaleTargetRef": RGW_SCALE_TARGET_REF,
                "query": query,
                "threshold": threshold,
                "minReplicaCount": min_replicas,
                "maxReplicaCount": max_replicas,
            }
        )
        logger.info(f"ScaledObject created: {scaled_object.name}")
        return scaled_object

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

        1. Create a ScaledObject to autoscale the RGW deployment with a low threshold
        2. Start a warp workload on an RGW bucket to run in the background
        3. Wait for the RGW pods to upscale to the target max replicas
        4. Stop the warp workload
        5. Wait for the RGW pods to downscale to the min replica count
        """
        # 1. Create a ScaledObject to autoscale the RGW deployment
        self._create_rgw_scaled_object(
            keda_class=keda_class,
            query="sum(rate(ceph_rgw_req[1m]))",
            threshold=self.DEFAULT_THRESHOLD,
            min_replicas=self.DEFAULT_MIN_REPLICAS,
            max_replicas=self.DEFAULT_MAX_REPLICAS,
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
            self._wait_for_rgw_replica_count(
                target_count=self.DEFAULT_MAX_REPLICAS, operation="scale up"
            )
        finally:
            # 4. Stop the warp workload
            warp_workload_runner.stop()

        # 5. Wait for the RGW pods to downscale to the min replica count
        self._wait_for_rgw_replica_count(
            target_count=self.DEFAULT_MIN_REPLICAS, operation="scale down"
        )

    @tier2
    def test_threshold_flapping(self, keda_class, pushgateway):
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
        JOB_NAME = "rgw_flapping_test"
        FLAPPING_DURATION = 120  # 2 minutes
        FLAPPING_INTERVAL = 5  # 5 seconds
        VALUE_ABOVE_THRESHOLD = "10.0"
        VALUE_BELOW_THRESHOLD = "1.0"

        # 1. Create a ScaledObject to autoscale the RGW deployment based on custom metric
        logger.info(f"Creating ScaledObject to monitor custom metric '{METRIC_NAME}'")
        self._create_rgw_scaled_object(
            keda_class=keda_class,
            query=f"{METRIC_NAME}{{job='{JOB_NAME}'}}",
            threshold=THRESHOLD,
            min_replicas=self.DEFAULT_MIN_REPLICAS,
            max_replicas=TARGET_MAX_REPLICAS,
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
            pushgateway.send_custom_metric(METRIC_NAME, metric_value, JOB_NAME)
            time.sleep(FLAPPING_INTERVAL)

        logger.info("Metric flapping phase completed")

        # 3. Set the metric above the threshold and wait for RGW to scale up
        logger.info(f"Setting metric above threshold ({VALUE_ABOVE_THRESHOLD})")
        pushgateway.send_custom_metric(METRIC_NAME, VALUE_ABOVE_THRESHOLD, JOB_NAME)
        self._wait_for_rgw_replica_count(
            target_count=TARGET_MAX_REPLICAS,
            operation="scale up after threshold flapping",
        )

        # 4. Set the metric below the threshold and wait for RGW to scale down
        logger.info(f"Setting metric below threshold ({VALUE_BELOW_THRESHOLD})")
        pushgateway.send_custom_metric(METRIC_NAME, VALUE_BELOW_THRESHOLD, JOB_NAME)
        self._wait_for_rgw_replica_count(
            target_count=self.DEFAULT_MIN_REPLICAS,
            operation="scale down after threshold flapping",
        )

    @tier2
    def test_min_max_boundaries(self, keda_class, pushgateway):
        """
        Test that KEDA respects minReplicaCount and maxReplicaCount boundaries

        1. Setup a ScaledObject to scale RGW based off a custom metric
        2. Assert RGW replica count matches the default minimum of 1
        3. Set minReplicaCount=2 and wait for RGW to scale up to 2 replicas
        4. Set maxReplicaCount=2 and push an above-threshold custom metric
        5. Assert RGW stays at replica count 2 for over 2 minutes
        """
        NEW_MIN_REPLICAS = 2
        NEW_MAX_REPLICAS = 2
        THRESHOLD = "5.0"
        METRIC_NAME = "custom_rgw_boundary_metric"
        JOB_NAME = "rgw_boundary_test"
        VALUE_ABOVE_THRESHOLD = "20.0"  # High value to trigger scaling
        VALUE_BELOW_THRESHOLD = "1.0"
        MONITORING_DURATION = 120  # 2 minutes
        MONITORING_INTERVAL = 15  # Check every 15 seconds

        # 1. Create a ScaledObject with initial min=1, max=5
        scaled_object = self._create_rgw_scaled_object(
            keda_class=keda_class,
            query=f"{METRIC_NAME}{{job='{JOB_NAME}'}}",
            threshold=THRESHOLD,
            min_replicas=self.DEFAULT_MIN_REPLICAS,
            max_replicas=self.DEFAULT_MAX_REPLICAS,
        )

        # Push a low metric value to keep it at minimum
        pushgateway.send_custom_metric(METRIC_NAME, VALUE_BELOW_THRESHOLD, JOB_NAME)

        # 2. Assert RGW replica count matches the initial minimum of 1
        self._wait_for_rgw_replica_count(
            target_count=self.DEFAULT_MIN_REPLICAS,
            operation="reach initial minimum",
        )

        # 3. Update minReplicaCount to 2 and wait for RGW to scale up
        logger.info(f"Updating ScaledObject minReplicaCount to {NEW_MIN_REPLICAS}")
        scaled_object.update_from_dict({"minReplicaCount": NEW_MIN_REPLICAS})
        self._wait_for_rgw_replica_count(
            target_count=NEW_MIN_REPLICAS,
            operation="scale up to new minimum",
        )

        # 4. Update maxReplicaCount to 2 and push high metric value
        logger.info(
            f"Updating ScaledObject maxReplicaCount to {NEW_MAX_REPLICAS} "
            "(same as minReplicaCount)"
        )
        scaled_object.update_from_dict({"maxReplicaCount": NEW_MAX_REPLICAS})

        logger.info(
            f"Pushing high metric value ({VALUE_ABOVE_THRESHOLD}) to trigger scale-up attempt"
        )
        pushgateway.send_custom_metric(METRIC_NAME, VALUE_ABOVE_THRESHOLD, JOB_NAME)

        # 5. Monitor RGW for 2+ minutes to ensure it stays at 2 replicas
        logger.info(
            f"Monitoring RGW for {MONITORING_DURATION} seconds to verify it respects "
            f"maxReplicaCount boundary of {NEW_MAX_REPLICAS}"
        )

        start_time = time.time()
        check_count = 0
        while time.time() - start_time < MONITORING_DURATION:
            rgw_pods = get_pods_having_label(
                label=constants.RGW_APP_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            current_count = len(rgw_pods)
            check_count += 1
            elapsed = int(time.time() - start_time)

            logger.info(
                f"Check {check_count} at {elapsed}s: RGW pod count is {current_count} "
                f"(expected: {NEW_MAX_REPLICAS})"
            )

            if current_count > NEW_MAX_REPLICAS:
                pod_names = "\n".join([pod["metadata"]["name"] for pod in rgw_pods])
                logger.error(
                    f"RGW exceeded maxReplicaCount boundary! "
                    f"Current: {current_count}, Max: {NEW_MAX_REPLICAS}\n"
                    f"RGW pods:\n{pod_names}"
                )
                raise AssertionError(
                    f"RGW scaled beyond maxReplicaCount boundary: "
                    f"{current_count} > {NEW_MAX_REPLICAS}"
                )

            if current_count < NEW_MIN_REPLICAS:
                logger.error(
                    f"RGW fell below minReplicaCount boundary! "
                    f"Current: {current_count}, Min: {NEW_MIN_REPLICAS}"
                )
                raise AssertionError(
                    f"RGW scaled below minReplicaCount boundary: "
                    f"{current_count} < {NEW_MIN_REPLICAS}"
                )

            time.sleep(MONITORING_INTERVAL)

        logger.info(
            f"Successfully verified: RGW respected maxReplicaCount boundary of "
            f"{NEW_MAX_REPLICAS} for {MONITORING_DURATION} seconds despite high metric value"
        )

    @pre_upgrade
    @ignore_leftovers
    def test_rgw_keda_pre_upgrade(self, request, keda_class):
        """
        Set up RGW HA with KEDA before upgrade

        1. Create a ScaledObject to autoscale the RGW deployment with a low threshold
        2. Store ScaledObject name for post-upgrade verification
        """
        logger.info("Setting up RGW KEDA autoscaling before upgrade")

        # 1. Create a ScaledObject to autoscale the RGW deployment
        scaled_object = self._create_rgw_scaled_object(
            keda_class=keda_class,
            query="sum(rate(ceph_rgw_req[1m]))",
            threshold=self.DEFAULT_THRESHOLD,
            min_replicas=self.DEFAULT_MIN_REPLICAS,
            max_replicas=self.DEFAULT_MAX_REPLICAS,
        )

        # 2. Store ScaledObject name in cache for post-upgrade test
        request.config.cache.set("rgw_keda_scaled_object_name", scaled_object.name)
        logger.info(
            f"Stored ScaledObject name '{scaled_object.name}' in cache for post-upgrade verification"
        )

        # Verify RGW is at minimum replica count
        self._wait_for_rgw_replica_count(
            target_count=self.DEFAULT_MIN_REPLICAS,
            operation="reach minimum before upgrade",
            raise_on_timeout=False,  # Don't fail pre-upgrade if timeout
        )

        logger.info("Pre-upgrade KEDA setup completed successfully")

    @post_upgrade
    def test_rgw_keda_post_upgrade(
        self, request, keda_class, rgw_bucket_factory, warp_workload_runner
    ):
        """
        Verify RGW HA with KEDA is still working after upgrade

        1. Retrieve ScaledObject from pre-upgrade test
        2. Verify ScaledObject still exists after upgrade
        3. Start a warp workload on an RGW bucket to run in the background
        4. Wait for the RGW pods to upscale to the target max replicas
        5. Stop the warp workload
        6. Wait for the RGW pods to downscale to the min replica count
        """
        logger.info("Verifying RGW KEDA autoscaling after upgrade")

        # 1. Retrieve ScaledObject name from pre-upgrade test
        scaled_object_name = request.config.cache.get(
            "rgw_keda_scaled_object_name", None
        )
        assert scaled_object_name, (
            "ScaledObject name not found in cache. "
            "Pre-upgrade test may not have run successfully."
        )
        logger.info(f"Retrieved ScaledObject name from cache: {scaled_object_name}")

        # 2. Verify ScaledObject still exists after upgrade
        logger.info("Verifying ScaledObject still exists after upgrade")
        ocp_obj = OCP(
            kind=constants.SCALED_OBJECT,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        try:
            ocp_obj.get(resource_name=scaled_object_name)
            logger.info(
                f"ScaledObject '{scaled_object_name}' verified - still exists after upgrade"
            )
        except Exception as e:
            logger.error(f"Failed to retrieve ScaledObject after upgrade: {e}")
            raise AssertionError(
                f"ScaledObject '{scaled_object_name}' not found after upgrade"
            )

        # 3. Start a warp workload on an RGW bucket to run in the background
        logger.info("Creating RGW bucket for warp workload")
        bucket = rgw_bucket_factory(1, "RGW-OC")[0]
        bucketname = bucket.name
        obc_obj = OBC(bucketname)

        host = f"{constants.RGW_SERVICE_INTERNAL_MODE}.{config.ENV_DATA['cluster_namespace']}.svc:443"
        logger.info(f"Starting warp workload on bucket '{bucketname}' at host '{host}'")
        warp_workload_runner.start(
            access_key=obc_obj.access_key_id,
            secret_key=obc_obj.access_key,
            bucket_name=bucketname,
            workload_type="mixed",
            duration="30s",
            concurrent=10,
            obj_size="1MiB",
        )

        # 4. Wait for the RGW pods to upscale to the target max replicas
        try:
            self._wait_for_rgw_replica_count(
                target_count=self.DEFAULT_MAX_REPLICAS,
                operation="scale up after upgrade",
            )
        finally:
            # 5. Stop the warp workload
            logger.info("Stopping warp workload")
            warp_workload_runner.stop()

        # 6. Wait for the RGW pods to downscale to the min replica count
        self._wait_for_rgw_replica_count(
            target_count=self.DEFAULT_MIN_REPLICAS,
            operation="scale down after upgrade",
        )

        logger.info(
            "Post-upgrade KEDA verification completed successfully - "
            "RGW autoscaling is working correctly after upgrade"
        )
