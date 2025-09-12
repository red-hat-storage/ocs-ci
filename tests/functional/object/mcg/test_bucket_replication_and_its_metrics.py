import logging
import time
import pytest


from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    polarion_id,
    red_squad,
    runs_on_provider,
    tier2,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework import config
from ocs_ci.ocs.bucket_utils import (
    update_replication_policy,
    wait_for_object_versions_match,
    copy_random_individual_objects,
    get_noobaa_bucket_replication_metrics_in_prometheus,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg_replication_policy import McgReplicationPolicy
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
class TestReplicationAndItsMetrics(MCGTest):
    """
    Test suite for MCG object replication policies and its metrics
    Here we will be testing newly added metrices to expose replication per bucket:
        bucket_last_cycle_total_objects_num,
        bucket_last_cycle_replicated_objects_num,
        bucket_last_cycle_error_objects_num
    """

    @pytest.fixture(scope="class", autouse=True)
    def reduce_replication_delay_setup(self, add_env_vars_to_noobaa_core_class):
        """
        A fixture to reduce the replication delay to one minute.
        Args:
            new_delay_in_milliseconds (function): A function to add env vars to the noobaa-core pod
        """
        new_delay_in_milliseconds = 60 * 1000
        new_env_var_tuples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_milliseconds),
        ]
        add_env_vars_to_noobaa_core_class(new_env_var_tuples)

    @tier2
    @polarion_id("OCS-6916")
    def test_bucket_replication_and_its_metrics(
        self, awscli_pod, mcg_obj, test_directory_setup, bucket_factory, threading_lock
    ):
        """
        1. Create source and target buckets.
        2. Set bucket replication policies on the source buckets
        3. Write some objects to the source buckets
        4. Verify the objects were replicated to the target buckets
        5. Get the metrics for the bucket replication
        6. Verify the bucket replication metrics from Prometheus
        """
        # 1. Create source and target buckets
        bucket_1, bucket_2 = bucket_factory(2, "OC")

        # 2. Set bucket replication policies on the source buckets
        replication_policy = McgReplicationPolicy(destination_bucket=bucket_2.name)
        update_replication_policy(bucket_1.name, replication_policy.to_dict())

        # 3. Write some objects to the source buckets
        test_dir = test_directory_setup.result_dir
        copy_random_individual_objects(
            podobj=awscli_pod,
            target=f"s3://{bucket_1.name}",
            file_dir=test_dir,
            pattern="test_obj-",
            s3_obj=mcg_obj,
            amount=1,
        )

        # 4. Verify the objects were replicated to the target buckets
        wait_for_object_versions_match(
            mcg_obj,
            awscli_pod,
            bucket_1.name,
            bucket_2.name,
            obj_key="test_obj-",
        )
        logger.info("All the objects were replicated successfully")

        # 5. Verify the bucket replication metrics from Prometheus
        expected_metrics = {
            "NooBaa_bucket_last_cycle_total_objects_num": 1,
            "NooBaa_bucket_last_cycle_replicated_objects_num": 1,
            "NooBaa_bucket_last_cycle_error_objects_num": 0,
        }
        for metric, expected_value in expected_metrics.items():
            metric_value = get_noobaa_bucket_replication_metrics_in_prometheus(
                metric, bucket_1.name, threading_lock
            )
            assert (
                metric_value == expected_value
            ), f"Metric {metric} has unexpected value. Expected: {expected_value}, Got: {metric_value}"

    @tier2
    @polarion_id("OCS-6917")
    def test_failing_bucket_replication_and_its_metrics(
        self, awscli_pod, mcg_obj, test_directory_setup, bucket_factory, threading_lock
    ):
        """
        1. Create source and target buckets.
        2. Set bucket replication policies on the source buckets
        3. Patch the target bucket to change the quota
        4. Write some objects to the source buckets
        5. Verify the objects were replicated to the target buckets
        6. Write an object to the source bucket so that the quota will get exceeded on target bucket after replication
        7. Verify the metrics from Prometheus after the replication and get failed object metrics in the last cycle
        """
        # 1. Create source and target buckets
        bucket_1, bucket_2 = bucket_factory(2, "OC")

        # 2. Set bucket replication policies on the source buckets
        replication_policy = McgReplicationPolicy(destination_bucket=bucket_2.name)
        update_replication_policy(bucket_1.name, replication_policy.to_dict())

        # 3. Patch the target bucket to change the quota
        quota_str = '{"spec": {"additionalConfig":{"maxObjects": "1"}}}'
        obc_obj = OCP(
            kind="obc",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=bucket_2.name,
        )
        obc_obj.patch(params=quota_str, format_type="merge")
        logger.info(f"Patched quota to obc {bucket_2.name}")

        # Wait a bit for the quota to take effect
        logger.info("Waiting maxObjects quota to take effect on the target bucket")
        time.sleep(60)

        # 4. Write some objects to the source buckets
        test_dir = test_directory_setup.result_dir
        copy_random_individual_objects(
            podobj=awscli_pod,
            target=f"s3://{bucket_1.name}",
            file_dir=test_dir,
            pattern="test_obj-",
            s3_obj=mcg_obj,
            amount=1,
        )

        # 5. Verify the objects were replicated to the target buckets
        wait_for_object_versions_match(
            mcg_obj,
            awscli_pod,
            bucket_1.name,
            bucket_2.name,
            obj_key="test_obj-",
        )
        logger.info("All the objects were replicated successfully")

        logger.info(
            "Waiting for the quantity of objects inside target bucket to get updated"
        )
        time.sleep(120)

        # 6. Write an object to the source bucket so that the quota will get exceeded
        copy_random_individual_objects(
            podobj=awscli_pod,
            target=f"s3://{bucket_1.name}",
            file_dir=test_dir,
            pattern="test_obj-over-quota-",
            s3_obj=mcg_obj,
            amount=1,
        )

        logger.info(
            "Waiting for objects to get replicated and new metrics to be exposed"
        )
        time.sleep(120)

        # 7. Verify the metrics from Prometheus, will get on error object in the last cycle
        expected_metrics = {
            "NooBaa_bucket_last_cycle_total_objects_num": 1,
            "NooBaa_bucket_last_cycle_replicated_objects_num": 0,
            "NooBaa_bucket_last_cycle_error_objects_num": 1,
        }
        for metric, expected_value in expected_metrics.items():
            metric_value = get_noobaa_bucket_replication_metrics_in_prometheus(
                metric, bucket_1.name, threading_lock
            )
            assert (
                metric_value == expected_value
            ), f"Metric {metric} has unexpected value. Expected: {expected_value}, Got: {metric_value}"

    @tier2
    @polarion_id("OCS-6918")
    def test_bidirectional_replication_and_its_metrics(
        self, awscli_pod, mcg_obj, test_directory_setup, bucket_factory, threading_lock
    ):
        """
        1. Create two buckets
        2. Set bucket replication on both buckets
        3. Write some objects to each bucket
        4. Verify the objects were replicated to their targets
        5. Verify the bucket replication metrics from Prometheus
        """
        a_to_b_prefix = "a_to_b"
        b_to_a_prefix = "b_to_a"

        # 1. Create two buckets
        bucket_a, bucket_b = bucket_factory(2, "OC")

        # 2. Set bucket replication on both buckets
        replication_policy = McgReplicationPolicy(
            destination_bucket=bucket_b.name, prefix=a_to_b_prefix
        )
        update_replication_policy(bucket_a.name, replication_policy.to_dict())

        replication_policy = McgReplicationPolicy(
            destination_bucket=bucket_a.name, prefix=b_to_a_prefix
        )
        update_replication_policy(bucket_b.name, replication_policy.to_dict())

        # 3. Write some objects to each bucket
        test_dir = test_directory_setup.result_dir
        for bucket in (bucket_a, bucket_b):
            prefix = a_to_b_prefix if bucket == bucket_a else b_to_a_prefix
            copy_random_individual_objects(
                podobj=awscli_pod,
                target=f"s3://{bucket.name}/{prefix}/",
                file_dir=test_dir,
                pattern=f"{prefix}-",
                s3_obj=mcg_obj,
                amount=1,
            )

        # 4. Verify the objects were replicated to their targets
        for first_bucket, second_bucket, prefix in [
            (bucket_a.name, bucket_b.name, a_to_b_prefix),
            (bucket_b.name, bucket_a.name, b_to_a_prefix),
        ]:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                first_bucket,
                second_bucket,
                obj_key=f"{prefix}/{prefix}-",
            )
        logger.info("All the objects were replicated successfully")

        # Wait for the metrics to be updated
        logger.info("Waiting for the metrics to be updated")
        time.sleep(60)

        # 5. Verify the metrics from Prometheus
        expected_metrics = {
            "NooBaa_bucket_last_cycle_total_objects_num": 1,
            "NooBaa_bucket_last_cycle_replicated_objects_num": 1,
            "NooBaa_bucket_last_cycle_error_objects_num": 0,
        }
        for bucket in (bucket_a, bucket_b):
            for metric, expected_value in expected_metrics.items():
                metric_value = get_noobaa_bucket_replication_metrics_in_prometheus(
                    metric, bucket.name, threading_lock
                )
                assert (
                    metric_value == expected_value
                ), f"Metric {metric} has unexpected value. Expected: {expected_value}, Got: {metric_value}"
