import logging
import re
import time


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
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg_replication_policy import McgReplicationPolicy
from ocs_ci.ocs.resources.pod import get_noobaa_core_pod
from ocs_ci.helpers.helpers import get_noobaa_metrics_token_from_secret

logger = logging.getLogger(__name__)


TIMEOUT = 300


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

    @tier2
    @polarion_id("OCS-6916")
    def test_bucket_replication_and_its_metrics(
        self, awscli_pod, mcg_obj, test_directory_setup, bucket_factory
    ):
        """
        1. Create source and target buckets.
        2. Set bucket replication policies on the source buckets
        3. Write some objects to the source buckets
        4. Verify the objects were replicated to the target buckets
        5. Get the metrics for the bucket replication
        6. Verify the metrics
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
            amount=5,
        )

        # 4. Verify the objects were replicated to the target buckets
        wait_for_object_versions_match(
            mcg_obj,
            awscli_pod,
            bucket_1.name,
            bucket_2.name,
            obj_key="test_obj-",
        )
        logger.info("All the versions were replicated successfully")

        # 5. get JWT TOKEN
        jwt_token = get_noobaa_metrics_token_from_secret()

        # 6. Get the metrics for the bucket replication
        noobaa_core_pod = get_noobaa_core_pod()
        metrics_cmd = f"curl -k -H 'Authorization: Bearer {jwt_token}' localhost:8080/metrics/bg_workers"
        metrics_output = noobaa_core_pod.exec_cmd_on_pod(
            metrics_cmd, out_yaml_format=False
        )

        # 7. Verify the metrics
        expected_metrics = {
            "NooBaa_bucket_last_cycle_total_objects_num": 5,
            "NooBaa_bucket_last_cycle_replicated_objects_num": 5,
            "NooBaa_bucket_last_cycle_error_objects_num": 0,
        }
        for metric, expected_value in expected_metrics.items():
            pattern = rf"^{metric}{{bucket_name=\"{bucket_1.name}\"}} (\d)$"
            output_value = re.findall(pattern, metrics_output, re.MULTILINE)[-1]
            logger.info(f"Metrics {metric} : {output_value}")
            assert (
                int(output_value) == expected_value
            ), f"Metric {metric} has unexpected value"

    @tier2
    @polarion_id("OCS-6917")
    def test_failing_bucket_replication_and_its_metrics(
        self, awscli_pod, mcg_obj, test_directory_setup, bucket_factory
    ):
        """
        1. Create source and target buckets.
        2. Set bucket replication policies on the source buckets
        3. Patch the target bucket to change the quota
        4. Write some objects to the source buckets
        5. Verify the objects were replicated to the target buckets
        6. Wait before writing to the source bucket so that the replication
            policy will be applied and quota will get exceeded
        7. Write an object to the source bucket so that the quota will get
            exceeded
        8. Wait before getting the metrics so that the replication
        policy will be applied and quota will get exceeded
        9. Get the metrics for the bucket replication
        10. Verify the metrics
        """
        # 1. Create source and target buckets
        bucket_1, bucket_2 = bucket_factory(2, "OC")

        # 2. Set bucket replication policies on the source buckets
        replication_policy = McgReplicationPolicy(destination_bucket=bucket_2.name)
        update_replication_policy(bucket_1.name, replication_policy.to_dict())

        # 3. Patch the target bucket to change the quota
        quota_str = '{"spec": {"additionalConfig":{"maxObjects": "4"}}}'
        cmd = f"patch obc {bucket_2.name} -p '{quota_str}' -n {config.ENV_DATA['cluster_namespace']} --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(f"Patched quota to obc {bucket_2.name}")
        time.sleep(60)

        # 4. Write some objects to the source buckets
        test_dir = test_directory_setup.result_dir
        copy_random_individual_objects(
            podobj=awscli_pod,
            target=f"s3://{bucket_1.name}",
            file_dir=test_dir,
            pattern="test_obj-",
            s3_obj=mcg_obj,
            amount=4,
        )

        # 5. Verify the objects were replicated to the target buckets
        wait_for_object_versions_match(
            mcg_obj,
            awscli_pod,
            bucket_1.name,
            bucket_2.name,
            obj_key="test_obj-",
        )
        logger.info("All the versions were replicated successfully")

        time.sleep(120)
        logger.info(
            "Waiting before writing to the source bucket so that the \
            replication policy will be applied and quota will get exceeded"
        )

        # 6. Write an object to the source bucket so that the quota will get exceeded
        copy_random_individual_objects(
            podobj=awscli_pod,
            target=f"s3://{bucket_1.name}",
            file_dir=test_dir,
            pattern="test_obj-over-quota-",
            s3_obj=mcg_obj,
            amount=1,
        )

        time.sleep(120)
        logger.info(
            "Waiting before getting the metrics so that the replication policy \
            will be applied and quota will get exceeded"
        )

        # 7. get JWT TOKEN
        jwt_token = get_noobaa_metrics_token_from_secret()

        # 8. Get the metrics for the bucket replication
        noobaa_core_pod = get_noobaa_core_pod()
        metrics_cmd = f"curl -k -H 'Authorization: Bearer {jwt_token}' localhost:8080/metrics/bg_workers"
        metrics_output = noobaa_core_pod.exec_cmd_on_pod(
            metrics_cmd, out_yaml_format=False
        )

        # 9. Verify the metrics, will get on error object in the last cycle
        expected_metrics = {
            "NooBaa_bucket_last_cycle_total_objects_num": 1,
            "NooBaa_bucket_last_cycle_replicated_objects_num": 0,
            "NooBaa_bucket_last_cycle_error_objects_num": 1,
        }
        for metric, expected_value in expected_metrics.items():
            pattern = rf"^{metric}{{bucket_name=\"{bucket_1.name}\"}} (\d)$"
            output_value = re.findall(pattern, metrics_output, re.MULTILINE)[-1]
            logger.info(f"Metrics {metric} : {output_value}")
            assert (
                int(output_value) == expected_value
            ), f"Metric {metric} has unexpected value"
