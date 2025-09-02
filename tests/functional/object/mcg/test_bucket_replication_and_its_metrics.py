import logging
import base64
import re

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    polarion_id,
    red_squad,
    runs_on_provider,
    tier1,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    update_replication_policy,
    wait_for_object_versions_match,
    copy_random_individual_objects,
)
from ocs_ci.ocs.resources.mcg_replication_policy import McgReplicationPolicy
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources.pod import get_noobaa_core_pod

logger = logging.getLogger(__name__)


TIMEOUT = 300


@mcg
@red_squad
@runs_on_provider
class TestReplicationAndItsMetrics(MCGTest):
    """
    Test suite for MCG object replication policies
    """

    @pytest.fixture(autouse=True, scope="class")
    def reduce_replication_delay_setup(self, add_env_vars_to_noobaa_core_class):
        """
        Reduce the replication delay to one minute

        Args:
            new_delay_in_miliseconds (function): A function to add env vars to the noobaa-core pod
        """
        new_delay_in_miliseconds = 60 * 1000
        new_env_var_tuples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
            (constants.BUCKET_LOG_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
        ]
        add_env_vars_to_noobaa_core_class(new_env_var_tuples)

    @pytest.fixture()
    def make_buckets(self, bucket_factory):
        """
        A factory that creates MCG buckets

        Args:
            bucket_factory: Fixture for creating new buckets

        Returns:
            function: The factory function
        """

        def _factory(amount):
            """
            Create buckets

            Args:
                amount (int): The number of buckets to create

            Returns:
                list(Bucket): The created buckets
            """
            # Using the OC interface allows patching a replication policy on the OBC
            buckets = bucket_factory(amount, "OC")
            return buckets

        return _factory

    @tier1
    @polarion_id("OCS-6916")
    def test_bucket_replication_and_its_metrics(
        self, awscli_pod, mcg_obj, make_buckets, test_directory_setup
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
        bucket_1, bucket_2 = make_buckets(2)

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
        token_cmd = "oc get secret noobaa-metrics-auth-secret -n openshift-storage -o jsonpath='{.data.metrics_token}'"
        jwt_token = base64.b64decode(run_cmd(cmd=token_cmd)).decode()
        noobaa_core_pod = get_noobaa_core_pod()

        # 6. Get the metrics for the bucket replication
        metrics_output = noobaa_core_pod.exec_cmd_on_pod(
            f"curl -k -H 'Authorization: Bearer {jwt_token}' localhost:8080/metrics/bg_workers",
            out_yaml_format=False,
        )

        # 7. Verify the metrics
        expected_metrics = {
            "NooBaa_bucket_last_cycle_total_objects_num": 5,
            "NooBaa_bucket_last_cycle_replicated_objects_num": 5,
            "NooBaa_bucket_last_cycle_error_objects_num": 0,
        }
        for metric, expected_value in expected_metrics.items():
            pattern = rf'^{metric}{{bucket_name="{bucket_1.name}"}} (\d)$'
            output_value = re.findall(pattern, metrics_output, re.MULTILINE)[-1]
            logger.info(f"Metrics {metric} : {output_value}")
            assert (
                int(output_value) == expected_value
            ), f"Metric {metric} has unexpected value"
