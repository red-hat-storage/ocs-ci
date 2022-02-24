import logging
import pytest
import json
import time

from ocs_ci.framework.testlib import scale, E2ETest
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs import hsbench
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.bucket_utils import compare_bucket_object_list
from ocs_ci.ocs import scale_noobaa_lib

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def s3bench(request):
    s3bench = hsbench.HsBench()
    s3bench.create_resource_hsbench()
    s3bench.install_hsbench()

    def finalizer():
        s3bench.cleanup()

    request.addfinalizer(finalizer)
    return s3bench


@scale
@skipif_ocs_version("<4.9")
class TestScaleBucketReplication(E2ETest):
    """
    Test MCG scale bucket replication
    """

    MCG_S3_OBJ = 1000
    MCG_BUCKET = 50

    @pytest.mark.parametrize(
        argnames=["bucketclass", "replication_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[pytest.mark.polarion_id("OCS-2721")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                marks=[pytest.mark.polarion_id("OCS-2722")],
            ),
        ],
    )
    def test_scale_unidirectional_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        bucket_factory,
        bucketclass,
        replication_bucketclass,
        s3bench,
        wait_time=120,
    ):
        """
        Test unidirectional bucket replication adding objects to:
            - Object buckets - backingstore
            - Namespace buckets - namespacestore
        """

        replication_buckets = bucket_factory(
            amount=self.MCG_BUCKET,
            bucketclass=replication_bucketclass,
        )
        endpoints = list()
        source_buckets = list()
        for bucket in replication_buckets:
            replication_policy = ("basic-replication-rule", bucket.name, None)
            source_bucket = bucket_factory(
                amount=1,
                bucketclass=bucketclass,
                replication_policy=replication_policy,
            )[0]
            end_point = (
                "http://"
                + mcg_obj.s3_internal_endpoint.split("/")[2].split(":")[0]
                + "/"
                + f"{source_bucket.name}"
            )
            endpoints.append(end_point)
            source_buckets.append(source_bucket)

        for endpoint in endpoints:
            s3bench.run_benchmark(
                num_obj=self.MCG_S3_OBJ,
                timeout=7200,
                access_key=mcg_obj.access_key_id,
                secret_key=mcg_obj.access_key,
                end_point=endpoint,
                run_mode="pg",
            )
        time.sleep(wait_time)
        # Restart Noobaa-core pod
        scale_noobaa_lib.noobaa_running_node_restart(pod_name="noobaa-db")

        # Verify bucket replication
        for i in range(len(replication_buckets)):
            compare_bucket_object_list(
                mcg_obj, replication_buckets[i].name, source_buckets[i].name
            )

    @pytest.mark.parametrize(
        argnames=["first_bucketclass", "second_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[pytest.mark.polarion_id("OCS-2723")],
            ),
        ],
    )
    def test_scale_bidirectional_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        bucket_factory,
        first_bucketclass,
        second_bucketclass,
        test_directory_setup,
        s3bench,
        wait_time=120,
    ):
        """
        Test bidirectional bucket replication.
        """

        first_buckets = bucket_factory(
            amount=self.MCG_BUCKET, bucketclass=first_bucketclass
        )
        endpoints = list()
        second_buckets = list()
        for bucket in first_buckets:
            replication_policy = ("basic-replication-rule", bucket.name, None)
            second_bucket = bucket_factory(
                1,
                bucketclass=second_bucketclass,
                replication_policy=replication_policy,
            )[0]
            replication_policy_patch_dict = {
                "spec": {
                    "additionalConfig": {
                        "replicationPolicy": json.dumps(
                            [
                                {
                                    "rule_id": "basic-replication-rule-2",
                                    "destination_bucket": second_bucket.name,
                                }
                            ]
                        )
                    }
                }
            }
            OCP(
                kind="obc",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=bucket.name,
            ).patch(
                params=json.dumps(replication_policy_patch_dict), format_type="merge"
            )
            first_end_point = (
                "http://"
                + mcg_obj.s3_internal_endpoint.split("/")[2].split(":")[0]
                + "/"
                + f"{bucket.name}"
            )
            second_end_point = (
                "http://"
                + mcg_obj.s3_internal_endpoint.split("/")[2].split(":")[0]
                + "/"
                + f"{second_bucket.name}"
            )
            endpoints.append(first_end_point)
            endpoints.append(second_end_point)
            second_buckets.append(second_bucket)

        # Write objects to the buckets
        for endpoint in endpoints:
            s3bench.run_benchmark(
                num_obj=self.MCG_S3_OBJ,
                timeout=7200,
                access_key=mcg_obj.access_key_id,
                secret_key=mcg_obj.access_key,
                end_point=endpoint,
                run_mode="pg",
            )
        time.sleep(wait_time)
        # Restart Noobaa-db pod
        scale_noobaa_lib.noobaa_running_node_restart(pod_name="noobaa-db")

        # Verify bucket replication
        for i in range(len(first_buckets)):
            compare_bucket_object_list(
                mcg_obj, first_buckets[i].name, second_buckets[i].name
            )
