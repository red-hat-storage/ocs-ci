import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.bucket_utils import compare_bucket_contents, sync_object_directory
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR

logger = logging.getLogger(__name__)


class TestReplication(MCGTest):
    """
    Test creation of a namespace resources and buckets via OpenShift CRDs.
    """

    @tier1
    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"gcp": [(1, None)]},
                },
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                # TODO: add polarion id
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                {"interface": "CLI", "backingstore_dict": {"gcp": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "CLI", "backingstore_dict": {"azure": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier1],
            ),
        ],
        ids=["AWStoAZURE-OC", "GCPtoAWS-OC", "AZUREtoCGP-CLI", "AWStoAZURE-CLI"],
    )
    def test_unidirectional_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
    ):
        """
        Test namespace bucket creation using the MCG CRDs.
        """

        # Create a bucket that replicates its objects to first.bucket
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name
        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket_name = bucket_factory(
            1, bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0].name
        full_object_path = f"s3://{source_bucket_name}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )
        written_objects = mcg_obj.s3_list_all_objects_in_bucket(source_bucket_name)

        assert set(downloaded_files) == {
            obj.key for obj in written_objects
        }, "Needed uploaded objects could not be found"

        compare_bucket_contents(mcg_obj, source_bucket_name, target_bucket_name)
