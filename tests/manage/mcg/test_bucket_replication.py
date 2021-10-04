import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.bucket_utils import sync_object_directory
from ocs_ci.ocs.constants import DEFAULT_NOOBAA_BUCKET, AWSCLI_TEST_OBJ_DIR

logger = logging.getLogger(__name__)


class TestReplication(MCGTest):
    """
    Test creation of a namespace resources and buckets via OpenShift CRDs.
    """

    @tier1
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                    "replication_policy": [
                        {
                            "rule-id": "rule1",
                            "destination-bucket": DEFAULT_NOOBAA_BUCKET,
                        }
                    ],
                },
                # TODO: add polarion id
                marks=[tier1, pytest.mark.polarion_id("OCS-")],
            ),
        ],
        ids=[
            "AWS-OC-1",
        ],
    )
    @pytest.mark.polarion_id("OCS-2255")
    def test_uni_direction_replication(
        self, awscli_pod_session, mcg_obj, bucket_factory, bucketclass_dict
    ):
        """
        Test namespace bucket creation using the MCG CRDs.
        """

        # Create the namespace bucket on top of the namespace resource
        bucketname = bucket_factory(1, bucketclass=bucketclass_dict)[0].name
        full_object_path = f"s3://{bucketname}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )
        objects_to_replicate = mcg_obj.s3_list_all_objects_in_bucket(bucketname)

        assert set(downloaded_files).issubset(
            obj.key for obj in objects_to_replicate
        ), "Failed to upload files correctly"

        # TODO: Normal timeout sampler replication verification
        assert set(objects_to_replicate).issubset(
            obj.key
            for obj in mcg_obj.s3_list_all_objects_in_bucket(DEFAULT_NOOBAA_BUCKET)
        ), "Replication failed"


