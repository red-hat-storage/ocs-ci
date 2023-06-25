import pytest
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    update_replication_policy,
)
from ocs_ci.ocs.resources.mockup_bucket_logger import MockupBucketLogger

from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import skipif_aws_creds_are_missing, tier1, tier2
from ocs_ci.ocs.resources.replication_policy import LogBasedReplicationPolicy


@pytest.fixture()
def log_based_replication_setup(awscli_pod_session, mcg_obj_session, bucket_factory):
    """
    Return a tuple of the following:
    - mockup_logger: A MockupBucketLogger object
    - source_bucket: An OCBucket object
    - A target bucket: An OCBucket object
    """
    bucketclass_dict = {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Single",
            "namespacestore_dict": {constants.AWS_PLATFORM: [(1, "us-east-2")]},
        },
    }
    target_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]

    mockup_logger = MockupBucketLogger(
        awscli_pod=awscli_pod_session,
        mcg_obj=mcg_obj_session,
        bucket_factory=bucket_factory,
        platform=constants.AWS_PLATFORM,
        region="us-east-2",
    )
    logs_bucket_name = mockup_logger.logs_bucket_uls_name

    replication_policy = LogBasedReplicationPolicy(
        destination_bucket=target_bucket.name,
        sync_deletions=True,
        logs_bucket=logs_bucket_name,
    )

    source_bucket = bucket_factory(
        1, bucketclass=bucketclass_dict, replication_policy=replication_policy
    )[0]

    return mockup_logger, source_bucket, target_bucket


@skipif_aws_creds_are_missing
class TestLogBasedBucketReplication(MCGTest):
    """
    Test log-based replication with deletions sync.
    """

    DEFAULT_AWS_REGION = "us-east-2"
    TIMEOUT = 15 * 60

    @tier1
    def test_deletion_sync(self, mcg_obj_session, log_based_replication_setup):
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        mockup_logger.upload_test_objs_and_log(source_bucket.name)

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Standard replication failed to complete in {self.TIMEOUT} seconds."

        mockup_logger.delete_all_objects_and_log(source_bucket.name)

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Deletion sync failed to complete in {self.TIMEOUT} seconds."

    @tier2
    def test_deletion_sync_opt_out(self, mcg_obj_session, log_based_replication_setup):
        """
        Test that deletion sync can be disabled.
        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        mockup_logger.upload_test_objs_and_log(source_bucket.name)

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Standard replication failed to complete in {self.TIMEOUT} seconds."

        disabled_del_sync_policy = source_bucket.replication_policy
        disabled_del_sync_policy["rules"][0]["sync_deletions"] = False

        update_replication_policy(source_bucket.name, disabled_del_sync_policy)

        mockup_logger.delete_all_objects_and_log(source_bucket.name)

        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), "Deletion sync occurred despite being disabled."
