import pytest
import logging

from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    update_replication_policy,
)
from ocs_ci.ocs.resources.mockup_bucket_logger import MockupBucketLogger

from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import skipif_aws_creds_are_missing, tier1, tier2
from ocs_ci.ocs.resources.replication_policy import LogBasedReplicationPolicy

logger = logging.getLogger(__name__)


@pytest.fixture()
def log_based_replication_setup(awscli_pod_session, mcg_obj_session, bucket_factory):
    """
    A fixture to set up standard log-based replication with deletion sync.

    Args:
        awscli_pod_session(Pod): A pod running the AWS CLI
        mcg_obj_session(MCG): An MCG object
        bucket_factory: A bucket factory fixture

    Returns:
        MockupBucketLogger: A MockupBucketLogger object
        Bucket: The source bucket
        Bucket: The target bucket
    """

    logger.info("Starting log-based replication setup")

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
    replication_policy = LogBasedReplicationPolicy(
        destination_bucket=target_bucket.name,
        sync_deletions=True,
        logs_bucket=mockup_logger.logs_bucket_uls_name,
    )

    source_bucket = bucket_factory(
        1, bucketclass=bucketclass_dict, replication_policy=replication_policy
    )[0]

    logger.info("log-based replication setup complete")

    return mockup_logger, source_bucket, target_bucket


@skipif_aws_creds_are_missing
class TestLogBasedBucketReplication(MCGTest):
    """
    Test log-based replication with deletion sync.

    Log-based replication requires reading AWS bucket logs from an AWS bucket in the same region as the source bucket.
    As these logs may take several hours to become available, this test suite utilizes MockupBucketLogger to upload
    mockup logs for each I/O operation performed on the source bucket to a dedicated log bucket on AWS.

    """

    DEFAULT_AWS_REGION = "us-east-2"
    TIMEOUT = 15 * 60

    @tier1
    def test_deletion_sync(self, mcg_obj_session, log_based_replication_setup):
        """
        Test log-based replication with deletion sync.

        1. Upload a set of objects to the source bucket
        2. Wait for the objects to be replicated to the target bucket
        3. Delete all objects from the source bucket
        4. Wait for the objects to be deleted from the target bucket

        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        logger.info("Testing log-based replication with deletion sync")

        mockup_logger.upload_test_objs_and_log(source_bucket.name)
        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Standard replication failed to complete in {self.TIMEOUT} seconds"

        mockup_logger.delete_all_objects_and_log(source_bucket.name)
        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Deletion sync failed to complete in {self.TIMEOUT} seconds"

    @tier1
    def test_deletion_sync_opt_out(self, mcg_obj_session, log_based_replication_setup):
        """
        Test that deletion sync can be disabled.

        1. Upload a set of objects to the source bucket
        2. Wait for the objects to be replicated to the target bucket
        3. Disable deletion sync
        4. Delete all objects from the source bucket
        5. Verify that the objects are not deleted from the target bucket

        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        logger.info("Uploading test objects and waiting for replication to complete")
        mockup_logger.upload_test_objs_and_log(source_bucket.name)
        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Standard replication failed to complete in {self.TIMEOUT} seconds"

        logger.info("Disabling the deletion sync")
        disabled_del_sync_policy = source_bucket.replication_policy
        disabled_del_sync_policy["rules"][0]["sync_deletions"] = False
        update_replication_policy(source_bucket.name, disabled_del_sync_policy)

        logger.info("Deleting source objects and verifying they remain on target")
        mockup_logger.delete_all_objects_and_log(source_bucket.name)
        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), "Deletion sync completed even though the policy was disabled!"

    @tier2
    def test_patch_deletion_sync_to_existing_bucket(
        self, awscli_pod_session, mcg_obj_session, bucket_factory
    ):
        """
        Test patching deletion sync onto an existing bucket.

        1. Create a source bucket
        2. Create a target bucket
        3. Patch the source bucket with a replication policy that includes deletion sync
        4. Upload a set of objects to the source bucket
        5. Wait for the objects to be replicated to the target bucket
        6. Delete all objects from the source bucket
        7. Wait for the objects to be deleted from the target bucket

        """

        logger.info("Creating source and target buckets")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {constants.AWS_PLATFORM: [(1, "us-east-2")]},
            },
        }
        target_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]
        source_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]

        logger.info("Patching the policy to the source bucket")
        mockup_logger = MockupBucketLogger(
            awscli_pod=awscli_pod_session,
            mcg_obj=mcg_obj_session,
            bucket_factory=bucket_factory,
            platform=constants.AWS_PLATFORM,
            region="us-east-2",
        )
        replication_policy = LogBasedReplicationPolicy(
            destination_bucket=target_bucket.name,
            sync_deletions=True,
            logs_bucket=mockup_logger.logs_bucket_uls_name,
        )
        update_replication_policy(source_bucket.name, replication_policy.to_dict())

        logger.info("Testing log-based replication with deletion sync")

        mockup_logger.upload_test_objs_and_log(source_bucket.name)
        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT,
        ), f"Standard replication failed to complete in {self.TIMEOUT} seconds"

        mockup_logger.delete_all_objects_and_log(source_bucket.name)
        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.TIMEOUT * 2,
        ), f"Deletion sync failed to complete in {self.TIMEOUT * 2} seconds"
