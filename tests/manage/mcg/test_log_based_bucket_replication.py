from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.resources.mockup_bucket_logger import MockupBucketLogger

from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import skipif_aws_creds_are_missing
from ocs_ci.ocs.resources.replication_policy import LogBasedReplicationPolicy


@skipif_aws_creds_are_missing
class TestLogBasedBucketReplication(MCGTest):
    """
    Test log-based replication with deletions sync.
    """

    # TODO do we already have this const in the project?
    DEFAULT_AWS_REGION = "us-east-2"

    def test_deletion_sync(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
    ):
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {
                    constants.AWS_PLATFORM: [(1, self.DEFAULT_AWS_REGION)]
                },
            },
        }
        target_bucket_name = bucket_factory(bucketclass=bucketclass_dict)[0].name

        mockup_logger = MockupBucketLogger(
            awscli_pod=awscli_pod_session,
            mcg_obj=mcg_obj_session,
            bucket_factory=bucket_factory,
            platform=constants.AWS_PLATFORM,
            region=self.DEFAULT_AWS_REGION,
        )
        logs_bucket_name = mockup_logger.logs_bucket_uls_name

        replication_policy = LogBasedReplicationPolicy(
            destination_bucket=target_bucket_name,
            sync_deletions=True,
            logs_bucket=logs_bucket_name,
        )

        source_bucket_name = bucket_factory(
            1, bucketclass=bucketclass_dict, replication_policy=replication_policy
        )[0].name

        mockup_logger.upload_test_objs_and_log(source_bucket_name)

        assert source_bucket_name
