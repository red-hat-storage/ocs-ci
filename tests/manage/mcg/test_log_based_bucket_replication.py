from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.resources.mockup_aws_bucket_logger import MockupAwsBucketLogger

# from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import skipif_aws_creds_are_missing


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
        cloud_uls_factory,
        bucket_factory,
    ):
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {"aws": [(1, self.DEFAULT_AWS_REGION)]},
            },
        }
        target_bucket_name = bucket_factory(bucketclass=bucketclass_dict)[0].name

        mockup_logger = MockupAwsBucketLogger(
            awscli_pod_session, cloud_uls_factory, self.DEFAULT_AWS_REGION
        )
        logs_bucket_name = mockup_logger.logs_bucket_name

        # TODO wait for replication_policy overhaul PR to get merged
        replication_policy = (
            "basic-replication-rule",
            target_bucket_name,
            logs_bucket_name,
            None,
        )

        source_bucket_name = bucket_factory(
            1, bucketclass=bucketclass_dict, replication_policy=replication_policy
        )[0].name

        assert source_bucket_name
