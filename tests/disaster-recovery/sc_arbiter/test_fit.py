import logging

from ocs_ci.ocs.bucket_utils import patch_replication_policy_to_bucket

logger = logging.getLogger(__name__)


class TestFITonSC:
    def test_fit_on_sc(self, bucket_factory):

        # MCG bucket replication on RGW bucket and any other cloud provider. Both uni-directional & bi-directional
        first_bucket_class_dict = {
            "interface": "cli",
            "backingstore_dict": {"rgw": [(1, None)]},
        }

        second_bucket_class_dict = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }
        first_bucket = bucket_factory(bucketclass=first_bucket_class_dict)[0].name
        replication_policy = ("basic-replication-rule", first_bucket, None)
        second_bucket = bucket_factory(
            1,
            bucketclass=second_bucket_class_dict,
            replication_policy=replication_policy,
        )[0].name
        logger.info(f"First bucket: {first_bucket} Second bucket: {second_bucket}")
        patch_replication_policy_to_bucket(
            first_bucket, "basic-replication-rule-2", second_bucket
        )

        # Noobaa caching

        # MCG NSFS

        # RGW kafka notification
