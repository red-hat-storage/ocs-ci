from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    update_replication_policy,
)

from ocs_ci.ocs.resources.mcg_bucket_replication.log_based.abstract_handler import (
    LbrHandler,
)
from ocs_ci.ocs.resources.mcg_bucket_replication.log_based.mockup_bucket_logger import (
    MockupBucketLogger,
)
from ocs_ci.ocs.resources.mcg_bucket_replication.policy import (
    AwsLbrPolicy,
)


class AwsLbrHandler(LbrHandler):
    """
    A class for handling log-based replication between MCG buckets on
    top of AWS Namespacestores.

    Since the logs can take up to 24 hours to be delivered on AWS, this implementation
    writes a mockup log to the logs bucket for each PUT and DELETE operation
    on the source bucket.
    """

    DEFAULT_AWS_REGION = "us-east-2"

    def _setup_buckets_with_log_based_replication(self, patch_to_existing_bucket=False):
        """
        Create the source and target buckets and set up log-based replication between them,
        but also setup the mockup logger which will be used in the methods of this class.

        Args:
            patch_to_existing_bucket(bool): Whether to set the replication policy
                on an the source bucket after it has been created, or when creating
                it.
        """

        #
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {
                    constants.AWS_PLATFORM: [(1, self.DEFAULT_AWS_REGION)]
                },
            },
        }

        target_bucket = self.bucket_factory(bucketclass=bucketclass_dict)[0]

        mockup_logger = MockupBucketLogger(
            awscli_pod=self.awscli_pod,
            mcg_obj=self.mcg_obj,
            bucket_factory=self.bucket_factory,
            platform=constants.AWS_PLATFORM,
            region=self.DEFAULT_AWS_REGION,
        )
        replication_policy = AwsLbrPolicy(
            destination_bucket=target_bucket.name,
            sync_deletions=True,
            logs_bucket=mockup_logger.logs_bucket_uls_name,
        )
        if patch_to_existing_bucket:
            source_bucket = self.bucket_factory(bucketclass=bucketclass_dict)[0]
            update_replication_policy(source_bucket.name, replication_policy.to_dict())

        else:
            source_bucket = self.bucket_factory(
                1, bucketclass=bucketclass_dict, replication_policy=replication_policy
            )[0]

        self._source_bucket = source_bucket
        self._target_bucket = target_bucket
        self.mockup_logger = mockup_logger

    def upload_random_objects_to_source(self, amount, prefix=None):
        """
        Upload random objects to the source bucket and upload a matching PUT
        mockup log for each object.

        Args:
            amount(int): The amount of random objects to upload
            prefix(str): The prefix under which to upload the objects

        Returns:
            list: A list of the uploaded object keys
        """
        return self.mockup_logger.upload_test_objs_and_log(
            self.source_bucket.name, amount, prefix
        )

    def delete_recursively_from_source(self, prefix=""):
        """
        Delete objects from the source bucket recursively and upload a matching DELETE
        mockup log for each object.

        Args:
            prefix(str): The prefix of the objects to delete
                - The default is an empty string - delete all objects
        """

        # TODO: change to support prefix
        self.mockup_logger.delete_all_objects_and_log(self.source_bucket.name)

    @property
    def deletion_sync_enabled(self):
        """
        Returns:
            bool: True if deletion sync is enabled, False otherwise
        """
        policy = self.source_bucket.replication_policy["rules"][0]
        return policy["sync_deletions"]

    @deletion_sync_enabled.setter
    def deletion_sync_enabled(self, value):
        """
        Toggle the deletion sync on the source bucket.

        Args:
            value(bool): True to enable deletion sync, False to disable it
        """

        policy = self.source_bucket.replication_policy["rules"][0]
        policy["sync_deletions"] = value
        update_replication_policy(self.source_bucket.name, policy)
