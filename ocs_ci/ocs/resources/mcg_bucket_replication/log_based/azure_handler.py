from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    update_replication_policy,
)

from ocs_ci.ocs.resources.mcg_bucket_replication.log_based.abstract_handler import (
    LbrHandler,
)
from ocs_ci.ocs.resources.mcg_bucket_replication.policy import (
    AzureLbrPolicy,
)


class AzureHandler(LbrHandler):
    """
    A class for handling log-based replication between MCG buckets,
    when the source bucket is using an Azure Namespacestore.
    """

    DEFAULT_AWS_REGION = "us-east-2"

    def __init__(
        self,
        mcg_obj,
        bucket_factory,
        awscli_pod,
        patch_to_existing_bucket=False,
    ):
        """
        Create the source and target buckets and set up log-based replication between them.
        """
        super().__init__(mcg_obj, bucket_factory, awscli_pod, patch_to_existing_bucket)

        target_bucket = self.bucket_factory()[0]

        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {
                    constants.AZURE_WITH_LOGS_PLATFORM: [(1, None)]
                },
            },
        }

        replication_policy = AzureLbrPolicy(
            destination_bucket=target_bucket.name,
            sync_deletions=True,
        )

        if patch_to_existing_bucket:
            source_bucket = self.bucket_factory(bucketclass=bucketclass_dict)[0]
            update_replication_policy(source_bucket.name, replication_policy.to_dict())

        else:
            source_bucket = self.bucket_factory(
                1, bucketclass=bucketclass_dict, replication_policy=replication_policy
            )[0]

        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
