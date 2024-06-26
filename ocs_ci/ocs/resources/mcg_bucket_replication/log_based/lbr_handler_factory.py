from ocs_ci.ocs import constants


class LbrHandlerFactory:
    """
    A factory class to encapsulate the creation logic for log-based replication handlers.
    """

    def __init__(self, mcg_obj, bucket_factory, awscli_pod):
        self.mcg_obj = mcg_obj
        self.bucket_factory = bucket_factory
        self.awscli_pod = awscli_pod

    def create(self, platform, patch_to_existing_bucket=False):
        """
        Create a log-based replication handler for a specific underlying
        storage provider platform.

        Args:
            platform (str): The platform of the underlying storage provider.
            patch_to_existing_bucket (bool): Whether to set the replication policy
                on an existing bucket after it has been created.

        Returns:
            LbrHandler: An instance of a log-based replication handler.
        """
        # Importing here to avoid circular import issues
        from ocs_ci.ocs.resources.mcg_bucket_replication.log_based.aws_handler import (
            AwsLbrHandler,
        )

        if platform == constants.AWS_PLATFORM:
            return AwsLbrHandler(
                self.mcg_obj,
                self.bucket_factory,
                self.awscli_pod,
                patch_to_existing_bucket,
            )
        else:
            raise NotImplementedError(
                f"Log-based replication is not yet supported on {platform}"
            )
