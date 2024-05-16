from abc import ABC

from ocs_ci.ocs.bucket_utils import compare_bucket_object_list


class LbrHandler(ABC):
    """
    An abstract class for handling log-based replication between two buckets.
    """

    def __init__(
        self,
        mcg_obj,
        bucket_factory,
        awscli_pod,
        patch_to_existing_bucket=False,
    ):
        self.mcg_obj = mcg_obj
        self.bucket_factory = bucket_factory
        self.awscli_pod = awscli_pod
        self._source_bucket = None
        self._source_bucket = None
        self._setup_buckets_with_log_based_replication(patch_to_existing_bucket)

    def _setup_buckets_with_log_based_replication(self, patch_to_existing_bucket=False):
        """
        Set the _source_bucket and _target_bucket attributes with buckets that have
        log-based replication enabled between them.

        Args:
            patch_to_existing_bucket(bool): Whether to set the replication policy
                on an the source bucket after it has been created, or when creating
                it.
        """
        raise NotImplementedError()

    @property
    def source_bucket(self):
        """
        Returns:
            str: The name of the source bucket
        """
        if not self._source_bucket:
            raise NotImplementedError()
        return self._source_bucket

    @property
    def target_bucket(self):
        """ "
        Returns:
            str: The name of the target bucket
        """
        if not self._target_bucket:
            raise NotImplementedError()
        return self._target_bucket

    @property
    def deletion_sync_enabled(self):
        """
        Returns:
            bool: True if deletion sync is enabled, False otherwise
        """
        raise NotImplementedError()

    def upload_random_objects_to_source(self, amount, prefix=""):
        """
        Upload random objects to the source bucket.

        Args:
            amount(int): The amount of random objects to upload
            prefix(str): The prefix under which to upload the objects
        Returns:
            list: A list of the uploaded object keys
        """
        raise NotImplementedError()

    def delete_recursively_from_source(self, prefix=""):
        """
        Delete all objects from a bucket recursively.

        Args:
            prefix(str): The prefix of the objects to delete
                - The default is an empty string - delete all objects
        """
        raise NotImplementedError()

    def wait_for_sync(self, timeout=600, prefix=""):
        """
        Wait for the target bucket to sync with the source bucket.

        Args:
            timeout(int): The amount of time to wait for the sync to complete
            prefix(str): The prefix under which to compare the objects
                - The default is an empty string - compare all objects in the bucket

        Returns:
            bool: True if the source and target buckets have synced during the timeout period, False otherwise
        """
        return compare_bucket_object_list(
            self.mcg_obj,
            self.source_bucket.name,
            self.target_bucket.name,
            timeout=timeout,
        )
