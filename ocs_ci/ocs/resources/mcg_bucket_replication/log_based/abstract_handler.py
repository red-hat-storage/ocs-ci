from abc import ABC
import logging

from ocs_ci.helpers.helpers import craft_s3_command, setup_pod_directories
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    list_objects_from_bucket,
    update_replication_policy,
    write_random_test_objects_to_bucket,
)

logger = logging.getLogger(__name__)


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
        self.source_bucket = None
        self.target_bucket = None
        self.tmp_objs_dir = setup_pod_directories(awscli_pod, ["tmp_objs_dir"])[0]

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
        if value != self.deletion_sync_enabled:
            policy = self.source_bucket.replication_policy
            policy["rules"][0]["sync_deletions"] = value
            update_replication_policy(self.source_bucket.name, policy)

    @property
    def policy_prefix_filter(self):
        """
        Returns:
            str: The prefix filter of the policy
        """
        policy = self.source_bucket.replication_policy["rules"][0]
        return policy["filter"]["prefix"]

    @policy_prefix_filter.setter
    def policy_prefix_filter(self, prefix):
        """
        Set the prefix filter of the policy

        Args:
            prefix(str): The prefix filter of the policy
        """
        if prefix != self.policy_prefix_filter:
            policy = self.source_bucket.replication_policy
            policy["rules"][0]["filter"]["prefix"] = prefix
            update_replication_policy(self.source_bucket.name, policy)

    def upload_random_objects_to_source(self, amount, prefix=""):
        """
        Upload random objects to the source bucket.

        Args:
            amount(int): The amount of random objects to upload
            prefix(str): The prefix under which to upload the objects
        Returns:
            list: A list of the uploaded object keys
        """
        logger.info(f"Uploading test objects to {self.source_bucket.name}/{prefix}")

        written_objs = write_random_test_objects_to_bucket(
            io_pod=self.awscli_pod,
            file_dir=self.tmp_objs_dir,
            bucket_to_write=self.source_bucket.name,
            mcg_obj=self.mcg_obj,
            prefix=prefix,
            amount=amount,
        )

        if prefix:
            written_objs = [f"{prefix}/{obj}" for obj in written_objs]

        return written_objs

    def delete_recursively_from_source(self, prefix=""):
        """
        Delete all objects from a bucket recursively.

        Args:
            prefix(str): The prefix of the objects to delete
                - The default is an empty string - delete all objects

        Returns:
            list: A list of the deleted object keys
        """
        logger.info(f"Deleting all objects from {self.source_bucket.name}/{prefix}")

        bucket_path = f"s3://{self.source_bucket.name}"

        deleted_objs_keys = list_objects_from_bucket(
            self.awscli_pod,
            bucket_path,
            s3_obj=self.mcg_obj,
            prefix=f"{prefix}/" if prefix else None,
            recursive=True,
        )

        if prefix:
            deleted_objs_keys = [f"{prefix}/{obj}" for obj in deleted_objs_keys]
            bucket_path += f"/{prefix}/"

        s3cmd = craft_s3_command(f"rm {bucket_path} --recursive", self.mcg_obj)
        self.awscli_pod.exec_cmd_on_pod(s3cmd)

        return deleted_objs_keys

    def wait_for_sync(self, timeout=600, prefix=""):
        """
        Wait for the target bucket to sync with the source bucket.

        Args:
            timeout(int): The time to wait for the sync to complete - in seconds
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
            prefix=prefix,
        )
