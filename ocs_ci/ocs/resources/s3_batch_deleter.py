import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Cap to prevent excessive memory usage
# this is a rough estimate to target a maximum of
# 1GB memory usage
MAX_OBJS_TO_KEEP_IN_MEMORY = 150000


class S3BatchDeleter:
    """
    This class offers two ways to clear all objects from an S3 bucket:

    1. Sequentially: Deletes objects in batches of 1000 sequentially.
    Use this for typical cases with manageable object counts.

    2. In parallel: Deletes objects in batches of 1000 using multiple threads.
    This method is designed for extreme cases where the bucket has hundreds of thousands
    of objects, and should only be used for scale and cleanup purposes.
    """

    MAX_BATCH_SIZE = 1000

    def __init__(self, s3_resource, bucket_name):
        self.s3_resource = s3_resource
        self.s3_client = s3_resource.meta.client
        self.bucket_name = bucket_name
        self.bucket = s3_resource.Bucket(bucket_name)

    def _delete_batch(self, objects_batch):
        """
        Delete a batch of objects from the S3 bucket.
        Args:
            objects_batch (list): List of dictionaries with object keys to delete.
        Returns:
            tuple: Number of deleted objects and a list of errors.
        """
        try:
            response = self.bucket.delete_objects(Delete={"Objects": objects_batch})
            num_deleted = len(response.get("Deleted", []))
            errors = response.get("Errors", [])
            logger.debug(f"Deleted batch of {num_deleted} objects")
            return num_deleted, errors
        except Exception as e:
            logger.error(f"Exception during batch deletion: {e}")
            return [], [{"Key": obj["Key"], "Error": str(e)} for obj in objects_batch]

    def _retry_failed(self, all_errors):
        if not all_errors:
            return
        logger.warning(f"{len(all_errors)} objects failed to delete, retrying once...")
        failed_keys = [e["Key"] for e in all_errors]
        retry_batches = [
            [{"Key": k} for k in failed_keys[i : i + self.MAX_BATCH_SIZE]]
            for i in range(0, len(failed_keys), self.MAX_BATCH_SIZE)
        ]

        final_errors = []
        for batch in retry_batches:
            _, errors = self._delete_batch(batch)
            final_errors.extend(errors)

        if final_errors:
            logger.error(f"Failed to delete {len(final_errors)} objects after retry")
            raise Exception(
                f"Deletion failed for {len(final_errors)} objects: {final_errors}"
            )

    def delete_sequentially(self):
        """
        Delete all objects from the S3 bucket in batches sequentially.

        This method is designed for normal use cases and should be used
        when the bucket has a manageable number of objects.
        """
        logger.info(f"Starting sequential deletion in bucket '{self.bucket_name}'")
        total_deleted = 0
        all_errors = []

        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name)

        for page in pages:
            objects = page.get("Contents", [])
            if not objects:
                continue

            obj_keys = [{"Key": obj["Key"]} for obj in objects]
            num_deleted, errors = self._delete_batch(obj_keys)
            total_deleted += num_deleted
            all_errors.extend(errors)

        logger.info(f"Deleted {total_deleted} objects from bucket '{self.bucket_name}'")
        self._retry_failed(all_errors)

    def delete_in_parallel(self):
        """
        Delete all objects from the S3 bucket in parallel using multiple threads.

        This method is designed for extreme cases where the bucket has
        hundreds of thousands of objects and should only be used for scale
        and cleanup purposes.

        This method uses a ThreadPoolExecutor to process batches of objects concurrently.
        It is designed to optimize deletion performance by leveraging multiple threads,
        while also managing memory usage to avoid excessive resource consumption.

        Raises:
            Exception: If any objects fail to delete after a retry attempt.
        """

        queued_obj_count = 0
        total_deleted = 0
        failed_deletions = []
        futures = []

        # Use 2 threads per CPU core to boost performance in I/O-bound S3 deletions,
        # but cap at 16 to prevent resource exhaustion on high-core systems.
        max_workers = min(multiprocessing.cpu_count() * 2, 16)
        logger.info(
            f"Starting threaded deletion in bucket '{self.bucket_name}' using a max of {max_workers} threads"
        )

        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page in pages:

                # Skip in case of no objects - rare but possible edge case
                page_contents = page.get("Contents", [])
                if not page_contents:
                    continue

                current_batch = [{"Key": obj["Key"]} for obj in page_contents]
                queued_obj_count += len(current_batch)
                futures.append(executor.submit(self._delete_batch, current_batch))

                if queued_obj_count >= MAX_OBJS_TO_KEEP_IN_MEMORY:
                    logger.info(
                        (
                            f"Queued {queued_obj_count} objects for deletion. "
                            "waiting for threads to finish..."
                        )
                    )

                    for future in as_completed(futures):
                        num_deleted, errors = future.result()
                        total_deleted += num_deleted
                        failed_deletions.extend(errors)
                        logger.info(
                            f"So far deleted {total_deleted} objects from bucket '{self.bucket_name}'"
                        )

                    queued_obj_count = 0
                    futures = []

            # Wait for any remaining threads to finish
            for future in as_completed(futures):
                num_deleted, errors = future.result()
                total_deleted += num_deleted
                failed_deletions.extend(errors)

        logger.info(f"Deleted {total_deleted} objects from bucket '{self.bucket_name}'")
        self._retry_failed(failed_deletions)
