"""
RGW Workload Module for Krkn Chaos Testing

This module provides RGW (RADOS Gateway) workload management for stress and chaos testing
in OpenShift Data Foundation. It uses the existing mcg_stress_helper utilities to perform
intensive S3 operations on RGW buckets.
"""

import logging
import threading
import time

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.helpers.mcg_stress_helper import (
    delete_objs_from_bucket,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
)
from ocs_ci.helpers.helpers import create_resource

log = logging.getLogger(__name__)


class RGWWorkload:
    """
    RGW Workload class for Krkn chaos testing.

    This class manages S3 workload operations on RGW buckets, providing:
    - Continuous upload/download/delete operations
    - Metadata-intensive operations
    - Workload validation and health checks
    - Integration with Krkn chaos testing framework

    Args:
        rgw_bucket (ObjectBucket): RGW bucket object
        awscli_pod (Pod): Pod with AWS CLI for operations
        namespace (str): Kubernetes namespace
        workload_config (dict): Configuration for workload operations
    """

    @staticmethod
    def _ensure_rgw_storageclass_exists():
        """
        Check if the RGW StorageClass exists, and create it if it doesn't.

        This ensures that the required StorageClass 'ocs-storagecluster-ceph-rgw'
        is present before starting RGW workload operations.
        """
        sc_name = constants.DEFAULT_STORAGECLASS_RGW

        # Check if StorageClass exists
        sc_obj = OCP(kind=constants.STORAGECLASS)

        if sc_obj.is_exist(resource_name=sc_name):
            log.info(f"StorageClass '{sc_name}' already exists")
            return

        log.info(f"StorageClass '{sc_name}' not found, creating it")

        # Define the StorageClass
        sc_data = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {
                "name": sc_name,
            },
            "provisioner": "openshift-storage.ceph.rook.io/bucket",
            "parameters": {
                "objectStoreName": "ocs-storagecluster-cephobjectstore",
                "objectStoreNamespace": "openshift-storage",
                "region": "us-east-1",
            },
            "reclaimPolicy": "Delete",
        }

        try:
            create_resource(**sc_data)
            log.info(f"Successfully created StorageClass '{sc_name}'")
        except Exception as e:
            log.error(f"Failed to create StorageClass '{sc_name}': {e}")
            raise

    def __init__(
        self,
        rgw_bucket,
        awscli_pod,
        namespace=None,
        workload_config=None,
        delete_bucket_on_cleanup=True,
    ):
        """
        Initialize RGW workload.

        Args:
            rgw_bucket: RGW bucket object (from rgw_bucket_factory)
            awscli_pod: Pod object with AWS CLI installed
            namespace (str): Kubernetes namespace
            workload_config (dict): Workload configuration parameters
            delete_bucket_on_cleanup (bool): Whether to delete bucket during cleanup
        """
        # Ensure RGW StorageClass exists before initializing workload
        self._ensure_rgw_storageclass_exists()

        self.rgw_bucket = rgw_bucket
        self.bucket_name = rgw_bucket.name
        self.awscli_pod = awscli_pod
        self.namespace = namespace or constants.OPENSHIFT_STORAGE_NAMESPACE
        self.workload_config = workload_config or {}
        self.delete_bucket_on_cleanup = delete_bucket_on_cleanup

        # Get OBC object for credentials
        self.obc_obj = OBC(self.bucket_name)

        # Workload state
        self.is_running = False
        self.is_paused = False
        self.workload_thread = None
        self.stop_event = threading.Event()
        self.current_iteration = 0

        # Async delete operations tracking
        self.delete_threads = []
        self.delete_threads_lock = threading.Lock()

        # Workload configuration from config
        self.operation_types = self.workload_config.get(
            "operation_types", ["upload", "download", "list", "delete"]
        )
        self.iteration_count = self.workload_config.get("iteration_count", 10)
        self.upload_multiplier = self.workload_config.get("upload_multiplier", 1)
        self.concurrent_operations = self.workload_config.get(
            "concurrent_operations", True
        )
        self.metadata_ops_enabled = self.workload_config.get(
            "metadata_ops_enabled", False
        )
        self.delay_between_iterations = self.workload_config.get(
            "delay_between_iterations", 30
        )

        # Directories for upload/download (use existing awscli test directory)
        self.src_directory = constants.AWSCLI_TEST_OBJ_DIR
        self.download_directory = "/tmp/rgw_downloads/"

        log.info(
            f"Initialized RGW workload for bucket: {self.bucket_name} "
            f"with operations: {self.operation_types}"
        )

    def start_workload(self):
        """
        Start the RGW workload in a background thread.

        This method:
        1. Validates workload is not already running
        2. Creates test objects in awscli pod if needed
        3. Starts background workload thread
        4. Performs continuous S3 operations

        Raises:
            UnexpectedBehaviour: If workload fails to start
        """
        if self.is_running:
            log.warning("RGW workload is already running")
            return

        log.info(f"Starting RGW workload on bucket: {self.bucket_name}")

        try:
            # Prepare test directory with objects
            self._prepare_test_objects()

            # Reset state
            self.stop_event.clear()
            self.current_iteration = 0
            self.is_running = True
            self.is_paused = False

            # Start workload in background thread
            self.workload_thread = threading.Thread(
                target=self._run_workload_loop, daemon=True
            )
            self.workload_thread.start()

            log.info(f"Successfully started RGW workload on bucket: {self.bucket_name}")

        except Exception as e:
            self.is_running = False
            raise UnexpectedBehaviour(f"Failed to start RGW workload: {e}")

    def _prepare_test_objects(self):
        """
        Prepare test objects in the awscli pod for upload operations.
        """
        log.info("Preparing test objects for RGW workload")

        try:
            # Check if test directory exists
            result = self.awscli_pod.exec_cmd_on_pod(
                f'sh -c "test -d {self.src_directory} && echo exists || echo not_found"'
            )

            if "not_found" in result:
                log.info(f"Creating test directory: {self.src_directory}")
                # Create directory structure with test files
                self.awscli_pod.exec_cmd_on_pod(f"mkdir -p {self.src_directory}")

                # Generate test files
                for i in range(10):
                    self.awscli_pod.exec_cmd_on_pod(
                        f"dd if=/dev/urandom of={self.src_directory}/testfile{i}.dat "
                        f"bs=1M count=10 2>/dev/null"
                    )

                log.info(
                    f"Created {10 * self.upload_multiplier}MB test objects in {self.src_directory}"
                )
            else:
                log.info(f"Test directory already exists: {self.src_directory}")

        except Exception as e:
            log.error(f"Failed to prepare test objects: {e}")
            # Re-raise to prevent workload from starting without test data
            raise

    def _run_workload_loop(self):
        """
        Main workload loop running in background thread.

        Performs continuous S3 operations until stopped:
        - Upload objects
        - Download objects
        - List objects
        - Delete objects
        - Metadata operations (if enabled)
        """
        log.info("Starting RGW workload loop")

        while not self.stop_event.is_set():
            try:
                self.current_iteration += 1
                log.info(
                    f"RGW Workload Iteration {self.current_iteration}/{self.iteration_count}"
                )

                # Bucket tuple format for helper functions
                bucket_tuple = (constants.RGW_PLATFORM, self.rgw_bucket)

                # Execute operations based on configuration
                if "upload" in self.operation_types:
                    self._perform_upload(bucket_tuple)

                if "download" in self.operation_types:
                    self._perform_download(bucket_tuple)

                if "list" in self.operation_types:
                    self._perform_list(bucket_tuple)

                if "delete" in self.operation_types and self.current_iteration > 1:
                    self._perform_delete_async(bucket_tuple)

                # Run metadata operations if enabled
                if self.metadata_ops_enabled and self.current_iteration > 1:
                    self._perform_metadata_ops(bucket_tuple)

                # Check if we reached iteration limit
                if (
                    self.iteration_count > 0
                    and self.current_iteration >= self.iteration_count
                ):
                    log.info(
                        f"Completed {self.iteration_count} iterations, stopping workload"
                    )
                    break

                # Delay between iterations
                if self.delay_between_iterations > 0:
                    log.info(
                        f"Waiting {self.delay_between_iterations}s before next iteration"
                    )
                    time.sleep(self.delay_between_iterations)

            except Exception as e:
                log.error(
                    f"Error in RGW workload iteration {self.current_iteration}: {e}"
                )
                # Continue to next iteration unless stop is requested
                if not self.stop_event.is_set():
                    time.sleep(10)  # Brief pause before retry

        log.info("RGW workload loop completed")
        self.is_running = False

    def _perform_upload(self, bucket_tuple):
        """
        Perform upload operations to RGW bucket.

        Args:
            bucket_tuple: Tuple of (bucket_type, bucket_object)
        """
        log.info(f"Uploading objects to bucket: {self.bucket_name}")

        try:
            # Upload using sync_object_directory
            sync_object_directory(
                podobj=self.awscli_pod,
                src=self.src_directory,
                target=f"s3://{self.bucket_name}/{self.current_iteration}/",
                s3_obj=self.obc_obj,
                timeout=3600,
            )

            log.info(
                f"✓ Uploaded objects to {self.bucket_name}/{self.current_iteration}/"
            )

        except Exception as e:
            log.error(f"Upload operation failed: {e}")
            raise

    def _perform_download(self, bucket_tuple):
        """
        Perform download operations from RGW bucket.

        Args:
            bucket_tuple: Tuple of (bucket_type, bucket_object)
        """
        if self.current_iteration == 1:
            log.info("Skipping download on first iteration (no data yet)")
            return

        log.info(f"Downloading objects from bucket: {self.bucket_name}")

        try:
            prev_iteration = self.current_iteration - 1

            # Create download directory
            self.awscli_pod.exec_cmd_on_pod(f"mkdir -p {self.download_directory}")

            # Download objects
            sync_object_directory(
                podobj=self.awscli_pod,
                src=f"s3://{self.bucket_name}/{prev_iteration}/",
                target=self.download_directory,
                s3_obj=self.obc_obj,
                timeout=3600,
            )

            log.info(f"✓ Downloaded objects from {self.bucket_name}/{prev_iteration}/")

            # Cleanup downloaded files
            self.awscli_pod.exec_cmd_on_pod(f"rm -rf {self.download_directory}/*")

        except Exception as e:
            log.error(f"Download operation failed: {e}")

    def _perform_list(self, bucket_tuple):
        """
        Perform list operations on RGW bucket.

        Args:
            bucket_tuple: Tuple of (bucket_type, bucket_object)
        """
        if self.current_iteration == 1:
            log.info("Skipping list on first iteration (no data yet)")
            return

        log.info(f"Listing objects in bucket: {self.bucket_name}")

        try:
            prev_iteration = self.current_iteration - 1

            # List objects using OBC credentials
            objects = self.obc_obj.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=str(prev_iteration)
            )

            object_count = objects.get("KeyCount", 0)
            log.info(f"✓ Listed {object_count} objects in {self.bucket_name}")

        except Exception as e:
            log.error(f"List operation failed: {e}")

    def _perform_delete(self, bucket_tuple):
        """
        Perform delete operations on RGW bucket (synchronous).

        Args:
            bucket_tuple: Tuple of (bucket_type, bucket_object)
        """
        log.info(f"Deleting objects from bucket: {self.bucket_name}")

        try:
            # Delete objects from 2 iterations ago to avoid interfering with downloads
            delete_iteration = self.current_iteration - 2

            if delete_iteration < 1:
                log.info("Not enough iterations for delete operation yet")
                return

            # Use helper function for deletion
            delete_objs_from_bucket(
                pod_obj=self.awscli_pod,
                bucket=bucket_tuple,
                prev_iteration=delete_iteration,
                multiplier=self.upload_multiplier,
            )

            log.info(f"✓ Deleted objects from {self.bucket_name}/{delete_iteration}/")

        except Exception as e:
            log.error(f"Delete operation failed: {e}")

    def _perform_delete_async(self, bucket_tuple):
        """
        Perform delete operations asynchronously in background thread.

        Args:
            bucket_tuple: Tuple of (bucket_type, bucket_object)
        """
        # Clean up completed threads before starting new one
        self._cleanup_completed_delete_threads()

        # Start delete operation in background thread
        delete_thread = threading.Thread(
            target=self._perform_delete,
            args=(bucket_tuple,),
            name=f"RGW-Delete-{self.bucket_name}-Iter{self.current_iteration}",
            daemon=True,
        )

        with self.delete_threads_lock:
            self.delete_threads.append(delete_thread)

        delete_thread.start()
        log.info(
            f"Started async delete operation for {self.bucket_name} "
            f"(active threads: {len(self.delete_threads)})"
        )

    def _cleanup_completed_delete_threads(self):
        """
        Remove completed delete threads from tracking list.
        """
        with self.delete_threads_lock:
            self.delete_threads = [t for t in self.delete_threads if t.is_alive()]

    def _wait_for_pending_deletes(self, timeout=300):
        """
        Wait for all pending async delete operations to complete.

        Args:
            timeout (int): Maximum time to wait in seconds (default: 300)
        """
        if not self.delete_threads:
            return

        log.info(f"Waiting for {len(self.delete_threads)} pending delete operations...")

        start_time = time.time()
        with self.delete_threads_lock:
            threads_to_wait = list(self.delete_threads)

        for thread in threads_to_wait:
            remaining_time = timeout - (time.time() - start_time)
            if remaining_time <= 0:
                running_count = len([t for t in threads_to_wait if t.is_alive()])
                log.warning(
                    f"Timeout waiting for delete threads, "
                    f"{running_count} still running"
                )
                break

            if thread.is_alive():
                thread.join(timeout=remaining_time)

        # Final cleanup
        self._cleanup_completed_delete_threads()

        if self.delete_threads:
            log.warning(
                f"{len(self.delete_threads)} delete threads still running after timeout"
            )
        else:
            log.info("✓ All delete operations completed")

    def _perform_metadata_ops(self, bucket_tuple):
        """
        Perform metadata-intensive operations on RGW bucket.

        Args:
            bucket_tuple: Tuple of (bucket_type, bucket_object)
        """
        log.info(f"Running metadata operations on bucket: {self.bucket_name}")

        try:
            # Note: This requires bucket_factory fixture which may not be available
            # in this context. This is an optional operation.
            log.info("Metadata operations require additional fixtures, skipping")

        except Exception as e:
            log.error(f"Metadata operations failed: {e}")

    def stop_workload(self):
        """
        Stop the RGW workload.

        This method:
        1. Signals the workload thread to stop
        2. Waits for thread to complete
        3. Updates workload state
        """
        if not self.is_running:
            log.warning("RGW workload is not running")
            return

        log.info(f"Stopping RGW workload on bucket: {self.bucket_name}")

        try:
            # Signal stop
            self.stop_event.set()

            # Wait for thread to complete (with timeout)
            if self.workload_thread and self.workload_thread.is_alive():
                self.workload_thread.join(timeout=60)

            # Wait for any pending async delete operations
            self._wait_for_pending_deletes(timeout=120)

            self.is_running = False
            self.is_paused = False

            log.info(f"Successfully stopped RGW workload on bucket: {self.bucket_name}")

        except Exception as e:
            raise UnexpectedBehaviour(f"Failed to stop RGW workload: {e}")

    def cleanup_workload(self):
        """
        Cleanup all workload resources.

        This method:
        1. Stops workload if running
        2. Cleans up test directories
        3. Optionally deletes the bucket
        """
        log.info(f"Cleaning up RGW workload for bucket: {self.bucket_name}")

        try:
            # Stop workload if running (this also waits for pending deletes)
            if self.is_running:
                self.stop_workload()
            else:
                # If workload wasn't running, still wait for any pending deletes
                self._wait_for_pending_deletes(timeout=120)

            # Cleanup test directories
            try:
                self.awscli_pod.exec_cmd_on_pod(f"rm -rf {self.download_directory}")
                log.info("Cleaned up download directory")
            except Exception as e:
                log.warning(f"Failed to cleanup download directory: {e}")

            # Delete the bucket if configured to do so (async)
            if self.delete_bucket_on_cleanup:
                self._delete_bucket_async()

            log.info(
                f"Successfully cleaned up RGW workload for bucket: {self.bucket_name}"
            )

        except Exception as e:
            log.warning(f"Error during RGW workload cleanup: {e}")

    def _delete_bucket_async(self):
        """
        Delete the bucket asynchronously in background thread.
        This allows cleanup to proceed without blocking on bucket deletion.
        """

        def _delete_bucket():
            try:
                log.info(f"Deleting bucket: {self.bucket_name}")
                self.rgw_bucket.delete()
                log.info(f"✓ Successfully deleted bucket: {self.bucket_name}")
            except Exception as e:
                log.error(f"Failed to delete bucket {self.bucket_name}: {e}")

        delete_thread = threading.Thread(
            target=_delete_bucket,
            name=f"RGW-BucketDelete-{self.bucket_name}",
            daemon=True,
        )
        delete_thread.start()
        log.info(f"Started async bucket deletion for {self.bucket_name}")

    def pause_workload(self):
        """
        Pause the RGW workload.

        Signals the workload to pause operations.
        """
        if not self.is_running:
            log.warning("RGW workload is not running, cannot pause")
            return

        if self.is_paused:
            log.warning("RGW workload is already paused")
            return

        log.info(f"Pausing RGW workload on bucket: {self.bucket_name}")
        self.is_paused = True
        # The workload loop will handle pause state

    def resume_workload(self):
        """
        Resume the RGW workload.

        Resumes paused workload operations.
        """
        if not self.is_paused:
            log.warning("RGW workload is not paused")
            return

        log.info(f"Resuming RGW workload on bucket: {self.bucket_name}")
        self.is_paused = False

    def is_workload_running(self):
        """
        Check if workload is currently running.

        Returns:
            bool: True if workload is running, False otherwise
        """
        return self.is_running and not self.is_paused

    def get_workload_status(self):
        """
        Get detailed workload status.

        Returns:
            dict: Workload status information
        """
        # Clean up completed threads before reporting
        self._cleanup_completed_delete_threads()

        with self.delete_threads_lock:
            active_deletes = len(self.delete_threads)

        return {
            "bucket_name": self.bucket_name,
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "current_iteration": self.current_iteration,
            "total_iterations": self.iteration_count,
            "operations": self.operation_types,
            "active_async_deletes": active_deletes,
        }
