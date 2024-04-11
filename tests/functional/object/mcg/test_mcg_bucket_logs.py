import logging

from ocs_ci.helpers.helpers import craft_s3_command
from ocs_ci.utility.utils import TimeoutSampler

from ocs_ci.framework.pytest_customization.marks import mcg, red_squad
from ocs_ci.framework.testlib import (
    MCGTest,
    runs_on_provider,
    tier1,
    tier2,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.resources.mcg_bucket_logs import MCGBucketLoggingHandler

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider  # Check with Filip
class TestMCGBucketLogs(MCGTest):
    """
    Test class for the MCG bucket logs feature
    """

    TIMEOUT = 1200

    @tier1
    def test_mcg_bucket_logs_ops(
        self, mcg_obj_session, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test MCG bucket logs - Put, Get and Delete operations:
        1. Create two MCG buckets using the default backingstore: test bucket and logs bucket
        2. Configure bucket-logging on the test-bucket with logs bucket set as the logs-bucket target
        3. Verify that bucket logging was set on the test bucket via GetBucketLogging
        4. Upload an object to the bucket
        5. Download the object from the bucket
        6. Delete the object from the bucket
        7. Wait for matching logs to be written to the logs bucket

        """
        bucket_logging_handler = MCGBucketLoggingHandler(
            mcg_obj_session, awscli_pod_session
        )
        # 1. Create two MCG buckets using the default backingstore
        data_bucket, logs_bucket = (bucket.name for bucket in bucket_factory(amount=2))

        # 2. Configure bucket-logging on the test-bucket with logs bucket set as the logs-bucket target
        response = bucket_logging_handler.put_bucket_logging(data_bucket, logs_bucket)
        assert (
            response.status_code == 200
        ), f"Failed to put bucket logging: {response.content}"

        # 3. Verify that bucket logging was set on the test bucket via GetBucketLogging
        response = bucket_logging_handler.get_bucket_logging(data_bucket)

        # TODO: look for a better proof of the bucket logging configuration
        assert (
            response.status_code == 200
        ), f"Failed to get bucket logging: {response.content}"

        # 4. Upload an object to the bucket
        written_obj = write_random_test_objects_to_bucket(
            awscli_pod_session,
            data_bucket,
            test_directory_setup.origin_dir,
            amount=1,
            mcg_obj=mcg_obj_session,
        )[0]
        written_obj_path = f"s3://{data_bucket}/{written_obj}"

        # 5. Download the object from the bucket
        sync_object_directory(
            awscli_pod_session,
            test_directory_setup.result_dir,
            written_obj_path,
            mcg_obj_session,
        )

        # 6. Delete the object from the bucket
        awscli_pod_session.exec_cmd_on_pod(
            craft_s3_command(f"rm {written_obj_path}", self.mcg_obj)
        )

        # 7. Wait for all the matching logs to be written to the logs bucket
        target_ops_set = {"PUT", "GET", "DELETE"}

        for logs_batch in TimeoutSampler(
            timeout=self.TIMEOUT,
            sleep=30,
            func=bucket_logging_handler.get_bucket_logs,
            logs_bucket=logs_bucket,
        ):
            logged_ops_on_obj = [
                log.operation for log in logs_batch if log.obj == written_obj
            ]
            if target_ops_set.issubset(logged_ops_on_obj):
                break

    @tier2
    def test_bucket_logs_toggle_off(
        self, mcg_obj_session, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test toggling off MCG bucket logging:
        1. Create two MCG buckets using the default backingstore: test bucket and logs bucket
        2. Configure bucket-logging on the test-bucket with logs bucket set as the logs-bucket target
        3. Upload an object to the bucket
        4. Toggle off the logging via DeleteBucketLogging and verify via GetBucketLogging
        5. Delete the object
        6. Check that only the PUT operation was logged after the timeout

        """
        bucket_logging_handler = MCGBucketLoggingHandler(
            mcg_obj_session, awscli_pod_session
        )
        # 1. Create two MCG buckets using the default backingstore
        data_bucket, logs_bucket = (bucket.name for bucket in bucket_factory(amount=2))

        # 2. Configure bucket-logging on the test-bucket with logs bucket set as the logs-bucket target
        bucket_logging_handler.put_bucket_logging(data_bucket, logs_bucket)

        # 3. Upload an object to the bucket
        written_obj = write_random_test_objects_to_bucket(
            awscli_pod_session,
            data_bucket,
            test_directory_setup.origin_dir,
            amount=1,
            mcg_obj=mcg_obj_session,
        )[0]

        # 4. Toggle off the logging via DeleteBucketLogging and verify via GetBucketLogging
        bucket_logging_handler.delete_bucket_logging(data_bucket)

        # TODO: correct the assertion
        assert (
            bucket_logging_handler.get_bucket_logging(data_bucket).status_code == 404
        ), "Failed to delete bucket logging"

        # 5. Check that only the PUT operation was logged after the timeout
        logged_ops_on_obj = []
        try:
            for logs_batch in TimeoutSampler(
                timeout=self.TIMEOUT,
                sleep=30,
                func=bucket_logging_handler.get_bucket_logs,
                logs_bucket=logs_bucket,
            ):
                logged_ops_on_obj = [
                    log.operation for log in logs_batch if log.obj == written_obj
                ]
                if "DELETE" in logged_ops_on_obj:
                    # TODO: find a better exception to raise here
                    raise AssertionError(
                        f"Unexpected DELETE operation logged: {logged_ops_on_obj}"
                    )
        except TimeoutError:
            if "PUT" not in logged_ops_on_obj:
                raise AssertionError(
                    f"PUT operation was not logged: {logged_ops_on_obj} within {self.TIMEOUT} seconds"
                )
            logger.info("Only the PUT operation was logged as expected")

    @tier2
    def test_multiple_bucket_logs_setups(
        self, mcg_obj_session, bucket_factory, awscli_pod_session, test_directory_setup
    ):
        """
        Test simultaneous bucket logging setups:
        1. Create multiple pairs of MCG buckets: one logs bucket for each test bucket on the default backingstore
        2. Configure bucket logging on each pair
        3. Verify that bucket-logging was set for each pair via GetBucketLogging
        4. For each pair: upload, download and delete an object

        """
        SETUPS_AMOUNT = 10
        bucket_logging_handler = MCGBucketLoggingHandler(
            mcg_obj_session, awscli_pod_session
        )

        bucket_to_logs_bucket_map = {}
        for _ in range(SETUPS_AMOUNT):
            # 1. Create multiple pairs of MCG buckets
            data_bucket, logs_bucket = (
                bucket.name for bucket in bucket_factory(amount=2)
            )
            bucket_to_logs_bucket_map[data_bucket] = logs_bucket

            # 2. Configure bucket logging on each pair
            bucket_logging_handler.put_bucket_logging(data_bucket, logs_bucket)

            # 3. Verify that bucket-logging was set for each pair via GetBucketLogging
            # TODO: look for a better proof of the bucket logging configuration
            assert (
                bucket_logging_handler.get_bucket_logging(data_bucket).status_code
                == 200
            ), "Failed to get bucket logging"

            # 4. For each pair: upload, download and delete an object
            written_obj = write_random_test_objects_to_bucket(
                awscli_pod_session,
                data_bucket,
                test_directory_setup.origin_dir,
                amount=1,
                mcg_obj=mcg_obj_session,
            )[0]
            written_obj_path = f"s3://{data_bucket}/{written_obj}"

            sync_object_directory(
                awscli_pod_session,
                test_directory_setup.result_dir,
                written_obj_path,
                mcg_obj_session,
            )

            awscli_pod_session.exec_cmd_on_pod(
                craft_s3_command(f"rm {written_obj_path}", self.mcg_obj)
            )
