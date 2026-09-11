import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    red_squad,
    mcg,
)

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.resources.objectbucket import MCGCLIBucket
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.prometheus import PrometheusAPI, wait_for_alert_firing

from ocs_ci.ocs.bucket_utils import (
    get_bucket_status_value,
    rm_object_recursive,
    write_random_test_objects_to_bucket,
)

logger = logging.getLogger(__name__)


def wait_for_num_objects(mcg_obj, bucket_name, expected_count, timeout=360):
    """
    Poll bucket status until the number of objects reaches the expected count.

    Args:
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        bucket_name (str): Name of the bucket to check
        expected_count (int): Expected number of objects
        timeout (int): Maximum time to wait in seconds

    """
    logger.info(f"Waiting for object count to reach {expected_count}")
    last_seen = None
    try:
        for num_objects in TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=get_bucket_status_value,
            mcg_obj=mcg_obj,
            bucket_name=bucket_name,
            key="Num Objects",
        ):
            last_seen = num_objects
            if int(num_objects) == expected_count:
                logger.info(f"Object count reached {num_objects}")
                break
    except TimeoutExpiredError:
        assert False, (
            f"Object count did not reach {expected_count} within {timeout}s, "
            f"last observed: {last_seen}"
        )


def wait_for_quota_status(
    mcg_obj, bucket_name, expected_statuses, timeout=360, sleep=30
):
    """
    Poll bucket status until QuotaStatus matches one of expected_statuses.

    Args:
        mcg_obj (obj): An object representing the current state of the MCG in the cluster
        bucket_name (str): Name of the bucket to check
        expected_statuses (str or tuple): Expected QuotaStatus value(s)
        timeout (int): Maximum time to wait in seconds
        sleep (int): Seconds between polls

    """
    if isinstance(expected_statuses, str):
        expected_statuses = (expected_statuses,)
    logger.info(f"Waiting for quota status to reach {expected_statuses}")
    last_seen = None
    try:
        for quota_status in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_bucket_status_value,
            mcg_obj=mcg_obj,
            bucket_name=bucket_name,
            key="QuotaStatus",
        ):
            last_seen = quota_status
            if quota_status in expected_statuses:
                logger.info(f"Quota status reached {quota_status}")
                return quota_status
    except TimeoutExpiredError:
        assert False, (
            f"Quota status did not reach {expected_statuses} "
            f"within {timeout}s, last observed: {last_seen}"
        )


class QuotaStatus:
    NOT_SET = "QUOTA_NOT_SET"
    OPTIMAL = "OPTIMAL"
    APPROACHING = "APPROACHING_QUOTA"
    EXCEEDING = "EXCEEDING_QUOTA"


@tier2
@mcg
@red_squad
@polarion_id("OCS-7465")
class TestOBCQuota:
    """
    Test OBC Quota feature using MCG CLI
    """

    @config.run_with_provider_context_if_available
    def test_noobaa_space_available_using_cli(
        self, mcg_obj, awscli_pod_session, bucket_factory, test_directory_setup
    ):
        """
        Test that 'bucket update' command with --max-size and --max-object parameters work as expected
        1. Create bucket and check the initial quota status
        2. Set the max quota size and verify that it works as expected
        3. Update the max-objects parameter and then
            a. Write number of objects almost reaching max-objects, verify correct quota status
            b. Write number of objects exactly equal to max-objects, verify correct quota status
            c. Try to write one object more, verify that the command fails as expected
        4. Update the max-objects parameter to a bigger number, write another object and verify that this succeeds
        Args:
            mcg_obj (obj): An object representing the current state of the MCG in the cluster
            awscli_pod_session (pod): A pod running the AWSCLI tools
            bucket_factory: Calling this fixture creates a new bucket(s)
            test_directory_setup: Fixture that sets up origin and result directories for the test
        """

        # 1. Create bucket and check original quota status
        bucket_name = bucket_factory(amount=1, interface="CLI")[0].name

        quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
        assert (
            quota_status == QuotaStatus.NOT_SET
        ), f"Original quota status is {quota_status}, expected {QuotaStatus.NOT_SET}"

        # 2. Set max quota size to 2GB and make sure that this worked as expected
        new_data_space = 2
        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-size={str(new_data_space)}Gi {bucket_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )

        space_avail_after_update = get_bucket_status_value(
            mcg_obj, bucket_name, "Data Space Avail"
        )
        space_avail_after_update = float(space_avail_after_update.split(" ")[0])

        logger.info(f"Space available after update {space_avail_after_update}")
        assert (
            float(new_data_space) == space_avail_after_update
        ), "There is mismatch in updated data size and provided data size"
        quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
        assert (
            quota_status == QuotaStatus.OPTIMAL
        ), f"Quota status after is {quota_status}, expected {QuotaStatus.OPTIMAL}"
        logger.info("Data size was updated successfully")

        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="ObjKey1-",
            mcg_obj=mcg_obj,
        )

        logger.info("Waiting for space available to decrease after writing 1 object")
        last_seen = None
        try:
            for space_avail_after_write in TimeoutSampler(
                timeout=360,
                sleep=30,
                func=get_bucket_status_value,
                mcg_obj=mcg_obj,
                bucket_name=bucket_name,
                key="Data Space Avail",
            ):
                space_avail_after_write = float(space_avail_after_write.split(" ")[0])
                last_seen = space_avail_after_write
                if space_avail_after_write < space_avail_after_update:
                    logger.info(
                        f"Space available decreased: {space_avail_after_update} -> {space_avail_after_write}"
                    )
                    break
        except TimeoutExpiredError:
            assert False, (
                f"Space available did not decrease below {space_avail_after_update} "
                f"within 360s, last observed: {last_seen}"
            )

        # 3. Update bucket with --max-objects quota and verify that it worked as expected
        max_objects = 10
        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-objects={max_objects} {bucket_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )
        # one file was already copied, so now available objects number should be (max_objects -1)
        num_objects_avail_after_update = get_bucket_status_value(
            mcg_obj, bucket_name, "Num Objects Avail"
        )
        assert (
            int(num_objects_avail_after_update) == max_objects - 1
        ), f"Number of available objects is {num_objects_avail_after_update}, expected {max_objects - 1}"

        # 3 a. copy (max-objects-2) files to the bucket
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=max_objects - 2,
            pattern="ObjKey2-",
            mcg_obj=mcg_obj,
        )

        wait_for_num_objects(mcg_obj, bucket_name, max_objects - 1)

        num_objects_avail_after_write = get_bucket_status_value(
            mcg_obj, bucket_name, "Num Objects Avail"
        )
        assert (
            int(num_objects_avail_after_write) == 1
        ), f"Number of available objects is {num_objects_avail_after_write}, expected 1"
        quota_status_after_write = get_bucket_status_value(
            mcg_obj, bucket_name, "QuotaStatus"
        )
        assert (
            quota_status_after_write == QuotaStatus.APPROACHING
        ), f"Quota status is {quota_status_after_write}, expected {QuotaStatus.APPROACHING}"

        # 3 b, Copy last file allowed with the existing quota
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="ObjKey3-",
            mcg_obj=mcg_obj,
        )

        wait_for_num_objects(mcg_obj, bucket_name, max_objects)

        num_objects_avail_after_write = get_bucket_status_value(
            mcg_obj, bucket_name, "Num Objects Avail"
        )
        assert (
            int(num_objects_avail_after_write) == 0
        ), f"Number of available objects is {num_objects_avail_after_write}, expected 0"
        quota_status_after_write = get_bucket_status_value(
            mcg_obj, bucket_name, "QuotaStatus"
        )
        assert (
            quota_status_after_write == QuotaStatus.EXCEEDING
        ), f"Quota status is {quota_status_after_write}, expected {QuotaStatus.EXCEEDING}"

        # 3 c. Try to copy one more file and exceed the max_objects limit
        try:
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                bucket_name,
                file_dir=test_directory_setup.origin_dir,
                amount=1,
                pattern="ObjKey4-",
                mcg_obj=mcg_obj,
            )
            # should not get here is the tests pass, exception is supposed to be thrown in prev. command
            assert False, "Writing succeeded after quota was exceeded"
        except CommandFailed as e:
            logger.info("Expected CommandFailed exception was caught")
            logger.info(f"Message: {e}")

        # 4. Update again bucket with bigger --max-objects quota and verify that it worked as expected
        # -- it is possible to write a file and quota status is 'Optimal' again
        max_objects_increased = 20
        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-objects={max_objects_increased} {bucket_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )

        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="ObjKey5-",
            mcg_obj=mcg_obj,
        )

        wait_for_quota_status(mcg_obj, bucket_name, QuotaStatus.OPTIMAL)

    @config.run_with_provider_context_if_available
    def test_mcg_obc_max_size_quota_lifecycle(
        self, mcg_obj, awscli_pod_session, bucket_factory, test_directory_setup
    ):
        """
        Test max-size quota lifecycle including decrease, increase and removal

        1. Create bucket and verify initial quota status is QUOTA_NOT_SET
        2. Set max-size quota to 2Gi and verify status transitions to OPTIMAL
        3. Write ~88% of quota (9 x 200MB), verify APPROACHING_QUOTA status
        4. Write beyond 100% (3 more x 200MB), verify EXCEEDING_QUOTA status
        5. Attempt one more write, verify it is blocked (CommandFailed)
        6. Attempt to decrease max-size below current usage, verify it's rejected
        7. Increase max-size just above current usage, verify APPROACHING_QUOTA
        8. Remove max-size quota (set to 0), verify writes succeed again

        Args:
            mcg_obj (obj): An object representing the current state of the MCG in the cluster
            awscli_pod_session (pod): A pod running the AWSCLI tools
            bucket_factory: Calling this fixture creates a new bucket(s)
            test_directory_setup: Setup for test directories on the awscli pod
        """
        max_size_gi = 2
        obj_size_mb = "200M"
        approaching_count = 9
        exceeding_count = 3

        # 1. Create bucket and verify initial quota status
        bucket_name = bucket_factory(amount=1, interface="CLI")[0].name
        quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
        assert (
            quota_status == QuotaStatus.NOT_SET
        ), f"Initial quota status is {quota_status}, expected {QuotaStatus.NOT_SET}"

        # 2. Set max-size quota and verify OPTIMAL status
        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-size={max_size_gi}Gi {bucket_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )
        quota_status = get_bucket_status_value(mcg_obj, bucket_name, "QuotaStatus")
        assert (
            quota_status == QuotaStatus.OPTIMAL
        ), f"Quota status after setting max-size is {quota_status}, expected {QuotaStatus.OPTIMAL}"
        logger.info(f"Max-size quota set to {max_size_gi}Gi, status is OPTIMAL")

        # 3. Write ~88% of quota (9 x 200MB = 1.8GB, ~88% of 2Gi)
        logger.info(
            f"Writing {approaching_count} objects of {obj_size_mb} to approach quota"
        )
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=approaching_count,
            pattern="SizeKey1-",
            bs=obj_size_mb,
            mcg_obj=mcg_obj,
        )

        wait_for_quota_status(mcg_obj, bucket_name, QuotaStatus.APPROACHING)

        # 4. Write beyond 100% (3 more x 200MB, total ~2.4GB exceeding 2Gi)
        logger.info(
            f"Writing {exceeding_count} more objects of {obj_size_mb} to exceed quota"
        )
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=exceeding_count,
            pattern="SizeKey2-",
            bs=obj_size_mb,
            mcg_obj=mcg_obj,
        )

        wait_for_quota_status(mcg_obj, bucket_name, QuotaStatus.EXCEEDING)

        # 5. Verify that writes are blocked when quota is exceeded
        logger.info("Attempting to write one more object, expecting failure")
        try:
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                bucket_name,
                file_dir=test_directory_setup.origin_dir,
                amount=1,
                pattern="SizeKey3-",
                bs=obj_size_mb,
                mcg_obj=mcg_obj,
            )
            assert False, "Writing succeeded after max-size quota was exceeded"
        except CommandFailed as e:
            logger.info("Expected CommandFailed exception was caught")
            logger.info(f"Message: {e}")

        # 6. Attempt to decrease max-size below current usage, verify it's rejected
        decreased_size_gi = 1
        logger.info(
            f"Attempting to decrease max-size quota to {decreased_size_gi}Gi "
            "(below current usage, expecting rejection)"
        )
        try:
            mcg_obj.exec_mcg_cmd(
                cmd=f"bucket update --max-size={decreased_size_gi}Gi {bucket_name}",
                namespace=config.ENV_DATA["cluster_namespace"],
                use_yes=True,
            )
            assert False, "Decreasing max-size below current usage should have failed"
        except CommandFailed as e:
            logger.info("Expected rejection when decreasing quota below current usage")
            logger.info(f"Message: {e}")

        # 7. Increase max-size to just above current usage, verify APPROACHING_QUOTA
        #    Current usage is ~2.4GB (12 x 200MB), set quota to 2.5Gi (~94% used)
        approaching_size_gi = 2.5
        logger.info(
            f"Increasing max-size quota to {approaching_size_gi}Gi "
            "(just above current usage)"
        )
        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-size={approaching_size_gi}Gi {bucket_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )

        wait_for_quota_status(mcg_obj, bucket_name, QuotaStatus.APPROACHING)

        # 8. Remove max-size quota (set to 0 = unlimited), verify writes succeed
        logger.info("Removing max-size quota (setting to 0)")
        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-size=0 {bucket_name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )

        wait_for_quota_status(mcg_obj, bucket_name, QuotaStatus.NOT_SET)

        logger.info("Writing object after quota removal, expecting success")
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="SizeKey4-",
            bs=obj_size_mb,
            mcg_obj=mcg_obj,
        )
        logger.info("Write succeeded after quota removal as expected")


@tier2
@mcg
@red_squad
class TestMCGQuotaAlerts:
    """
    Tests for MCG quota Prometheus alerts that fire when buckets
    approach their quota limits (80% threshold).
    """

    def _verify_alert_for_bucket(self, threading_lock, alert_name, bucket_name):
        """
        Wait for a Prometheus alert to fire and verify it has the correct bucket_name label.

        Args:
            threading_lock: Lock for Prometheus API
            alert_name (str): Name of the alert to wait for
            bucket_name (str): Expected bucket_name in alert labels

        Raises:
            AssertionError: If no matching alert is found
        """
        prometheus_api = PrometheusAPI(threading_lock=threading_lock)
        alerts = wait_for_alert_firing(
            api=prometheus_api,
            alert_name=alert_name,
            timeout=900,
        )

        matching_alert = next(
            (
                alert
                for alert in alerts
                if alert.get("labels", {}).get("bucket_name") == bucket_name
            ),
            None,
        )
        assert (
            matching_alert
        ), f"No '{alert_name}' alert found for bucket_name={bucket_name}"
        logger.info(
            f"Alert {alert_name} verified for bucket "
            f"{matching_alert['labels'].get('bucket_name')}"
        )

    @pytest.fixture(params=["objects", "size"])
    def quota_approaching_bucket(
        self, request, mcg_obj, awscli_pod_session, test_directory_setup
    ):
        """
        Create a bucket approaching quota threshold (objects or size).

        Parameterized fixture that creates either:
        - objects: bucket with max-objects=10 and 9 objects uploaded
        - size: bucket with 1Gi quota and ~900MB uploaded

        Both configurations trigger APPROACHING_QUOTA status.
        """
        quota_type = request.param

        if quota_type == "objects":
            bucket_name = create_unique_resource_name(
                resource_description="bucket", resource_type="objquota"
            )
            bucket = MCGCLIBucket(bucket_name, mcg=mcg_obj)
            logger.info(f"Created bucket {bucket_name}")

            max_objects = 10
            mcg_obj.exec_mcg_cmd(
                cmd=f"bucket update --max-objects={max_objects} {bucket_name}",
                namespace=config.ENV_DATA["cluster_namespace"],
                use_yes=True,
            )
            logger.info(f"Set max-objects={max_objects} on bucket {bucket_name}")
            upload_params = {"amount": max_objects - 1, "bs": "1M"}
            logger.info(
                f"Uploading {max_objects - 1} objects to bucket {bucket_name} "
                f"(max-objects={max_objects})"
            )
        else:  # size
            bucket_name = create_unique_resource_name(
                resource_description="bucket", resource_type="sizequota"
            )
            bucket = MCGCLIBucket(bucket_name, mcg=mcg_obj, quota="1Gi")
            logger.info(f"Created bucket {bucket_name} with 1Gi size quota")
            upload_params = {"amount": 1, "bs": "900M"}
            logger.info(f"Uploading ~900MB to bucket {bucket_name} (quota 1Gi)")

        def finalizer():
            try:
                rm_object_recursive(awscli_pod_session, bucket_name, mcg_obj)
            except CommandFailed:
                logger.warning(f"Cleanup of bucket {bucket_name} objects failed")
            bucket.delete()

        request.addfinalizer(finalizer)

        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=bucket_name,
            file_dir=test_directory_setup.origin_dir,
            mcg_obj=mcg_obj,
            **upload_params,
        )

        wait_for_quota_status(mcg_obj, bucket_name, QuotaStatus.APPROACHING)
        return bucket_name, quota_type

    def test_mcg_quota_approaching_alert(
        self, quota_approaching_bucket, threading_lock
    ):
        """
        Verify that NooBaaBucketReachingQuantityQuotaState or
        NooBaaBucketReachingSizeQuotaState Prometheus alert fires when
        bucket approaches its quota.

        Steps:
            1. Fixture creates bucket with quota and fills it to ~90%
            2. Wait for appropriate alert to fire
            3. Verify alert has the correct bucket_name label
        """
        bucket_name, quota_type = quota_approaching_bucket

        if quota_type == "objects":
            alert_name = constants.ALERT_BUCKETREACHINGQUOTASTATE
            logger.info(
                f"Bucket {bucket_name} is in APPROACHING_QUOTA mode "
                f"(object count), waiting for Prometheus alert"
            )
        else:  # size
            alert_name = constants.ALERT_BUCKETREACHINGSIZEQUOTASTATE
            logger.info(
                f"Bucket {bucket_name} is in APPROACHING_QUOTA mode "
                f"(size), waiting for Prometheus alert"
            )

        self._verify_alert_for_bucket(
            threading_lock,
            alert_name,
            bucket_name,
        )
