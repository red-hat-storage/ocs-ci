import logging
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import botocore.exceptions as boto3exception
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest, skipif_ocs_version
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    nsfs_simulate_archive_restore,
    s3_put_object,
    s3_get_object,
    s3_head_object,
    s3_delete_object,
    s3_list_objects_v2,
    s3_restore_object,
)
from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@skipif_ocs_version("<4.23")
class TestDeepArchive(MCGTest):
    """
    Tests for the MCG S3 Deep Archive feature (RHSTOR-8823)

    """

    @pytest.fixture(scope="class", autouse=True)
    def enable_archive_config(
        self,
        add_env_vars_to_noobaa_core_class,
        add_env_vars_to_noobaa_endpoint_class,
    ):
        """
        Configure NooBaa for archive testing

        On core:
        NSFS_GLACIER_ENABLED - enables Glacier storage class in RestoreWorker
        ARCHIVE_TARGET_BUCKET_CHECK_ENABLED - bypasses IBM Deep Archive validation on the archive NSS
        RESTORE_WORKER_EMPTY_DELAY - reduces the RestoreWorker polling interval from 15min to 30s

        On endpoint:
        NSFS_GLACIER_ENABLED - enables Glacier storage class for S3 data path (PutObject, RestoreObject)

        """
        add_env_vars_to_noobaa_core_class(
            [
                (constants.NSFS_GLACIER_ENABLED, "true"),
                (constants.ARCHIVE_TARGET_BUCKET_CHECK_ENABLED, "false"),
                (constants.RESTORE_WORKER_EMPTY_DELAY, 30 * 1000),
            ]
        )
        add_env_vars_to_noobaa_endpoint_class(
            [
                (constants.NSFS_GLACIER_ENABLED, "true"),
            ]
        )

    @pytest.fixture()
    def archive_bucket(self, nsfs_bucket_factory, bucket_factory):
        """
        Create an archive OBC backed by an NSFS archive target.

        Returns:
            tuple: (OBC, NSFS) - the archive bucket and its NSFS backend

        """
        nsfs_obj = NSFS()
        nsfs_bucket_factory(nsfs_obj)
        logger.info(
            f"NSFS archive target: bucket={nsfs_obj.bucket_name}, "
            f"path={nsfs_obj.mounted_bucket_path}"
        )

        bucket = bucket_factory(
            1,
            "OC",
            bucketclass={
                "interface": "OC",
                "archive_nss_dict": {
                    constants.ARCHIVE_PLATFORM: [
                        (1, nsfs_obj.bucket_name, nsfs_obj.account_name)
                    ],
                },
            },
        )[0]
        logger.info(f"Archive OBC bound: {bucket.name}")

        return OBC(bucket.name), nsfs_obj

    @tier1
    @pytest.mark.polarion_id("OCS-8226")
    def test_deep_archive_write_restore_delete(self, archive_bucket):
        """
        Test direct archive write, restore, and delete

        1. Put object with DEEP_ARCHIVE storage class
        2. Put a standard object to the bucket
        3. Head the archived object - verify StorageClass is DEEP_ARCHIVE
        4. Head the standard object - verify no archive storage class
        5. List the bucket - verify both objects with correct storage classes
        6. Get the non-restored archived object - expect InvalidObjectState
        7. Get the standard object - expect success with matching data
        8. Restore the archived object - verify 202 Accepted
        9. Head the object while restore is in progress - verify ongoing
        10. Simulate restore completion via NSFS xattrs
        11. Wait for restore to complete
        12. Head the restored object - verify restored status with expiry date
        13. Get the restored object - verify data matches original
        14. Delete the archived object
        15. List the bucket - verify archived object is gone

        """
        archive_key = "test-archive-object"
        archive_data = "deep archive write restore delete test data"
        standard_key = "test-standard-object"
        standard_data = "standard object test data"
        deep_archive = "DEEP_ARCHIVE"
        restore_days = 1

        obc, nsfs_obj = archive_bucket

        # 1. Put object with DEEP_ARCHIVE storage class
        logger.test_step("Put object with DEEP_ARCHIVE storage class")
        s3_put_object(
            obc,
            obc.bucket_name,
            archive_key,
            archive_data,
            storage_class=deep_archive,
        )

        # 2. Put a standard object to the bucket
        logger.test_step("Put a standard object to the bucket")
        s3_put_object(obc, obc.bucket_name, standard_key, standard_data)

        # 3. Head the archived object - verify StorageClass
        logger.test_step("Verify archived object StorageClass is DEEP_ARCHIVE")
        head_resp = s3_head_object(obc, obc.bucket_name, archive_key)
        logger.assertion(
            f"StorageClass: expected={deep_archive}, "
            f"actual={head_resp.get('StorageClass')}"
        )
        assert (
            head_resp.get("StorageClass") == deep_archive
        ), f"Expected StorageClass={deep_archive}, got {head_resp.get('StorageClass')}"

        # 4. Head the standard object - verify no archive storage class
        logger.test_step("Verify standard object has no archive storage class")
        head_resp = s3_head_object(obc, obc.bucket_name, standard_key)
        std_storage_class = head_resp.get("StorageClass")
        logger.assertion(f"Standard object StorageClass: {std_storage_class!r}")
        assert (
            std_storage_class != deep_archive
        ), f"Standard object should not have {deep_archive} storage class"

        # 5. List the bucket - verify both objects with correct storage classes
        logger.test_step("Verify both objects listed with correct storage classes")
        list_resp = s3_list_objects_v2(obc, obc.bucket_name)
        contents = {obj["Key"]: obj for obj in list_resp.get("Contents", [])}
        logger.assertion(
            f"Listed keys: {list(contents.keys())}, "
            f"archive class={contents.get(archive_key, {}).get('StorageClass')}, "
            f"standard class={contents.get(standard_key, {}).get('StorageClass')}"
        )
        assert (
            archive_key in contents
        ), f"{archive_key} not found in listing: {list(contents.keys())}"
        assert (
            standard_key in contents
        ), f"{standard_key} not found in listing: {list(contents.keys())}"
        assert contents[archive_key].get("StorageClass") == deep_archive, (
            f"Expected {deep_archive} for archived object, "
            f"got {contents[archive_key].get('StorageClass')}"
        )

        # 6. Get the non-restored archived object - expect InvalidObjectState
        logger.test_step(
            "Verify GetObject on archived object raises InvalidObjectState"
        )
        with pytest.raises(boto3exception.ClientError, match="InvalidObjectState"):
            s3_get_object(obc, obc.bucket_name, archive_key)

        # 7. Get the standard object - expect success with matching data
        logger.test_step("Verify GetObject on standard object succeeds")
        get_resp = s3_get_object(obc, obc.bucket_name, standard_key)
        body = get_resp["Body"].read().decode()
        logger.assertion(
            f"Standard object data: expected={standard_data!r}, actual={body!r}"
        )
        assert (
            body == standard_data
        ), f"Standard object data mismatch: expected {standard_data!r}, got {body!r}"

        # 8. Restore the archived object - verify 202 Accepted
        logger.test_step("Initiate restore and verify 202 Accepted")
        restore_resp = s3_restore_object(
            obc, obc.bucket_name, archive_key, days=restore_days
        )
        status_code = restore_resp["ResponseMetadata"]["HTTPStatusCode"]
        logger.assertion(f"Restore status code: expected=202, actual={status_code}")
        assert status_code == 202, f"Expected 202 Accepted, got {status_code}"

        # 9. Head the object while restore is in progress - verify ongoing
        logger.test_step("Verify restore is ongoing")
        for head_resp in TimeoutSampler(
            timeout=60,
            sleep=5,
            func=s3_head_object,
            s3_obj=obc,
            bucketname=obc.bucket_name,
            object_key=archive_key,
        ):
            restore_header = head_resp.get("Restore", "")
            if 'ongoing-request="true"' in restore_header:
                logger.assertion(f"Restore header: {restore_header!r}")
                break

        # 10. Simulate restore completion via NSFS xattrs
        logger.test_step("Simulate restore completion via NSFS xattrs")
        nsfs_simulate_archive_restore(nsfs_obj, archive_key, restore_days)

        # 11. Wait for RestoreWorker to complete the restore
        logger.test_step("Wait for RestoreWorker to complete the restore")
        for head_resp in TimeoutSampler(
            timeout=600,
            sleep=20,
            func=s3_head_object,
            s3_obj=obc,
            bucketname=obc.bucket_name,
            object_key=archive_key,
        ):
            restore_header = head_resp.get("Restore", "")
            logger.debug(f"Restore header: {restore_header!r}")
            if 'ongoing-request="false"' in restore_header:
                logger.info("Restore completed successfully")
                break

        # 12. Head the restored object - verify restored status with expiry date
        logger.test_step("Verify restored object has expiry date")
        head_resp = s3_head_object(obc, obc.bucket_name, archive_key)
        restore_header = head_resp.get("Restore", "")
        logger.assertion(f"Restore header after completion: {restore_header!r}")
        assert (
            'ongoing-request="false"' in restore_header
        ), f"Expected completed restore, got: {restore_header!r}"
        expiry_match = re.search(r'expiry-date="([^"]+)"', restore_header)
        assert (
            expiry_match
        ), f"Expected expiry-date in restore header, got: {restore_header!r}"
        expiry_dt = parsedate_to_datetime(expiry_match.group(1))
        now = datetime.now(timezone.utc)
        logger.assertion(f"Expiry date: {expiry_dt.isoformat()}")
        assert now < expiry_dt <= now + timedelta(days=restore_days + 1), (
            f"Expected expiry within {restore_days}+1 days, "
            f"got {expiry_dt.isoformat()}"
        )

        # 13. Get the restored object - verify data matches original
        logger.test_step("Verify restored object data via GetObject")
        get_resp = s3_get_object(obc, obc.bucket_name, archive_key)
        body = get_resp["Body"].read().decode()
        logger.assertion(f"Restored data: expected={archive_data!r}, actual={body!r}")
        assert (
            body == archive_data
        ), f"Restored data mismatch: expected {archive_data!r}, got {body!r}"

        # 14. Delete the archived object
        logger.test_step("Delete the archived object")
        s3_delete_object(obc, obc.bucket_name, archive_key)

        # 15. List the bucket - verify archived object is gone
        logger.test_step("Verify archived object is gone from listing")
        list_resp = s3_list_objects_v2(obc, obc.bucket_name)
        listed_keys = [obj["Key"] for obj in list_resp.get("Contents", [])]
        logger.assertion(
            f"Listing after delete: keys={listed_keys}, "
            f"archive_present={archive_key in listed_keys}, "
            f"standard_present={standard_key in listed_keys}"
        )
        assert (
            archive_key not in listed_keys
        ), f"{archive_key} still present after deletion: {listed_keys}"
        assert (
            standard_key in listed_keys
        ), f"{standard_key} should still be present: {listed_keys}"
