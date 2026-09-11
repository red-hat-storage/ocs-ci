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
    skipif_noobaa_external_pgsql,
)
from ocs_ci.framework.testlib import MCGTest, skipif_ocs_version
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    restore_archived_object,
    s3_put_object,
    s3_get_object,
    s3_head_object,
    s3_delete_object,
    s3_list_objects_v2,
    expire_objects_restore,
    trigger_objs_transition,
    wait_for_objs_transition,
)
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    TransitionRule,
)
from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Timeout (s) to wait for a lifecycle policy to propagate and be readable back
PROP_TIMEOUT = 60


@mcg
@red_squad
@runs_on_provider
@skipif_ocs_version("<4.23")
@skipif_noobaa_external_pgsql
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
        LIFECYCLE_INTERVAL - reduces the lifecycle background worker wake interval from 8h to 2min
        LIFECYCLE_SCHEDULE_MIN - reduces the per-rule lifecycle schedule guard from 5min to 2min

        On endpoint:
        NSFS_GLACIER_ENABLED - enables Glacier storage class for S3 data path (PutObject, RestoreObject)

        """
        lifecycle_interval_in_ms = 2 * 60 * 1000
        add_env_vars_to_noobaa_core_class(
            [
                (constants.NSFS_GLACIER_ENABLED, "true"),
                (constants.ARCHIVE_TARGET_BUCKET_CHECK_ENABLED, "false"),
                (constants.RESTORE_WORKER_EMPTY_DELAY, 30 * 1000),
                (constants.LIFECYCLE_INTERVAL_PARAM, lifecycle_interval_in_ms),
                (constants.LIFECYCLE_SCHED_MINUTES, lifecycle_interval_in_ms),
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
        8. Restore the archived object and wait for the restore to complete
        9. Head the restored object - verify restored status with expiry date
        10. Get the restored object - verify data matches original
        11. Delete the archived object
        12. List the bucket - verify archived object is gone

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

        # 8. Restore the archived object and wait for the restore to complete
        logger.test_step("Restore the archived object and wait for completion")
        restore_header = restore_archived_object(
            obc, nsfs_obj, obc.bucket_name, archive_key, days=restore_days
        )

        # 9. Head the restored object - verify restored status with expiry date
        logger.test_step("Verify restored object has expiry date")
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

        # 10. Get the restored object - verify data matches original
        logger.test_step("Verify restored object data via GetObject")
        get_resp = s3_get_object(obc, obc.bucket_name, archive_key)
        body = get_resp["Body"].read().decode()
        logger.assertion(f"Restored data: expected={archive_data!r}, actual={body!r}")
        assert (
            body == archive_data
        ), f"Restored data mismatch: expected {archive_data!r}, got {body!r}"

        # 11. Delete the archived object
        logger.test_step("Delete the archived object")
        s3_delete_object(obc, obc.bucket_name, archive_key)

        # 12. List the bucket - verify archived object is gone
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

    @tier1
    @pytest.mark.polarion_id("OCS-8229")
    def test_lifecycle_transition_standard_to_archive(self, archive_bucket):
        """
        Test lifecycle transition of a standard object to DEEP_ARCHIVE

        1. Put a standard object to an archive-enabled bucket
        2. Set a lifecycle rule to transition objects to DEEP_ARCHIVE
        3. Read back the lifecycle configuration - verify the transition rule
        4. Wait for the lifecycle transition to complete
        5. Head the transitioned object - verify StorageClass is DEEP_ARCHIVE
        6. Get the transitioned object - expect InvalidObjectState
        7. Restore the transitioned object and wait for completion
        8. Get the restored object - verify data and a restore expiry date
        9. Expire the restore window by backdating the restore expiry
        10. Verify the object reverts to archive-only (restore is time-limited)

        """
        object_key = "test-transition-object"
        object_data = "lifecycle transition test data"
        deep_archive = "DEEP_ARCHIVE"
        transition_days = 1
        restore_days = 1

        obc, nsfs_obj = archive_bucket

        # 1. Put a standard object to the archive-enabled bucket
        logger.test_step("Put a standard object to the archive-enabled bucket")
        s3_put_object(obc, obc.bucket_name, object_key, object_data)
        # Confirm the object starts outside the archive storage class so that the
        # transition asserted later is genuine and not a spurious pass
        initial_storage_class = s3_head_object(obc, obc.bucket_name, object_key).get(
            "StorageClass"
        )
        logger.assertion(f"Initial StorageClass: {initial_storage_class!r}")
        assert (
            initial_storage_class != deep_archive
        ), f"Object should not start as {deep_archive}, got {initial_storage_class!r}"

        # 2. Set a lifecycle rule to transition objects to DEEP_ARCHIVE
        logger.test_step("Set a lifecycle rule to transition objects to DEEP_ARCHIVE")
        lifecycle_policy = LifecyclePolicy(
            TransitionRule(storage_class=deep_archive, days=transition_days)
        )
        obc.s3_client.put_bucket_lifecycle_configuration(
            Bucket=obc.bucket_name,
            LifecycleConfiguration=lifecycle_policy.as_dict(),
        )

        # 3. Read back the lifecycle configuration - verify the transition rule
        logger.test_step(
            "Verify the lifecycle configuration returns the transition rule"
        )
        # Poll the read-back until the rule propagates instead of a fixed sleep
        rules = []
        for lifecycle_config in TimeoutSampler(
            timeout=PROP_TIMEOUT,
            sleep=5,
            func=obc.s3_client.get_bucket_lifecycle_configuration,
            Bucket=obc.bucket_name,
        ):
            rules = lifecycle_config.get("Rules", [])
            if rules:
                break
        transitions = rules[0].get("Transitions", []) if rules else []
        logger.assertion(f"Returned lifecycle rules: {rules}")
        assert len(rules) == 1, f"Expected a single rule, got: {rules}"
        assert (
            len(transitions) == 1
        ), f"Expected a single transition, got: {transitions}"
        assert (
            transitions[0].get("StorageClass") == deep_archive
        ), f"Expected StorageClass={deep_archive}, got {transitions[0].get('StorageClass')}"
        assert (
            transitions[0].get("Days") == transition_days
        ), f"Expected Days={transition_days}, got {transitions[0].get('Days')}"
        assert (
            rules[0].get("Status") == "Enabled"
        ), f"Expected rule Status=Enabled, got {rules[0].get('Status')}"

        # 4. Wait for the lifecycle transition to complete
        logger.test_step("Trigger the transition and wait for it to complete")
        trigger_objs_transition(obc.bucket_name, [object_key], days=transition_days)
        wait_for_objs_transition(obc, obc.bucket_name, [object_key], deep_archive)

        # 5. Head the transitioned object - verify StorageClass is DEEP_ARCHIVE
        logger.test_step("Verify the transitioned object StorageClass is DEEP_ARCHIVE")
        head_resp = s3_head_object(obc, obc.bucket_name, object_key)
        logger.assertion(
            f"StorageClass after transition: expected={deep_archive}, "
            f"actual={head_resp.get('StorageClass')}"
        )
        assert (
            head_resp.get("StorageClass") == deep_archive
        ), f"Expected StorageClass={deep_archive}, got {head_resp.get('StorageClass')}"

        # 6. Get the transitioned object - expect InvalidObjectState
        logger.test_step(
            "Verify GetObject on the transitioned object raises InvalidObjectState"
        )
        with pytest.raises(boto3exception.ClientError, match="InvalidObjectState"):
            s3_get_object(obc, obc.bucket_name, object_key)

        # 7. Restore the transitioned object and wait for the restore to complete
        logger.test_step("Restore the transitioned object and wait for completion")
        restore_header = restore_archived_object(
            obc, nsfs_obj, obc.bucket_name, object_key, days=restore_days
        )

        # 8. Get the restored object - verify data and a restore expiry date
        logger.test_step("Verify the restored object is readable with an expiry date")
        body = s3_get_object(obc, obc.bucket_name, object_key)["Body"].read().decode()
        logger.assertion(f"Restored data: expected={object_data!r}, actual={body!r}")
        assert (
            body == object_data
        ), f"Restored data mismatch: expected {object_data!r}, got {body!r}"
        logger.assertion(f"Restore header while restored: {restore_header!r}")
        assert re.search(
            r'expiry-date="[^"]+"', restore_header
        ), f"Expected an expiry-date in the Restore header, got {restore_header!r}"

        # 9. Expire the restore window by backdating the restore expiry
        logger.test_step("Expire the restore window by backdating the restore expiry")
        expire_objects_restore(obc.bucket_name, [object_key])

        # 10. Verify the object reverts to archive-only (restore is time-limited)
        logger.test_step(
            "Verify the object reverts to archive-only after restore expiry"
        )
        for head_resp in TimeoutSampler(
            timeout=300,
            sleep=15,
            func=s3_head_object,
            s3_obj=obc,
            bucketname=obc.bucket_name,
            object_key=object_key,
        ):
            # The reclaimer clears restore_status, so the Restore header disappears
            if not head_resp.get("Restore"):
                logger.info("Restore window expired; object reverted to archive-only")
                break
        head_resp = s3_head_object(obc, obc.bucket_name, object_key)
        logger.assertion(
            f"After restore expiry: StorageClass={head_resp.get('StorageClass')!r}, "
            f"Restore={head_resp.get('Restore')!r}"
        )
        assert head_resp.get("StorageClass") == deep_archive, (
            f"Expected StorageClass={deep_archive} after restore expiry, "
            f"got {head_resp.get('StorageClass')!r}"
        )
        assert not head_resp.get(
            "Restore"
        ), f"Restore header should be gone after expiry, got {head_resp.get('Restore')!r}"
        with pytest.raises(boto3exception.ClientError, match="InvalidObjectState"):
            s3_get_object(obc, obc.bucket_name, object_key)
