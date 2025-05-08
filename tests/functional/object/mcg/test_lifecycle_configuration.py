from datetime import datetime, timedelta
import logging
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    runs_on_provider,
    mcg,
    skipif_noobaa_external_pgsql,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    change_versions_creation_date_in_noobaa_db,
    create_multipart_upload,
    expire_multipart_upload_in_noobaa_db,
    get_obj_versions,
    list_multipart_upload,
    list_objects_from_bucket,
    put_bucket_versioning_via_awscli,
    s3_delete_object,
    s3_list_object_versions,
    upload_obj_versions,
    upload_parts,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    AbortIncompleteMultipartUploadRule,
    ExpirationRule,
    LifecyclePolicy,
    NoncurrentVersionExpirationRule,
)
from ocs_ci.utility.utils import TimeoutSampler, exec_nb_db_query


logger = logging.getLogger(__name__)

PROP_SLEEP_TIME = 10
TIMEOUT_SLEEP_DURATION = 30
TIMEOUT_THRESHOLD = 600


@mcg
@red_squad
@runs_on_provider
@skipif_noobaa_external_pgsql
class TestLifecycleConfiguration(MCGTest):
    """
    Tests suite for lifecycle configurations on MCG

    """

    @pytest.fixture(scope="class", autouse=True)
    def reduce_expiration_interval(self, add_env_vars_to_noobaa_core_class):
        """
        Reduce the interval in which the lifecycle background worker is running

        Args:
            add_env_vars_to_noobaa_core_class (function): Factory fixture for adding environment variables
            to the noobaa-core statefulset

        """
        new_interval_in_miliseconds = 60 * 1000
        add_env_vars_to_noobaa_core_class(
            [
                (constants.LIFECYCLE_INTERVAL_PARAM, new_interval_in_miliseconds),
                (constants.LIFECYCLE_SCHED_MINUTES, 1),
            ]
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6541")
    def test_abort_incomplete_multipart_upload(
        self, mcg_obj, bucket_factory, awscli_pod, test_directory_setup
    ):
        """
        1. Create an MCG bucket
        2. Set lifecycle configuration to abort incomplete multipart uploads after 1 day
        3. Create a multipart upload for the bucket
        4. Upload a few parts
        5. Manually expire the multipart-upload
        6. Wait for the multipart-upload to expire
        7. Wait for the parts to get deleted at the noobaa-db
        """
        parts_amount = 5
        key = "test_obj_123"
        origin_dir = test_directory_setup.origin_dir
        res_dir = test_directory_setup.result_dir

        # 1. Create a bucket
        bucket = bucket_factory(interface="OC")[0].name

        # 2. Set lifecycle configuration
        lifecycle_policy = LifecyclePolicy(
            AbortIncompleteMultipartUploadRule(days_after_initiation=1)
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        # 3. Create a multipart upload
        upload_id = create_multipart_upload(mcg_obj, bucket, key)

        # 4. Upload a few parts
        awscli_pod.exec_cmd_on_pod(
            f'sh -c "dd if=/dev/urandom of={origin_dir}/{key} bs=1MB count={parts_amount}; '
            f'split -b 1m  {origin_dir}/{key} {res_dir}/part"'
        )
        parts = awscli_pod.exec_cmd_on_pod(f'sh -c "ls -1 {res_dir}"').split()
        upload_parts(
            mcg_obj,
            awscli_pod,
            bucket,
            key,
            res_dir,
            upload_id,
            parts,
        )

        # 5. Manually expire the parts and the multipart-upload
        expire_multipart_upload_in_noobaa_db(upload_id)

        # 6. Wait for the multipart-upload to expire
        for http_response in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=list_multipart_upload,
            s3_obj=mcg_obj,
            bucketname=bucket,
        ):
            if "Uploads" not in http_response or len(http_response["Uploads"]) == 0:
                break
            logger.warning(f"Upload has not expired yet: \n{http_response}")

        # 7. Wait for the parts to get deleted at the noobaa-db
        bucket_id = exec_nb_db_query(
            f"SELECT _id FROM buckets WHERE data->>'name' = '{bucket}'",
        )[0].strip()
        list_parts_md_query = (
            f"SELECT data FROM objectmultiparts WHERE data->>'bucket' = '{bucket_id}'"
        )
        for parts_md_list in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=exec_nb_db_query,
            query=list_parts_md_query,
        ):
            all_deleted = True
            for md_str in parts_md_list:
                if "deleted" not in md_str:
                    all_deleted = False
                    break
            if not all_deleted:
                logger.warning(
                    f"Some parts still don't appear deleted in the noobaa-db: {md_str}"
                )
            else:
                logger.info("Success: All the parts appear deleted in the noobaa-db:\n")
                break

    @pytest.mark.polarion_id("OCS-6559")
    def test_noncurrent_version_expiration(self, mcg_obj, bucket_factory, awscli_pod):
        """
        1. Create an MCG bucket with versioning enabled
        2. Set lifecycle configuration to delete non-current versions after 5 days and
        keep 7 newer non-current versions
        3. Upload versions
        4. Manually set the age of each version to be one day older than its successor
        5. Wait for versions to expire
        """
        key = "test_obj"
        older_versions_amount = 5
        newer_versions_amount = 7

        # 1. Create an MCG bucket with versioning enabled
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket)

        # 2. Set lifecycle configuration
        lifecycle_policy = LifecyclePolicy(
            NoncurrentVersionExpirationRule(
                non_current_days=older_versions_amount,
                newer_non_current_versions=newer_versions_amount,
            )
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        # 3. Upload versions
        # older versions + newer versions + the current version
        amount = 2 * older_versions_amount + 1
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            bucket,
            key,
            amount=amount,
        )

        # 4. Manually set the age of each version to be one day older than its successor
        uploaded_versions = get_obj_versions(mcg_obj, awscli_pod, bucket, key)
        version_ids = [version["VersionId"] for version in uploaded_versions]

        # Parse the timestamp from the first version
        mongodb_style_time = uploaded_versions[0]["LastModified"]
        iso_timestamp = mongodb_style_time.replace("Z", "+00:00")
        latest_version_creation_date = datetime.fromisoformat(iso_timestamp)

        for i, version_id in enumerate(version_ids):
            change_versions_creation_date_in_noobaa_db(
                bucket_name=bucket,
                object_key=key,
                version_ids=[version_id],
                new_creation_time=(
                    latest_version_creation_date - timedelta(days=i)
                ).timestamp(),
            )

        # 5. Wait for versions to expire
        # While older_versions_amount versions qualify for deletion due to
        # NoncurrentDays, the lifecycle policy should keep the NewerNoncurrentVersions
        # amount of versions.
        expected_remaining = set(
            version_ids[: newer_versions_amount + 1]
        )  # +1 for the current version

        for versions in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=get_obj_versions,
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod,
            bucket_name=bucket,
            obj_key=key,
        ):
            remaining = {v["VersionId"] for v in versions}

            # Expected end result
            if remaining == expected_remaining:
                logger.info("Only the expected versions remained")
                break

            # Newer versions were deleted
            elif not (expected_remaining <= remaining):
                raise UnexpectedBehaviour(
                    (
                        "Some versions were deleted when they shouldn't have!"
                        f"Versions that were deleted: {expected_remaining - remaining}"
                    )
                )

            # Some older versions are yet to be deleted
            else:
                logger.warning(
                    (
                        "Some older versions have not expired yet:\n"
                        f"Remaining: {remaining}\n"
                        f"Versions yet to expire: {remaining - expected_remaining}"
                    )
                )

    @tier1
    @pytest.mark.polarion_id("OCS-6802")
    def test_expired_object_delete_marker(self, mcg_obj, bucket_factory, awscli_pod):
        """
        1. Create an MCG bucket with versioning enabled
        2. Set onto the bucket a lifecycle configuration with ExpiredObjectDeleteMarker set to true
        3. Upload a few versions for the same object
        4. Delete the object, resulting in the creation of a delete marker
        5. Verify the creation of the delete marker
        6. Delete all the non-delete-marker versions by their VersionId
        7. Wait for the delete marker to get deleted
        8. Verify that the object no longer shows when listing the bucket
        """
        key = "test_obj"

        # 1. Create an MCG bucket with versioning enabled
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket)

        # 2. Set lifecycle configuration
        lifecycle_policy = LifecyclePolicy(
            ExpirationRule(expired_object_delete_marker=True)
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        # 3. Upload a few versions for the same object
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            bucket,
            key,
            amount=5,
        )

        # 4. Delete the object, resulting in the creation of a delete marker
        s3_delete_object(
            mcg_obj,
            bucket,
            key,
        )

        # 5. Verify the creation of the delete marker
        raw_versions = s3_list_object_versions(
            mcg_obj,
            bucket,
            key,
        )
        delete_markers = raw_versions.get("DeleteMarkers", [])
        assert (
            len(delete_markers) == 1 and delete_markers[0]["IsLatest"]
        ), "Object was deleted but delete marker was not created"

        # 6. Delete all the non-delete-marker versions by their VersionId
        version_ids = [
            v["VersionId"] for v in get_obj_versions(mcg_obj, awscli_pod, bucket, key)
        ]
        for v_id in version_ids:
            s3_delete_object(
                s3_obj=mcg_obj,
                bucketname=bucket,
                object_key=key,
                versionid=v_id,
            )

        # 7. Wait for the delete marker to get deleted
        for raw_versions in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=s3_list_object_versions,
            s3_obj=mcg_obj,
            bucketname=bucket,
            prefix=key,
        ):
            delete_markers = raw_versions.get("DeleteMarkers", [])
            if len(delete_markers) == 0:
                logger.info("Delete marker expired as expected")
                break
            else:
                logger.warning(
                    (
                        "Delete marker has not expired yet:\n"
                        f"Remaining: {delete_markers}\n"
                    )
                )

        # 8. Verify that the object no longer shows when listing the bucket
        objects_listed = list_objects_from_bucket(
            pod_obj=awscli_pod,
            target=bucket,
            s3_obj=mcg_obj,
        )
        assert len(objects_listed) == 0, "Object still shows when listing the bucket"

        versions_listed = s3_list_object_versions(
            mcg_obj,
            bucket,
        )
        assert key not in str(
            versions_listed
        ), "Object still shows when listing the bucket versions"
        logger.info("Object was deleted as expected")
