import json
import logging
from datetime import datetime, timedelta
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    mcg,
    magenta_squad,
    runs_on_provider,
    system_test,
    tier1,
)
from ocs_ci.framework.testlib import E2ETest, skipif_ocs_version
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.resources.pod import wait_for_noobaa_pods_running
from ocs_ci.utility.retry import retry
from ocs_ci.helpers.e2e_helpers import (
    assert_mcg_feature_verification_bucket_setup,
    cleanup_all_test_bucket_objects,
    perform_noobaa_db_backup_recovery_using_cli,
    setup_mcg_feature_verification_buckets,
    shutdown_primary_noobaa_db_node,
    shutdown_secondary_noobaa_db_node,
    start_primary_noobaa_db_node,
    start_secondary_noobaa_db_node,
    stop_mcg_background_features,
    verify_mcg_features_after_db_recovery,
    verify_noncurrent_versions_expired,
    verify_noncurrent_versions_and_delete_marker_expired,
    verify_unidirectional_replication,
    verify_bidirectional_replication,
    verify_deletion_sync_between_replication_buckets,
    verify_multipart_upload_aborted_and_cleaned_up,
)
from ocs_ci.ocs.bucket_utils import (
    abort_multipart,
    change_versions_creation_date_in_noobaa_db,
    compare_bucket_object_list,
    create_multipart_upload,
    expire_objects_in_bucket,
    get_obj_versions,
    get_replication_policy,
    list_multipart_upload,
    patch_replication_policy_to_bucket,
    s3_delete_object,
    s3_list_object_versions,
    update_replication_policy,
    upload_obj_versions,
    wait_for_object_count_in_bucket,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.ocp import get_all_resource_of_kind_containing_string
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    AbortIncompleteMultipartUploadRule,
    ExpirationRule,
    ExpiredObjectDeleteMarkerRule,
    LifecycleFilter,
    LifecyclePolicy,
    NoncurrentVersionExpirationRule,
)

logger = logging.getLogger(__name__)

PROP_SLEEP_TIME = 10
VERSION_EXPIRATION_TIMEOUT = 600
VERSION_EXPIRATION_SLEEP = 30
BG_BUCKET_AMOUNT = 5
BG_OBJECT_AMOUNT = 5
BG_SKIP_FEATURES = ["nsfs", "rgw kafka", "caching"]


@mcg
@magenta_squad
@system_test
@ignore_leftovers
@runs_on_provider
@skipif_ocs_version("<4.9")
class TestMCGNoobaaDBHAAndFeatureVerification(E2ETest):
    """
    End-to-end MCG verification of NooBaa CNPG DB HA, backup/recovery, rebuild,
    and impacted features (replication, versioning, expiration).

    """

    @pytest.fixture()
    def feature_verification_buckets(
        self,
        bucket_factory,
        mcg_obj,
        reduce_expiration_interval,
    ):
        return setup_mcg_feature_verification_buckets(
            bucket_factory=bucket_factory,
            mcg_obj=mcg_obj,
            reduce_expiration_interval=reduce_expiration_interval,
        )

    @tier1
    def test_mcg_noobaa_db_ha_and_feature_verification(
        self,
        setup_mcg_bg_features,
        feature_verification_buckets,
        bucket_factory,
        mcg_obj,
        mcg_obj_session,
        bucket_factory_session,
        awscli_pod_session,
        test_directory_setup,
        noobaa_db_recovery_patch,
        reduce_expiration_interval,
        validate_noobaa_rebuild_system,
        validate_mcg_bg_features,
    ):
        """
        1. Setup feature cases in the background and run I/O in the background
        2. Create namespace replication buckets (uni-directional, no deletion sync),
           expiration bucket, and versioning bucket
        3. Upload to expiration bucket, configure 1-day rule, expire objects manually
        4. Upload an object to the replication source bucket
        5. Upload a version of an object to the versioning bucket
        6. Perform complete NooBaa DB backup and recovery procedure
        7. Verify replication, expiration, and object versions after recovery
        8. Perform a complete NooBaa rebuild
        9. Recreate verification buckets after rebuild
        10. Configure versioning bucket with NoncurrentVersionExpiration rule
        11. Upload 3 versions of an object and expire non-current versions
        12. Upload an object to the replication source bucket
        13. Start a multipart upload on the expiration bucket
        14. Shutdown the primary NooBaa DB node
        15. Verify all object versions except the current version are expired
        16. Verify uni-directional replication works
        17. Configure AbortIncompleteMultipartUpload rule on expiration bucket
        18. Upload 3 more versions of the same object
        19. Enable bi-directional replication and upload an object
        20. Start the primary NooBaa DB node
        21. Abort the multipart upload on the expiration bucket
        22. Verify bi-directional replication works between the buckets
        23. Configure ExpiredObjectDeleteMarker on versioning bucket and delete object
        24. Enable deletion sync on replication buckets and delete an object
        25. Shutdown the secondary NooBaa DB node
        26. Verify non-current versions and delete marker are expired
        27. Verify deletion sync works while secondary NooBaa DB node is shut down
        28. Verify multipart upload is aborted and cleaned up while secondary DB node is down
        29. Start the secondary NooBaa DB node
        30. Verify background feature I/O and system health
        31. Delete all objects from all test buckets

        """
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=BG_BUCKET_AMOUNT,
            object_amount=BG_OBJECT_AMOUNT,
            is_disruptive=True,
            skip_any_features=BG_SKIP_FEATURES,
        )
        logger.info(
            "Step 1 complete: MCG background features running with background I/O"
        )

        source_bucket = feature_verification_buckets["source_bucket"]
        target_bucket = feature_verification_buckets["target_bucket"]
        expiration_bucket = feature_verification_buckets["expiration_bucket"]
        versioning_bucket = feature_verification_buckets["versioning_bucket"]

        assert_mcg_feature_verification_bucket_setup(
            feature_verification_buckets, mcg_obj
        )
        logger.info("Step 2 complete")

        expiration_prefix = "to_expire"
        logger.info("Uploading object to expiration bucket %s", expiration_bucket.name)
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=expiration_bucket.name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            prefix=expiration_prefix,
            mcg_obj=mcg_obj,
        )
        uploaded_objects = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {test_directory_setup.origin_dir}"
        ).split(" ")

        lifecycle_policy = LifecyclePolicy(
            ExpirationRule(
                days=1, filter=LifecycleFilter(prefix=expiration_prefix)
            )
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=expiration_bucket.name,
            LifecycleConfiguration=lifecycle_policy.as_dict(),
        )
        sleep(PROP_SLEEP_TIME)

        expire_objects_in_bucket(
            expiration_bucket.name,
            uploaded_objects,
            prefix=expiration_prefix,
        )
        assert wait_for_object_count_in_bucket(
            io_pod=awscli_pod_session,
            expected_count=0,
            bucket_name=expiration_bucket.name,
            prefix=expiration_prefix,
            s3_obj=mcg_obj,
            timeout=600,
            sleep=30,
        ), "Objects were not expired in time!"
        logger.info("Step 3 complete: expiration verified on %s", expiration_bucket.name)

        logger.info(
            "Uploading object to replication source bucket %s", source_bucket.name
        )
        written_replication_objects = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=source_bucket.name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="replication-",
            mcg_obj=mcg_obj,
        )
        assert {written_replication_objects[0]} == {
            obj.key
            for obj in mcg_obj.s3_list_all_objects_in_bucket(source_bucket.name)
        }
        logger.info("Step 4 complete: object uploaded to %s", source_bucket.name)

        versioning_object_key = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=versioning_bucket.name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="versioned-",
            mcg_obj=mcg_obj,
        )[0]
        upload_obj_versions(
            mcg_obj,
            awscli_pod_session,
            versioning_bucket.name,
            versioning_object_key,
            amount=1,
        )
        assert len(
            get_obj_versions(
                mcg_obj,
                awscli_pod_session,
                versioning_bucket.name,
                versioning_object_key,
            )
        ) >= 2
        logger.info(
            "Step 5 complete: object version uploaded to %s/%s",
            versioning_bucket.name,
            versioning_object_key,
        )

        stop_mcg_background_features(feature_setup_map)

        if not get_all_resource_of_kind_containing_string(
            "noobaa-db-pg-cluster", "Cluster"
        ):
            pytest.skip("CNPG NooBaa DB cluster not found; skipping DB backup/recovery")

        backup_name = perform_noobaa_db_backup_recovery_using_cli(
            mcg_obj=mcg_obj,
            awscli_pod_session=awscli_pod_session,
            test_directory_setup=test_directory_setup,
            noobaa_db_recovery_patch=noobaa_db_recovery_patch,
            buckets_for_health=[
                source_bucket,
                target_bucket,
                expiration_bucket,
                versioning_bucket,
            ],
            buckets_with_local_objects=[
                (source_bucket, test_directory_setup.origin_dir),
            ],
        )

        verify_mcg_features_after_db_recovery(
            mcg_obj=mcg_obj,
            awscli_pod_session=awscli_pod_session,
            source_bucket=source_bucket,
            target_bucket=target_bucket,
            expiration_bucket=expiration_bucket,
            versioning_bucket=versioning_bucket,
            replication_object_keys=written_replication_objects,
            versioning_object_key=versioning_object_key,
            test_directory_setup=test_directory_setup,
            expiration_prefix=expiration_prefix,
        )
        logger.info(
            "Step 6 complete: NooBaa DB backup/recovery verified using %s",
            backup_name,
        )
        logger.info(
            "Step 7 complete: replication, expiration, and versioning verified "
            "after DB recovery"
        )

        logger.info("Starting complete NooBaa rebuild")
        validate_noobaa_rebuild_system(bucket_factory_session, mcg_obj_session)
        mcg_obj.update_s3_creds()
        logger.info("Step 8 complete: NooBaa rebuild finished and S3 creds refreshed")

        post_rebuild_buckets = setup_mcg_feature_verification_buckets(
            bucket_factory=bucket_factory,
            mcg_obj=mcg_obj,
            reduce_expiration_interval=reduce_expiration_interval,
        )
        assert_mcg_feature_verification_bucket_setup(post_rebuild_buckets, mcg_obj)
        logger.info(
            "Step 9 complete: verification buckets recreated after rebuild"
        )

        versioning_bucket_post_rebuild = post_rebuild_buckets["versioning_bucket"]
        noncurrent_version_lifecycle = LifecyclePolicy(
            NoncurrentVersionExpirationRule(
                non_current_days=1,
                newer_non_current_versions=5,
            )
        )
        logger.info(
            "Configuring NoncurrentVersionExpiration on bucket %s",
            versioning_bucket_post_rebuild.name,
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=versioning_bucket_post_rebuild.name,
            LifecycleConfiguration=noncurrent_version_lifecycle.as_dict(),
        )
        sleep(PROP_SLEEP_TIME)

        lifecycle_config = mcg_obj.s3_client.get_bucket_lifecycle_configuration(
            Bucket=versioning_bucket_post_rebuild.name
        )
        assert any(
            "NoncurrentVersionExpiration" in rule
            for rule in lifecycle_config.get("Rules", [])
        ), "NoncurrentVersionExpiration rule not found on versioning bucket"
        configured_rule = next(
            rule
            for rule in lifecycle_config["Rules"]
            if "NoncurrentVersionExpiration" in rule
        )
        assert (
            configured_rule["NoncurrentVersionExpiration"]["NoncurrentDays"] == 1
        )
        assert (
            configured_rule["NoncurrentVersionExpiration"]["NewerNoncurrentVersions"]
            == 5
        )
        logger.info(
            "Step 10 complete: NoncurrentVersionExpiration configured on %s",
            versioning_bucket_post_rebuild.name,
        )

        versioning_key = "noncurrent-obj"
        versions_amount = 3
        expire_noncurrent_policy = LifecyclePolicy(
            NoncurrentVersionExpirationRule(non_current_days=1)
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=versioning_bucket_post_rebuild.name,
            LifecycleConfiguration=expire_noncurrent_policy.as_dict(),
        )
        sleep(PROP_SLEEP_TIME)

        logger.info(
            "Uploading %s versions of %s to bucket %s",
            versions_amount,
            versioning_key,
            versioning_bucket_post_rebuild.name,
        )
        upload_obj_versions(
            mcg_obj,
            awscli_pod_session,
            versioning_bucket_post_rebuild.name,
            versioning_key,
            amount=versions_amount,
        )
        uploaded_versions = get_obj_versions(
            mcg_obj,
            awscli_pod_session,
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )
        assert len(uploaded_versions) == versions_amount, (
            f"Expected {versions_amount} versions, found {len(uploaded_versions)}"
        )
        version_ids = [version["VersionId"] for version in uploaded_versions]

        latest_version_creation_date = datetime.fromisoformat(
            uploaded_versions[0]["LastModified"].replace("Z", "+00:00")
        )
        logger.info("Manually aging versions to trigger non-current expiration")
        for index, version_id in enumerate(version_ids):
            change_versions_creation_date_in_noobaa_db(
                bucket_name=versioning_bucket_post_rebuild.name,
                object_key=versioning_key,
                version_ids=[version_id],
                new_creation_time=(
                    latest_version_creation_date - timedelta(days=index + 2)
                ).timestamp(),
            )

        logger.info("Waiting for non-current versions to expire")
        for versions in TimeoutSampler(
            timeout=VERSION_EXPIRATION_TIMEOUT,
            sleep=VERSION_EXPIRATION_SLEEP,
            func=get_obj_versions,
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            bucket_name=versioning_bucket_post_rebuild.name,
            obj_key=versioning_key,
        ):
            if len(versions) == 1:
                logger.info("Non-current versions expired; current version retained")
                break
        else:
            assert False, "Non-current versions were not expired in time"

        logger.info(
            "Step 11 complete: uploaded and expired non-current versions for %s/%s",
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )

        source_bucket_post_rebuild = post_rebuild_buckets["source_bucket"]
        logger.info(
            "Uploading object to replication source bucket %s",
            source_bucket_post_rebuild.name,
        )
        post_rebuild_replication_objects = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=source_bucket_post_rebuild.name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="post-rebuild-replication-",
            mcg_obj=mcg_obj,
        )
        assert {post_rebuild_replication_objects[0]} == {
            obj.key
            for obj in mcg_obj.s3_list_all_objects_in_bucket(
                source_bucket_post_rebuild.name
            )
        }, (
            f"Uploaded object not found in replication source bucket "
            f"{source_bucket_post_rebuild.name}"
        )
        logger.info(
            "Step 12 complete: object uploaded to replication source %s",
            source_bucket_post_rebuild.name,
        )

        expiration_bucket_post_rebuild = post_rebuild_buckets["expiration_bucket"]
        multipart_object_key = "multipart-expiration-obj"
        logger.info(
            "Starting multipart upload on expiration bucket %s",
            expiration_bucket_post_rebuild.name,
        )
        multipart_upload_id = create_multipart_upload(
            mcg_obj,
            expiration_bucket_post_rebuild.name,
            multipart_object_key,
        )
        multipart_uploads = list_multipart_upload(
            mcg_obj, expiration_bucket_post_rebuild.name
        )
        assert "Uploads" in multipart_uploads, "No in-progress multipart uploads found"
        assert any(
            upload["UploadId"] == multipart_upload_id
            and upload["Key"] == multipart_object_key
            for upload in multipart_uploads["Uploads"]
        ), (
            f"Multipart upload {multipart_upload_id} not found on "
            f"{expiration_bucket_post_rebuild.name}"
        )
        logger.info(
            "Step 13 complete: multipart upload %s started on %s/%s",
            multipart_upload_id,
            expiration_bucket_post_rebuild.name,
            multipart_object_key,
        )

        try:
            primary_nb_db_node = shutdown_primary_noobaa_db_node()
        except ResourceNotFoundError:
            pytest.skip(
                "Primary NooBaa DB pod not found; skipping primary DB node shutdown"
            )
        logger.info(
            "Step 14 complete: primary NooBaa DB node %s shut down",
            primary_nb_db_node,
        )

        current_version = verify_noncurrent_versions_expired(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            bucket_name=versioning_bucket_post_rebuild.name,
            obj_key=versioning_key,
            timeout=VERSION_EXPIRATION_TIMEOUT,
            sleep=VERSION_EXPIRATION_SLEEP,
        )
        logger.info(
            "Step 15 complete: only current version %s remains for %s/%s",
            current_version["VersionId"],
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )

        target_bucket_post_rebuild = post_rebuild_buckets["target_bucket"]
        verify_unidirectional_replication(
            mcg_obj=mcg_obj,
            source_bucket=source_bucket_post_rebuild,
            target_bucket=target_bucket_post_rebuild,
            replication_object_keys=post_rebuild_replication_objects,
        )
        logger.info(
            "Step 16 complete: uni-directional replication verified from %s to %s",
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
        )

        abort_multipart_lifecycle = LifecyclePolicy(
            ExpirationRule(days=1),
            AbortIncompleteMultipartUploadRule(days_after_initiation=1),
        )
        logger.info(
            "Configuring AbortIncompleteMultipartUpload on expiration bucket %s",
            expiration_bucket_post_rebuild.name,
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=expiration_bucket_post_rebuild.name,
            LifecycleConfiguration=abort_multipart_lifecycle.as_dict(),
        )
        sleep(PROP_SLEEP_TIME)

        lifecycle_config = mcg_obj.s3_client.get_bucket_lifecycle_configuration(
            Bucket=expiration_bucket_post_rebuild.name
        )
        assert any(
            "AbortIncompleteMultipartUpload" in rule
            for rule in lifecycle_config.get("Rules", [])
        ), (
            "AbortIncompleteMultipartUpload rule not found on expiration bucket "
            f"{expiration_bucket_post_rebuild.name}"
        )
        abort_multipart_rule = next(
            rule
            for rule in lifecycle_config["Rules"]
            if "AbortIncompleteMultipartUpload" in rule
        )
        assert (
            abort_multipart_rule["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"]
            == 1
        )
        logger.info(
            "Step 17 complete: AbortIncompleteMultipartUpload configured on %s",
            expiration_bucket_post_rebuild.name,
        )

        additional_versions_amount = 3
        versions_before_upload = get_obj_versions(
            mcg_obj,
            awscli_pod_session,
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )
        logger.info(
            "Uploading %s more versions of %s to bucket %s",
            additional_versions_amount,
            versioning_key,
            versioning_bucket_post_rebuild.name,
        )
        upload_obj_versions(
            mcg_obj,
            awscli_pod_session,
            versioning_bucket_post_rebuild.name,
            versioning_key,
            amount=additional_versions_amount,
        )
        uploaded_versions = get_obj_versions(
            mcg_obj,
            awscli_pod_session,
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )
        expected_version_count = (
            len(versions_before_upload) + additional_versions_amount
        )
        assert len(uploaded_versions) == expected_version_count, (
            f"Expected {expected_version_count} versions for {versioning_key}, "
            f"found {len(uploaded_versions)}"
        )
        logger.info(
            "Step 18 complete: %s versions of %s/%s (%s added)",
            len(uploaded_versions),
            versioning_bucket_post_rebuild.name,
            versioning_key,
            additional_versions_amount,
        )

        bidi_replication_prefix = "bidi-site"
        logger.info(
            "Enabling bi-directional replication from %s to %s",
            target_bucket_post_rebuild.name,
            source_bucket_post_rebuild.name,
        )
        patch_replication_policy_to_bucket(
            target_bucket_post_rebuild.name,
            "basic-replication-rule-2",
            source_bucket_post_rebuild.name,
            prefix=bidi_replication_prefix,
        )
        sleep(PROP_SLEEP_TIME)
        assert source_bucket_post_rebuild.name in get_replication_policy(
            target_bucket_post_rebuild.name
        ), (
            f"Bi-directional replication policy not found on "
            f"{target_bucket_post_rebuild.name}"
        )

        logger.info(
            "Uploading object to replication target bucket %s",
            target_bucket_post_rebuild.name,
        )
        bidi_replication_objects = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=target_bucket_post_rebuild.name,
            file_dir=test_directory_setup.origin_dir,
            amount=1,
            pattern="bidi-replication-",
            prefix=bidi_replication_prefix,
            mcg_obj=mcg_obj,
        )
        expected_bidi_keys = {
            f"{bidi_replication_prefix}/{obj_key}"
            for obj_key in bidi_replication_objects
        }
        assert compare_bucket_object_list(
            mcg_obj,
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
            timeout=1200,
        ), (
            f"Bi-directional replication verification failed: objects in "
            f"{source_bucket_post_rebuild.name} and "
            f"{target_bucket_post_rebuild.name} do not match"
        )
        source_object_keys = {
            obj.key
            for obj in mcg_obj.s3_list_all_objects_in_bucket(
                source_bucket_post_rebuild.name
            )
        }
        assert expected_bidi_keys <= source_object_keys, (
            f"Object uploaded to {target_bucket_post_rebuild.name} was not replicated "
            f"to {source_bucket_post_rebuild.name}"
        )
        logger.info(
            "Step 19 complete: bi-directional replication enabled and object "
            "replicated from %s to %s",
            target_bucket_post_rebuild.name,
            source_bucket_post_rebuild.name,
        )

        start_primary_noobaa_db_node(primary_nb_db_node)
        logger.info(
            "Step 20 complete: primary NooBaa DB node %s started",
            primary_nb_db_node,
        )

        logger.info(
            "Aborting multipart upload %s on %s/%s",
            multipart_upload_id,
            expiration_bucket_post_rebuild.name,
            multipart_object_key,
        )
        abort_multipart(
            mcg_obj,
            expiration_bucket_post_rebuild.name,
            multipart_object_key,
            multipart_upload_id,
        )
        multipart_uploads = list_multipart_upload(
            mcg_obj, expiration_bucket_post_rebuild.name
        )
        if "Uploads" in multipart_uploads:
            assert not any(
                upload["UploadId"] == multipart_upload_id
                for upload in multipart_uploads["Uploads"]
            ), (
                f"Multipart upload {multipart_upload_id} was not aborted on "
                f"{expiration_bucket_post_rebuild.name}"
            )
        logger.info(
            "Step 21 complete: multipart upload %s aborted on %s/%s",
            multipart_upload_id,
            expiration_bucket_post_rebuild.name,
            multipart_object_key,
        )

        verify_bidirectional_replication(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            source_bucket=source_bucket_post_rebuild,
            target_bucket=target_bucket_post_rebuild,
            test_directory_setup=test_directory_setup,
            target_to_source_prefix=bidi_replication_prefix,
        )
        logger.info(
            "Step 22 complete: bi-directional replication verified between %s and %s",
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
        )

        delete_marker_lifecycle = LifecyclePolicy(
            NoncurrentVersionExpirationRule(non_current_days=1),
            ExpiredObjectDeleteMarkerRule(),
        )
        logger.info(
            "Configuring ExpiredObjectDeleteMarker on versioning bucket %s",
            versioning_bucket_post_rebuild.name,
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=versioning_bucket_post_rebuild.name,
            LifecycleConfiguration=delete_marker_lifecycle.as_dict(),
        )
        sleep(PROP_SLEEP_TIME)

        lifecycle_config = mcg_obj.s3_client.get_bucket_lifecycle_configuration(
            Bucket=versioning_bucket_post_rebuild.name
        )
        assert any(
            rule.get("Expiration", {}).get("ExpiredObjectDeleteMarker")
            for rule in lifecycle_config.get("Rules", [])
        ), (
            "ExpiredObjectDeleteMarker rule not found on versioning bucket "
            f"{versioning_bucket_post_rebuild.name}"
        )

        latest_version = next(
            version
            for version in get_obj_versions(
                mcg_obj,
                awscli_pod_session,
                versioning_bucket_post_rebuild.name,
                versioning_key,
            )
            if version.get("IsLatest")
        )
        logger.info(
            "Deleting latest version of %s/%s (version %s)",
            versioning_bucket_post_rebuild.name,
            versioning_key,
            latest_version["VersionId"],
        )
        s3_delete_object(
            mcg_obj,
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )

        object_versions = s3_list_object_versions(
            mcg_obj,
            versioning_bucket_post_rebuild.name,
            prefix=versioning_key,
        )
        delete_markers = object_versions.get("DeleteMarkers", [])
        assert (
            len(delete_markers) == 1 and delete_markers[0]["IsLatest"]
        ), (
            f"Delete marker was not created for latest version of "
            f"{versioning_bucket_post_rebuild.name}/{versioning_key}"
        )
        remaining_versions = object_versions.get("Versions", [])
        assert any(
            version["VersionId"] == latest_version["VersionId"]
            for version in remaining_versions
        ), (
            f"Deleted latest version {latest_version['VersionId']} is not retained "
            f"in version history for {versioning_bucket_post_rebuild.name}/{versioning_key}"
        )
        logger.info(
            "Step 23 complete: ExpiredObjectDeleteMarker configured and latest "
            "version deleted for %s/%s",
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )

        for bucket_name in (
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
        ):
            replication_policy = json.loads(get_replication_policy(bucket_name))
            replication_policy["rules"][0]["sync_deletions"] = True
            update_replication_policy(bucket_name, replication_policy)
        sleep(PROP_SLEEP_TIME)

        for bucket_name in (
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
        ):
            replication_policy = json.loads(get_replication_policy(bucket_name))
            assert replication_policy["rules"][0]["sync_deletions"] is True, (
                f"Deletion sync not enabled on replication bucket {bucket_name}"
            )
        logger.info(
            "Deletion sync enabled on %s and %s",
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
        )

        object_to_delete = post_rebuild_replication_objects[0]
        logger.info(
            "Deleting object %s from source bucket %s",
            object_to_delete,
            source_bucket_post_rebuild.name,
        )
        s3_delete_object(
            mcg_obj,
            source_bucket_post_rebuild.name,
            object_to_delete,
        )
        assert compare_bucket_object_list(
            mcg_obj,
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
            timeout=1200,
        ), (
            f"Deletion sync failed: objects in {source_bucket_post_rebuild.name} and "
            f"{target_bucket_post_rebuild.name} do not match after deleting "
            f"{object_to_delete}"
        )
        target_object_keys = {
            obj.key
            for obj in mcg_obj.s3_list_all_objects_in_bucket(
                target_bucket_post_rebuild.name
            )
        }
        assert object_to_delete not in target_object_keys, (
            f"Object {object_to_delete} was not deleted from target bucket "
            f"{target_bucket_post_rebuild.name}"
        )
        logger.info(
            "Step 24 complete: deletion sync verified after deleting %s from %s",
            object_to_delete,
            source_bucket_post_rebuild.name,
        )

        try:
            secondary_nb_db_node = shutdown_secondary_noobaa_db_node()
        except ResourceNotFoundError:
            pytest.skip(
                "Secondary NooBaa DB pod not found; skipping secondary DB node shutdown"
            )
        logger.info(
            "Step 25 complete: secondary NooBaa DB node %s shut down",
            secondary_nb_db_node,
        )

        verify_noncurrent_versions_and_delete_marker_expired(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            bucket_name=versioning_bucket_post_rebuild.name,
            obj_key=versioning_key,
            timeout=VERSION_EXPIRATION_TIMEOUT,
            sleep=VERSION_EXPIRATION_SLEEP,
        )
        logger.info(
            "Step 26 complete: non-current versions and delete marker expired for "
            "%s/%s",
            versioning_bucket_post_rebuild.name,
            versioning_key,
        )

        verify_deletion_sync_between_replication_buckets(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            source_bucket=source_bucket_post_rebuild,
            target_bucket=target_bucket_post_rebuild,
            test_directory_setup=test_directory_setup,
            target_to_source_prefix=bidi_replication_prefix,
        )
        logger.info(
            "Step 27 complete: deletion sync verified between %s and %s while "
            "secondary NooBaa DB node %s is shut down",
            source_bucket_post_rebuild.name,
            target_bucket_post_rebuild.name,
            secondary_nb_db_node,
        )

        secondary_shutdown_multipart_key = "secondary-shutdown-multipart-obj"
        verify_multipart_upload_aborted_and_cleaned_up(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            bucket_name=expiration_bucket_post_rebuild.name,
            test_directory_setup=test_directory_setup,
            object_key=secondary_shutdown_multipart_key,
            timeout=VERSION_EXPIRATION_TIMEOUT,
            sleep=VERSION_EXPIRATION_SLEEP,
        )
        logger.info(
            "Step 28 complete: multipart upload aborted and cleaned up on %s while "
            "secondary NooBaa DB node %s is shut down",
            expiration_bucket_post_rebuild.name,
            secondary_nb_db_node,
        )

        start_secondary_noobaa_db_node(secondary_nb_db_node)
        logger.info(
            "Step 29 complete: secondary NooBaa DB node %s started",
            secondary_nb_db_node,
        )

        wait_for_noobaa_pods_running(timeout=1200)
        mcg_obj.update_s3_creds()
        sleep(60)

        retry(Exception, tries=5, delay=10)(validate_mcg_bg_features)(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=BG_SKIP_FEATURES,
            object_amount=BG_OBJECT_AMOUNT,
        )
        logger.info("Background MCG feature I/O validation completed successfully")

        assert Sanity().health_check(
            cluster_check=False, tries=50
        ), "System health check failed after recovery"
        logger.info(
            "Step 30 complete: background feature I/O verified and system is healthy"
        )

        buckets_to_cleanup = list(feature_setup_map.get("all_buckets", []))
        buckets_to_cleanup.extend(
            [
                source_bucket,
                target_bucket,
                expiration_bucket,
                versioning_bucket,
                source_bucket_post_rebuild,
                target_bucket_post_rebuild,
                expiration_bucket_post_rebuild,
                versioning_bucket_post_rebuild,
            ]
        )
        cleanup_all_test_bucket_objects(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod_session,
            buckets=buckets_to_cleanup,
        )
        logger.info(
            "Step 31 complete: deleted all objects from %s test buckets",
            len({bucket.name for bucket in buckets_to_cleanup}),
        )
