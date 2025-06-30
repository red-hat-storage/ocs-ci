from datetime import datetime, timedelta
import json
import logging
import os
import tempfile
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    tier3,
    jira,
    red_squad,
    runs_on_provider,
    mcg,
    skipif_noobaa_external_pgsql,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.helpers.helpers import (
    craft_s3_command,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    change_versions_creation_date_in_noobaa_db,
    craft_s3cmd_command,
    create_multipart_upload,
    expire_multipart_upload_in_noobaa_db,
    expire_objects_in_bucket,
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
    ExpiredObjectDeleteMarkerRule,
    LifecyclePolicy,
    NoncurrentVersionExpirationRule,
)
from ocs_ci.utility.utils import TimeoutSampler


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
                (constants.LIFECYCLE_SCHED_MINUTES, 0),
            ]
        )

    @tier3
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

    @pytest.mark.polarion_id("OCS-6559")
    @tier2
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

    @tier2
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
        lifecycle_policy = LifecyclePolicy(ExpiredObjectDeleteMarkerRule())
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

    @tier2
    @pytest.mark.polarion_id("OCS-6804")
    def test_lifecycle_rules_combined(
        self,
        mcg_obj,
        bucket_factory,
        awscli_pod,
        test_directory_setup,
    ):
        """
        1. Create an MCG bucket with versioning enabled
        2. Set onto the bucket a lifecycle configuration with:
        Expiration.Days, Expiration.ExpiredObjectDeleteMarker
        NoncurrentVersionExpiration.NoncurrentDays,
        NoncurrentVersionExpiration.NewerNoncurrentVersions,
        AbortIncompleteMultipartUpload.DaysAfterInitiation

        3. Create a multipart-upload and upload parts to it
        4. Expire the multipart-upload and wait for it to expire
        5. Upload 10 versions of the same object onto the bucket
        6. Expire the non-current versions by setting back their creation date
        7. Wait for the any non-current versions after the first 5 to get deleted
        8. Expire the versioned object and wait for the delete marker to get created
        9. Delete all the remaining non-delete-marker versions by their VersionId
        10. Wait for the delete marker to get deleted
        """
        max_non_current_versions = 5

        # 1. Create an MCG bucket with versioning enabled
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket)

        # 2. Set lifecycle configuration
        lifecycle_policy = LifecyclePolicy(
            ExpirationRule(days=5),
            ExpiredObjectDeleteMarkerRule(),
            NoncurrentVersionExpirationRule(
                non_current_days=1, newer_non_current_versions=max_non_current_versions
            ),
            AbortIncompleteMultipartUploadRule(days_after_initiation=7),
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        # 3. Create a multipart-upload and upload parts to it
        parts_amount = 5
        origin_dir = test_directory_setup.origin_dir
        res_dir = test_directory_setup.result_dir

        multipart_upload_obj_key = "multipart_object"
        upload_id = create_multipart_upload(mcg_obj, bucket, multipart_upload_obj_key)
        awscli_pod.exec_cmd_on_pod(
            f'sh -c "dd if=/dev/urandom of={origin_dir}/{multipart_upload_obj_key} bs=1MB count={parts_amount}; '
            f'split -b 1m  {origin_dir}/{multipart_upload_obj_key} {res_dir}/part"'
        )
        parts = awscli_pod.exec_cmd_on_pod(f'sh -c "ls -1 {res_dir}"').split()
        upload_parts(
            mcg_obj,
            awscli_pod,
            bucket,
            multipart_upload_obj_key,
            res_dir,
            upload_id,
            parts,
        )

        # 4. Expire the multipart-upload and wait for it to expire
        expire_multipart_upload_in_noobaa_db(upload_id)  # Changed function call
        for http_response in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=list_multipart_upload,
            s3_obj=mcg_obj,
            bucketname=bucket,
        ):
            if "Uploads" not in http_response or len(http_response["Uploads"]) == 0:
                logger.info("Multipart upload expired as expected")
                break
            logger.warning(f"Upload has not expired yet: \n{http_response}")

        # 5. Upload 10 versions of the same object onto the bucket
        versions_amount = 10
        versioned_obj_key = "versioned_object"
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            bucket,
            versioned_obj_key,
            amount=versions_amount,
        )

        # 6. Expire the non-current versions by setting back their creation date
        uploaded_versions = get_obj_versions(
            mcg_obj, awscli_pod, bucket, versioned_obj_key
        )
        version_ids = [version["VersionId"] for version in uploaded_versions]
        base_time = datetime.now()
        for _, v_id in enumerate(version_ids):
            change_versions_creation_date_in_noobaa_db(
                bucket_name=bucket,
                object_key=versioned_obj_key,
                version_ids=[v_id],
                new_creation_time=(base_time - timedelta(days=1)).timestamp(),
            )

        # 7. Wait for the any non-current versions after the first 5 to get deleted
        expected_remaining = set(version_ids[: max_non_current_versions + 1])

        for versions in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=get_obj_versions,
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod,
            bucket_name=bucket,
            obj_key=versioned_obj_key,
        ):
            remaining = {v["VersionId"] for v in versions}
            if remaining == expected_remaining:
                logger.info("Only the expected versions remained")
                break
            else:
                logger.warning(
                    (
                        "Some older versions have not expired yet:\n"
                        f"Remaining: {remaining}\n"
                        f"Versions yet to expire: {remaining - expected_remaining}"
                    )
                )

        # 8. Expire the versioned object and wait for the delete marker to get created
        expire_objects_in_bucket(bucket, [versioned_obj_key])
        for raw_versions in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=s3_list_object_versions,
            s3_obj=mcg_obj,
            bucketname=bucket,
            prefix=versioned_obj_key,
        ):
            delete_markers = raw_versions.get("DeleteMarkers", [])
            if len(delete_markers) == 1:
                logger.info("Delete marker created as expected")
                break
            else:
                logger.warning(
                    (
                        "Delete marker has not expired yet:\n"
                        f"Remaining: {delete_markers}\n"
                    )
                )

        # 9. Delete all the remaining non-delete-marker versions by their VersionId
        remaining_version_ids = expected_remaining
        for v_id in remaining_version_ids:
            s3_delete_object(
                s3_obj=mcg_obj,
                bucketname=bucket,
                object_key=versioned_obj_key,
                versionid=v_id,
            )

        # 10. Wait for the delete marker to get deleted
        for raw_versions in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=s3_list_object_versions,
            s3_obj=mcg_obj,
            bucketname=bucket,
            prefix=versioned_obj_key,
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

    @tier3
    @jira("DFBUGS-2306")
    @pytest.mark.parametrize(
        argnames=["rule_cls", "rule_kwargs_list"],
        argvalues=[
            pytest.param(
                ExpirationRule,
                [{"days": val} for val in [True, -1, 1.5, "string", None]],
                marks=[pytest.mark.polarion_id("OCS-6835")],
            ),
            pytest.param(
                ExpiredObjectDeleteMarkerRule,
                [{"expire_object_delete_marker": val} for val in [1, "string", None]],
                marks=[pytest.mark.polarion_id("OCS-6836")],
            ),
            pytest.param(
                NoncurrentVersionExpirationRule,
                [{"non_current_days": val} for val in [True, -1, 1.5, "string", None]],
                marks=[pytest.mark.polarion_id("OCS-6837")],
            ),
            pytest.param(
                NoncurrentVersionExpirationRule,
                [
                    {"newer_non_current_versions": val}
                    for val in [True, -1, 1.5, "string", None]
                ],
                marks=[pytest.mark.polarion_id("OCS-6838")],
            ),
            pytest.param(
                AbortIncompleteMultipartUploadRule,
                [
                    {"days_after_initiation": val}
                    for val in [True, -1, 1.5, "string", None]
                ],
                marks=[pytest.mark.polarion_id("OCS-6839")],
            ),
        ],
        ids=[
            "Expiration.Days",
            "Expiration.ExpiredObjectDeleteMarker",
            "NoncurrentVersionExpiration.NoncurrentDays",
            "NoncurrentVersionExpiration.NewerNoncurrentVersions",
            "AbortIncompleteMultipartUploadRule",
        ],
    )
    def test_lifecycle_rules_invalid_values(
        self, mcg_obj, bucket_factory, awscli_pod, rule_cls, rule_kwargs_list
    ):
        """
        Test various lifecycle rule fields with invalid values.
        Expect all of them to raise a meaningful error.
        """
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket)

        for rule_kwargs in rule_kwargs_list:
            with pytest.raises(Exception, match="Invalid|Malformed|must be set"):
                lifecycle_policy = LifecyclePolicy(rule_cls(**rule_kwargs))
                mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                    Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
                )

        logger.info("Invalid lifecycle configurations were rejected as expected")

    @tier3
    @pytest.mark.polarion_id("OCS-6840")
    def test_lifecycle_config_ops_s3_clients_compatibility(
        self, mcg_obj, bucket_factory, awscli_pod
    ):
        """
        Verify compatibility of lifecycle configuration operations (PUT, GET, DELETE)
        across boto3, AWS CLI, and s3cmd for an MCG bucket.

        1. Create an MCG bucket with versioning enabled
        2. boto3 operations:
            2.1 boto3 Put
            2.2 boto3 Get
            2.3 boto3 Delete
        3. AWSCLI operations:
            3.1 AWSCLI Put
            3.2 AWSCLI Get
            3.3 AWSCLI Delete
        4. s3cmd operations:
            4.1 s3cmd Put
            4.2 s3cmd Get
            4.3 s3cmd Delete
        """
        # 1. Create an MCG bucket with versioning enabled
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket)

        lifecycle_policy = LifecyclePolicy(
            ExpirationRule(days=5),
            ExpiredObjectDeleteMarkerRule(),
            NoncurrentVersionExpirationRule(
                non_current_days=1,
            ),
            AbortIncompleteMultipartUploadRule(days_after_initiation=7),
        )
        lifecycle_policy_dict = lifecycle_policy.as_dict()

        # 2. boto3 operations
        # 2.1 boto3 Put
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy_dict
        )
        # 2.2 boto3 Get
        assert (
            mcg_obj.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
            == lifecycle_policy_dict["Rules"]
        ), "Boto3: Lifecycle configuration mismatch"
        # 2.3 boto3 Delete
        mcg_obj.s3_client.delete_bucket_lifecycle(Bucket=bucket)
        assert "Rules" not in mcg_obj.s3_client.get_bucket_lifecycle_configuration(
            Bucket=bucket
        ), "Boto3: Lifecycle configuration not deleted"

        # 3. AWSCLI operations
        with tempfile.TemporaryDirectory() as tmp_dir:
            json_path = os.path.join(tmp_dir, "lifecycle_config.json")
            with open(json_path, "w") as f:
                f.write(json.dumps(lifecycle_policy_dict))

            target_path_json = f"/tmp/{os.path.basename(json_path)}"
            awscli_pod.copy_to_pod_rsync(src_path=f"{tmp_dir}/", target_path="/tmp")

        # 3.1 AWSCLI Put
        awscli_pod.exec_cmd_on_pod(
            craft_s3_command(
                (
                    "put-bucket-lifecycle-configuration "
                    f"--bucket {bucket} "
                    f"--lifecycle-configuration file://{target_path_json}"
                ),
                mcg_obj=mcg_obj,
                api=True,
            )
        )
        # 3.2 AWSCLI Get
        get_lifecycle_policy_resp = awscli_pod.exec_cmd_on_pod(
            craft_s3_command(
                f"get-bucket-lifecycle-configuration --bucket {bucket}",
                mcg_obj=mcg_obj,
                api=True,
            ),
            out_yaml_format=False,
        )
        assert (
            json.loads(get_lifecycle_policy_resp)["Rules"]
            == lifecycle_policy_dict["Rules"]
        ), "AWS CLI: Lifecycle configuration mismatch"
        # 3.3 AWSCLI Delete
        awscli_pod.exec_cmd_on_pod(
            craft_s3_command(
                f"delete-bucket-lifecycle --bucket {bucket}",
                mcg_obj=mcg_obj,
                api=True,
            )
        )
        assert "Rules" not in mcg_obj.s3_client.get_bucket_lifecycle_configuration(
            Bucket=bucket
        ), "AWS CLI: Lifecycle configuration not deleted"

        # s3cmd operations
        with tempfile.TemporaryDirectory() as tmp_dir:
            xml_path = os.path.join(tmp_dir, "lifecycle_config.xml")
            with open(xml_path, "w") as f:
                # s3cmd only uses the XML format for lifecycle configuration
                lifecycle_policy_xml = lifecycle_policy.as_xml()
                f.write(lifecycle_policy_xml)

            target_path_xml = f"/tmp/{os.path.basename(xml_path)}"
            awscli_pod.copy_to_pod_rsync(src_path=f"{tmp_dir}/", target_path="/tmp")

        # 4.1 s3cmd Put
        awscli_pod.exec_cmd_on_pod(
            craft_s3cmd_command(
                f"setlifecycle {target_path_xml} s3://{bucket}", mcg_obj=mcg_obj
            )
        )
        # 4.2 s3cmd Get
        get_lifecycle_policy_resp = awscli_pod.exec_cmd_on_pod(
            craft_s3cmd_command(f"getlifecycle s3://{bucket}", mcg_obj=mcg_obj),
            out_yaml_format=False,
        )
        # Check if the lifecycle policy contains the expected keys
        # exact comparison is problematic due to the difference in XML formatting
        expected_rule_keys = [
            "Expiration",
            "ExpiredObjectDeleteMarker",
            "NoncurrentDays",
            "AbortIncompleteMultipartUpload",
        ]
        assert all(
            key in get_lifecycle_policy_resp for key in expected_rule_keys
        ), "s3cmd: Lifecycle configuration mismatch"

        # 4.3 s3cmd Delete
        awscli_pod.exec_cmd_on_pod(
            craft_s3cmd_command(f"dellifecycle s3://{bucket}", mcg_obj=mcg_obj)
        )
        assert "Rules" not in mcg_obj.s3_client.get_bucket_lifecycle_configuration(
            Bucket=bucket
        ), "s3cmd: Lifecycle configuration not deleted"
