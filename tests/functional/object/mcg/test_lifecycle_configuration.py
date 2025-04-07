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
    expire_multipart_upload,
    expire_parts,
    get_obj_versions,
    list_multipart_upload,
    list_uploaded_parts,
    put_bucket_versioning_via_awscli,
    upload_obj_versions,
    upload_parts,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    AbortIncompleteMultipartUploadRule,
    LifecyclePolicy,
    NoncurrentVersionExpirationRule,
)
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)

PROP_SLEEP_TIME = 10
TIMEOUT_SLEEP_DURATION = 30
TIMEOUT_THRESHOLD = 420


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

        """
        new_interval_in_miliseconds = 60 * 1000
        add_env_vars_to_noobaa_core_class(
            [(constants.LIFECYCLE_INTERVAL_PARAM, new_interval_in_miliseconds)]
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
        5. Manually expire the parts and the multipart-upload
        6. Wait for the parts and multipart-upload to expire
        """
        parts_amount = 5
        key = "test_obj"
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
        expire_parts(upload_id)
        expire_multipart_upload(upload_id)

        # 7. Wait for the parts and multipart-upload to expire
        # TODO: Unify this part(?)
        for parts_dict in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=PROP_SLEEP_TIME,
            func=list_uploaded_parts,
            s3_obj=mcg_obj,
            bucketname=bucket,
            object_key=key,
            upload_id=upload_id,
        ):
            if len(parts_dict) == 0:
                break
            logger.warning(f"Parts have not expired yet: \n{parts_dict}")

        for upload in TimeoutSampler(
            timeout=TIMEOUT_THRESHOLD,
            sleep=TIMEOUT_SLEEP_DURATION,
            func=list_multipart_upload,
            s3_obj=mcg_obj,
            bucketname=bucket,
        ):
            if len(upload) == 0:
                break
            logger.warning(f"Upload has not expired yet: \n{upload}")

    @tier1
    @pytest.mark.polarion_id("OCS-6559")
    def test_noncurrent_version_expiration_non_current_days(
        self, mcg_obj, bucket_factory, awscli_pod
    ):
        """
        1. Create an MCG bucket with versioning enabled
        2. Set lifecycle configuration to delete non-current versions after 5 days
        3. Upload versions
        4. Manually set the age of each version to be one day older than its successor
        5. Wait for the older versions to expire
        """
        key = "test_obj"
        older_versions_amount = 5

        # 1. Create an MCG bucket with versioning enabled
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket)

        # 2. Set lifecycle configuration
        lifecycle_policy = LifecyclePolicy(
            NoncurrentVersionExpirationRule(non_current_days=5)
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

        # 5. Wait for the older versions to expire
        original = set(version_ids)
        older = set(version_ids[-older_versions_amount:])
        newer = original - older

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
            if remaining == newer:
                logger.info("Only the older versions expired as expected")
                break
            elif not (newer <= remaining):
                raise UnexpectedBehaviour(
                    (
                        "Some newer versions were deleted when they shouldn't have!"
                        f"Newer versions that were deleted: {newer - remaining}"
                    )
                )
            else:
                logger.warning(
                    (
                        "Some older versions have not expired yet:\n"
                        f"Remaining: {remaining}\n"
                        f"Versions yet to expire: {remaining - newer}"
                    )
                )
