import logging

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
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    NoncurrentVersionExpirationRule,
)


logger = logging.getLogger(__name__)

PROP_SLEEP_TIME = 10


@mcg
@red_squad
@runs_on_provider
@skipif_noobaa_external_pgsql
class TestObjectExpiration(MCGTest):
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
        5. Manually expire the parts
        6. Wait for the parts to expire
        7. Manually expire the multipart upload itself
        8. Wait for the multipart-upload to expire
        """
        parts_amount = 5
        key = "test_obj"
        origin_dir = test_directory_setup.origin_dir
        res_dir = test_directory_setup.result_dir

        # 1. Create a bucket
        bucket = bucket_factory(interface="OC")[0].name

        # 2. Set lifecycle configuration
        # TODO: Uncomment once the feature is available for testing
        # lifecycle_policy = LifecyclePolicy(AbortIncompleteMultipartUploadRule(days=1))
        # mcg_obj.s3_client.put_bucket_lifecycle_configuration(
        #     Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        # )

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

        # 5. Manually expire the parts
        expire_parts(upload_id)
        list_uploaded_parts(mcg_obj, bucket, key, upload_id)

        # 6. Wait for the parts to expire
        # TODO: Uncomment once the feature is available for testing
        # for parts_dict in TimeoutSampler(
        #     timeout=180,
        #     sleep=PROP_SLEEP_TIME,
        #     func=list_uploaded_parts,
        #     s3_obj=mcg_obj,
        #     bucketname=bucket,
        #     object_key=key,
        #     upload_id=upload_id,
        # ):
        #     if len(parts_dict) == 0:
        #         break
        #     logger.warning(f"Parts have not expired yet: \n{parts_dict}")

        # 7. Manually expire the multipart upload itself
        expire_multipart_upload(upload_id)
        list_multipart_upload(mcg_obj, bucket)

        # 8. Wait for the multipart-upload to expire
        # TODO: Uncomment once the feature is available for testing
        # for upload in TimeoutSampler(
        #     timeout=180,
        #     sleep=PROP_SLEEP_TIME,
        #     func=list_multipart_upload,
        #     s3_obj=mcg_obj,
        #     bucketname=bucket,
        # ):
        #     if len(upload) == 0:
        #         break
        #     logger.warning(f"Upload has not expired yet: \n{upload}")

    @tier1
    @pytest.mark.polarion_id("OCS-")  # TODO
    def test_noncurrent_version_expiration_non_current_days(
        self, mcg_obj, bucket_factory, awscli_pod
    ):
        """
        1. Create an MCG bucket with versioning enabled
        2. Set lifecycle configuration to delete non-current versions after 5 days
        3. Upload versions
        4. Manually set the age of each version to be one day older than its predecessor
        5. Wait for the older versions to expire
        """
        key = "test_obj"
        older_versions_amount = 5

        # 1. Create an MCG bucket with versioning enabled
        bucket = bucket_factory(interface="OC")[0].name
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod, bucket.name)

        # 2. Set lifecycle configuration
        lifecycle_policy = LifecyclePolicy(
            NoncurrentVersionExpirationRule(non_current_days=5)
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
        )

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

        # 4. Manually set the age of each version to be one day older than its predecessor
        uploaded_versions = get_obj_versions(mcg_obj, awscli_pod, bucket, key)
        version_ids = [version["VersionId"] for version in uploaded_versions]

        for i, version_id in enumerate(version_ids):
            if i == 0:
                continue
