import logging

import pytest
from flaky import flaky

from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity,
)

from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    skipif_mcg_only,
    rgw,
    runs_on_provider,
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR

logger = logging.getLogger(__name__)


@rgw
@red_squad
@runs_on_provider
@skipif_mcg_only
class TestObjectIntegrity(ManageTest):
    """
    Test data integrity of RGW buckets
    """

    @tier1
    @flaky
    @pytest.mark.polarion_id("OCS-2246")
    def test_check_object_integrity(
        self, awscli_pod_session, rgw_bucket_factory, test_directory_setup
    ):
        """
        Test object integrity using md5sum
        """
        bucketname = rgw_bucket_factory(1, "rgw-oc")[0].name
        obc_obj = OBC(bucketname)
        original_dir = AWSCLI_TEST_OBJ_DIR
        result_dir = test_directory_setup.result_dir
        full_object_path = f"s3://{bucketname}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {original_dir}"
        ).split(" ")
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, original_dir, full_object_path, obc_obj
        )

        logger.info("Downloading all objects from RGW bucket to awscli pod")
        sync_object_directory(awscli_pod_session, full_object_path, result_dir, obc_obj)

        # Checksum is compared between original and result object
        for obj in downloaded_files:
            assert verify_s3_object_integrity(
                original_object_path=f"{original_dir}/{obj}",
                result_object_path=f"{result_dir}/{obj}",
                awscli_pod=awscli_pod_session,
            ), "Checksum comparision between original and result object failed"

    @pytest.mark.polarion_id("OCS-2243")
    @tier2
    def test_empty_file_integrity(
        self, awscli_pod_session, rgw_bucket_factory, test_directory_setup
    ):
        """
        Test write empty files to bucket and check integrity
        """

        original_dir = test_directory_setup.origin_dir
        result_dir = test_directory_setup.result_dir
        bucketname = rgw_bucket_factory(1, "rgw-oc")[0].name
        obc_obj = OBC(bucketname)
        full_object_path = f"s3://{bucketname}"

        # Touch create 1000 empty files in pod
        command = f"for i in $(seq 1 100); do touch {original_dir}/test$i; done"
        awscli_pod_session.exec_sh_cmd_on_pod(command=command, sh="sh")
        # Write all empty objects to the new bucket
        sync_object_directory(
            awscli_pod_session, original_dir, full_object_path, obc_obj
        )

        # Retrieve all objects from RGW bucket to result dir in Pod
        logger.info("Downloading objects from RGW bucket to awscli pod")
        sync_object_directory(awscli_pod_session, full_object_path, result_dir, obc_obj)

        # Checksum is compared between original and result object
        original_md5 = awscli_pod_session.exec_cmd_on_pod(
            f'sh -c "cat {original_dir}/* | md5sum"'
        )
        result_md5 = awscli_pod_session.exec_cmd_on_pod(
            f'sh -c "cat {original_dir}/* | md5sum"'
        )
        assert (
            original_md5 == result_md5
        ), "Origin and result folders checksum mismatch found"
